"""
Crawl Engine — discovers and crawls pages on a website.

Behavior modeled after Screaming Frog:
- URLs are kept exactly as discovered / returned by the server
- A normalized set is used for deduplication (lowercase, strip www., strip trailing slash)
- Redirects are followed transparently — only the final page is saved
- 4xx/5xx pages are saved with no SEO issues
- Link discovery extracts <a>, <link rel=alternate/canonical>, <area>, <iframe>
"""
import asyncio
import time
import datetime
import logging
from urllib.parse import urljoin, urlparse
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from app.models.models import Crawl, Page
from app.crawler.analyzer import SEOAnalyzer
from app.crawler.sitemap import SitemapParser
from app.crawler.robots import RobotsParser
from app.core.database import async_session

logger = logging.getLogger("crawler")
logging.basicConfig(level=logging.INFO)

# ── Global registry of active crawls ──────────────────────
# Maps crawl_id -> CrawlEngine instance for pause/stop/resume
active_crawls: dict[int, "CrawlEngine"] = {}


def _normalize_domain(netloc: str) -> str:
    """Strip 'www.' prefix so www.example.com and example.com match."""
    return netloc.lower().removeprefix("www.")


def _normalize_url(url: str) -> str:
    """Normalize a URL for deduplication.
    Lowercase, strip www., strip trailing slash, drop fragment & query."""
    parsed = urlparse(url.lower())
    netloc = parsed.netloc.removeprefix("www.")
    path = parsed.path.rstrip("/")
    return f"{parsed.scheme}://{netloc}{path}"


class CrawlEngine:
    """Async crawler that discovers pages and runs SEO analysis."""

    MAX_PAGES = 10000
    CONCURRENCY = 10
    TIMEOUT = 15

    def __init__(self, crawl_id: int, base_url: str, ignore_robots: bool = False):
        self.crawl_id = crawl_id
        self.base_url = base_url.rstrip("/")
        self.domain = urlparse(base_url).netloc
        self._base_domain = _normalize_domain(self.domain)
        self.ignore_robots = ignore_robots
        # Normalized set for deduplication — prevents crawling same page twice
        self._visited_normalized: set[str] = set()
        self.queue: asyncio.Queue = asyncio.Queue()
        self.robots_parser: Optional[RobotsParser] = None
        self._lock = asyncio.Lock()
        self._page_count = 0
        # Pause/stop control
        self._paused = asyncio.Event()
        self._paused.set()  # not paused by default (set = running)
        self._stopped = False
        self._workers: list[asyncio.Task] = []

    def _is_visited(self, url: str) -> bool:
        """Check if a normalized version of this URL was already crawled."""
        return _normalize_url(url) in self._visited_normalized

    def _mark_visited(self, url: str):
        """Mark a URL as visited (stores normalized form)."""
        self._visited_normalized.add(_normalize_url(url))

    def pause(self):
        """Pause crawling — workers will wait until resumed."""
        self._paused.clear()
        logger.info(f"Crawl {self.crawl_id} paused")

    def resume(self):
        """Resume a paused crawl."""
        self._paused.set()
        logger.info(f"Crawl {self.crawl_id} resumed")

    def stop(self):
        """Stop crawling — workers will exit cleanly."""
        self._stopped = True
        self._paused.set()  # unblock any paused workers so they can exit
        logger.info(f"Crawl {self.crawl_id} stopped")

    async def _update_crawl(self, **kwargs):
        """Update crawl record with a fresh DB session."""
        async with async_session() as db:
            crawl = await db.get(Crawl, self.crawl_id)
            for k, v in kwargs.items():
                setattr(crawl, k, v)
            await db.commit()

    async def _save_page(self, result: dict, content_type: str):
        """Save a page result with a fresh DB session."""
        async with async_session() as db:
            page = Page(
                crawl_id=self.crawl_id,
                url=result["url"],
                status_code=result["status_code"],
                response_time=result["response_time"],
                content_type=content_type,
                content_length=result["content_length"],
                title=result["title"],
                title_length=result["title_length"],
                meta_description=result["meta_description"],
                meta_description_length=result["meta_description_length"],
                canonical_url=result["canonical_url"],
                canonical_issues=result.get("canonical_issues"),
                robots_meta=result["robots_meta"],
                is_noindex=result.get("is_noindex", False),
                is_nofollow_meta=result.get("is_nofollow_meta", False),
                h1_count=result["h1_count"],
                h1_texts=result["h1_texts"],
                h2_count=result["h2_count"],
                h3_count=result["h3_count"],
                h4_count=result["h4_count"],
                h5_count=result["h5_count"],
                h6_count=result["h6_count"],
                total_images=result["total_images"],
                images_without_alt=result["images_without_alt"],
                images_without_alt_urls=result.get("images_without_alt_urls"),
                images_with_empty_alt=result.get("images_with_empty_alt", 0),
                images_with_empty_alt_urls=result.get("images_with_empty_alt_urls"),
                internal_links=result["internal_links"],
                external_links=result["external_links"],
                nofollow_links=result.get("nofollow_links", 0),
                nofollow_internal_links=result.get("nofollow_internal_links"),
                broken_links=result["broken_links"],
                has_schema_markup=result["has_schema_markup"],
                schema_types=result["schema_types"],
                has_viewport_meta=result["has_viewport_meta"],
                word_count=result["word_count"],
                has_lazy_loading=result["has_lazy_loading"],
                code_to_text_ratio=result.get("code_to_text_ratio"),
                html_size=result.get("html_size"),
                text_size=result.get("text_size"),
                og_title=result["og_title"],
                og_description=result["og_description"],
                og_image=result["og_image"],
                has_hreflang=result.get("has_hreflang", False),
                hreflang_entries=result.get("hreflang_entries"),
                hreflang_issues=result.get("hreflang_issues"),
                has_placeholders=result.get("has_placeholders", False),
                placeholder_content=result.get("placeholder_content"),
                redirect_target=result.get("redirect_target"),
                issues=result["issues"],
                score=result["score"],
            )
            db.add(page)
            await db.commit()

    async def _resolve_start_url(self):
        """Follow redirects on the starting URL to find the real base domain.
        E.g. www.example.com -> example.com: we crawl under example.com only."""
        try:
            async with httpx.AsyncClient(
                timeout=self.TIMEOUT,
                follow_redirects=True,
                verify=False,
                headers={"User-Agent": "Mozilla/5.0 (compatible; SEOCrawlerBot/1.0; +https://ai.tudordaniel.ro)"},
            ) as client:
                resp = await client.get(self.base_url)
                final_url = str(resp.url).rstrip("/")
                final_domain = urlparse(final_url).netloc

                if final_domain != self.domain:
                    logger.info(
                        f"Start URL redirected: {self.base_url} ({self.domain}) "
                        f"-> {final_url} ({final_domain})"
                    )
                    self.base_url = final_url
                    self.domain = final_domain
                    self._base_domain = _normalize_domain(final_domain)
                elif final_url != self.base_url:
                    logger.info(f"Start URL resolved to: {final_url}")
                    self.base_url = final_url
        except httpx.RequestError as e:
            logger.warning(f"Could not resolve start URL: {e} — using original")

    # File extensions to skip — these are never HTML pages
    _SKIP_EXTENSIONS = frozenset((
        # Data / API
        '.json', '.xml', '.rss', '.atom', '.rdf',
        # Stylesheets / Scripts
        '.css', '.js', '.mjs', '.ts', '.map',
        # Documents / Archives
        '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
        '.zip', '.gz', '.tar', '.rar', '.7z', '.bz2',
        # Images
        '.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp', '.ico', '.bmp', '.tiff', '.avif',
        # Fonts
        '.woff', '.woff2', '.ttf', '.eot', '.otf',
        # Media
        '.mp3', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.ogg', '.wav',
        # Other
        '.exe', '.dmg', '.apk', '.ics', '.vcf', '.csv', '.txt', '.log',
    ))

    # URL path prefixes to skip — known non-HTML endpoints
    _SKIP_PATH_PREFIXES = (
        '/wp-json/', '/wp-json', '/feed/', '/feed',
        '/xmlrpc.php', '/wp-admin/',
        '/api/', '/_api/',
    )

    def _should_skip_url(self, url: str) -> bool:
        """Return True if the URL points to a non-HTML resource."""
        parsed = urlparse(url)
        path = parsed.path.lower()

        # Check file extension
        dot_pos = path.rfind('.')
        if dot_pos != -1:
            ext = path[dot_pos:]
            if ext in self._SKIP_EXTENSIONS:
                return True

        # Check known non-HTML path prefixes
        for prefix in self._SKIP_PATH_PREFIXES:
            if path.startswith(prefix):
                return True

        return False

    def _enqueue(self, url: str):
        """Add a URL to the crawl queue if not already visited and within limits."""
        if not self._is_visited(url) and self._page_count < self.MAX_PAGES:
            parsed = urlparse(url)
            if _normalize_domain(parsed.netloc) == self._base_domain:
                if self._should_skip_url(url):
                    return
                try:
                    self.queue.put_nowait(url)
                except asyncio.QueueFull:
                    pass

    async def run(self, resume_from_stopped: bool = False):
        """Start the crawl."""
        logger.info(f"Starting crawl {self.crawl_id} for {self.base_url} (resume={resume_from_stopped})")

        # Register in global registry
        active_crawls[self.crawl_id] = self

        await self._update_crawl(
            status="running",
            started_at=datetime.datetime.utcnow(),
        )

        try:
            # Resolve the starting URL — follow redirects to find the real domain
            await self._resolve_start_url()
            logger.info(f"Resolved base domain: {self.domain}")

            if resume_from_stopped:
                # Load already-visited URLs from DB so we don't re-crawl
                async with async_session() as db:
                    from sqlalchemy import select
                    result = await db.execute(
                        select(Page.url).where(Page.crawl_id == self.crawl_id)
                    )
                    for row in result.fetchall():
                        self._mark_visited(row[0])
                    self._page_count = len(self._visited_normalized)
                    logger.info(f"Resumed with {self._page_count} already-crawled URLs")

            # Parse robots.txt (always fetch for bot analysis, but optionally ignore for crawling)
            logger.info("Fetching robots.txt...")
            self.robots_parser = RobotsParser(self.base_url)
            await self.robots_parser.fetch()

            if self.ignore_robots:
                logger.info("Ignoring robots.txt restrictions (user override)")

            # Save robots.txt info to crawl
            robots_status = "found" if self.robots_parser.content else "not_found"
            await self._update_crawl(
                robots_txt_status=robots_status,
                robots_txt_content=self.robots_parser.content if hasattr(self.robots_parser, 'content') else None,
            )

            # Try sitemap first for URL discovery
            logger.info("Fetching sitemap.xml...")
            sitemap_parser = SitemapParser(self.base_url)
            sitemap_urls = await sitemap_parser.fetch()
            logger.info(f"Found {len(sitemap_urls)} URLs in sitemap")

            # Save sitemap data to crawl
            await self._update_crawl(
                sitemaps_found=sitemap_parser.sitemaps_found,
            )

            # Seed the queue — only URLs matching our resolved domain
            self._enqueue(self.base_url)
            for url in sitemap_urls[:self.MAX_PAGES]:
                self._enqueue(url)

            logger.info(f"Queue seeded with {self.queue.qsize()} URLs. Starting workers...")

            # Run workers
            self._workers = [asyncio.create_task(self._worker()) for _ in range(self.CONCURRENCY)]

            # Wait until queue is empty with a timeout
            try:
                await asyncio.wait_for(self.queue.join(), timeout=7200)
            except asyncio.TimeoutError:
                logger.warning("Crawl timed out after 2 hours")

            # Cancel workers
            for w in self._workers:
                w.cancel()
            await asyncio.gather(*self._workers, return_exceptions=True)

            # Determine final status
            if self._stopped:
                final_status = "stopped"
            else:
                final_status = "completed"

            await self._update_crawl(
                status=final_status,
                completed_at=datetime.datetime.utcnow(),
                pages_crawled=self._page_count,
                pages_total=self._page_count,
            )
            logger.info(f"Crawl {final_status}. {self._page_count} pages crawled.")

        except Exception as e:
            logger.error(f"Crawl failed: {e}", exc_info=True)
            await self._update_crawl(
                status="failed",
                completed_at=datetime.datetime.utcnow(),
            )
        finally:
            active_crawls.pop(self.crawl_id, None)

    async def _worker(self):
        """Worker that processes URLs from the queue."""
        async with httpx.AsyncClient(
            timeout=self.TIMEOUT,
            follow_redirects=True,
            verify=False,
            headers={"User-Agent": "Mozilla/5.0 (compatible; SEOCrawlerBot/1.0; +https://ai.tudordaniel.ro)"},
        ) as client:
            while True:
                # Check if stopped
                if self._stopped:
                    return

                # Wait if paused
                await self._paused.wait()

                url = await self.queue.get()
                try:
                    # Check stop again after getting URL
                    if self._stopped:
                        continue

                    # Wait if paused
                    await self._paused.wait()

                    # Thread-safe check: skip if already visited or at max
                    async with self._lock:
                        if self._is_visited(url) or self._page_count >= self.MAX_PAGES:
                            continue
                        self._mark_visited(url)

                    # Check robots.txt (skip if user chose to bypass)
                    if not self.ignore_robots and self.robots_parser and not self.robots_parser.is_allowed(url):
                        logger.info(f"Blocked by robots.txt: {url}")
                        continue

                    logger.info(f"Crawling [{self._page_count}]: {url}")
                    await self._crawl_page(client, url)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error crawling {url}: {e}")
                finally:
                    self.queue.task_done()

    async def _crawl_page(self, client: httpx.AsyncClient, url: str):
        """Fetch and analyze a single page.

        Uses follow_redirects=True so redirects resolve in one request.
        Only the FINAL page is saved (with the final URL as it appears on
        the server).  No separate 3xx records — just like Screaming Frog
        when it resolves a chain to the final destination.
        """
        start = time.monotonic()

        try:
            resp = await client.get(url)
        except httpx.RequestError as e:
            logger.warning(f"Request failed for {url}: {e}")
            return

        response_time = time.monotonic() - start

        # Keep the REAL final URL exactly as the server returned it
        final_url = str(resp.url)
        final_domain = urlparse(final_url).netloc
        final_status = resp.status_code

        # Detect redirects
        was_redirected = len(resp.history) > 0

        if was_redirected:
            original_status = resp.history[0].status_code
            logger.info(f"URL {url} redirected ({original_status}) -> {final_url} (status {final_status})")

            # If the final destination is off our domain, skip entirely
            if _normalize_domain(final_domain) != self._base_domain:
                logger.info(f"Redirect landed off-domain ({final_domain}), skipping")
                return

            # For REAL redirects (different normalized URL, e.g. /en/contact -> /en/kontakt):
            # check if we already crawled the final URL from a different entry point
            norm_final = _normalize_url(final_url)
            norm_original = _normalize_url(url)
            if norm_final != norm_original:
                # Real redirect — different page
                async with self._lock:
                    if norm_final in self._visited_normalized:
                        return  # final page was already crawled directly
                    self._mark_visited(final_url)
            # For trivial redirects (trailing slash, www, etc.): same normalized URL
            # The worker already marked it visited — just fall through to analyze

            # Fall through to save/analyze the final page

        content_type = resp.headers.get("content-type", "")

        # ── 4xx/5xx errors: save minimal record, NO SEO issues ──
        if final_status >= 400:
            logger.info(f"Error status {final_status} for {final_url}")
            error_result = {
                "url": final_url,
                "status_code": final_status,
                "response_time": response_time, "content_length": len(resp.content),
                "title": None, "title_length": 0,
                "meta_description": None, "meta_description_length": 0,
                "canonical_url": None, "canonical_issues": None,
                "robots_meta": None, "is_noindex": False, "is_nofollow_meta": False,
                "h1_count": 0, "h1_texts": None,
                "h2_count": 0, "h3_count": 0, "h4_count": 0, "h5_count": 0, "h6_count": 0,
                "total_images": 0, "images_without_alt": 0, "images_without_alt_urls": None,
                "images_with_empty_alt": 0, "images_with_empty_alt_urls": None,
                "internal_links": 0, "external_links": 0,
                "nofollow_links": 0, "nofollow_internal_links": None, "broken_links": 0,
                "has_schema_markup": False, "schema_types": None,
                "has_viewport_meta": False, "word_count": 0, "has_lazy_loading": False,
                "code_to_text_ratio": None, "html_size": None, "text_size": None,
                "og_title": None, "og_description": None, "og_image": None,
                "has_hreflang": False, "hreflang_entries": None, "hreflang_issues": None,
                "has_placeholders": False, "placeholder_content": None,
                "redirect_target": None, "issues": [], "score": 0,
            }
            try:
                await self._save_page(error_result, content_type or "error")
                async with self._lock:
                    self._page_count += 1
            except Exception as e:
                logger.error(f"DB save failed for error page {final_url}: {e}")
            try:
                await self._update_crawl(pages_crawled=self._page_count)
            except Exception:
                pass
            return

        if "text/html" not in content_type:
            logger.info(f"Skipping non-HTML: {final_url} ({content_type})")
            return

        # ── Analyze the page (the final 200 content) ──
        html = resp.text
        logger.info(f"Got {len(html)} bytes from {final_url} (status {final_status})")

        analyzer = SEOAnalyzer(final_url, html, final_status, response_time)
        result = analyzer.analyze()

        # Save to DB
        try:
            await self._save_page(result, content_type)
            async with self._lock:
                self._page_count += 1
        except Exception as e:
            logger.error(f"DB save failed for {final_url}: {e}")
            return

        # Update crawl progress
        try:
            await self._update_crawl(pages_crawled=self._page_count)
        except Exception as e:
            logger.error(f"Progress update failed: {e}")

        # ── Discover new internal links from ALL sources on the page ──
        soup = BeautifulSoup(html, "lxml")
        found_hrefs = set()

        # 1. All <a href="..."> links
        for tag in soup.find_all("a", href=True):
            found_hrefs.add(tag["href"])

        # 2. <link rel="alternate" hreflang="..."> (language versions)
        for tag in soup.find_all("link", href=True, rel=True):
            rels = tag.get("rel", [])
            if "alternate" in rels or "canonical" in rels:
                found_hrefs.add(tag["href"])

        # 3. <area href="..."> in image maps
        for tag in soup.find_all("area", href=True):
            found_hrefs.add(tag["href"])

        # 4. <iframe src="..."> on same domain
        for tag in soup.find_all("iframe", src=True):
            found_hrefs.add(tag["src"])

        # Queue all discovered same-domain URLs (keep URLs as-is, no stripping)
        discovered = 0
        for href in found_hrefs:
            if href.startswith(("#", "mailto:", "tel:", "javascript:", "data:")):
                continue
            full_url = urljoin(final_url, href).split("#")[0].split("?")[0]
            self._enqueue(full_url)
            discovered += 1

        logger.info(f"Discovered {discovered} new URLs from {final_url}")
