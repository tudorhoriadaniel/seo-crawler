"""
Crawl Engine — discovers and crawls pages on a website.
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


class CrawlEngine:
    """Async crawler that discovers pages and runs SEO analysis."""

    MAX_PAGES = 100
    CONCURRENCY = 3
    TIMEOUT = 15

    def __init__(self, crawl_id: int, base_url: str):
        self.crawl_id = crawl_id
        self.base_url = base_url.rstrip("/")
        self.domain = urlparse(base_url).netloc
        self.visited: set[str] = set()
        self.queue: asyncio.Queue = asyncio.Queue()
        self.robots_parser: Optional[RobotsParser] = None
        self._lock = asyncio.Lock()
        # Pause/stop control
        self._paused = asyncio.Event()
        self._paused.set()  # not paused by default (set = running)
        self._stopped = False
        self._workers: list[asyncio.Task] = []

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
                elif final_url != self.base_url:
                    logger.info(f"Start URL resolved to: {final_url}")
                    self.base_url = final_url
        except httpx.RequestError as e:
            logger.warning(f"Could not resolve start URL: {e} — using original")

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
                    existing_urls = {row[0] for row in result.fetchall()}
                    self.visited = existing_urls
                    logger.info(f"Resumed with {len(existing_urls)} already-crawled URLs")

            # Parse robots.txt
            logger.info("Fetching robots.txt...")
            self.robots_parser = RobotsParser(self.base_url)
            await self.robots_parser.fetch()

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
            if self.base_url not in self.visited:
                self.queue.put_nowait(self.base_url)
            for url in sitemap_urls[:self.MAX_PAGES]:
                if url not in self.visited and urlparse(url).netloc == self.domain:
                    self.queue.put_nowait(url)

            logger.info(f"Queue seeded with {self.queue.qsize()} URLs. Starting workers...")

            # Run workers
            self._workers = [asyncio.create_task(self._worker()) for _ in range(self.CONCURRENCY)]

            # Wait until queue is empty with a timeout
            try:
                await asyncio.wait_for(self.queue.join(), timeout=300)
            except asyncio.TimeoutError:
                logger.warning("Crawl timed out after 5 minutes")

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
                pages_crawled=len(self.visited),
                pages_total=len(self.visited),
            )
            logger.info(f"Crawl {final_status}. {len(self.visited)} pages crawled.")

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
                        if url in self.visited or len(self.visited) >= self.MAX_PAGES:
                            continue
                        self.visited.add(url)

                    # Check robots.txt
                    if self.robots_parser and not self.robots_parser.is_allowed(url):
                        logger.info(f"Blocked by robots.txt: {url}")
                        continue

                    logger.info(f"Crawling [{len(self.visited)}]: {url}")
                    await self._crawl_page(client, url)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error crawling {url}: {e}")
                finally:
                    self.queue.task_done()

    async def _crawl_page(self, client: httpx.AsyncClient, url: str):
        """Fetch and analyze a single page.

        Uses follow_redirects=True so if a URL 301s we transparently
        land on the final 200 page and analyze that.  The redirect is
        noted as an issue on the page but does NOT consume an extra slot.
        """
        start = time.monotonic()

        try:
            resp = await client.get(url)
        except httpx.RequestError as e:
            logger.warning(f"Request failed for {url}: {e}")
            return

        response_time = time.monotonic() - start
        final_url = str(resp.url).rstrip("/")
        final_domain = urlparse(final_url).netloc
        final_status = resp.status_code

        # If we were redirected, figure out where we ended up
        was_redirected = final_url != url.rstrip("/")

        if was_redirected:
            logger.info(f"URL {url} redirected to {final_url} (status {final_status})")

            # If the final destination is off our domain, skip it entirely
            if final_domain != self.domain:
                logger.info(f"Redirect landed off-domain ({final_domain}), skipping")
                return

            # Mark the final URL as visited too so we don't crawl it again
            async with self._lock:
                self.visited.add(final_url)

        content_type = resp.headers.get("content-type", "")

        if "text/html" not in content_type:
            logger.info(f"Skipping non-HTML: {final_url} ({content_type})")
            return

        html = resp.text
        logger.info(f"Got {len(html)} bytes from {final_url} (status {final_status})")

        # Run SEO analysis on the FINAL page (the actual 200 content)
        analyzer = SEOAnalyzer(final_url, html, final_status, response_time)
        result = analyzer.analyze()

        # If there was a redirect, note it as an issue on this page
        if was_redirected:
            result["redirect_target"] = final_url
            # Override the URL to the final destination (that's what we actually analyzed)
            result["url"] = final_url
            result.setdefault("issues", []).append({
                "severity": "info",
                "type": "redirect",
                "message": f"Reached via redirect from {url}"
            })

        # Save to DB
        try:
            await self._save_page(result, content_type)
        except Exception as e:
            logger.error(f"DB save failed for {final_url}: {e}")
            return

        # Update crawl progress
        try:
            await self._update_crawl(pages_crawled=len(self.visited))
        except Exception as e:
            logger.error(f"Progress update failed: {e}")

        # Discover new internal links (only same domain)
        soup = BeautifulSoup(html, "lxml")
        discovered = 0
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue
            full_url = urljoin(final_url, href).split("#")[0].split("?")[0].rstrip("/")
            parsed = urlparse(full_url)
            if parsed.netloc == self.domain and full_url not in self.visited:
                if len(self.visited) < self.MAX_PAGES:
                    try:
                        self.queue.put_nowait(full_url)
                        discovered += 1
                    except asyncio.QueueFull:
                        pass
        logger.info(f"Discovered {discovered} new URLs from {final_url}")
