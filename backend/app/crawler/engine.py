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
                issues=result["issues"],
                score=result["score"],
            )
            db.add(page)
            await db.commit()

    async def run(self):
        """Start the crawl."""
        logger.info(f"Starting crawl {self.crawl_id} for {self.base_url}")

        await self._update_crawl(
            status="running",
            started_at=datetime.datetime.utcnow(),
        )

        try:
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

            # Seed the queue
            self.queue.put_nowait(self.base_url)
            for url in sitemap_urls[:self.MAX_PAGES]:
                self.queue.put_nowait(url)

            logger.info(f"Queue seeded with {self.queue.qsize()} URLs. Starting workers...")

            # Run workers
            workers = [asyncio.create_task(self._worker()) for _ in range(self.CONCURRENCY)]

            # Wait until queue is empty with a timeout
            try:
                await asyncio.wait_for(self.queue.join(), timeout=300)
            except asyncio.TimeoutError:
                logger.warning("Crawl timed out after 5 minutes")

            # Cancel workers
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

            # Update crawl status
            await self._update_crawl(
                status="completed",
                completed_at=datetime.datetime.utcnow(),
                pages_crawled=len(self.visited),
                pages_total=len(self.visited),
            )
            logger.info(f"Crawl completed. {len(self.visited)} pages crawled.")

        except Exception as e:
            logger.error(f"Crawl failed: {e}", exc_info=True)
            await self._update_crawl(
                status="failed",
                completed_at=datetime.datetime.utcnow(),
            )

    async def _worker(self):
        """Worker that processes URLs from the queue."""
        # Use TWO clients: one that does NOT follow redirects (to capture 301/302),
        # and one that does follow them (to get the final HTML).
        async with httpx.AsyncClient(
            timeout=self.TIMEOUT,
            follow_redirects=False,
            verify=False,
            headers={"User-Agent": "Mozilla/5.0 (compatible; SEOCrawlerBot/1.0; +https://ai.tudordaniel.ro)"},
        ) as client_nofollow, httpx.AsyncClient(
            timeout=self.TIMEOUT,
            follow_redirects=True,
            verify=False,
            headers={"User-Agent": "Mozilla/5.0 (compatible; SEOCrawlerBot/1.0; +https://ai.tudordaniel.ro)"},
        ) as client_follow:
            while True:
                url = await self.queue.get()
                try:
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
                    await self._crawl_page(client_nofollow, client_follow, url)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(f"Error crawling {url}: {e}")
                finally:
                    self.queue.task_done()

    async def _crawl_page(self, client_nofollow: httpx.AsyncClient, client_follow: httpx.AsyncClient, url: str):
        """Fetch and analyze a single page."""
        start = time.monotonic()

        # First request: no redirects, to capture the real status code
        try:
            resp_initial = await client_nofollow.get(url)
        except httpx.RequestError as e:
            logger.warning(f"Request failed for {url}: {e}")
            return

        original_status = resp_initial.status_code

        # If it's a redirect, follow it to get the HTML but keep the original status
        if original_status in (301, 302, 303, 307, 308):
            redirect_target = resp_initial.headers.get("location", "")
            if redirect_target:
                redirect_target = urljoin(url, redirect_target)
            logger.info(f"URL {url} redirects ({original_status}) to {redirect_target}")
            try:
                resp = await client_follow.get(url)
            except httpx.RequestError as e:
                logger.warning(f"Follow redirect failed for {url}: {e}")
                return
        else:
            resp = resp_initial
            redirect_target = None

        response_time = time.monotonic() - start
        content_type = resp.headers.get("content-type", "")

        if "text/html" not in content_type:
            logger.info(f"Skipping non-HTML: {url} ({content_type})")
            return

        html = resp.text
        logger.info(f"Got {len(html)} bytes from {url} (status {original_status})")

        # Run SEO analysis — pass the ORIGINAL status code, not the final one
        analyzer = SEOAnalyzer(url, html, original_status, response_time)
        result = analyzer.analyze()

        # Store redirect info if applicable
        if redirect_target:
            result["redirect_target"] = redirect_target
            result.setdefault("issues", []).append({
                "severity": "warning",
                "type": "redirect",
                "message": f"URL redirects ({original_status}) to {redirect_target}"
            })
            # Recalculate score with the new issue
            result["score"] = max(0, (result.get("score") or 100) - 7)

        # Save to DB (each save uses its own session)
        try:
            await self._save_page(result, content_type)
        except Exception as e:
            logger.error(f"DB save failed for {url}: {e}")
            return

        # Update crawl progress
        try:
            await self._update_crawl(pages_crawled=len(self.visited))
        except Exception as e:
            logger.error(f"Progress update failed: {e}")

        # Discover new internal links
        soup = BeautifulSoup(html, "lxml")
        discovered = 0
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue
            full_url = urljoin(url, href).split("#")[0].split("?")[0].rstrip("/")
            parsed = urlparse(full_url)
            if parsed.netloc == self.domain and full_url not in self.visited:
                if len(self.visited) < self.MAX_PAGES:
                    try:
                        self.queue.put_nowait(full_url)
                        discovered += 1
                    except asyncio.QueueFull:
                        pass
        logger.info(f"Discovered {discovered} new URLs from {url}")
