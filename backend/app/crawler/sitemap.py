"""Sitemap parser — discovers URLs from XML sitemaps of all types."""
import logging
from urllib.parse import urljoin
import httpx
import xmltodict

logger = logging.getLogger("crawler")

# Common sitemap locations to check
SITEMAP_PATHS = [
    "/sitemap.xml",
    "/sitemap_index.xml",
    "/sitemaps.xml",
    "/sitemap/sitemap.xml",
    "/wp-sitemap.xml",
    "/sitemap-index.xml",
    "/post-sitemap.xml",
    "/page-sitemap.xml",
    "/news-sitemap.xml",
    "/video-sitemap.xml",
    "/image-sitemap.xml",
]


class SitemapParser:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.urls: list[str] = []
        self.sitemaps_found: list[dict] = []

    async def fetch(self) -> list[str]:
        """Try multiple sitemap locations and return all discovered URLs."""
        async with httpx.AsyncClient(timeout=10, follow_redirects=True, verify=False) as client:
            for path in SITEMAP_PATHS:
                sitemap_url = urljoin(self.base_url, path)
                try:
                    resp = await client.get(sitemap_url)
                    ct = resp.headers.get("content-type", "")
                    if resp.status_code == 200 and ("xml" in ct or resp.text.strip().startswith("<?xml")):
                        sitemap_type = self._detect_type(resp.text)
                        self.sitemaps_found.append({
                            "url": sitemap_url, "type": sitemap_type,
                            "status": "found", "urls_count": 0,
                        })
                        count_before = len(self.urls)
                        await self._parse(resp.text, client)
                        self.sitemaps_found[-1]["urls_count"] = len(self.urls) - count_before
                        logger.info(f"Sitemap found: {sitemap_url} ({sitemap_type})")
                except Exception as e:
                    logger.debug(f"Sitemap {sitemap_url} failed: {e}")

            # Also check robots.txt for Sitemap directives
            await self._check_robots_for_sitemaps(client)

        return self.urls

    async def _check_robots_for_sitemaps(self, client: httpx.AsyncClient):
        """Check robots.txt for Sitemap: directives."""
        try:
            resp = await client.get(urljoin(self.base_url, "/robots.txt"))
            if resp.status_code == 200:
                for line in resp.text.splitlines():
                    line = line.strip()
                    if line.lower().startswith("sitemap:"):
                        sm_url = line.split(":", 1)[1].strip()
                        if sm_url and not any(s["url"] == sm_url for s in self.sitemaps_found):
                            try:
                                resp2 = await client.get(sm_url)
                                if resp2.status_code == 200:
                                    self.sitemaps_found.append({
                                        "url": sm_url, "type": self._detect_type(resp2.text),
                                        "status": "found", "urls_count": 0,
                                    })
                                    count_before = len(self.urls)
                                    await self._parse(resp2.text, client)
                                    self.sitemaps_found[-1]["urls_count"] = len(self.urls) - count_before
                            except Exception:
                                pass
        except Exception:
            pass

    def _detect_type(self, xml_content: str) -> str:
        if "<sitemapindex" in xml_content:
            return "sitemap_index"
        if "<urlset" in xml_content:
            if "<video:" in xml_content: return "video_sitemap"
            if "<image:" in xml_content: return "image_sitemap"
            if "<news:" in xml_content: return "news_sitemap"
            return "urlset"
        return "unknown"

    async def _parse(self, xml_content: str, client: httpx.AsyncClient):
        try:
            data = xmltodict.parse(xml_content)
        except Exception:
            return

        # Handle sitemap index
        if "sitemapindex" in data:
            sitemaps = data["sitemapindex"].get("sitemap", [])
            if isinstance(sitemaps, dict):
                sitemaps = [sitemaps]
            for sm in sitemaps[:20]:
                loc = sm.get("loc")
                if loc:
                    try:
                        resp = await client.get(loc)
                        if resp.status_code == 200:
                            self.sitemaps_found.append({
                                "url": loc, "type": self._detect_type(resp.text),
                                "status": "found", "urls_count": 0,
                            })
                            count_before = len(self.urls)
                            self._parse_urlset(resp.text)
                            self.sitemaps_found[-1]["urls_count"] = len(self.urls) - count_before
                    except Exception:
                        self.sitemaps_found.append({
                            "url": loc, "type": "sub_sitemap", "status": "error", "urls_count": 0,
                        })

        # Handle urlset
        if "urlset" in data:
            self._parse_urlset(xml_content)

    def _parse_urlset(self, xml_content: str):
        try:
            data = xmltodict.parse(xml_content)
        except Exception:
            return
        if "urlset" not in data:
            return
        urls = data["urlset"].get("url", [])
        if isinstance(urls, dict):
            urls = [urls]
        for entry in urls:
            loc = entry.get("loc")
            if loc and loc not in self.urls:
                self.urls.append(loc)
