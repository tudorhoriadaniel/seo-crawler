"""Robots.txt parser."""
from urllib.parse import urljoin
import httpx


class RobotsParser:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.disallowed: list[str] = []
        self.sitemaps: list[str] = []
        self.content: str | None = None

    async def fetch(self):
        robots_url = urljoin(self.base_url, "/robots.txt")
        try:
            async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
                resp = await client.get(robots_url)
                if resp.status_code == 200:
                    self.content = resp.text
                    self._parse(resp.text)
        except httpx.RequestError:
            pass

    def _parse(self, content: str):
        current_agent = None
        for line in content.splitlines():
            line = line.strip()
            if line.startswith("#") or not line:
                continue
            if ":" not in line:
                continue
            key, _, value = line.partition(":")
            key = key.strip().lower()
            value = value.strip()

            if key == "user-agent":
                current_agent = value
            elif key == "disallow" and current_agent in ("*", "SEOCrawlerBot"):
                if value:
                    self.disallowed.append(value)
            elif key == "sitemap":
                self.sitemaps.append(value)

    def is_allowed(self, url: str) -> bool:
        from urllib.parse import urlparse
        path = urlparse(url).path
        for pattern in self.disallowed:
            if path.startswith(pattern):
                return False
        return True
