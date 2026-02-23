"""Robots.txt parser with per-bot access analysis."""
from urllib.parse import urljoin, urlparse
import httpx


# ── All major bots to check ──────────────────────────────
KNOWN_BOTS = [
    # Search engine crawlers
    {"name": "Googlebot", "agents": ["Googlebot"], "category": "Search Engine"},
    {"name": "Googlebot-Image", "agents": ["Googlebot-Image"], "category": "Search Engine"},
    {"name": "Googlebot-News", "agents": ["Googlebot-News"], "category": "Search Engine"},
    {"name": "Googlebot-Video", "agents": ["Googlebot-Video"], "category": "Search Engine"},
    {"name": "Google-InspectionTool", "agents": ["Google-InspectionTool"], "category": "Search Engine"},
    {"name": "Bingbot", "agents": ["bingbot", "msnbot"], "category": "Search Engine"},
    {"name": "Yandex", "agents": ["YandexBot", "Yandex"], "category": "Search Engine"},
    {"name": "Baiduspider", "agents": ["Baiduspider"], "category": "Search Engine"},
    {"name": "DuckDuckBot", "agents": ["DuckDuckBot"], "category": "Search Engine"},
    {"name": "Yahoo! Slurp", "agents": ["Slurp"], "category": "Search Engine"},
    {"name": "Applebot", "agents": ["Applebot"], "category": "Search Engine"},
    # Social media
    {"name": "Twitterbot", "agents": ["Twitterbot"], "category": "Social Media"},
    {"name": "facebookexternalhit", "agents": ["facebookexternalhit"], "category": "Social Media"},
    {"name": "LinkedInBot", "agents": ["LinkedInBot"], "category": "Social Media"},
    # AI / LLM bots
    {"name": "GPTBot (OpenAI)", "agents": ["GPTBot"], "category": "AI / LLM"},
    {"name": "ChatGPT-User", "agents": ["ChatGPT-User"], "category": "AI / LLM"},
    {"name": "OAI-SearchBot", "agents": ["OAI-SearchBot"], "category": "AI / LLM"},
    {"name": "ClaudeBot (Anthropic)", "agents": ["ClaudeBot", "anthropic-ai", "Claude-Web"], "category": "AI / LLM"},
    {"name": "Google-Extended", "agents": ["Google-Extended"], "category": "AI / LLM"},
    {"name": "Bytespider (TikTok)", "agents": ["Bytespider"], "category": "AI / LLM"},
    {"name": "CCBot (Common Crawl)", "agents": ["CCBot"], "category": "AI / LLM"},
    {"name": "PerplexityBot", "agents": ["PerplexityBot"], "category": "AI / LLM"},
    {"name": "Cohere-ai", "agents": ["cohere-ai"], "category": "AI / LLM"},
    {"name": "Meta-ExternalAgent", "agents": ["Meta-ExternalAgent"], "category": "AI / LLM"},
    {"name": "Amazonbot", "agents": ["Amazonbot"], "category": "AI / LLM"},
    {"name": "Diffbot", "agents": ["Diffbot"], "category": "AI / LLM"},
    {"name": "Omgilibot", "agents": ["Omgilibot"], "category": "AI / LLM"},
    # SEO tools
    {"name": "AhrefsBot", "agents": ["AhrefsBot"], "category": "SEO Tool"},
    {"name": "SemrushBot", "agents": ["SemrushBot"], "category": "SEO Tool"},
    {"name": "DotBot (Moz)", "agents": ["DotBot", "dotbot"], "category": "SEO Tool"},
    {"name": "MJ12bot (Majestic)", "agents": ["MJ12bot"], "category": "SEO Tool"},
    {"name": "Screaming Frog", "agents": ["Screaming Frog SEO Spider"], "category": "SEO Tool"},
]


class RobotsParser:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.disallowed: list[str] = []  # for our own crawler (backwards compat)
        self.sitemaps: list[str] = []
        self.content: str | None = None
        # Per-agent rules: {agent_lower: {"allow": [...], "disallow": [...]}}
        self._agent_rules: dict[str, dict[str, list[str]]] = {}

    async def fetch(self):
        robots_url = urljoin(self.base_url, "/robots.txt")
        try:
            async with httpx.AsyncClient(
                timeout=10, follow_redirects=True, verify=False
            ) as client:
                resp = await client.get(robots_url)
                if resp.status_code == 200:
                    self.content = resp.text
                    self._parse(resp.text)
        except httpx.RequestError:
            pass

    def _parse(self, content: str):
        current_agents: list[str] = []
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
                # New user-agent block (could be multiple user-agent lines in a row)
                if not current_agents or (current_agents and self._agent_rules.get(current_agents[-1].lower())):
                    current_agents = []
                current_agents.append(value)
                agent_key = value.lower()
                if agent_key not in self._agent_rules:
                    self._agent_rules[agent_key] = {"allow": [], "disallow": []}
            elif key == "disallow" and current_agents:
                for agent in current_agents:
                    agent_key = agent.lower()
                    if agent_key not in self._agent_rules:
                        self._agent_rules[agent_key] = {"allow": [], "disallow": []}
                    if value:
                        self._agent_rules[agent_key]["disallow"].append(value)
                    # Empty disallow = allow all (explicit)
                # Also track for our own crawler
                if any(a in ("*", "SEOCrawlerBot") for a in current_agents):
                    if value:
                        self.disallowed.append(value)
            elif key == "allow" and current_agents:
                for agent in current_agents:
                    agent_key = agent.lower()
                    if agent_key not in self._agent_rules:
                        self._agent_rules[agent_key] = {"allow": [], "disallow": []}
                    if value:
                        self._agent_rules[agent_key]["allow"].append(value)
            elif key == "sitemap":
                self.sitemaps.append(value)

    def is_allowed(self, url: str) -> bool:
        """Check if a URL is allowed for our crawler."""
        path = urlparse(url).path
        for pattern in self.disallowed:
            if path.startswith(pattern):
                return False
        return True

    def analyze_bot_access(self) -> list[dict]:
        """Analyze access for all known bots.
        Returns a list of dicts with bot info and access status."""
        results = []
        for bot in KNOWN_BOTS:
            # Find the most specific matching rule set for this bot
            rules = None
            for agent_name in bot["agents"]:
                agent_key = agent_name.lower()
                if agent_key in self._agent_rules:
                    rules = self._agent_rules[agent_key]
                    break

            # Fall back to wildcard rules
            wildcard_rules = self._agent_rules.get("*")

            # Determine status
            if rules is not None:
                disallow = rules["disallow"]
                allow = rules["allow"]
                if disallow == ["/"] and not allow:
                    status = "blocked"
                elif disallow and allow:
                    status = "partially_blocked"
                elif disallow:
                    status = "partially_blocked"
                elif not disallow:
                    # Explicit user-agent section with no disallow = fully allowed
                    status = "allowed"
                else:
                    status = "allowed"
                disallow_rules = disallow
                allow_rules = allow
            elif wildcard_rules is not None:
                disallow = wildcard_rules["disallow"]
                allow = wildcard_rules["allow"]
                if disallow == ["/"] and not allow:
                    status = "blocked"
                elif disallow and allow:
                    status = "partially_blocked"
                elif disallow:
                    status = "partially_blocked"
                else:
                    status = "allowed"
                disallow_rules = disallow
                allow_rules = allow
            else:
                # No rules at all = allowed
                status = "allowed"
                disallow_rules = []
                allow_rules = []

            results.append({
                "name": bot["name"],
                "category": bot["category"],
                "status": status,
                "disallow": disallow_rules,
                "allow": allow_rules,
            })

        return results
