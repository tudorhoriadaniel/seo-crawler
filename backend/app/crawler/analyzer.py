"""
SEO Page Analyzer — extracts all SEO signals from a single HTML page.
"""
import re
import json
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup


# Placeholder patterns to detect dev/test content
# Only match clear, unambiguous placeholder text — no framework syntax ($var, {{ }})
PLACEHOLDER_PATTERNS = [
    r'lorem\s+ipsum',              # lorem ipsum
    r'dolor\s+sit\s+amet',         # dolor sit amet
    r'consectetur\s+adipiscing',   # consectetur adipiscing
]
# These patterns are case-sensitive (no IGNORECASE) to avoid matching
# normal words like Spanish "todo" or common text
PLACEHOLDER_PATTERNS_STRICT = [
    r'TODO:\s',                    # TODO: items (requires colon)
    r'FIXME:\s',                   # FIXME: items (requires colon)
]
PLACEHOLDER_RE = re.compile('|'.join(PLACEHOLDER_PATTERNS), re.IGNORECASE)
PLACEHOLDER_STRICT_RE = re.compile('|'.join(PLACEHOLDER_PATTERNS_STRICT))


class SEOAnalyzer:
    """Analyze a single page's HTML for SEO issues."""

    def __init__(self, url: str, html: str, status_code: int, response_time: float):
        self.url = url
        self.html = html
        self.status_code = status_code
        self.response_time = response_time
        self.soup = BeautifulSoup(html, "lxml")
        self.domain = urlparse(url).netloc
        self.issues = []

    def analyze(self) -> dict:
        """Run full SEO audit and return results dict."""
        result = {
            "url": self.url,
            "status_code": self.status_code,
            "response_time": round(self.response_time, 3),
            "content_length": len(self.html),
            "content_type": "",
        }

        result.update(self._analyze_title())
        result.update(self._analyze_meta_description())
        result.update(self._analyze_canonical())
        result.update(self._analyze_robots_meta())
        result.update(self._analyze_headings())
        result.update(self._analyze_images())
        result.update(self._analyze_links())
        result.update(self._analyze_structured_data())
        result.update(self._analyze_viewport())
        result.update(self._analyze_content())
        result.update(self._analyze_open_graph())
        result.update(self._analyze_performance_hints())
        result.update(self._analyze_hreflang())
        result.update(self._analyze_nofollow())
        result.update(self._analyze_code_to_text_ratio())
        result.update(self._analyze_placeholders())

        result["issues"] = self.issues
        result["score"] = self._calculate_score()

        return result

    # ─── Title ────────────────────────────────────────────────
    def _analyze_title(self) -> dict:
        title_tag = self.soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else None
        title_length = len(title) if title else 0

        if not title:
            self.issues.append({"severity": "critical", "type": "missing_title", "message": "Page is missing a <title> tag"})
        elif title_length < 30:
            self.issues.append({"severity": "warning", "type": "short_title", "message": f"Title too short ({title_length} chars). Aim for 30-60."})
        elif title_length > 60:
            self.issues.append({"severity": "warning", "type": "long_title", "message": f"Title too long ({title_length} chars). Aim for 30-60."})

        return {"title": title, "title_length": title_length}

    # ─── Meta Description ─────────────────────────────────────
    def _analyze_meta_description(self) -> dict:
        meta = self.soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
        desc = meta.get("content", "").strip() if meta else None
        desc_length = len(desc) if desc else 0

        if not desc:
            self.issues.append({"severity": "critical", "type": "missing_meta_description", "message": "Missing meta description"})
        elif desc_length < 120:
            self.issues.append({"severity": "warning", "type": "short_meta_description", "message": f"Meta description short ({desc_length} chars). Aim for 120-160."})
        elif desc_length > 160:
            self.issues.append({"severity": "warning", "type": "long_meta_description", "message": f"Meta description long ({desc_length} chars). Aim for 120-160."})

        return {"meta_description": desc, "meta_description_length": desc_length}

    # ─── Canonical ────────────────────────────────────────────
    def _analyze_canonical(self) -> dict:
        canonical = self.soup.find("link", attrs={"rel": "canonical"})
        canonical_url = canonical.get("href", "").strip() if canonical else None
        canonical_issues = []

        if not canonical_url:
            self.issues.append({"severity": "warning", "type": "missing_canonical", "message": "Missing canonical URL"})
            canonical_issues.append("missing")
        else:
            # Check if canonical is self-referencing
            parsed_canonical = urlparse(canonical_url)
            parsed_page = urlparse(self.url)

            # Canonical points to different domain
            if parsed_canonical.netloc and parsed_canonical.netloc != self.domain:
                self.issues.append({"severity": "warning", "type": "canonical_external", "message": f"Canonical points to external domain: {parsed_canonical.netloc}"})
                canonical_issues.append("external")

            # Canonical is relative (not absolute)
            if not parsed_canonical.scheme:
                self.issues.append({"severity": "info", "type": "canonical_relative", "message": "Canonical URL is relative, should be absolute"})
                canonical_issues.append("relative")

            # Canonical doesn't match current URL (not self-referencing)
            canon_normalized = canonical_url.rstrip("/").split("?")[0].split("#")[0]
            url_normalized = self.url.rstrip("/").split("?")[0].split("#")[0]
            if canon_normalized and canon_normalized != url_normalized:
                canonical_issues.append("not_self_referencing")

        return {"canonical_url": canonical_url, "canonical_issues": canonical_issues}

    # ─── Robots Meta ──────────────────────────────────────────
    def _analyze_robots_meta(self) -> dict:
        robots = self.soup.find("meta", attrs={"name": re.compile(r"robots", re.I)})
        robots_content = robots.get("content", "").strip() if robots else None
        is_noindex = False
        is_nofollow = False

        if robots_content:
            lower = robots_content.lower()
            if "noindex" in lower:
                is_noindex = True
                self.issues.append({"severity": "warning", "type": "noindex", "message": "Page has noindex directive"})
            if "nofollow" in lower:
                is_nofollow = True
                self.issues.append({"severity": "warning", "type": "nofollow_meta", "message": "Page has nofollow meta directive"})

        return {"robots_meta": robots_content, "is_noindex": is_noindex, "is_nofollow_meta": is_nofollow}

    # ─── Headings ─────────────────────────────────────────────
    def _analyze_headings(self) -> dict:
        h1_tags = self.soup.find_all("h1")
        h1_texts = [h.get_text(strip=True) for h in h1_tags]
        h1_count = len(h1_tags)

        if h1_count == 0:
            self.issues.append({"severity": "critical", "type": "missing_h1", "message": "Missing H1 heading"})
        elif h1_count > 1:
            self.issues.append({"severity": "warning", "type": "multiple_h1", "message": f"Page has {h1_count} H1 headings. Use only one."})

        return {
            "h1_count": h1_count,
            "h1_texts": h1_texts,
            "h2_count": len(self.soup.find_all("h2")),
            "h3_count": len(self.soup.find_all("h3")),
            "h4_count": len(self.soup.find_all("h4")),
            "h5_count": len(self.soup.find_all("h5")),
            "h6_count": len(self.soup.find_all("h6")),
        }

    # ─── Images ───────────────────────────────────────────────
    def _analyze_images(self) -> dict:
        # Find all <img> tags
        images = self.soup.find_all("img")
        # Also find <img> inside <picture> tags (already caught by above)
        # Also find elements with role="img" that should have alt
        role_imgs = self.soup.find_all(attrs={"role": "img"})
        # Also find <svg> used as images (inline SVGs without aria-label)
        inline_svgs = self.soup.find_all("svg")

        total = len(images)
        without_alt = []
        with_empty_alt = 0

        empty_alt_urls = []

        for img in images:
            alt = img.get("alt")
            src = img.get("src", img.get("data-src", img.get("data-lazy-src", "")))
            if alt is None:
                without_alt.append(src)
            elif alt.strip() == "":
                with_empty_alt += 1
                empty_alt_urls.append(src)

        # Check role="img" elements for aria-label
        role_img_missing = 0
        for el in role_imgs:
            if el.name == "img":
                continue  # already checked above
            label = el.get("aria-label", el.get("aria-labelledby", ""))
            if not label or not str(label).strip():
                role_img_missing += 1

        # Check inline SVGs for accessibility
        svg_missing = 0
        for svg in inline_svgs:
            has_title = svg.find("title")
            has_label = svg.get("aria-label", "")
            has_labelledby = svg.get("aria-labelledby", "")
            if not has_title and not has_label and not has_labelledby:
                svg_missing += 1

        if without_alt:
            self.issues.append({
                "severity": "warning",
                "type": "images_missing_alt",
                "message": f"{len(without_alt)} of {total} images missing alt attribute"
            })

        if with_empty_alt:
            self.issues.append({
                "severity": "warning",
                "type": "images_empty_alt",
                "message": f"{with_empty_alt} of {total} images have empty alt text (alt='')"
            })

        if role_img_missing:
            self.issues.append({
                "severity": "warning",
                "type": "role_img_missing_label",
                "message": f"{role_img_missing} elements with role='img' missing aria-label"
            })

        if svg_missing:
            self.issues.append({
                "severity": "info",
                "type": "svg_missing_title",
                "message": f"{svg_missing} inline SVGs missing <title> or aria-label"
            })

        return {
            "total_images": total,
            "images_without_alt": len(without_alt),
            "images_without_alt_urls": without_alt[:20],  # cap at 20
            "images_with_empty_alt": with_empty_alt,
            "images_with_empty_alt_urls": empty_alt_urls[:20],
        }

    # ─── Links ────────────────────────────────────────────────
    def _analyze_links(self) -> dict:
        links = self.soup.find_all("a", href=True)
        internal = 0
        external = 0
        nofollow_links = 0
        link_details = []

        for link in links:
            href = link["href"]
            if href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue
            parsed = urlparse(urljoin(self.url, href))
            rel = link.get("rel", [])
            if isinstance(rel, str):
                rel = [rel]
            is_nofollow = "nofollow" in [r.lower() for r in rel]

            if is_nofollow:
                nofollow_links += 1

            if parsed.netloc == self.domain or not parsed.netloc:
                internal += 1
            else:
                external += 1

        return {
            "internal_links": internal,
            "external_links": external,
            "nofollow_links": nofollow_links,
            "broken_links": [],
        }

    # ─── Structured Data ──────────────────────────────────────
    def _analyze_structured_data(self) -> dict:
        schema_scripts = self.soup.find_all("script", attrs={"type": "application/ld+json"})
        schema_types = []

        for script in schema_scripts:
            try:
                data = json.loads(script.string or "")
                if isinstance(data, dict):
                    if "@type" in data:
                        schema_types.append(data["@type"])
                    if "@graph" in data:
                        for item in data["@graph"]:
                            if isinstance(item, dict) and "@type" in item:
                                schema_types.append(item["@type"])
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "@type" in item:
                            schema_types.append(item["@type"])
            except (json.JSONDecodeError, TypeError):
                pass

        has_schema = len(schema_types) > 0
        if not has_schema:
            self.issues.append({"severity": "info", "type": "no_schema_markup", "message": "No structured data (JSON-LD) found"})

        return {"has_schema_markup": has_schema, "schema_types": schema_types}

    # ─── Viewport ─────────────────────────────────────────────
    def _analyze_viewport(self) -> dict:
        viewport = self.soup.find("meta", attrs={"name": "viewport"})
        has_viewport = viewport is not None

        if not has_viewport:
            self.issues.append({"severity": "critical", "type": "missing_viewport", "message": "Missing viewport meta tag"})

        return {"has_viewport_meta": has_viewport}

    # ─── Content ──────────────────────────────────────────────
    def _analyze_content(self) -> dict:
        # Clone soup to avoid mutating for other analyzers
        soup_copy = BeautifulSoup(self.html, "lxml")
        for tag in soup_copy(["script", "style", "noscript"]):
            tag.decompose()
        text = soup_copy.get_text(separator=" ", strip=True)
        words = len(text.split())

        if words < 300:
            self.issues.append({"severity": "warning", "type": "thin_content", "message": f"Thin content: only {words} words. Aim for 300+."})

        return {"word_count": words}

    # ─── Open Graph ───────────────────────────────────────────
    def _analyze_open_graph(self) -> dict:
        og_title = self.soup.find("meta", property="og:title")
        og_desc = self.soup.find("meta", property="og:description")
        og_image = self.soup.find("meta", property="og:image")

        if not og_title:
            self.issues.append({"severity": "info", "type": "missing_og_title", "message": "Missing Open Graph title"})
        if not og_image:
            self.issues.append({"severity": "info", "type": "missing_og_image", "message": "Missing Open Graph image"})

        return {
            "og_title": og_title.get("content", "") if og_title else None,
            "og_description": og_desc.get("content", "") if og_desc else None,
            "og_image": og_image.get("content", "") if og_image else None,
        }

    # ─── Performance ──────────────────────────────────────────
    def _analyze_performance_hints(self) -> dict:
        images = self.soup.find_all("img")
        has_lazy = any(img.get("loading") == "lazy" for img in images)

        if not has_lazy and len(images) > 5:
            self.issues.append({"severity": "info", "type": "no_lazy_loading", "message": "No lazy-loaded images. Add loading='lazy'."})

        return {"has_lazy_loading": has_lazy}

    # ─── Hreflang ─────────────────────────────────────────────
    def _analyze_hreflang(self) -> dict:
        hreflang_tags = self.soup.find_all("link", attrs={"rel": "alternate", "hreflang": True})
        hreflang_entries = []
        hreflang_issues = []

        for tag in hreflang_tags:
            lang = tag.get("hreflang", "").strip()
            href = tag.get("href", "").strip()
            hreflang_entries.append({"lang": lang, "href": href})

            if not href:
                hreflang_issues.append(f"Hreflang '{lang}' has empty href")
            if not lang:
                hreflang_issues.append("Hreflang tag has empty language code")

        # Check for x-default
        if hreflang_entries and not any(h["lang"] == "x-default" for h in hreflang_entries):
            hreflang_issues.append("Hreflang set found but missing x-default")

        # Check self-referencing
        if hreflang_entries:
            self_ref = any(h["href"].rstrip("/") == self.url.rstrip("/") for h in hreflang_entries)
            if not self_ref:
                hreflang_issues.append("Hreflang set doesn't include self-referencing tag")

        # ─── Canonical / Hreflang Conflict Detection ──────────
        if hreflang_entries:
            canonical = self.soup.find("link", attrs={"rel": "canonical"})
            canonical_url = canonical.get("href", "").strip() if canonical else None
            if canonical_url:
                canon_norm = canonical_url.rstrip("/").split("?")[0].split("#")[0]
                url_norm = self.url.rstrip("/").split("?")[0].split("#")[0]
                # Conflict: canonical points elsewhere but page has hreflang
                if canon_norm and canon_norm != url_norm:
                    hreflang_issues.append(
                        f"Canonical points to {canonical_url} but page has hreflang tags — conflicting signals"
                    )
            # Conflict: page has noindex + hreflang
            robots = self.soup.find("meta", attrs={"name": re.compile(r"robots", re.I)})
            if robots:
                robots_content = robots.get("content", "").lower()
                if "noindex" in robots_content:
                    hreflang_issues.append(
                        "Page has noindex meta but also hreflang tags — search engines will ignore hreflang"
                    )

        for issue_msg in hreflang_issues:
            self.issues.append({"severity": "warning", "type": "hreflang_issue", "message": issue_msg})

        return {
            "hreflang_entries": hreflang_entries,
            "hreflang_issues": hreflang_issues,
            "has_hreflang": len(hreflang_entries) > 0,
        }

    # ─── Nofollow Analysis ────────────────────────────────────
    def _analyze_nofollow(self) -> dict:
        links = self.soup.find_all("a", href=True)
        nofollow_internal = []

        for link in links:
            href = link["href"]
            if href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue
            rel = link.get("rel", [])
            if isinstance(rel, str):
                rel = [rel]
            is_nofollow = "nofollow" in [r.lower() for r in rel]
            parsed = urlparse(urljoin(self.url, href))

            if is_nofollow and (parsed.netloc == self.domain or not parsed.netloc):
                nofollow_internal.append(href)

        if nofollow_internal:
            self.issues.append({
                "severity": "warning",
                "type": "nofollow_internal",
                "message": f"{len(nofollow_internal)} internal links have nofollow"
            })

        return {"nofollow_internal_links": nofollow_internal[:20]}

    # ─── Code-to-Text Ratio ───────────────────────────────────
    def _analyze_code_to_text_ratio(self) -> dict:
        html_size = len(self.html)
        soup_copy = BeautifulSoup(self.html, "lxml")
        for tag in soup_copy(["script", "style", "noscript"]):
            tag.decompose()
        text = soup_copy.get_text(separator=" ", strip=True)
        text_size = len(text.encode("utf-8"))

        ratio = round((text_size / html_size * 100), 1) if html_size > 0 else 0

        if ratio < 10:
            self.issues.append({
                "severity": "warning",
                "type": "low_text_ratio",
                "message": f"Low text-to-HTML ratio ({ratio}%). Aim for 25-70%."
            })
        elif ratio > 90:
            self.issues.append({
                "severity": "info",
                "type": "high_text_ratio",
                "message": f"Very high text-to-HTML ratio ({ratio}%). Page may lack structure."
            })

        return {"code_to_text_ratio": ratio, "html_size": html_size, "text_size": text_size}

    # ─── Placeholder / Lorem Ipsum Detection ──────────────────
    def _analyze_placeholders(self) -> dict:
        soup_copy = BeautifulSoup(self.html, "lxml")
        for tag in soup_copy(["script", "style"]):
            tag.decompose()
        text = soup_copy.get_text(separator=" ", strip=True)

        found = []
        for match in PLACEHOLDER_RE.finditer(text):
            snippet = text[max(0, match.start() - 20):match.end() + 20].strip()
            found.append({"match": match.group(), "context": snippet})
        for match in PLACEHOLDER_STRICT_RE.finditer(text):
            snippet = text[max(0, match.start() - 20):match.end() + 20].strip()
            found.append({"match": match.group(), "context": snippet})

        if found:
            self.issues.append({
                "severity": "critical",
                "type": "placeholder_content",
                "message": f"Found {len(found)} placeholder/lorem ipsum content on page"
            })

        return {"placeholder_content": found[:20], "has_placeholders": len(found) > 0}

    # ─── Score ────────────────────────────────────────────────
    def _calculate_score(self) -> int:
        score = 100
        for issue in self.issues:
            if issue["severity"] == "critical":
                score -= 15
            elif issue["severity"] == "warning":
                score -= 7
            elif issue["severity"] == "info":
                score -= 2
        return max(0, min(100, score))
