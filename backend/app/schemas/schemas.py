from pydantic import BaseModel
from typing import Optional, Any
from datetime import datetime


# --- Projects ---
class ProjectCreate(BaseModel):
    name: str
    url: str


class ProjectResponse(BaseModel):
    id: int
    name: str
    url: str
    created_at: datetime

    class Config:
        from_attributes = True


# --- Crawls ---
class CrawlCreate(BaseModel):
    project_id: int


class CrawlResponse(BaseModel):
    id: int
    project_id: int
    status: str
    pages_crawled: int
    pages_total: int
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: datetime
    robots_txt_status: Optional[str] = None
    sitemaps_found: Optional[list] = None

    class Config:
        from_attributes = True


# --- Pages ---
class PageSummary(BaseModel):
    id: int
    url: str
    status_code: Optional[int]
    title: Optional[str]
    score: Optional[int]
    issues_count: int = 0
    response_time: Optional[float]

    class Config:
        from_attributes = True


class PageTableRow(BaseModel):
    id: int
    url: str
    status_code: Optional[int]
    score: Optional[int]
    title: Optional[str]
    title_length: Optional[int]
    meta_description_length: Optional[int]
    canonical_url: Optional[str]
    is_noindex: bool = False
    is_nofollow_meta: bool = False
    h1_count: int = 0
    h2_count: int = 0
    total_images: int = 0
    images_without_alt: int = 0
    images_with_empty_alt: int = 0
    internal_links: int = 0
    external_links: int = 0
    nofollow_links: int = 0
    word_count: int = 0
    code_to_text_ratio: Optional[float] = None
    has_schema_markup: bool = False
    has_hreflang: bool = False
    has_viewport_meta: bool = False
    has_lazy_loading: bool = False
    has_placeholders: bool = False
    response_time: Optional[float] = None
    issues_count: int = 0

    class Config:
        from_attributes = True


class PageDetail(BaseModel):
    id: int
    url: str
    status_code: Optional[int]
    response_time: Optional[float]
    content_type: Optional[str] = None
    content_length: Optional[int]
    title: Optional[str]
    title_length: Optional[int]
    meta_description: Optional[str]
    meta_description_length: Optional[int]
    canonical_url: Optional[str]
    canonical_issues: Optional[list] = None
    robots_meta: Optional[str]
    is_noindex: bool = False
    is_nofollow_meta: bool = False
    h1_count: int
    h1_texts: Optional[list]
    h2_count: int
    h3_count: int
    h4_count: int = 0
    h5_count: int = 0
    h6_count: int = 0
    total_images: int
    images_without_alt: int
    images_without_alt_urls: Optional[list] = None
    images_with_empty_alt: int = 0
    internal_links: int
    external_links: int
    nofollow_links: int = 0
    nofollow_internal_links: Optional[list] = None
    broken_links: Optional[list] = None
    has_schema_markup: bool
    schema_types: Optional[list]
    has_viewport_meta: bool
    word_count: int
    has_lazy_loading: bool = False
    code_to_text_ratio: Optional[float] = None
    html_size: Optional[int] = None
    text_size: Optional[int] = None
    og_title: Optional[str]
    og_description: Optional[str]
    og_image: Optional[str]
    has_hreflang: bool = False
    hreflang_entries: Optional[list] = None
    hreflang_issues: Optional[list] = None
    has_placeholders: bool = False
    placeholder_content: Optional[list] = None
    issues: Optional[list]
    score: Optional[int]

    class Config:
        from_attributes = True


# --- Dashboard Summary ---
class DuplicateGroup(BaseModel):
    value: str
    pages: list[dict]  # [{url, page_id}]
    count: int


class IssueGroup(BaseModel):
    category: str
    severity: str
    count: int
    pages: list[dict]  # [{url, page_id, detail}]


class StatusCodeGroup(BaseModel):
    status_code: int
    count: int
    pages: list[dict]


class CrawlSummary(BaseModel):
    total_pages: int
    avg_score: float
    critical_issues: int
    warnings: int
    info_issues: int = 0

    # Duplicates
    duplicate_titles: list[DuplicateGroup] = []
    duplicate_meta_descriptions: list[DuplicateGroup] = []

    # Status codes
    status_code_breakdown: list[StatusCodeGroup] = []

    # Canonical
    canonical_issues: list[dict] = []

    # Indexability
    noindex_pages: list[dict] = []
    nofollow_pages: list[dict] = []

    # Images
    pages_missing_alt: list[dict] = []
    total_images_missing_alt: int = 0

    # Hreflang
    hreflang_issues: list[dict] = []

    # Content
    thin_content_pages: list[dict] = []
    low_text_ratio_pages: list[dict] = []
    placeholder_pages: list[dict] = []

    # Structure
    pages_missing_title: int = 0
    pages_missing_meta: int = 0
    pages_missing_h1: int = 0
    pages_missing_viewport: int = 0

    # Performance
    avg_response_time: float = 0
    slow_pages: list[dict] = []

    # Robots & Sitemaps
    robots_txt_status: Optional[str] = None
    sitemaps_found: Optional[list] = None

    # Schema
    pages_without_schema: int = 0

    # All issues grouped
    issue_groups: list[IssueGroup] = []
