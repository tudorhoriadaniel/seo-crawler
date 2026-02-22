import datetime
from sqlalchemy import Column, Integer, String, Text, Float, Boolean, DateTime, ForeignKey, JSON
from sqlalchemy.orm import relationship
from app.core.database import Base


class Project(Base):
    __tablename__ = "projects"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    url = Column(String(2048), nullable=False)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)

    crawls = relationship("Crawl", back_populates="project", cascade="all, delete-orphan")


class Crawl(Base):
    __tablename__ = "crawls"

    id = Column(Integer, primary_key=True, index=True)
    project_id = Column(Integer, ForeignKey("projects.id"), nullable=False)
    status = Column(String(50), default="pending")
    pages_crawled = Column(Integer, default=0)
    pages_total = Column(Integer, default=0)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    # Site-wide data discovered during crawl
    robots_txt_status = Column(String(50), nullable=True)  # found, not_found, blocked
    robots_txt_content = Column(Text, nullable=True)
    sitemaps_found = Column(JSON, nullable=True)  # list of {url, type, status}

    project = relationship("Project", back_populates="crawls")
    pages = relationship("Page", back_populates="crawl", cascade="all, delete-orphan")


class Page(Base):
    __tablename__ = "pages"

    id = Column(Integer, primary_key=True, index=True)
    crawl_id = Column(Integer, ForeignKey("crawls.id"), nullable=False)
    url = Column(String(2048), nullable=False)
    status_code = Column(Integer, nullable=True)
    response_time = Column(Float, nullable=True)
    content_type = Column(String(255), nullable=True)
    content_length = Column(Integer, nullable=True)

    # On-page SEO
    title = Column(String(1024), nullable=True)
    title_length = Column(Integer, nullable=True)
    meta_description = Column(Text, nullable=True)
    meta_description_length = Column(Integer, nullable=True)
    canonical_url = Column(String(2048), nullable=True)
    canonical_issues = Column(JSON, nullable=True)
    robots_meta = Column(String(255), nullable=True)
    is_noindex = Column(Boolean, default=False)
    is_nofollow_meta = Column(Boolean, default=False)
    h1_count = Column(Integer, default=0)
    h1_texts = Column(JSON, nullable=True)
    h2_count = Column(Integer, default=0)
    h3_count = Column(Integer, default=0)
    h4_count = Column(Integer, default=0)
    h5_count = Column(Integer, default=0)
    h6_count = Column(Integer, default=0)

    # Images
    total_images = Column(Integer, default=0)
    images_without_alt = Column(Integer, default=0)
    images_without_alt_urls = Column(JSON, nullable=True)
    images_with_empty_alt = Column(Integer, default=0)

    # Links
    internal_links = Column(Integer, default=0)
    external_links = Column(Integer, default=0)
    nofollow_links = Column(Integer, default=0)
    nofollow_internal_links = Column(JSON, nullable=True)
    broken_links = Column(JSON, nullable=True)

    # Structured data
    has_schema_markup = Column(Boolean, default=False)
    schema_types = Column(JSON, nullable=True)

    # Mobile / viewport
    has_viewport_meta = Column(Boolean, default=False)

    # Content
    word_count = Column(Integer, default=0)
    has_lazy_loading = Column(Boolean, default=False)
    code_to_text_ratio = Column(Float, nullable=True)
    html_size = Column(Integer, nullable=True)
    text_size = Column(Integer, nullable=True)

    # Open Graph
    og_title = Column(String(1024), nullable=True)
    og_description = Column(Text, nullable=True)
    og_image = Column(String(2048), nullable=True)

    # Hreflang
    has_hreflang = Column(Boolean, default=False)
    hreflang_entries = Column(JSON, nullable=True)
    hreflang_issues = Column(JSON, nullable=True)

    # Placeholder content
    has_placeholders = Column(Boolean, default=False)
    placeholder_content = Column(JSON, nullable=True)

    # Issues found
    issues = Column(JSON, nullable=True)
    score = Column(Integer, nullable=True)

    crawled_at = Column(DateTime, default=datetime.datetime.utcnow)

    crawl = relationship("Crawl", back_populates="pages")
