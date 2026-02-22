"""API routes for SEO Crawler."""
import asyncio
from collections import defaultdict
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.core.database import get_db
from app.models.models import Project, Crawl, Page
from app.schemas.schemas import (
    ProjectCreate, ProjectResponse,
    CrawlCreate, CrawlResponse,
    PageSummary, PageDetail, PageTableRow, CrawlSummary,
    DuplicateGroup, StatusCodeGroup, IssueGroup,
)
from app.crawler.engine import CrawlEngine

router = APIRouter()


# ─── Projects ───────────────────────────────────────────────
@router.post("/projects", response_model=ProjectResponse)
async def create_project(data: ProjectCreate, db: AsyncSession = Depends(get_db)):
    project = Project(name=data.name, url=data.url)
    db.add(project)
    await db.commit()
    await db.refresh(project)
    return project


@router.get("/projects", response_model=list[ProjectResponse])
async def list_projects(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Project).order_by(Project.created_at.desc()))
    return result.scalars().all()


@router.get("/projects/{project_id}", response_model=ProjectResponse)
async def get_project(project_id: int, db: AsyncSession = Depends(get_db)):
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project


@router.delete("/projects/{project_id}")
async def delete_project(project_id: int, db: AsyncSession = Depends(get_db)):
    project = await db.get(Project, project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    await db.delete(project)
    await db.commit()
    return {"message": "Project deleted"}


# ─── Crawls ─────────────────────────────────────────────────
async def _run_crawl(crawl_id: int, base_url: str):
    """Background task to execute the crawl."""
    engine = CrawlEngine(crawl_id, base_url)
    await engine.run()


@router.post("/crawls", response_model=CrawlResponse)
async def start_crawl(data: CrawlCreate, background_tasks: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    project = await db.get(Project, data.project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")

    crawl = Crawl(project_id=project.id, status="pending")
    db.add(crawl)
    await db.commit()
    await db.refresh(crawl)

    background_tasks.add_task(_run_crawl, crawl.id, project.url)
    return crawl


@router.get("/crawls/{crawl_id}", response_model=CrawlResponse)
async def get_crawl(crawl_id: int, db: AsyncSession = Depends(get_db)):
    crawl = await db.get(Crawl, crawl_id)
    if not crawl:
        raise HTTPException(status_code=404, detail="Crawl not found")
    return crawl


@router.get("/projects/{project_id}/crawls", response_model=list[CrawlResponse])
async def list_crawls(project_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Crawl).where(Crawl.project_id == project_id).order_by(Crawl.created_at.desc())
    )
    return result.scalars().all()


# ─── Pages ──────────────────────────────────────────────────
@router.get("/crawls/{crawl_id}/pages", response_model=list[PageSummary])
async def list_pages(crawl_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Page).where(Page.crawl_id == crawl_id).order_by(Page.score.asc())
    )
    pages = result.scalars().all()
    return [
        PageSummary(
            id=p.id,
            url=p.url,
            status_code=p.status_code,
            title=p.title,
            score=p.score,
            issues_count=len(p.issues) if p.issues else 0,
            response_time=p.response_time,
        )
        for p in pages
    ]


@router.get("/crawls/{crawl_id}/pages/table", response_model=list[PageTableRow])
async def list_pages_table(crawl_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        select(Page).where(Page.crawl_id == crawl_id).order_by(Page.score.asc())
    )
    pages = result.scalars().all()
    return [
        PageTableRow(
            id=p.id,
            url=p.url,
            status_code=p.status_code,
            score=p.score,
            title=p.title,
            title_length=p.title_length,
            meta_description_length=p.meta_description_length,
            canonical_url=p.canonical_url,
            is_noindex=p.is_noindex or False,
            is_nofollow_meta=p.is_nofollow_meta or False,
            h1_count=p.h1_count or 0,
            h2_count=p.h2_count or 0,
            total_images=p.total_images or 0,
            images_without_alt=p.images_without_alt or 0,
            images_with_empty_alt=p.images_with_empty_alt or 0,
            internal_links=p.internal_links or 0,
            external_links=p.external_links or 0,
            nofollow_links=p.nofollow_links or 0,
            word_count=p.word_count or 0,
            code_to_text_ratio=p.code_to_text_ratio,
            has_schema_markup=p.has_schema_markup or False,
            has_hreflang=p.has_hreflang or False,
            has_viewport_meta=p.has_viewport_meta or False,
            has_lazy_loading=p.has_lazy_loading or False,
            has_placeholders=p.has_placeholders or False,
            response_time=p.response_time,
            issues_count=len(p.issues) if p.issues else 0,
        )
        for p in pages
    ]


@router.get("/pages/{page_id}", response_model=PageDetail)
async def get_page(page_id: int, db: AsyncSession = Depends(get_db)):
    page = await db.get(Page, page_id)
    if not page:
        raise HTTPException(status_code=404, detail="Page not found")
    return page


# ─── Dashboard / Summary ────────────────────────────────────
@router.get("/crawls/{crawl_id}/summary")
async def get_crawl_summary(crawl_id: int, db: AsyncSession = Depends(get_db)):
    crawl = await db.get(Crawl, crawl_id)
    if not crawl:
        raise HTTPException(status_code=404, detail="Crawl not found")

    result = await db.execute(select(Page).where(Page.crawl_id == crawl_id))
    pages = result.scalars().all()

    if not pages:
        raise HTTPException(status_code=404, detail="No pages found for this crawl")

    total = len(pages)
    avg_score = sum(p.score or 0 for p in pages) / total

    # Count issues by severity
    critical = 0
    warnings = 0
    info_count = 0
    issue_map = defaultdict(list)  # type -> [{url, page_id, detail}]

    for p in pages:
        for i in (p.issues or []):
            sev = i.get("severity", "")
            if sev == "critical":
                critical += 1
            elif sev == "warning":
                warnings += 1
            elif sev == "info":
                info_count += 1
            itype = i.get("type", "unknown")
            issue_map[itype].append({
                "url": p.url, "page_id": p.id,
                "detail": i.get("message", ""),
            })

    # --- Duplicate titles ---
    title_groups = defaultdict(list)
    for p in pages:
        if p.title:
            title_groups[p.title].append({"url": p.url, "page_id": p.id})
    duplicate_titles = [
        DuplicateGroup(value=title, pages=pg, count=len(pg))
        for title, pg in title_groups.items() if len(pg) > 1
    ]

    # --- Duplicate meta descriptions ---
    meta_groups = defaultdict(list)
    for p in pages:
        if p.meta_description:
            meta_groups[p.meta_description].append({"url": p.url, "page_id": p.id})
    duplicate_metas = [
        DuplicateGroup(value=desc, pages=pg, count=len(pg))
        for desc, pg in meta_groups.items() if len(pg) > 1
    ]

    # --- Status code breakdown ---
    status_groups = defaultdict(list)
    for p in pages:
        if p.status_code:
            status_groups[p.status_code].append({"url": p.url, "page_id": p.id})
    status_breakdown = [
        StatusCodeGroup(status_code=code, count=len(pg), pages=pg)
        for code, pg in sorted(status_groups.items())
    ]

    # --- Canonical issues ---
    canonical_issues = []
    for p in pages:
        if p.canonical_issues and len(p.canonical_issues) > 0:
            canonical_issues.append({
                "url": p.url, "page_id": p.id,
                "canonical_url": p.canonical_url,
                "issues": p.canonical_issues,
            })

    # --- Noindex / Nofollow ---
    noindex_pages = [{"url": p.url, "page_id": p.id} for p in pages if p.is_noindex]
    nofollow_pages = [{"url": p.url, "page_id": p.id, "nofollow_internal": p.nofollow_internal_links} for p in pages if p.is_nofollow_meta]

    # --- Images missing alt ---
    pages_missing_alt = []
    total_images_missing = 0
    pages_empty_alt = []
    total_images_empty_alt = 0
    for p in pages:
        if p.images_without_alt and p.images_without_alt > 0:
            pages_missing_alt.append({
                "url": p.url, "page_id": p.id,
                "missing_count": p.images_without_alt,
                "total_images": p.total_images,
                "missing_urls": p.images_without_alt_urls or [],
            })
            total_images_missing += p.images_without_alt
        if p.images_with_empty_alt and p.images_with_empty_alt > 0:
            pages_empty_alt.append({
                "url": p.url, "page_id": p.id,
                "empty_count": p.images_with_empty_alt,
                "total_images": p.total_images,
            })
            total_images_empty_alt += p.images_with_empty_alt

    # --- Hreflang issues ---
    hreflang_issues = []
    for p in pages:
        if p.hreflang_issues and len(p.hreflang_issues) > 0:
            hreflang_issues.append({
                "url": p.url, "page_id": p.id,
                "issues": p.hreflang_issues,
                "entries": p.hreflang_entries,
            })

    # --- Content issues ---
    thin_content = [{"url": p.url, "page_id": p.id, "word_count": p.word_count} for p in pages if p.word_count and p.word_count < 300]
    low_ratio = [{"url": p.url, "page_id": p.id, "ratio": p.code_to_text_ratio} for p in pages if p.code_to_text_ratio is not None and p.code_to_text_ratio < 10]
    placeholder_pages = [{"url": p.url, "page_id": p.id, "content": p.placeholder_content} for p in pages if p.has_placeholders]

    # --- Structure ---
    missing_title = sum(1 for p in pages if not p.title)
    missing_meta = sum(1 for p in pages if not p.meta_description)
    missing_h1 = sum(1 for p in pages if p.h1_count == 0)
    missing_viewport = sum(1 for p in pages if not p.has_viewport_meta)

    # --- Performance ---
    avg_resp = sum(p.response_time or 0 for p in pages) / total
    slow_pages = [{"url": p.url, "page_id": p.id, "response_time": p.response_time} for p in pages if p.response_time and p.response_time > 3]

    # --- Schema ---
    no_schema = sum(1 for p in pages if not p.has_schema_markup)

    # --- Issue groups for the grouped issues table ---
    issue_groups = []
    severity_map = {
        "missing_title": "critical", "missing_meta_description": "critical",
        "missing_h1": "critical", "missing_viewport": "critical",
        "placeholder_content": "critical",
        "noindex": "warning", "nofollow_meta": "warning",
        "short_title": "warning", "long_title": "warning",
        "short_meta_description": "warning", "long_meta_description": "warning",
        "missing_canonical": "warning", "canonical_external": "warning",
        "images_missing_alt": "warning", "thin_content": "warning",
        "low_text_ratio": "warning", "multiple_h1": "warning",
        "nofollow_internal": "warning", "hreflang_issue": "warning",
        "no_schema_markup": "info", "no_lazy_loading": "info",
        "missing_og_title": "info", "missing_og_image": "info",
        "canonical_relative": "info", "high_text_ratio": "info",
    }
    for itype, pages_list in issue_map.items():
        issue_groups.append(IssueGroup(
            category=itype,
            severity=severity_map.get(itype, "info"),
            count=len(pages_list),
            pages=pages_list[:50],  # cap at 50 per group
        ))
    issue_groups.sort(key=lambda g: ({"critical": 0, "warning": 1, "info": 2}.get(g.severity, 3), -g.count))

    return {
        "total_pages": total,
        "avg_score": round(avg_score, 1),
        "critical_issues": critical,
        "warnings": warnings,
        "info_issues": info_count,
        "duplicate_titles": duplicate_titles,
        "duplicate_meta_descriptions": duplicate_metas,
        "status_code_breakdown": status_breakdown,
        "canonical_issues": canonical_issues,
        "noindex_pages": noindex_pages,
        "nofollow_pages": nofollow_pages,
        "pages_missing_alt": pages_missing_alt,
        "total_images_missing_alt": total_images_missing,
        "pages_empty_alt": pages_empty_alt,
        "total_images_empty_alt": total_images_empty_alt,
        "hreflang_issues": hreflang_issues,
        "thin_content_pages": thin_content,
        "low_text_ratio_pages": low_ratio,
        "placeholder_pages": placeholder_pages,
        "pages_missing_title": missing_title,
        "pages_missing_meta": missing_meta,
        "pages_missing_h1": missing_h1,
        "pages_missing_viewport": missing_viewport,
        "avg_response_time": round(avg_resp, 3),
        "slow_pages": slow_pages,
        "robots_txt_status": crawl.robots_txt_status,
        "sitemaps_found": crawl.sitemaps_found,
        "pages_without_schema": no_schema,
        "issue_groups": issue_groups,
    }
