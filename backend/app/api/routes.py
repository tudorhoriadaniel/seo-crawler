"""API routes for SEO Crawler."""
import io
import asyncio
from collections import defaultdict
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
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

    # Separate redirect pages — they should NOT be counted for content/SEO issues
    REDIRECT_CODES = {301, 302, 303, 307, 308}
    content_pages = [p for p in pages if (p.status_code or 0) not in REDIRECT_CODES]
    content_total = len(content_pages) or 1  # avoid division by zero

    avg_score = sum(p.score or 0 for p in content_pages) / content_total

    # Count issues by severity (only non-redirect pages for content issues)
    critical = 0
    warnings = 0
    info_count = 0
    issue_map = defaultdict(list)  # type -> [{url, page_id, detail}]

    for p in content_pages:
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
    # Also count redirect issues separately
    for p in pages:
        if (p.status_code or 0) in REDIRECT_CODES:
            for i in (p.issues or []):
                if i.get("type") == "redirect":
                    warnings += 1
                    issue_map["redirect"].append({
                        "url": p.url, "page_id": p.id,
                        "detail": i.get("message", ""),
                    })

    # --- Duplicate titles (exclude redirects) ---
    title_groups = defaultdict(list)
    for p in content_pages:
        if p.title:
            title_groups[p.title].append({"url": p.url, "page_id": p.id})
    duplicate_titles = [
        DuplicateGroup(value=title, pages=pg, count=len(pg))
        for title, pg in title_groups.items() if len(pg) > 1
    ]

    # --- Duplicate meta descriptions (exclude redirects) ---
    meta_groups = defaultdict(list)
    for p in content_pages:
        if p.meta_description:
            meta_groups[p.meta_description].append({"url": p.url, "page_id": p.id})
    duplicate_metas = [
        DuplicateGroup(value=desc, pages=pg, count=len(pg))
        for desc, pg in meta_groups.items() if len(pg) > 1
    ]

    # --- Status code breakdown (ALL pages including redirects) ---
    status_groups = defaultdict(list)
    for p in pages:
        if p.status_code:
            status_groups[p.status_code].append({"url": p.url, "page_id": p.id})
    status_breakdown = [
        StatusCodeGroup(status_code=code, count=len(pg), pages=pg)
        for code, pg in sorted(status_groups.items())
    ]

    # --- Canonical issues (exclude redirects) ---
    canonical_issues = []
    for p in content_pages:
        if p.canonical_issues and len(p.canonical_issues) > 0:
            canonical_issues.append({
                "url": p.url, "page_id": p.id,
                "canonical_url": p.canonical_url,
                "issues": p.canonical_issues,
            })

    # --- Noindex / Nofollow (exclude redirects) ---
    noindex_pages = [{"url": p.url, "page_id": p.id} for p in content_pages if p.is_noindex]
    nofollow_pages = [{"url": p.url, "page_id": p.id, "nofollow_internal": p.nofollow_internal_links} for p in content_pages if p.is_nofollow_meta]

    # --- Images missing alt (exclude redirects) ---
    pages_missing_alt = []
    total_images_missing = 0
    pages_empty_alt = []
    total_images_empty_alt = 0
    for p in content_pages:
        if p.images_without_alt and p.images_without_alt > 0:
            sample_img = (p.images_without_alt_urls or [None])[0]
            pages_missing_alt.append({
                "url": p.url, "page_id": p.id,
                "missing_count": p.images_without_alt,
                "total_images": p.total_images,
                "sample_image_url": sample_img,
            })
            total_images_missing += p.images_without_alt
        if p.images_with_empty_alt and p.images_with_empty_alt > 0:
            sample_img = (p.images_with_empty_alt_urls or [None])[0] if hasattr(p, 'images_with_empty_alt_urls') and p.images_with_empty_alt_urls else None
            pages_empty_alt.append({
                "url": p.url, "page_id": p.id,
                "empty_count": p.images_with_empty_alt,
                "total_images": p.total_images,
                "sample_image_url": sample_img,
            })
            total_images_empty_alt += p.images_with_empty_alt

    # --- Hreflang issues (exclude redirects) ---
    hreflang_issues = []
    for p in content_pages:
        if p.hreflang_issues and len(p.hreflang_issues) > 0:
            hreflang_issues.append({
                "url": p.url, "page_id": p.id,
                "issues": p.hreflang_issues,
                "entries": p.hreflang_entries,
            })

    # --- Content issues (exclude redirects) ---
    thin_content = [{"url": p.url, "page_id": p.id, "word_count": p.word_count} for p in content_pages if p.word_count and p.word_count < 300]
    low_ratio = [{"url": p.url, "page_id": p.id, "ratio": p.code_to_text_ratio} for p in content_pages if p.code_to_text_ratio is not None and p.code_to_text_ratio < 10]
    placeholder_pages = [{"url": p.url, "page_id": p.id, "content": p.placeholder_content} for p in content_pages if p.has_placeholders]

    # --- Structure (exclude redirects) ---
    missing_title = sum(1 for p in content_pages if not p.title)
    missing_meta = sum(1 for p in content_pages if not p.meta_description)
    missing_h1 = sum(1 for p in content_pages if p.h1_count == 0)
    missing_viewport = sum(1 for p in content_pages if not p.has_viewport_meta)

    # --- Performance (all pages) ---
    avg_resp = sum(p.response_time or 0 for p in pages) / total
    slow_pages = [{"url": p.url, "page_id": p.id, "response_time": p.response_time} for p in pages if p.response_time and p.response_time > 3]

    # --- Schema (exclude redirects) ---
    no_schema = sum(1 for p in content_pages if not p.has_schema_markup)

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


# ─── PDF Export ────────────────────────────────────────────
@router.get("/crawls/{crawl_id}/export/pdf")
async def export_crawl_pdf(crawl_id: int, db: AsyncSession = Depends(get_db)):
    """Generate a comprehensive PDF report for a crawl."""
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether
    )
    from sqlalchemy.orm import selectinload

    crawl_result = await db.execute(
        select(Crawl).options(selectinload(Crawl.project)).where(Crawl.id == crawl_id)
    )
    crawl = crawl_result.scalar_one_or_none()
    if not crawl:
        raise HTTPException(status_code=404, detail="Crawl not found")

    result = await db.execute(select(Page).where(Page.crawl_id == crawl_id))
    pages = result.scalars().all()
    if not pages:
        raise HTTPException(status_code=404, detail="No pages found")

    total = len(pages)
    REDIRECT_CODES = {301, 302, 303, 307, 308}
    content_pages = [p for p in pages if (p.status_code or 0) not in REDIRECT_CODES]
    content_total = len(content_pages) or 1
    avg_score = round(sum(p.score or 0 for p in content_pages) / content_total, 1)
    critical = sum(1 for p in content_pages for i in (p.issues or []) if i.get("severity") == "critical")
    warnings_count = sum(1 for p in content_pages for i in (p.issues or []) if i.get("severity") == "warning")
    info_count = sum(1 for p in content_pages for i in (p.issues or []) if i.get("severity") == "info")

    # Collect all issue data
    missing_title_pages = [p for p in content_pages if not p.title]
    missing_meta_pages = [p for p in content_pages if not p.meta_description]
    missing_h1_pages = [p for p in content_pages if p.h1_count == 0]
    missing_viewport_pages = [p for p in content_pages if not p.has_viewport_meta]
    no_schema_pages = [p for p in content_pages if not p.has_schema_markup]

    title_groups = defaultdict(list)
    for p in content_pages:
        if p.title:
            title_groups[p.title].append(p)
    dup_titles = {t: pgs for t, pgs in title_groups.items() if len(pgs) > 1}

    meta_groups = defaultdict(list)
    for p in content_pages:
        if p.meta_description:
            meta_groups[p.meta_description].append(p)
    dup_metas = {m: pgs for m, pgs in meta_groups.items() if len(pgs) > 1}

    canon_issues = [p for p in content_pages if p.canonical_issues and len(p.canonical_issues) > 0]
    noindex_pages = [p for p in content_pages if p.is_noindex]
    nofollow_pages = [p for p in content_pages if p.is_nofollow_meta]
    hreflang_issue_pages = [p for p in content_pages if p.hreflang_issues and len(p.hreflang_issues) > 0]
    img_missing_alt = [p for p in content_pages if p.images_without_alt and p.images_without_alt > 0]
    img_empty_alt = [p for p in content_pages if p.images_with_empty_alt and p.images_with_empty_alt > 0]
    thin_pages = [p for p in content_pages if p.word_count and p.word_count < 300]
    low_ratio_pages = [p for p in content_pages if p.code_to_text_ratio is not None and p.code_to_text_ratio < 10]
    placeholder_pgs = [p for p in content_pages if p.has_placeholders]
    slow_pages = [p for p in pages if p.response_time and p.response_time > 3]
    redirect_pages = [p for p in pages if (p.status_code or 0) in REDIRECT_CODES]

    # Build PDF
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=20*mm, bottomMargin=15*mm, leftMargin=15*mm, rightMargin=15*mm)
    styles = getSampleStyleSheet()

    # Custom styles
    accent = colors.HexColor("#6c5ce7")
    dark = colors.HexColor("#2d3436")
    light_bg = colors.HexColor("#f8f9fa")
    red = colors.HexColor("#ff6b6b")
    green_c = colors.HexColor("#00b894")
    blue_c = colors.HexColor("#0984e3")
    orange_c = colors.HexColor("#e17055")

    styles.add(ParagraphStyle(name="SmallBody", parent=styles["Normal"], fontSize=7, leading=9))
    styles.add(ParagraphStyle(name="TinyBody", parent=styles["Normal"], fontSize=6.5, leading=8))
    styles.add(ParagraphStyle(name="SectionHead", parent=styles["Heading2"], fontSize=13, spaceAfter=6, spaceBefore=14, textColor=accent))
    styles.add(ParagraphStyle(name="SubHead", parent=styles["Heading3"], fontSize=10, spaceAfter=4, spaceBefore=8))
    styles.add(ParagraphStyle(name="ChapterIntro", parent=styles["Normal"], fontSize=9, leading=12, textColor=colors.HexColor("#555555"), spaceAfter=8))

    header_style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), accent),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("FONTSIZE", (0, 1), (-1, -1), 7),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#dddddd")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, light_bg]),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ])

    dark_header_style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), dark),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, 0), 8),
        ("FONTSIZE", (0, 1), (-1, -1), 7),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#dddddd")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, light_bg]),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ])

    def p(text, style="SmallBody"):
        return Paragraph(str(text), styles[style])

    def url_p(url, max_len=65):
        t = url[:max_len] + ("..." if len(url) > max_len else "")
        return Paragraph(t, styles["SmallBody"])

    story = []

    # ═══════════════ COVER / TITLE ═══════════════
    story.append(Spacer(1, 30))
    story.append(Paragraph("SEO Crawl Report", styles["Title"]))
    site_url = crawl.project.url if crawl.project else "N/A"
    site_name = crawl.project.name if crawl.project else "N/A"
    story.append(Paragraph(f"<b>{site_name}</b> — {site_url}", styles["Normal"]))
    story.append(Paragraph(f"Report generated: {crawl.completed_at or crawl.created_at}", styles["Normal"]))
    story.append(Spacer(1, 20))

    # ═══════════════ EXECUTIVE SUMMARY ═══════════════
    story.append(Paragraph("Executive Summary", styles["SectionHead"]))
    score_color = green_c if avg_score >= 70 else (orange_c if avg_score >= 40 else red)
    summary_text = f"The crawl analyzed <b>{total}</b> pages. "
    summary_text += f"The average SEO score is <b>{avg_score}/100</b>. "
    summary_text += f"Found <b>{critical}</b> critical issues, <b>{warnings_count}</b> warnings, and <b>{info_count}</b> informational items."
    if redirect_pages:
        summary_text += f" {len(redirect_pages)} pages return redirect status codes and are excluded from content checks."
    story.append(Paragraph(summary_text, styles["ChapterIntro"]))

    # Scorecards table
    sc_data = [
        ["Pages Crawled", "Avg Score", "Critical", "Warnings", "Info", "Redirects"],
        [str(total), str(avg_score), str(critical), str(warnings_count), str(info_count), str(len(redirect_pages))],
    ]
    t = Table(sc_data, colWidths=[80, 70, 70, 70, 70, 70])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), accent),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
        ("BACKGROUND", (0, 1), (-1, -1), light_bg),
        ("FONTSIZE", (0, 1), (-1, 1), 12),
        ("TEXTCOLOR", (1, 1), (1, 1), score_color),
        ("TEXTCOLOR", (2, 1), (2, 1), red),
        ("TEXTCOLOR", (3, 1), (3, 1), orange_c),
    ]))
    story.append(t)
    story.append(Spacer(1, 8))

    # Issue summary table
    story.append(Paragraph("Issue Breakdown", styles["SubHead"]))
    issue_summary = [["Category", "Count", "Severity"]]
    issue_rows = [
        ("Missing Title", len(missing_title_pages), "Critical"),
        ("Missing Meta Description", len(missing_meta_pages), "Critical"),
        ("Missing H1", len(missing_h1_pages), "Critical"),
        ("Missing Viewport", len(missing_viewport_pages), "Critical"),
        ("Placeholder Content", len(placeholder_pgs), "Critical"),
        ("Duplicate Titles", len(dup_titles), "Warning"),
        ("Duplicate Meta Descriptions", len(dup_metas), "Warning"),
        ("Canonical Issues", len(canon_issues), "Warning"),
        ("Noindex Pages", len(noindex_pages), "Warning"),
        ("Images Missing Alt", len(img_missing_alt), "Warning"),
        ("Images Empty Alt", len(img_empty_alt), "Warning"),
        ("Thin Content (<300 words)", len(thin_pages), "Warning"),
        ("Low Text/HTML Ratio", len(low_ratio_pages), "Warning"),
        ("Slow Pages (>3s)", len(slow_pages), "Warning"),
        ("Hreflang Issues", len(hreflang_issue_pages), "Warning"),
        ("No Schema Markup", len(no_schema_pages), "Info"),
    ]
    for name, count, sev in issue_rows:
        if count > 0:
            issue_summary.append([name, str(count), sev])
    if len(issue_summary) > 1:
        t = Table(issue_summary, colWidths=[250, 60, 80])
        t.setStyle(dark_header_style)
        story.append(t)

    # ═══════════════ CHAPTER 1: ALL URLS ═══════════════
    story.append(PageBreak())
    story.append(Paragraph("Chapter 1: All URLs Overview", styles["SectionHead"]))
    story.append(Paragraph(f"Complete listing of all {total} crawled pages with key SEO metrics.", styles["ChapterIntro"]))

    url_data = [["URL", "Status", "Score", "Title Len", "Meta Len", "H1s", "Words", "Imgs", "No Alt", "Time"]]
    for pg in pages:
        url_data.append([
            url_p(pg.url, 55),
            str(pg.status_code or "?"),
            str(pg.score or "?"),
            str(pg.title_length or 0),
            str(pg.meta_description_length or 0),
            str(pg.h1_count or 0),
            str(pg.word_count or 0),
            str(pg.total_images or 0),
            str(pg.images_without_alt or 0),
            f"{pg.response_time:.2f}s" if pg.response_time else "?",
        ])
    t = Table(url_data, colWidths=[155, 32, 32, 36, 36, 28, 36, 32, 32, 36])
    style_cmds = list(header_style.getCommands())
    for i, pg in enumerate(pages, 1):
        sc = pg.status_code or 0
        if sc >= 500: style_cmds.append(("TEXTCOLOR", (1, i), (1, i), red))
        elif sc >= 400: style_cmds.append(("TEXTCOLOR", (1, i), (1, i), orange_c))
        elif sc >= 300: style_cmds.append(("TEXTCOLOR", (1, i), (1, i), blue_c))
        elif sc >= 200: style_cmds.append(("TEXTCOLOR", (1, i), (1, i), green_c))
        # Score colors
        s_val = pg.score or 0
        if s_val >= 70: style_cmds.append(("TEXTCOLOR", (2, i), (2, i), green_c))
        elif s_val >= 40: style_cmds.append(("TEXTCOLOR", (2, i), (2, i), orange_c))
        else: style_cmds.append(("TEXTCOLOR", (2, i), (2, i), red))
    t.setStyle(TableStyle(style_cmds))
    story.append(t)

    # ═══════════════ CHAPTER 2: DUPLICATE TITLES ═══════════════
    if dup_titles:
        story.append(PageBreak())
        story.append(Paragraph("Chapter 2: Duplicate Titles", styles["SectionHead"]))
        story.append(Paragraph(f"{len(dup_titles)} groups of pages share identical title tags. Each page should have a unique, descriptive title.", styles["ChapterIntro"]))
        for title_val, pgs in dup_titles.items():
            story.append(Paragraph(f'<b>"{title_val[:80]}{"..." if len(title_val) > 80 else ""}"</b> — shared by {len(pgs)} pages:', styles["SmallBody"]))
            for pg in pgs:
                story.append(Paragraph(f"  • {pg.url}", styles["TinyBody"]))
            story.append(Spacer(1, 4))

    # ═══════════════ CHAPTER 3: DUPLICATE META DESCRIPTIONS ═══════════════
    if dup_metas:
        story.append(PageBreak())
        ch = 3 if dup_titles else 2
        story.append(Paragraph(f"Chapter {ch}: Duplicate Meta Descriptions", styles["SectionHead"]))
        story.append(Paragraph(f"{len(dup_metas)} groups of pages share identical meta descriptions. Each page should have a unique meta description.", styles["ChapterIntro"]))
        for meta_val, pgs in dup_metas.items():
            story.append(Paragraph(f'<b>"{meta_val[:80]}{"..." if len(meta_val) > 80 else ""}"</b> — shared by {len(pgs)} pages:', styles["SmallBody"]))
            for pg in pgs:
                story.append(Paragraph(f"  • {pg.url}", styles["TinyBody"]))
            story.append(Spacer(1, 4))

    # ═══════════════ CHAPTER: STRUCTURE ISSUES ═══════════════
    ch_num = 2 + bool(dup_titles) + bool(dup_metas)
    has_structure = missing_title_pages or missing_meta_pages or missing_h1_pages or missing_viewport_pages
    if has_structure:
        story.append(PageBreak())
        story.append(Paragraph(f"Chapter {ch_num}: Structure Issues", styles["SectionHead"]))
        story.append(Paragraph("Pages missing essential HTML elements that search engines rely on.", styles["ChapterIntro"]))
        ch_num += 1

        if missing_title_pages:
            story.append(Paragraph(f"Missing Title Tag ({len(missing_title_pages)} pages)", styles["SubHead"]))
            d = [["URL"]]
            for pg in missing_title_pages:
                d.append([url_p(pg.url)])
            t = Table(d, colWidths=[460])
            t.setStyle(dark_header_style)
            story.append(t)
            story.append(Spacer(1, 6))

        if missing_meta_pages:
            story.append(Paragraph(f"Missing Meta Description ({len(missing_meta_pages)} pages)", styles["SubHead"]))
            d = [["URL"]]
            for pg in missing_meta_pages:
                d.append([url_p(pg.url)])
            t = Table(d, colWidths=[460])
            t.setStyle(dark_header_style)
            story.append(t)
            story.append(Spacer(1, 6))

        if missing_h1_pages:
            story.append(Paragraph(f"Missing H1 ({len(missing_h1_pages)} pages)", styles["SubHead"]))
            d = [["URL"]]
            for pg in missing_h1_pages:
                d.append([url_p(pg.url)])
            t = Table(d, colWidths=[460])
            t.setStyle(dark_header_style)
            story.append(t)
            story.append(Spacer(1, 6))

        if missing_viewport_pages:
            story.append(Paragraph(f"Missing Viewport ({len(missing_viewport_pages)} pages)", styles["SubHead"]))
            d = [["URL"]]
            for pg in missing_viewport_pages:
                d.append([url_p(pg.url)])
            t = Table(d, colWidths=[460])
            t.setStyle(dark_header_style)
            story.append(t)

    # ═══════════════ CHAPTER: CANONICAL & HREFLANG ═══════════════
    if canon_issues or hreflang_issue_pages:
        story.append(PageBreak())
        story.append(Paragraph(f"Chapter {ch_num}: Canonical & Hreflang Issues", styles["SectionHead"]))
        story.append(Paragraph("Issues with canonical tags and international targeting signals.", styles["ChapterIntro"]))
        ch_num += 1

        if canon_issues:
            story.append(Paragraph(f"Canonical Issues ({len(canon_issues)} pages)", styles["SubHead"]))
            d = [["Page URL", "Canonical URL", "Issues"]]
            for pg in canon_issues:
                d.append([
                    url_p(pg.url, 50),
                    url_p(pg.canonical_url or "none", 45),
                    p(", ".join(pg.canonical_issues or [])[:80]),
                ])
            t = Table(d, colWidths=[180, 150, 130])
            t.setStyle(dark_header_style)
            story.append(t)
            story.append(Spacer(1, 8))

        if hreflang_issue_pages:
            story.append(Paragraph(f"Hreflang Issues ({len(hreflang_issue_pages)} pages)", styles["SubHead"]))
            d = [["Page URL", "Issues"]]
            for pg in hreflang_issue_pages:
                d.append([
                    url_p(pg.url, 55),
                    p("; ".join(pg.hreflang_issues or [])[:120]),
                ])
            t = Table(d, colWidths=[200, 260])
            t.setStyle(dark_header_style)
            story.append(t)

    # ═══════════════ CHAPTER: INDEXABILITY ═══════════════
    if noindex_pages or nofollow_pages:
        story.append(PageBreak())
        story.append(Paragraph(f"Chapter {ch_num}: Indexability Issues", styles["SectionHead"]))
        story.append(Paragraph("Pages that are blocked from indexing or have nofollow directives.", styles["ChapterIntro"]))
        ch_num += 1

        if noindex_pages:
            story.append(Paragraph(f"Noindex Pages ({len(noindex_pages)})", styles["SubHead"]))
            d = [["URL"]]
            for pg in noindex_pages:
                d.append([url_p(pg.url)])
            t = Table(d, colWidths=[460])
            t.setStyle(dark_header_style)
            story.append(t)
            story.append(Spacer(1, 6))

        if nofollow_pages:
            story.append(Paragraph(f"Nofollow Meta Pages ({len(nofollow_pages)})", styles["SubHead"]))
            d = [["URL"]]
            for pg in nofollow_pages:
                d.append([url_p(pg.url)])
            t = Table(d, colWidths=[460])
            t.setStyle(dark_header_style)
            story.append(t)

    # ═══════════════ CHAPTER: IMAGE ALT TEXT ═══════════════
    if img_missing_alt or img_empty_alt:
        story.append(PageBreak())
        story.append(Paragraph(f"Chapter {ch_num}: Image Alt Text Issues", styles["SectionHead"]))
        total_miss = sum(pg.images_without_alt for pg in img_missing_alt)
        total_empty = sum(pg.images_with_empty_alt or 0 for pg in img_empty_alt)
        story.append(Paragraph(f"{total_miss} images missing alt attributes and {total_empty} with empty alt text. Alt text is critical for accessibility and image SEO.", styles["ChapterIntro"]))
        ch_num += 1

        if img_missing_alt:
            story.append(Paragraph(f"Missing Alt Attribute ({total_miss} images on {len(img_missing_alt)} pages)", styles["SubHead"]))
            d = [["Page URL", "Count", "Sample Image URL"]]
            for pg in img_missing_alt:
                sample = (pg.images_without_alt_urls or [None])[0]
                d.append([
                    url_p(pg.url, 50),
                    str(pg.images_without_alt),
                    p((sample or "—")[:55]),
                ])
            t = Table(d, colWidths=[210, 35, 215])
            t.setStyle(dark_header_style)
            story.append(t)
            story.append(Spacer(1, 6))

        if img_empty_alt:
            story.append(Paragraph(f"Empty Alt Text ({total_empty} images on {len(img_empty_alt)} pages)", styles["SubHead"]))
            d = [["Page URL", "Count", "Sample Image URL"]]
            for pg in img_empty_alt:
                sample = (pg.images_with_empty_alt_urls or [None])[0] if hasattr(pg, 'images_with_empty_alt_urls') and pg.images_with_empty_alt_urls else None
                d.append([
                    url_p(pg.url, 50),
                    str(pg.images_with_empty_alt or 0),
                    p((sample or "—")[:55]),
                ])
            t = Table(d, colWidths=[210, 35, 215])
            t.setStyle(dark_header_style)
            story.append(t)

    # ═══════════════ CHAPTER: CONTENT ISSUES ═══════════════
    if thin_pages or low_ratio_pages or placeholder_pgs:
        story.append(PageBreak())
        story.append(Paragraph(f"Chapter {ch_num}: Content Issues", styles["SectionHead"]))
        story.append(Paragraph("Pages with thin content, poor text-to-HTML ratios, or placeholder text.", styles["ChapterIntro"]))
        ch_num += 1

        if thin_pages:
            story.append(Paragraph(f"Thin Content — under 300 words ({len(thin_pages)} pages)", styles["SubHead"]))
            d = [["URL", "Word Count"]]
            for pg in thin_pages:
                d.append([url_p(pg.url, 60), str(pg.word_count or 0)])
            t = Table(d, colWidths=[400, 60])
            t.setStyle(dark_header_style)
            story.append(t)
            story.append(Spacer(1, 6))

        if low_ratio_pages:
            story.append(Paragraph(f"Low Text-to-HTML Ratio — under 10% ({len(low_ratio_pages)} pages)", styles["SubHead"]))
            d = [["URL", "Ratio"]]
            for pg in low_ratio_pages:
                d.append([url_p(pg.url, 60), f"{pg.code_to_text_ratio}%"])
            t = Table(d, colWidths=[400, 60])
            t.setStyle(dark_header_style)
            story.append(t)
            story.append(Spacer(1, 6))

        if placeholder_pgs:
            story.append(Paragraph(f"Placeholder / Lorem Ipsum Content ({len(placeholder_pgs)} pages)", styles["SubHead"]))
            d = [["URL", "Found Text"]]
            for pg in placeholder_pgs:
                matches = ", ".join([(c.get("match") or "")[:30] for c in (pg.placeholder_content or [])[:3]])
                d.append([url_p(pg.url, 55), p(matches[:80])])
            t = Table(d, colWidths=[250, 210])
            t.setStyle(dark_header_style)
            story.append(t)

    # ═══════════════ CHAPTER: PERFORMANCE ═══════════════
    if slow_pages:
        story.append(PageBreak())
        story.append(Paragraph(f"Chapter {ch_num}: Performance Issues", styles["SectionHead"]))
        story.append(Paragraph(f"{len(slow_pages)} pages take more than 3 seconds to respond. Slow pages hurt user experience and search rankings.", styles["ChapterIntro"]))
        ch_num += 1

        d = [["URL", "Response Time"]]
        for pg in slow_pages:
            d.append([url_p(pg.url, 60), f"{pg.response_time:.2f}s"])
        t = Table(d, colWidths=[400, 60])
        t.setStyle(dark_header_style)
        story.append(t)

    # ═══════════════ CHAPTER: REDIRECTS ═══════════════
    if redirect_pages:
        story.append(PageBreak())
        story.append(Paragraph(f"Chapter {ch_num}: Redirects", styles["SectionHead"]))
        story.append(Paragraph(f"{len(redirect_pages)} pages return redirect status codes (301, 302, etc). These are excluded from content-level SEO checks.", styles["ChapterIntro"]))
        ch_num += 1

        d = [["URL", "Status Code"]]
        for pg in redirect_pages:
            d.append([url_p(pg.url, 65), str(pg.status_code)])
        t = Table(d, colWidths=[420, 40])
        t.setStyle(dark_header_style)
        story.append(t)

    # ═══════════════ CHAPTER: ROBOTS & SITEMAPS ═══════════════
    story.append(PageBreak())
    story.append(Paragraph(f"Chapter {ch_num}: Robots.txt & Sitemaps", styles["SectionHead"]))
    robots_status = crawl.robots_txt_status or "unknown"
    sitemaps = crawl.sitemaps_found or []
    story.append(Paragraph(f"robots.txt status: <b>{robots_status}</b>. {len(sitemaps)} sitemap(s) detected.", styles["ChapterIntro"]))
    if sitemaps:
        d = [["Sitemap URL", "Type", "URLs Count"]]
        for sm in sitemaps:
            d.append([url_p(sm.get("url", "?"), 55), sm.get("type", "?"), str(sm.get("urls_count", 0))])
        t = Table(d, colWidths=[300, 80, 80])
        t.setStyle(dark_header_style)
        story.append(t)

    # Footer
    story.append(Spacer(1, 30))
    story.append(Paragraph("Generated by SEO Crawler Pro — ai.tudordaniel.ro", styles["SmallBody"]))

    doc.build(story)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=seo-report-crawl-{crawl_id}.pdf"},
    )
