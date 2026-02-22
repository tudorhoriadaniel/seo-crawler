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
    """Generate a PDF summary report for a crawl."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
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
    avg_score = round(sum(p.score or 0 for p in pages) / total, 1)
    critical = sum(1 for p in pages for i in (p.issues or []) if i.get("severity") == "critical")
    warnings = sum(1 for p in pages for i in (p.issues or []) if i.get("severity") == "warning")

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=20*mm, bottomMargin=15*mm, leftMargin=15*mm, rightMargin=15*mm)
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="SmallBody", parent=styles["Normal"], fontSize=8, leading=10))
    styles.add(ParagraphStyle(name="SectionHead", parent=styles["Heading2"], fontSize=12, spaceAfter=6, spaceBefore=12, textColor=colors.HexColor("#6c5ce7")))
    styles.add(ParagraphStyle(name="SubHead", parent=styles["Heading3"], fontSize=10, spaceAfter=4, spaceBefore=8))

    story = []

    # Title
    story.append(Paragraph("SEO Crawl Report", styles["Title"]))
    site_url = crawl.project.url if crawl.project else "N/A"
    story.append(Paragraph(f"Site: {site_url}", styles["Normal"]))
    story.append(Paragraph(f"Date: {crawl.completed_at or crawl.created_at}", styles["Normal"]))
    story.append(Spacer(1, 8))

    # Summary stats table
    summary_data = [
        ["Pages Crawled", "Avg Score", "Critical Issues", "Warnings"],
        [str(total), str(avg_score), str(critical), str(warnings)],
    ]
    t = Table(summary_data, colWidths=[120, 100, 120, 100])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#6c5ce7")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
        ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#f8f9fa")),
    ]))
    story.append(t)
    story.append(Spacer(1, 10))

    # Structure issues
    story.append(Paragraph("Structure Issues", styles["SectionHead"]))
    missing_title = sum(1 for p in pages if not p.title)
    missing_meta = sum(1 for p in pages if not p.meta_description)
    missing_h1 = sum(1 for p in pages if p.h1_count == 0)
    structure_data = [
        ["Issue", "Count"],
        ["Missing Title", str(missing_title)],
        ["Missing Meta Description", str(missing_meta)],
        ["Missing H1", str(missing_h1)],
        ["Missing Viewport", str(sum(1 for p in pages if not p.has_viewport_meta))],
        ["No Schema Markup", str(sum(1 for p in pages if not p.has_schema_markup))],
    ]
    t = Table(structure_data, colWidths=[300, 80])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2d3436")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 9),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f9fa")]),
    ]))
    story.append(t)
    story.append(Spacer(1, 8))

    # Images issues
    pages_with_missing = [(p.url, p.images_without_alt, (p.images_without_alt_urls or [None])[0]) for p in pages if p.images_without_alt and p.images_without_alt > 0]
    pages_with_empty = [(p.url, p.images_with_empty_alt, (p.images_with_empty_alt_urls or [None])[0] if hasattr(p, 'images_with_empty_alt_urls') and p.images_with_empty_alt_urls else None) for p in pages if p.images_with_empty_alt and p.images_with_empty_alt > 0]

    if pages_with_missing or pages_with_empty:
        story.append(Paragraph("Image Alt Text Issues", styles["SectionHead"]))
        if pages_with_missing:
            story.append(Paragraph(f"Images Missing Alt Attribute ({sum(c for _, c, _ in pages_with_missing)} images)", styles["SubHead"]))
            img_data = [["Page URL", "Count", "Sample Image URL"]]
            for url, cnt, sample in pages_with_missing[:30]:
                img_data.append([
                    Paragraph(url[:60] + ("..." if len(url) > 60 else ""), styles["SmallBody"]),
                    str(cnt),
                    Paragraph((sample or "—")[:50], styles["SmallBody"]),
                ])
            t = Table(img_data, colWidths=[220, 40, 200])
            t.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2d3436")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]))
            story.append(t)
            story.append(Spacer(1, 6))

        if pages_with_empty:
            story.append(Paragraph(f"Images With Empty Alt ({sum(c for _, c, _ in pages_with_empty)} images)", styles["SubHead"]))
            img_data = [["Page URL", "Count", "Sample Image URL"]]
            for url, cnt, sample in pages_with_empty[:30]:
                img_data.append([
                    Paragraph(url[:60] + ("..." if len(url) > 60 else ""), styles["SmallBody"]),
                    str(cnt),
                    Paragraph((sample or "—")[:50], styles["SmallBody"]),
                ])
            t = Table(img_data, colWidths=[220, 40, 200])
            t.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2d3436")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]))
            story.append(t)

    # All URLs table
    story.append(PageBreak())
    story.append(Paragraph("All URLs Overview", styles["SectionHead"]))

    url_data = [["URL", "Status", "Score", "Title Len", "H1s", "Words", "Images", "No Alt", "Time"]]
    for p in pages:
        url_data.append([
            Paragraph(p.url[:55] + ("..." if len(p.url) > 55 else ""), styles["SmallBody"]),
            str(p.status_code or "?"),
            str(p.score or "?"),
            str(p.title_length or 0),
            str(p.h1_count or 0),
            str(p.word_count or 0),
            str(p.total_images or 0),
            str(p.images_without_alt or 0),
            f"{p.response_time:.2f}s" if p.response_time else "?",
        ])

    t = Table(url_data, colWidths=[170, 35, 35, 40, 30, 40, 40, 35, 40])
    style_cmds = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#6c5ce7")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTSIZE", (0, 0), (-1, -1), 7),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8f9fa")]),
    ]
    # Color-code status codes
    for i, p in enumerate(pages, 1):
        sc = p.status_code or 0
        if sc >= 500:
            style_cmds.append(("TEXTCOLOR", (1, i), (1, i), colors.HexColor("#ff6b6b")))
        elif sc >= 400:
            style_cmds.append(("TEXTCOLOR", (1, i), (1, i), colors.HexColor("#e17055")))
        elif sc >= 300:
            style_cmds.append(("TEXTCOLOR", (1, i), (1, i), colors.HexColor("#0984e3")))
        elif sc >= 200:
            style_cmds.append(("TEXTCOLOR", (1, i), (1, i), colors.HexColor("#00b894")))

    t.setStyle(TableStyle(style_cmds))
    story.append(t)

    # Canonical / Hreflang issues
    canon_issues = [(p.url, p.canonical_url, p.canonical_issues) for p in pages if p.canonical_issues and len(p.canonical_issues) > 0]
    hreflang_issues_list = [(p.url, p.hreflang_issues) for p in pages if p.hreflang_issues and len(p.hreflang_issues) > 0]

    if canon_issues or hreflang_issues_list:
        story.append(PageBreak())
        story.append(Paragraph("Canonical & Hreflang Issues", styles["SectionHead"]))

        if canon_issues:
            story.append(Paragraph(f"Canonical Issues ({len(canon_issues)} pages)", styles["SubHead"]))
            c_data = [["Page URL", "Canonical", "Issues"]]
            for url, canon, issues in canon_issues[:30]:
                c_data.append([
                    Paragraph(url[:50], styles["SmallBody"]),
                    Paragraph((canon or "none")[:50], styles["SmallBody"]),
                    Paragraph(", ".join(issues or [])[:60], styles["SmallBody"]),
                ])
            t = Table(c_data, colWidths=[180, 150, 130])
            t.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2d3436")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]))
            story.append(t)
            story.append(Spacer(1, 8))

        if hreflang_issues_list:
            story.append(Paragraph(f"Hreflang Issues ({len(hreflang_issues_list)} pages)", styles["SubHead"]))
            h_data = [["Page URL", "Issues"]]
            for url, issues in hreflang_issues_list[:30]:
                h_data.append([
                    Paragraph(url[:55], styles["SmallBody"]),
                    Paragraph("; ".join(issues or [])[:100], styles["SmallBody"]),
                ])
            t = Table(h_data, colWidths=[200, 260])
            t.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#2d3436")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]))
            story.append(t)

    # Footer note
    story.append(Spacer(1, 20))
    story.append(Paragraph("Generated by SEO Crawler Pro — ai.tudordaniel.ro", styles["SmallBody"]))

    doc.build(story)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=seo-report-crawl-{crawl_id}.pdf"},
    )
