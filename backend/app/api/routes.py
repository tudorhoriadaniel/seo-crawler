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
from app.crawler.engine import CrawlEngine, active_crawls

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


# ─── Crawl Control (Pause / Stop / Resume) ──────────────────
@router.post("/crawls/{crawl_id}/pause")
async def pause_crawl(crawl_id: int, db: AsyncSession = Depends(get_db)):
    crawl = await db.get(Crawl, crawl_id)
    if not crawl:
        raise HTTPException(status_code=404, detail="Crawl not found")
    if crawl.status != "running":
        raise HTTPException(status_code=400, detail=f"Cannot pause crawl with status '{crawl.status}'")

    engine = active_crawls.get(crawl_id)
    if not engine:
        raise HTTPException(status_code=400, detail="Crawl engine not found in memory")

    engine.pause()
    crawl.status = "paused"
    await db.commit()
    return {"message": "Crawl paused", "status": "paused"}


@router.post("/crawls/{crawl_id}/resume")
async def resume_crawl(crawl_id: int, background_tasks: BackgroundTasks, db: AsyncSession = Depends(get_db)):
    crawl = await db.get(Crawl, crawl_id)
    if not crawl:
        raise HTTPException(status_code=404, detail="Crawl not found")

    if crawl.status == "paused":
        # Resume in-memory engine
        engine = active_crawls.get(crawl_id)
        if not engine:
            raise HTTPException(status_code=400, detail="Crawl engine not found in memory")
        engine.resume()
        crawl.status = "running"
        await db.commit()
        return {"message": "Crawl resumed", "status": "running"}

    elif crawl.status == "stopped":
        # Re-start engine, loading already-crawled URLs from DB
        project = await db.get(Project, crawl.project_id)
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        async def _resume_crawl(cid: int, url: str):
            engine = CrawlEngine(cid, url)
            await engine.run(resume_from_stopped=True)

        background_tasks.add_task(_resume_crawl, crawl.id, project.url)
        return {"message": "Crawl resuming from stopped state", "status": "running"}

    else:
        raise HTTPException(status_code=400, detail=f"Cannot resume crawl with status '{crawl.status}'")


@router.post("/crawls/{crawl_id}/stop")
async def stop_crawl(crawl_id: int, db: AsyncSession = Depends(get_db)):
    crawl = await db.get(Crawl, crawl_id)
    if not crawl:
        raise HTTPException(status_code=404, detail="Crawl not found")
    if crawl.status not in ("running", "paused"):
        raise HTTPException(status_code=400, detail=f"Cannot stop crawl with status '{crawl.status}'")

    engine = active_crawls.get(crawl_id)
    if engine:
        engine.stop()
        # The engine will set status to "stopped" when workers finish
    else:
        # Engine not in memory (server restarted?), just update DB
        crawl.status = "stopped"
        crawl.completed_at = __import__("datetime").datetime.utcnow()
        await db.commit()

    return {"message": "Crawl stopping", "status": "stopped"}


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
    missing_canonical = sum(1 for p in content_pages if p.canonical_issues and "missing" in p.canonical_issues)

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
        "pages_missing_canonical": missing_canonical,
        "issue_groups": issue_groups,
    }


# ─── Excel Export ──────────────────────────────────────────
@router.get("/crawls/{crawl_id}/export/excel")
async def export_crawl_excel(crawl_id: int, db: AsyncSession = Depends(get_db)):
    """Generate an Excel report with separate sheets per issue type."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
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

    REDIRECT_CODES = {301, 302, 303, 307, 308}
    content_pages = [p for p in pages if (p.status_code or 0) not in REDIRECT_CODES]

    wb = Workbook()
    header_font = Font(bold=True, color="FFFFFF", size=10)
    header_fill = PatternFill(start_color="6C5CE7", end_color="6C5CE7", fill_type="solid")
    thin_border = Border(
        left=Side(style="thin", color="DDDDDD"),
        right=Side(style="thin", color="DDDDDD"),
        top=Side(style="thin", color="DDDDDD"),
        bottom=Side(style="thin", color="DDDDDD"),
    )

    def style_header(ws, cols):
        for c, val in enumerate(cols, 1):
            cell = ws.cell(row=1, column=c, value=val)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal="center")
            cell.border = thin_border

    def add_row(ws, row_num, values):
        for c, val in enumerate(values, 1):
            cell = ws.cell(row=row_num, column=c, value=val)
            cell.border = thin_border
            cell.alignment = Alignment(wrap_text=True)

    # ── Sheet 1: All URLs ──
    ws = wb.active
    ws.title = "All URLs"
    cols = ["URL", "Status Code", "Score", "Title", "Title Length", "Meta Desc Length",
            "H1 Count", "H2 Count", "Word Count", "Text/HTML %", "Internal Links",
            "External Links", "Total Images", "Missing Alt", "Empty Alt", "Response Time (s)",
            "Noindex", "Nofollow", "Has Schema", "Has Hreflang", "Has Viewport", "Issues Count"]
    style_header(ws, cols)
    for i, pg in enumerate(pages, 2):
        add_row(ws, i, [
            pg.url, pg.status_code, pg.score, pg.title, pg.title_length or 0,
            pg.meta_description_length or 0, pg.h1_count or 0, pg.h2_count or 0,
            pg.word_count or 0, pg.code_to_text_ratio,
            pg.internal_links or 0, pg.external_links or 0,
            pg.total_images or 0, pg.images_without_alt or 0, pg.images_with_empty_alt or 0,
            round(pg.response_time, 3) if pg.response_time else None,
            "Yes" if pg.is_noindex else "No", "Yes" if pg.is_nofollow_meta else "No",
            "Yes" if pg.has_schema_markup else "No", "Yes" if pg.has_hreflang else "No",
            "Yes" if pg.has_viewport_meta else "No", len(pg.issues) if pg.issues else 0,
        ])
    ws.column_dimensions["A"].width = 60
    for col_letter in "BCDEFGHIJKLMNOPQRSTUV":
        ws.column_dimensions[col_letter].width = 14

    # ── Issue sheets helper ──
    def add_issue_sheet(title, pages_list, extra_cols=None, extra_fn=None):
        if not pages_list:
            return
        safe_title = title[:31]  # Excel sheet name max 31 chars
        ws = wb.create_sheet(title=safe_title)
        cols = ["URL"]
        if extra_cols:
            cols.extend(extra_cols)
        style_header(ws, cols)
        for i, pg in enumerate(pages_list, 2):
            values = [pg.url if hasattr(pg, 'url') else pg.get('url', '')]
            if extra_fn:
                values.extend(extra_fn(pg))
            add_row(ws, i, values)
        ws.column_dimensions["A"].width = 60
        for c in range(2, len(cols) + 1):
            ws.column_dimensions[chr(64 + c)].width = 20

    # ── Missing Title ──
    add_issue_sheet("Missing Title", [p for p in content_pages if not p.title])

    # ── Missing Meta Description ──
    add_issue_sheet("Missing Meta Desc", [p for p in content_pages if not p.meta_description])

    # ── Missing H1 ──
    add_issue_sheet("Missing H1", [p for p in content_pages if p.h1_count == 0])

    # ── Missing Viewport ──
    add_issue_sheet("Missing Viewport", [p for p in content_pages if not p.has_viewport_meta])

    # ── Duplicate Titles ──
    title_groups = defaultdict(list)
    for p in content_pages:
        if p.title:
            title_groups[p.title].append(p)
    dup_title_pages = []
    for title_val, pgs in title_groups.items():
        if len(pgs) > 1:
            for pg in pgs:
                dup_title_pages.append({"url": pg.url, "title": title_val, "group_size": len(pgs)})
    if dup_title_pages:
        ws = wb.create_sheet(title="Duplicate Titles")
        style_header(ws, ["URL", "Title", "Duplicated With (count)"])
        for i, d in enumerate(dup_title_pages, 2):
            add_row(ws, i, [d["url"], d["title"], d["group_size"]])
        ws.column_dimensions["A"].width = 60
        ws.column_dimensions["B"].width = 50
        ws.column_dimensions["C"].width = 20

    # ── Duplicate Meta Descriptions ──
    meta_groups = defaultdict(list)
    for p in content_pages:
        if p.meta_description:
            meta_groups[p.meta_description].append(p)
    dup_meta_pages = []
    for meta_val, pgs in meta_groups.items():
        if len(pgs) > 1:
            for pg in pgs:
                dup_meta_pages.append({"url": pg.url, "meta": meta_val[:100], "group_size": len(pgs)})
    if dup_meta_pages:
        ws = wb.create_sheet(title="Duplicate Meta Desc")
        style_header(ws, ["URL", "Meta Description", "Duplicated With (count)"])
        for i, d in enumerate(dup_meta_pages, 2):
            add_row(ws, i, [d["url"], d["meta"], d["group_size"]])
        ws.column_dimensions["A"].width = 60
        ws.column_dimensions["B"].width = 50
        ws.column_dimensions["C"].width = 20

    # ── Canonical Issues ──
    canon_pages = [p for p in content_pages if p.canonical_issues and len(p.canonical_issues) > 0]
    if canon_pages:
        ws = wb.create_sheet(title="Canonical Issues")
        style_header(ws, ["URL", "Canonical URL", "Issues"])
        for i, pg in enumerate(canon_pages, 2):
            add_row(ws, i, [pg.url, pg.canonical_url or "none", ", ".join(pg.canonical_issues or [])])
        ws.column_dimensions["A"].width = 60
        ws.column_dimensions["B"].width = 60
        ws.column_dimensions["C"].width = 40

    # ── Noindex ──
    add_issue_sheet("Noindex Pages", [p for p in content_pages if p.is_noindex])

    # ── Nofollow ──
    add_issue_sheet("Nofollow Pages", [p for p in content_pages if p.is_nofollow_meta])

    # ── Images Missing Alt ──
    img_miss = [p for p in content_pages if p.images_without_alt and p.images_without_alt > 0]
    add_issue_sheet("Images Missing Alt", img_miss,
                    extra_cols=["Missing Count", "Total Images", "Sample Image URL"],
                    extra_fn=lambda pg: [pg.images_without_alt, pg.total_images, (pg.images_without_alt_urls or [None])[0] or ""])

    # ── Images Empty Alt ──
    img_empty = [p for p in content_pages if p.images_with_empty_alt and p.images_with_empty_alt > 0]
    add_issue_sheet("Images Empty Alt", img_empty,
                    extra_cols=["Empty Alt Count", "Total Images"],
                    extra_fn=lambda pg: [pg.images_with_empty_alt, pg.total_images])

    # ── Hreflang Issues ──
    hreflang_pgs = [p for p in content_pages if p.hreflang_issues and len(p.hreflang_issues) > 0]
    add_issue_sheet("Hreflang Issues", hreflang_pgs,
                    extra_cols=["Issues"],
                    extra_fn=lambda pg: ["; ".join(pg.hreflang_issues or [])])

    # ── Thin Content ──
    thin_pgs = [p for p in content_pages if p.word_count and p.word_count < 300]
    add_issue_sheet("Thin Content", thin_pgs,
                    extra_cols=["Word Count"],
                    extra_fn=lambda pg: [pg.word_count or 0])

    # ── Low Text/HTML Ratio ──
    low_ratio = [p for p in content_pages if p.code_to_text_ratio is not None and p.code_to_text_ratio < 10]
    add_issue_sheet("Low Text Ratio", low_ratio,
                    extra_cols=["Text/HTML %"],
                    extra_fn=lambda pg: [pg.code_to_text_ratio])

    # ── Placeholder Content ──
    placeholder = [p for p in content_pages if p.has_placeholders]
    add_issue_sheet("Placeholder Content", placeholder,
                    extra_cols=["Found Text"],
                    extra_fn=lambda pg: [", ".join([(c.get("match") or "")[:30] for c in (pg.placeholder_content or [])[:3]])])

    # ── Slow Pages ──
    slow = [p for p in pages if p.response_time and p.response_time > 3]
    add_issue_sheet("Slow Pages", slow,
                    extra_cols=["Response Time (s)"],
                    extra_fn=lambda pg: [round(pg.response_time, 3) if pg.response_time else 0])

    # ── No Schema ──
    add_issue_sheet("No Schema Markup", [p for p in content_pages if not p.has_schema_markup])

    # ── Redirects ──
    redir = [p for p in pages if (p.status_code or 0) in REDIRECT_CODES]
    add_issue_sheet("Redirects", redir,
                    extra_cols=["Status Code", "Redirect Target"],
                    extra_fn=lambda pg: [pg.status_code, pg.redirect_target or ""])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=seo-report-crawl-{crawl_id}.xlsx"},
    )


# ─── PDF Export ────────────────────────────────────────────
@router.get("/crawls/{crawl_id}/export/pdf")
async def export_crawl_pdf(crawl_id: int, db: AsyncSession = Depends(get_db)):
    """Generate a PDF report with scorecards, pie charts, and 3 URL examples per issue."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether
    )
    from reportlab.graphics.shapes import Drawing, String, Wedge
    from reportlab.graphics import renderPDF
    import math
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

    # Collect issue data
    missing_title_pages = [p for p in content_pages if not p.title]
    missing_meta_pages = [p for p in content_pages if not p.meta_description]
    missing_h1_pages = [p for p in content_pages if p.h1_count == 0]
    missing_viewport_pages = [p for p in content_pages if not p.has_viewport_meta]
    no_schema_pages = [p for p in content_pages if not p.has_schema_markup]
    noindex_pages = [p for p in content_pages if p.is_noindex]
    nofollow_pages = [p for p in content_pages if p.is_nofollow_meta]
    canon_issues = [p for p in content_pages if p.canonical_issues and len(p.canonical_issues) > 0]
    hreflang_issue_pages = [p for p in content_pages if p.hreflang_issues and len(p.hreflang_issues) > 0]
    img_missing_alt = [p for p in content_pages if p.images_without_alt and p.images_without_alt > 0]
    img_empty_alt = [p for p in content_pages if p.images_with_empty_alt and p.images_with_empty_alt > 0]
    thin_pages = [p for p in content_pages if p.word_count and p.word_count < 300]
    low_ratio_pages = [p for p in content_pages if p.code_to_text_ratio is not None and p.code_to_text_ratio < 10]
    placeholder_pgs = [p for p in content_pages if p.has_placeholders]
    slow_pages = [p for p in pages if p.response_time and p.response_time > 3]
    redirect_pages = [p for p in pages if (p.status_code or 0) in REDIRECT_CODES]

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

    # Sitemap URLs count
    sitemaps = crawl.sitemaps_found or []
    sitemap_url_count = sum(sm.get("urls_count", 0) for sm in sitemaps)

    # Build PDF
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, topMargin=20*mm, bottomMargin=15*mm, leftMargin=15*mm, rightMargin=15*mm)
    styles = getSampleStyleSheet()

    accent = colors.HexColor("#6c5ce7")
    accent2 = colors.HexColor("#a29bfe")
    dark = colors.HexColor("#2d3436")
    light_bg = colors.HexColor("#f8f9fa")
    red = colors.HexColor("#ff6b6b")
    green_c = colors.HexColor("#00b894")
    blue_c = colors.HexColor("#0984e3")
    orange_c = colors.HexColor("#e17055")
    gray_c = colors.HexColor("#b2bec3")

    styles.add(ParagraphStyle(name="SmallBody", parent=styles["Normal"], fontSize=7, leading=9))
    styles.add(ParagraphStyle(name="TinyBody", parent=styles["Normal"], fontSize=6.5, leading=8))
    styles.add(ParagraphStyle(name="SectionHead", parent=styles["Heading2"], fontSize=13, spaceAfter=6, spaceBefore=14, textColor=accent))
    styles.add(ParagraphStyle(name="SubHead", parent=styles["Heading3"], fontSize=10, spaceAfter=4, spaceBefore=8))
    styles.add(ParagraphStyle(name="ChapterIntro", parent=styles["Normal"], fontSize=9, leading=12, textColor=colors.HexColor("#555555"), spaceAfter=8))

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

    # ── Pie chart helper ──
    def make_pie(data_items, width=200, height=150):
        """data_items: list of (label, value, color)"""
        d = Drawing(width, height)
        total_val = sum(v for _, v, _ in data_items) or 1
        cx, cy, r = width * 0.35, height * 0.5, min(width, height) * 0.3
        start = 90
        for label, val, clr in data_items:
            if val <= 0:
                continue
            extent = (val / total_val) * 360
            w = Wedge(cx, cy, r, start - extent, start, fillColor=clr, strokeColor=colors.white, strokeWidth=1)
            d.add(w)
            start -= extent
        # Legend
        lx = width * 0.7
        ly = height - 15
        for label, val, clr in data_items:
            if val <= 0:
                continue
            pct = round(val / total_val * 100, 1)
            w = Wedge(lx, ly + 3, 4, 0, 360, fillColor=clr, strokeColor=clr, strokeWidth=0)
            d.add(w)
            d.add(String(lx + 8, ly, f"{label}: {val} ({pct}%)", fontSize=7, fillColor=dark))
            ly -= 12
        return d

    story = []

    # ═══════════════ COVER ═══════════════
    story.append(Spacer(1, 30))
    story.append(Paragraph("SEO Crawl Report", styles["Title"]))
    site_url = crawl.project.url if crawl.project else "N/A"
    site_name = crawl.project.name if crawl.project else "N/A"
    story.append(Paragraph(f"<b>{site_name}</b> — {site_url}", styles["Normal"]))
    story.append(Paragraph(f"Report generated: {crawl.completed_at or crawl.created_at}", styles["Normal"]))
    story.append(Spacer(1, 20))

    # ═══════════════ SCORECARDS ═══════════════
    story.append(Paragraph("Executive Summary", styles["SectionHead"]))
    score_color = green_c if avg_score >= 70 else (orange_c if avg_score >= 40 else red)
    summary_text = f"The crawl analyzed <b>{total}</b> pages. Average SEO score: <b>{avg_score}/100</b>. "
    summary_text += f"Found <b>{critical}</b> critical, <b>{warnings_count}</b> warnings, <b>{info_count}</b> info items."
    story.append(Paragraph(summary_text, styles["ChapterIntro"]))

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
    story.append(Spacer(1, 14))

    # ═══════════════ PIE CHARTS ═══════════════
    story.append(Paragraph("Visual Overview", styles["SubHead"]))

    # Pie 1: Crawled vs Sitemap URLs
    pie1 = make_pie([
        ("Crawled", total, accent),
        ("In Sitemap", sitemap_url_count, green_c),
    ], width=240, height=120)

    # Pie 2: Issue severity
    pie2 = make_pie([
        ("Critical", critical, red),
        ("Warning", warnings_count, orange_c),
        ("Info", info_count, blue_c),
    ], width=240, height=120)

    pie_table = Table([[pie1, pie2]], colWidths=[250, 250])
    pie_table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP")]))
    story.append(pie_table)
    story.append(Spacer(1, 8))

    # Pie 3: Status codes
    status_groups = defaultdict(int)
    for pg in pages:
        sc = pg.status_code or 0
        if sc >= 200 and sc < 300:
            status_groups["2xx"] += 1
        elif sc >= 300 and sc < 400:
            status_groups["3xx"] += 1
        elif sc >= 400 and sc < 500:
            status_groups["4xx"] += 1
        elif sc >= 500:
            status_groups["5xx"] += 1
    pie3 = make_pie([
        ("2xx OK", status_groups.get("2xx", 0), green_c),
        ("3xx Redirect", status_groups.get("3xx", 0), accent2),
        ("4xx Client Error", status_groups.get("4xx", 0), orange_c),
        ("5xx Server Error", status_groups.get("5xx", 0), red),
    ], width=300, height=130)
    story.append(pie3)
    story.append(Spacer(1, 12))

    # ═══════════════ ISSUE BREAKDOWN TABLE ═══════════════
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
        ("Nofollow Pages", len(nofollow_pages), "Warning"),
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

    # ═══════════════ ISSUE CHAPTERS (3 URL examples each) ═══════════════
    ch_num = 1

    def issue_chapter(title, description, pages_list, cols, row_fn):
        nonlocal ch_num
        if not pages_list:
            return
        story.append(PageBreak())
        story.append(Paragraph(f"Chapter {ch_num}: {title}", styles["SectionHead"]))
        story.append(Paragraph(f"{len(pages_list)} affected. {description}", styles["ChapterIntro"]))
        ch_num += 1
        d = [cols]
        for pg in pages_list[:3]:
            d.append(row_fn(pg))
        if len(pages_list) > 3:
            remaining = len(pages_list) - 3
            d.append([p(f"... and {remaining} more. See Excel export for full list.")] + [""] * (len(cols) - 1))
        t = Table(d, colWidths=[int(460 / len(cols))] * len(cols))
        t.setStyle(dark_header_style)
        story.append(t)

    # Missing Title
    issue_chapter("Missing Title Tag",
                  "Every page needs a unique, descriptive title tag for search engines.",
                  missing_title_pages, ["URL"],
                  lambda pg: [url_p(pg.url)])

    # Missing Meta Description
    issue_chapter("Missing Meta Description",
                  "Meta descriptions help improve click-through rates from search results.",
                  missing_meta_pages, ["URL"],
                  lambda pg: [url_p(pg.url)])

    # Missing H1
    issue_chapter("Missing H1 Tag",
                  "The H1 tag is the main heading and important for SEO structure.",
                  missing_h1_pages, ["URL"],
                  lambda pg: [url_p(pg.url)])

    # Missing Viewport
    issue_chapter("Missing Viewport Meta",
                  "Viewport meta is required for proper mobile rendering.",
                  missing_viewport_pages, ["URL"],
                  lambda pg: [url_p(pg.url)])

    # Placeholder Content
    issue_chapter("Placeholder / Lorem Ipsum Content",
                  "Placeholder text indicates unfinished pages.",
                  placeholder_pgs, ["URL", "Found Text"],
                  lambda pg: [url_p(pg.url, 45), p(", ".join([(c.get("match") or "")[:25] for c in (pg.placeholder_content or [])[:2]]))])

    # Duplicate Titles
    if dup_titles:
        story.append(PageBreak())
        story.append(Paragraph(f"Chapter {ch_num}: Duplicate Titles", styles["SectionHead"]))
        story.append(Paragraph(f"{len(dup_titles)} groups share identical titles.", styles["ChapterIntro"]))
        ch_num += 1
        shown = 0
        for title_val, pgs in dup_titles.items():
            if shown >= 3:
                break
            story.append(Paragraph(f'<b>"{title_val[:70]}{"..." if len(title_val) > 70 else ""}"</b> — {len(pgs)} pages', styles["SmallBody"]))
            for pg in pgs[:3]:
                story.append(Paragraph(f"  • {pg.url[:80]}", styles["TinyBody"]))
            story.append(Spacer(1, 4))
            shown += 1
        if len(dup_titles) > 3:
            story.append(Paragraph(f"... and {len(dup_titles) - 3} more groups. See Excel export.", styles["SmallBody"]))

    # Duplicate Meta Descriptions
    if dup_metas:
        story.append(PageBreak())
        story.append(Paragraph(f"Chapter {ch_num}: Duplicate Meta Descriptions", styles["SectionHead"]))
        story.append(Paragraph(f"{len(dup_metas)} groups share identical meta descriptions.", styles["ChapterIntro"]))
        ch_num += 1
        shown = 0
        for meta_val, pgs in dup_metas.items():
            if shown >= 3:
                break
            story.append(Paragraph(f'<b>"{meta_val[:70]}{"..." if len(meta_val) > 70 else ""}"</b> — {len(pgs)} pages', styles["SmallBody"]))
            for pg in pgs[:3]:
                story.append(Paragraph(f"  • {pg.url[:80]}", styles["TinyBody"]))
            story.append(Spacer(1, 4))
            shown += 1
        if len(dup_metas) > 3:
            story.append(Paragraph(f"... and {len(dup_metas) - 3} more groups. See Excel export.", styles["SmallBody"]))

    # Canonical Issues
    issue_chapter("Canonical Tag Issues",
                  "Canonical tags help prevent duplicate content issues.",
                  canon_issues, ["URL", "Canonical URL", "Issues"],
                  lambda pg: [url_p(pg.url, 40), url_p(pg.canonical_url or "none", 35), p(", ".join(pg.canonical_issues or [])[:60])])

    # Noindex
    issue_chapter("Noindex Pages",
                  "These pages are blocked from search engine indexing.",
                  noindex_pages, ["URL"],
                  lambda pg: [url_p(pg.url)])

    # Nofollow
    issue_chapter("Nofollow Meta Pages",
                  "Nofollow meta prevents link equity flow from these pages.",
                  nofollow_pages, ["URL"],
                  lambda pg: [url_p(pg.url)])

    # Images Missing Alt
    issue_chapter("Images Missing Alt Attribute",
                  "Alt text is critical for accessibility and image SEO.",
                  img_missing_alt, ["URL", "Missing", "Total Imgs"],
                  lambda pg: [url_p(pg.url, 45), str(pg.images_without_alt), str(pg.total_images)])

    # Images Empty Alt
    issue_chapter("Images With Empty Alt",
                  "Empty alt text provides no context to screen readers.",
                  img_empty_alt, ["URL", "Empty", "Total Imgs"],
                  lambda pg: [url_p(pg.url, 45), str(pg.images_with_empty_alt or 0), str(pg.total_images)])

    # Hreflang Issues
    issue_chapter("Hreflang Issues",
                  "Hreflang implementation issues affect international targeting.",
                  hreflang_issue_pages, ["URL", "Issues"],
                  lambda pg: [url_p(pg.url, 45), p("; ".join(pg.hreflang_issues or [])[:80])])

    # Thin Content
    issue_chapter("Thin Content (<300 words)",
                  "Pages with very little content may not rank well.",
                  thin_pages, ["URL", "Word Count"],
                  lambda pg: [url_p(pg.url, 55), str(pg.word_count or 0)])

    # Low Text Ratio
    issue_chapter("Low Text-to-HTML Ratio",
                  "Low ratio indicates pages are mostly code with little readable content.",
                  low_ratio_pages, ["URL", "Ratio"],
                  lambda pg: [url_p(pg.url, 55), f"{pg.code_to_text_ratio}%"])

    # Slow Pages
    issue_chapter("Slow Pages (>3s response)",
                  "Slow pages hurt user experience and search rankings.",
                  slow_pages, ["URL", "Response Time"],
                  lambda pg: [url_p(pg.url, 55), f"{pg.response_time:.2f}s"])

    # No Schema
    issue_chapter("No Schema Markup",
                  "Schema markup helps search engines understand page content.",
                  no_schema_pages, ["URL"],
                  lambda pg: [url_p(pg.url)])

    # Redirects
    issue_chapter("Redirects",
                  "Pages returning redirect status codes (301, 302, etc).",
                  redirect_pages, ["URL", "Status Code"],
                  lambda pg: [url_p(pg.url, 55), str(pg.status_code)])

    # Robots & Sitemaps
    story.append(PageBreak())
    story.append(Paragraph(f"Chapter {ch_num}: Robots.txt & Sitemaps", styles["SectionHead"]))
    robots_status = crawl.robots_txt_status or "unknown"
    story.append(Paragraph(f"robots.txt: <b>{robots_status}</b>. {len(sitemaps)} sitemap(s) found with {sitemap_url_count} total URLs.", styles["ChapterIntro"]))
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
