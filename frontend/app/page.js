'use client';

import { useState, useEffect } from 'react';
import { api } from '../lib/api';
import ScoreBadge from '../components/ScoreBadge';
import StatCard from '../components/StatCard';
import IssueTag from '../components/IssueTag';

export default function Home() {
  const [projects, setProjects] = useState([]);
  const [newUrl, setNewUrl] = useState('');
  const [newName, setNewName] = useState('');
  const [loading, setLoading] = useState(false);
  const [activeCrawl, setActiveCrawl] = useState(null);
  const [crawlStatus, setCrawlStatus] = useState(null);
  const [summary, setSummary] = useState(null);
  const [pages, setPages] = useState([]);
  const [selectedPage, setSelectedPage] = useState(null);
  const [view, setView] = useState('projects'); // projects | crawling | results | detail

  useEffect(() => {
    loadProjects();
  }, []);

  // Poll crawl status
  useEffect(() => {
    if (!activeCrawl || crawlStatus?.status === 'completed' || crawlStatus?.status === 'failed') return;
    const interval = setInterval(async () => {
      try {
        const status = await api.getCrawl(activeCrawl);
        setCrawlStatus(status);
        if (status.status === 'completed') {
          const [s, p] = await Promise.all([
            api.getCrawlSummary(activeCrawl),
            api.getCrawlPages(activeCrawl),
          ]);
          setSummary(s);
          setPages(p);
          setView('results');
        }
      } catch (e) {}
    }, 2000);
    return () => clearInterval(interval);
  }, [activeCrawl, crawlStatus]);

  async function loadProjects() {
    try {
      const data = await api.getProjects();
      setProjects(data);
    } catch (e) {}
  }

  async function handleCreateAndCrawl(e) {
    e.preventDefault();
    if (!newUrl) return;
    setLoading(true);
    try {
      let url = newUrl.trim();
      if (!url.startsWith('http')) url = 'https://' + url;
      const name = newName.trim() || new URL(url).hostname;
      const project = await api.createProject({ name, url });
      const crawl = await api.startCrawl(project.id);
      setActiveCrawl(crawl.id);
      setCrawlStatus(crawl);
      setView('crawling');
      setNewUrl('');
      setNewName('');
      loadProjects();
    } catch (e) {
      alert('Error: ' + e.message);
    }
    setLoading(false);
  }

  async function handleStartCrawl(projectId) {
    try {
      const crawl = await api.startCrawl(projectId);
      setActiveCrawl(crawl.id);
      setCrawlStatus(crawl);
      setView('crawling');
    } catch (e) {
      alert('Error: ' + e.message);
    }
  }

  async function handleViewPage(pageId) {
    try {
      const detail = await api.getPage(pageId);
      setSelectedPage(detail);
      setView('detail');
    } catch (e) {
      alert('Error: ' + e.message);
    }
  }

  // ─── Render ────────────────────────────────────────────────
  if (view === 'detail' && selectedPage) {
    return (
      <div>
        <button onClick={() => setView('results')} className="text-[var(--accent)] hover:underline mb-6 text-sm">
          ← Back to results
        </button>
        <div className="flex items-center gap-4 mb-6">
          <ScoreBadge score={selectedPage.score} size="lg" />
          <div>
            <h1 className="text-xl font-bold truncate max-w-2xl">{selectedPage.title || '(No title)'}</h1>
            <p className="text-sm text-[var(--text-muted)] truncate max-w-2xl">{selectedPage.url}</p>
          </div>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
          <StatCard label="Status Code" value={selectedPage.status_code} />
          <StatCard label="Response Time" value={`${selectedPage.response_time}s`} />
          <StatCard label="Word Count" value={selectedPage.word_count} />
          <StatCard label="Images" value={`${selectedPage.total_images} (${selectedPage.images_without_alt} no alt)`} />
        </div>

        <div className="grid md:grid-cols-2 gap-6 mb-8">
          <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl p-5">
            <h3 className="font-semibold mb-3">Meta Info</h3>
            <div className="space-y-2 text-sm">
              <p><span className="text-[var(--text-muted)]">Title ({selectedPage.title_length} chars):</span> {selectedPage.title || '—'}</p>
              <p><span className="text-[var(--text-muted)]">Description ({selectedPage.meta_description_length} chars):</span> {selectedPage.meta_description || '—'}</p>
              <p><span className="text-[var(--text-muted)]">Canonical:</span> {selectedPage.canonical_url || '—'}</p>
              <p><span className="text-[var(--text-muted)]">Robots:</span> {selectedPage.robots_meta || '—'}</p>
            </div>
          </div>
          <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl p-5">
            <h3 className="font-semibold mb-3">Structure</h3>
            <div className="space-y-2 text-sm">
              <p><span className="text-[var(--text-muted)]">H1:</span> {selectedPage.h1_count} — {(selectedPage.h1_texts || []).join(', ') || '—'}</p>
              <p><span className="text-[var(--text-muted)]">H2:</span> {selectedPage.h2_count} | <span className="text-[var(--text-muted)]">H3:</span> {selectedPage.h3_count}</p>
              <p><span className="text-[var(--text-muted)]">Internal Links:</span> {selectedPage.internal_links} | <span className="text-[var(--text-muted)]">External:</span> {selectedPage.external_links}</p>
              <p><span className="text-[var(--text-muted)]">Schema:</span> {selectedPage.has_schema_markup ? (selectedPage.schema_types || []).join(', ') : 'None'}</p>
              <p><span className="text-[var(--text-muted)]">Viewport:</span> {selectedPage.has_viewport_meta ? '✅' : '❌'}</p>
            </div>
          </div>
        </div>

        {selectedPage.og_title && (
          <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl p-5 mb-8">
            <h3 className="font-semibold mb-3">Open Graph</h3>
            <div className="text-sm space-y-1">
              <p><span className="text-[var(--text-muted)]">OG Title:</span> {selectedPage.og_title}</p>
              <p><span className="text-[var(--text-muted)]">OG Description:</span> {selectedPage.og_description || '—'}</p>
            </div>
          </div>
        )}

        <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl p-5">
          <h3 className="font-semibold mb-3">Issues ({(selectedPage.issues || []).length})</h3>
          <div className="flex flex-wrap gap-2">
            {(selectedPage.issues || []).map((issue, i) => (
              <IssueTag key={i} severity={issue.severity} message={issue.message} />
            ))}
            {(!selectedPage.issues || selectedPage.issues.length === 0) && (
              <p className="text-sm text-[var(--success)]">No issues found!</p>
            )}
          </div>
        </div>
      </div>
    );
  }

  if (view === 'results' && summary) {
    return (
      <div>
        <button onClick={() => setView('projects')} className="text-[var(--accent)] hover:underline mb-6 text-sm">
          ← Back to projects
        </button>
        <h1 className="text-2xl font-bold mb-6">Crawl Results</h1>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
          <StatCard label="Average Score" value={summary.avg_score} color={summary.avg_score >= 80 ? 'var(--success)' : summary.avg_score >= 50 ? 'var(--warning)' : 'var(--danger)'} />
          <StatCard label="Pages Crawled" value={summary.total_pages} />
          <StatCard label="Critical Issues" value={summary.critical_issues} color="var(--danger)" />
          <StatCard label="Warnings" value={summary.warnings} color="var(--warning)" />
        </div>

        <div className="grid grid-cols-2 md:grid-cols-3 gap-4 mb-8">
          <StatCard label="Missing Title" value={summary.pages_with_missing_title} />
          <StatCard label="Missing Meta Desc" value={summary.pages_with_missing_meta} />
          <StatCard label="Missing H1" value={summary.pages_with_missing_h1} />
        </div>

        <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--border)] text-left text-[var(--text-muted)]">
                <th className="px-4 py-3">Score</th>
                <th className="px-4 py-3">URL</th>
                <th className="px-4 py-3">Title</th>
                <th className="px-4 py-3">Status</th>
                <th className="px-4 py-3">Issues</th>
                <th className="px-4 py-3">Speed</th>
              </tr>
            </thead>
            <tbody>
              {pages.map((page) => (
                <tr
                  key={page.id}
                  className="border-b border-[var(--border)] hover:bg-white/5 cursor-pointer"
                  onClick={() => handleViewPage(page.id)}
                >
                  <td className="px-4 py-3"><ScoreBadge score={page.score} /></td>
                  <td className="px-4 py-3 truncate max-w-xs">{page.url}</td>
                  <td className="px-4 py-3 truncate max-w-xs">{page.title || '—'}</td>
                  <td className="px-4 py-3">{page.status_code}</td>
                  <td className="px-4 py-3">{page.issues_count}</td>
                  <td className="px-4 py-3">{page.response_time}s</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  }

  if (view === 'crawling') {
    return (
      <div className="flex flex-col items-center justify-center py-20">
        <div className="w-16 h-16 border-4 border-[var(--accent)] border-t-transparent rounded-full animate-spin mb-6"></div>
        <h2 className="text-xl font-bold mb-2">Crawling...</h2>
        <p className="text-[var(--text-muted)]">
          {crawlStatus?.pages_crawled || 0} pages crawled
          {crawlStatus?.status === 'failed' && ' — Crawl failed!'}
        </p>
      </div>
    );
  }

  // ─── Projects list (default) ───────────────────────────────
  return (
    <div>
      <div className="mb-10">
        <h1 className="text-3xl font-bold mb-2">SEO Crawler</h1>
        <p className="text-[var(--text-muted)]">Enter a URL to run a full SEO audit</p>
      </div>

      <form onSubmit={handleCreateAndCrawl} className="bg-[var(--card)] border border-[var(--border)] rounded-xl p-6 mb-10">
        <div className="flex gap-3">
          <input
            type="text"
            placeholder="Project name (optional)"
            value={newName}
            onChange={(e) => setNewName(e.target.value)}
            className="flex-1 max-w-xs bg-[var(--bg)] border border-[var(--border)] rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-[var(--accent)]"
          />
          <input
            type="text"
            placeholder="https://example.com"
            value={newUrl}
            onChange={(e) => setNewUrl(e.target.value)}
            required
            className="flex-[2] bg-[var(--bg)] border border-[var(--border)] rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-[var(--accent)]"
          />
          <button
            type="submit"
            disabled={loading}
            className="bg-[var(--accent)] hover:bg-[var(--accent-hover)] text-white px-6 py-2.5 rounded-lg text-sm font-medium disabled:opacity-50 transition-colors"
          >
            {loading ? 'Starting...' : 'Crawl'}
          </button>
        </div>
      </form>

      {projects.length > 0 && (
        <div>
          <h2 className="text-lg font-semibold mb-4">Projects</h2>
          <div className="space-y-3">
            {projects.map((project) => (
              <div key={project.id} className="bg-[var(--card)] border border-[var(--border)] rounded-xl p-4 flex items-center justify-between">
                <div>
                  <p className="font-medium">{project.name}</p>
                  <p className="text-sm text-[var(--text-muted)]">{project.url}</p>
                </div>
                <button
                  onClick={() => handleStartCrawl(project.id)}
                  className="bg-[var(--accent)] hover:bg-[var(--accent-hover)] text-white px-4 py-2 rounded-lg text-sm transition-colors"
                >
                  Re-crawl
                </button>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
