const API_BASE = '/api';

async function fetchAPI(path, options = {}) {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options.headers },
    ...options,
  });
  if (!res.ok) {
    const error = await res.json().catch(() => ({ detail: 'Request failed' }));
    throw new Error(error.detail || 'Request failed');
  }
  return res.json();
}

export const api = {
  // Projects
  getProjects: () => fetchAPI('/projects'),
  createProject: (data) => fetchAPI('/projects', { method: 'POST', body: JSON.stringify(data) }),
  deleteProject: (id) => fetchAPI(`/projects/${id}`, { method: 'DELETE' }),

  // Crawls
  startCrawl: (projectId) => fetchAPI('/crawls', { method: 'POST', body: JSON.stringify({ project_id: projectId }) }),
  getCrawl: (id) => fetchAPI(`/crawls/${id}`),
  getProjectCrawls: (projectId) => fetchAPI(`/projects/${projectId}/crawls`),

  // Pages
  getCrawlPages: (crawlId) => fetchAPI(`/crawls/${crawlId}/pages`),
  getPage: (pageId) => fetchAPI(`/pages/${pageId}`),

  // Summary
  getCrawlSummary: (crawlId) => fetchAPI(`/crawls/${crawlId}/summary`),
};
