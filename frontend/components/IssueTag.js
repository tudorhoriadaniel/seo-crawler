'use client';

const COLORS = {
  critical: { bg: '#7f1d1d', text: '#fca5a5' },
  warning: { bg: '#713f12', text: '#fde047' },
  info: { bg: '#1e3a5f', text: '#93c5fd' },
};

export default function IssueTag({ severity, message }) {
  const c = COLORS[severity] || COLORS.info;
  return (
    <span
      className="inline-flex items-center gap-1 px-2 py-1 rounded text-xs font-medium"
      style={{ backgroundColor: c.bg, color: c.text }}
    >
      {severity === 'critical' ? '🔴' : severity === 'warning' ? '🟡' : 'ℹ️'} {message}
    </span>
  );
}
