'use client';

export default function ScoreBadge({ score, size = 'md' }) {
  const color = score >= 80 ? 'var(--success)' : score >= 50 ? 'var(--warning)' : 'var(--danger)';
  const sizeClass = size === 'lg' ? 'w-16 h-16 text-xl' : 'w-10 h-10 text-sm';

  return (
    <div
      className={`${sizeClass} rounded-full flex items-center justify-center font-bold border-2`}
      style={{ borderColor: color, color }}
    >
      {score ?? '—'}
    </div>
  );
}
