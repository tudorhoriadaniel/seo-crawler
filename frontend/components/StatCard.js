'use client';

export default function StatCard({ label, value, color }) {
  return (
    <div className="bg-[var(--card)] border border-[var(--border)] rounded-xl p-5">
      <p className="text-sm text-[var(--text-muted)] mb-1">{label}</p>
      <p className="text-2xl font-bold" style={{ color: color || 'var(--text)' }}>
        {value}
      </p>
    </div>
  );
}
