import './globals.css';

export const metadata = {
  title: 'SEO Crawler — ai.tudordaniel.ro',
  description: 'Full SEO audit crawler SaaS',
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body className="min-h-screen">
        <nav className="border-b border-[var(--border)] px-6 py-4">
          <div className="max-w-7xl mx-auto flex items-center justify-between">
            <a href="/" className="text-xl font-bold">
              <span className="text-[var(--accent)]">SEO</span> Crawler
            </a>
            <span className="text-sm text-[var(--text-muted)]">ai.tudordaniel.ro</span>
          </div>
        </nav>
        <main className="max-w-7xl mx-auto px-6 py-8">
          {children}
        </main>
      </body>
    </html>
  );
}
