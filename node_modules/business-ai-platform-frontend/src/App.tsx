import { ChangeEvent, CSSProperties, DragEvent, FormEvent, useEffect, useMemo, useState } from 'react';

type InsightResponse = {
  metrics: Array<{ label: string; value: number; trend: string }>;
  recommendations: string[];
};

type Workflow = {
  name: string;
  owner: string;
  status: string;
  steps: number;
};

type Dataset = {
  id: string;
  fileName: string;
  uploadedAt: string;
  rows: number;
  columns: number;
  headers: string[];
  preview: Record<string, string>[];
  chartColumn: string;
  labelColumn: string;
  chart: Array<{ label: string; value: number }>;
  numericSummary: Array<{ column: string; total: number; average: number; min: number; max: number }>;
  insights: string[];
};

type ChartType = 'bar' | 'line' | 'donut';
type Theme = 'light' | 'dark';
type ChatMessage = { role: 'assistant' | 'user'; text: string };
type AuthMode = 'login' | 'signup';
type User = {
  id: string;
  name: string;
  email: string;
  role: 'admin' | 'user';
};
type SavedDashboard = {
  id: string;
  name: string;
  datasetId: string;
  datasetName: string;
  ownerName?: string;
  ownerEmail?: string;
  chartType: ChartType;
  snapshot?: {
    dataset?: Dataset;
    savedAt?: string;
    insights?: string[];
  };
  createdAt?: string;
  updatedAt: string;
};
type ReportHistoryItem = {
  id: string;
  title: string;
  datasetId: string;
  datasetName: string;
  ownerName?: string;
  ownerEmail?: string;
  reportType: string;
  content?: {
    lines?: string[];
    dataset?: Dataset;
    chartType?: ChartType;
  };
  createdAt: string;
};
const API_BASE = 'https://backend-six-pied-39.vercel.app';
const fallbackInsights: InsightResponse = {
  metrics: [
    { label: 'Automations live', value: 0, trend: '0%' },
    { label: 'Hours saved', value: 0, trend: '0%' },
    { label: 'Active workflows', value: 0, trend: '0%' }
  ],
  recommendations: ['Start the backend to load live recommendations.']
};

export function App() {
  const [insights, setInsights] = useState<InsightResponse>(fallbackInsights);
  const [workflows, setWorkflows] = useState<Workflow[]>([]);
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [activeDataset, setActiveDataset] = useState<Dataset | null>(null);
  const [status, setStatus] = useState('Connecting');
  const [uploadState, setUploadState] = useState('Drop a CSV here or browse to start analysis.');
  const [isDragging, setIsDragging] = useState(false);
  const [chartType, setChartType] = useState<ChartType>('bar');
  const [theme, setTheme] = useState<Theme>(() => (localStorage.getItem('theme') as Theme) || 'light');
  const [token, setToken] = useState(() => localStorage.getItem('authToken') || '');
  const [user, setUser] = useState<User | null>(null);
  const [authDisabled, setAuthDisabled] = useState(false);
  const [configLoaded, setConfigLoaded] = useState(false);
  const [authMode, setAuthMode] = useState<AuthMode>('login');
  const [authName, setAuthName] = useState('');
  const [authEmail, setAuthEmail] = useState('');
  const [authPassword, setAuthPassword] = useState('');
  const [authMessage, setAuthMessage] = useState('Sign in to access your dashboards and datasets.');
  const [currentView, setCurrentView] = useState<'dashboard' | 'settings'>('dashboard');
  const [accountOpen, setAccountOpen] = useState(false);
  const [question, setQuestion] = useState('');
  const [dashboards, setDashboards] = useState<SavedDashboard[]>([]);
  const [reports, setReports] = useState<ReportHistoryItem[]>([]);
  const [persistenceState, setPersistenceState] = useState('SQL Server storage ready for saved work.');
  const [chat, setChat] = useState<ChatMessage[]>([
    { role: 'assistant', text: 'Upload or select a dataset, then ask about rows, columns, totals, averages, or outliers.' }
  ]);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem('theme', theme);
  }, [theme]);

  useEffect(() => {
    fetch(`${API_BASE}/api/config`, {
  credentials: 'include'
})
      .then((response) => response.json())
      .then((config) => {
        setAuthDisabled(Boolean(config.authDisabled));
        setPersistenceState(config.storage === 'sql-server' ? 'SQL Server storage ready for saved work.' : 'Local MVP storage ready.');
      })
      .catch(() => {
        setAuthDisabled(false);
        setPersistenceState('Local MVP storage ready.');
      })
      .finally(() => setConfigLoaded(true));
  }, []);

  useEffect(() => {
    if (!configLoaded) {
      return;
    }

    if (!token && !authDisabled) {
      setStatus('Signed out');
      return;
    }

    Promise.all([
      apiFetch('/api/auth/me'),
     // apiFetch('/api/insights'),
      apiFetch('/api/workflows'),
      apiFetch('/api/datasets'),
      apiFetch('/api/dashboards'),
      apiFetch('/api/reports')
    ])
      .then(async ([meResponse, insightsResponse, workflowsResponse, datasetsResponse, dashboardsResponse, reportsResponse]) => {
        setUser((await meResponse.json()).user);
        setInsights(await insightsResponse.json());
        setWorkflows((await workflowsResponse.json()).workflows);
        const saved = (await datasetsResponse.json()).datasets;
        const savedDashboards = (await dashboardsResponse.json()).dashboards;
        const savedReports = (await reportsResponse.json()).reports;
        const latestDashboard = savedDashboards[0];
        const latestDataset = latestDashboard
          ? saved.find((dataset: Dataset) => dataset.id === latestDashboard.datasetId) ?? latestDashboard.snapshot?.dataset
          : undefined;
        setDatasets(saved);
        setActiveDataset(latestDataset ?? saved[0] ?? null);
        if (latestDashboard?.chartType) {
          setChartType(latestDashboard.chartType);
        }
        setDashboards(savedDashboards);
        setReports(savedReports);
        setStatus('Live');
      })
      .catch(() => {
        if (authDisabled) {
          setStatus('Offline');
        } else {
          logout();
        }
      });
  }, [authDisabled, configLoaded, token]);

  function apiFetch(path: string, options: RequestInit = {}) {
    const headers = new Headers(options.headers);
    if (token) {
      headers.set('Authorization', `Bearer ${token}`);
    }

    return fetch(path, { ...options, headers }).then((response) => {
      if (response.status === 401) {
        logout();
        throw new Error('Authentication required.');
      }
      return response;
    });
  }

  function logout() {
    localStorage.removeItem('authToken');
    setToken('');
    setUser(null);
    setDatasets([]);
    setActiveDataset(null);
    setDashboards([]);
    setReports([]);
    setCurrentView('dashboard');
    setAccountOpen(false);
    setStatus('Signed out');
  }

  async function handleAuth(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setAuthMessage(authMode === 'login' ? 'Signing in...' : 'Creating account...');
    await submitAuth({
      name: authName,
      email: authEmail,
      password: authPassword,
      mode: authMode
    });
  }

  async function handleDemoLogin() {
    setAuthMode('login');
    setAuthEmail('admin@businessai.com');
    setAuthPassword('admin123');
    setAuthMessage('Opening demo workspace...');
    await submitAuth({
      email: 'admin@businessai.com',
      password: 'admin123',
      mode: 'login'
    });
  }

  async function submitAuth(credentials: { name?: string; email: string; password: string; mode: AuthMode }) {
    try {
     const response = await fetch(`${API_BASE}/api/auth/${credentials.mode}`, {
  method: 'POST',
  credentials: 'include',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    name: credentials.name,
    email: credentials.email,
    password: credentials.password
  })
});

      let payload;

try {
  payload = await response.json();
} catch (error) {
  setAuthMessage('Server response error.');
  return;
}

      if (!response.ok) {
        throw new Error(payload.error || 'Authentication failed.');
      }

      localStorage.setItem('authToken', payload.token);
      setToken(payload.token);
      setUser(payload.user);
      setAuthPassword('');
      setAuthMessage('Welcome back.');
      window.location.reload();
    } catch (error) {
      setAuthMessage(error instanceof Error ? error.message : 'Authentication failed.');
    }
  }

  const chartMax = useMemo(() => {
    if (!activeDataset?.chart.length) {
      return 1;
    }
    return Math.max(...activeDataset.chart.map((point) => point.value), 1);
  }, [activeDataset]);

  const linePoints = useMemo(() => {
    if (!activeDataset?.chart.length) {
      return '';
    }

    return activeDataset.chart
      .map((point, index) => {
        const x = activeDataset.chart.length === 1 ? 50 : (index / (activeDataset.chart.length - 1)) * 100;
        const y = 100 - (point.value / chartMax) * 88;
        return `${x},${Math.max(y, 8)}`;
      })
      .join(' ');
  }, [activeDataset, chartMax]);

  async function uploadCsv(file: File) {
    const formData = new FormData();
    formData.append('file', file);
    setUploadState(`Analyzing ${file.name}...`);

    try {
      const response = await apiFetch('/api/csv/upload', {
        method: 'POST',
        body: formData
      });

      if (!response.ok) {
        const payload = await response.json();
        throw new Error(payload.error || 'Upload failed.');
      }

      const dataset = await response.json();
      setActiveDataset(dataset);
      setDatasets((current) => [dataset, ...current.filter((item) => item.id !== dataset.id)]);
      setChat([{ role: 'assistant', text: `I loaded ${dataset.fileName}. Ask me what changed, what stands out, or how many rows it has.` }]);
      setUploadState(authDisabled ? 'CSV analysis ready and saved locally.' : 'CSV analysis ready and saved to SQL Server.');
    } catch (error) {
      setUploadState(error instanceof Error ? error.message : 'Upload failed.');
    }
  }

  function handleCsvUpload(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    if (file) {
      uploadCsv(file);
    }
  }

  function handleDrop(event: DragEvent<HTMLLabelElement>) {
    event.preventDefault();
    setIsDragging(false);
    const file = event.dataTransfer.files[0];
    if (file) {
      uploadCsv(file);
    }
  }

  async function askAssistant(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!activeDataset || !question.trim()) {
      return;
    }

    const nextQuestion = question.trim();
    setQuestion('');
    setChat((current) => [...current, { role: 'user', text: nextQuestion }]);

    try {
      const response = await apiFetch(`/api/datasets/${activeDataset.id}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: nextQuestion })
      });
      const payload = await response.json();
      setChat((current) => [...current, { role: 'assistant', text: payload.answer }]);
    } catch {
      setChat((current) => [...current, { role: 'assistant', text: 'I could not answer that yet. Check the backend connection and try again.' }]);
    }
  }

  async function saveCurrentDashboard() {
    if (!activeDataset) {
      return;
    }

    const response = await apiFetch('/api/dashboards', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: `${activeDataset.fileName} ${chartType} dashboard`,
        datasetId: activeDataset.id,
        chartType,
        config: {
          chartColumn: activeDataset.chartColumn,
          labelColumn: activeDataset.labelColumn
        },
        snapshot: buildDashboardSnapshot(activeDataset, chartType)
      })
    });
    const payload = await response.json();
    setDashboards((current) => [payload.dashboard, ...current.filter((dashboard) => dashboard.id !== payload.dashboard.id)]);
    setPersistenceState(authDisabled ? 'Dashboard saved locally.' : 'Dashboard saved to SQL Server.');
  }

  function openDashboard(dashboard: SavedDashboard) {
    const dataset = datasets.find((item) => item.id === dashboard.datasetId) ?? dashboard.snapshot?.dataset;
    if (dataset) {
      setActiveDataset(dataset);
      setChartType(dashboard.chartType);
      setPersistenceState(`Reopened ${dashboard.name}.`);
    }
  }

  async function refreshHistory() {
    const [dashboardsResponse, reportsResponse] = await Promise.all([
      apiFetch('/api/dashboards'),
      apiFetch('/api/reports')
    ]);
    setDashboards((await dashboardsResponse.json()).dashboards);
    setReports((await reportsResponse.json()).reports);
  }

  async function downloadPdfReport() {
    if (!activeDataset) {
      return;
    }

    const lines = buildReportLines(activeDataset);
    downloadPdf(lines, `${activeDataset.fileName.replace(/\.csv$/i, '')}-report.pdf`);
    const response = await apiFetch('/api/reports', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        datasetId: activeDataset.id,
        title: `${activeDataset.fileName} PDF report`,
        reportType: 'pdf',
        content: {
          chartType,
          rows: activeDataset.rows,
          columns: activeDataset.columns,
          insights: activeDataset.insights,
          lines,
          dataset: activeDataset
        }
      })
    });
    const payload = await response.json();
    setReports((current) => [payload.report, ...current]);
    setPersistenceState('Report downloaded and added to history.');
  }

  function downloadHistoricalReport(report: ReportHistoryItem) {
    const lines = report.content?.lines
      ?? (report.content?.dataset ? buildReportLines(report.content.dataset) : [
        'Business AI Platform CSV Report',
        `Report: ${report.title}`,
        `Dataset: ${report.datasetName}`,
        `Created: ${new Date(report.createdAt).toLocaleString()}`
      ]);
    downloadPdf(lines, `${report.title.replace(/[^a-z0-9]+/gi, '-').replace(/^-|-$/g, '').toLowerCase() || 'business-ai-report'}.pdf`);
    setPersistenceState(`Downloaded ${report.title}.`);
  }

  function openSettings() {
    setCurrentView('settings');
    setAccountOpen(false);
  }

  if (!configLoaded) {
    return (
      <main className="auth-shell">
        <section className="auth-card">
          <div className="brand auth-brand">
            <span className="brand-icon">AI</span>
            <span>Business AI</span>
          </div>
          <p className="auth-copy">Loading local workspace...</p>
        </section>
      </main>
    );
  }

  if (!user && !authDisabled) {
    return (
      <main className="auth-shell">
        <section className="auth-card">
          <div className="brand auth-brand">
            <span className="brand-icon">AI</span>
            <span>Business AI</span>
          </div>
          <p className="eyebrow">Secure workspace</p>
          <h1>{authMode === 'login' ? 'Welcome back' : 'Create your account'}</h1>
          <p className="auth-copy">{authMessage}</p>
          <form className="auth-form" onSubmit={handleAuth}>
            {authMode === 'signup' && (
              <label>
                Name
                <input autoComplete="name" value={authName} onChange={(event) => setAuthName(event.target.value)} />
              </label>
            )}
            <label>
              Email
              <input autoComplete="email" type="email" value={authEmail} onChange={(event) => setAuthEmail(event.target.value)} />
            </label>
            <label>
              Password
              <input autoComplete={authMode === 'login' ? 'current-password' : 'new-password'} minLength={8} type="password" value={authPassword} onChange={(event) => setAuthPassword(event.target.value)} />
            </label>
            <button type="submit">{authMode === 'login' ? 'Log in' : 'Sign up'}</button>
          </form>
          <button className="demo-login-button" type="button" onClick={handleDemoLogin}>
            Use demo login
          </button>
          <p className="demo-credentials">admin@businessai.com / admin123</p>
          <button className="link-button" type="button" onClick={() => setAuthMode(authMode === 'login' ? 'signup' : 'login')}>
            {authMode === 'login' ? 'Need an account? Sign up' : 'Already have an account? Log in'}
          </button>
        </section>
      </main>
    );
  }

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-icon">AI</span>
          <span>Business AI</span>
        </div>
        <nav aria-label="Primary">
          <button className={currentView === 'dashboard' ? 'active' : ''} type="button" onClick={() => setCurrentView('dashboard')}>Overview</button>
          <a href="#csv" onClick={() => setCurrentView('dashboard')}>CSV Studio</a>
          <a href="#assistant" onClick={() => setCurrentView('dashboard')}>Assistant</a>
          <button className={currentView === 'settings' ? 'active' : ''} type="button" onClick={openSettings}>Settings</button>
        </nav>
      </aside>

      <section className="content" id="overview">
        <header className="topbar">
          <div>
            <p className="eyebrow">Operations command center</p>
            <h1>AI workflow performance</h1>
          </div>
          <div className="top-actions">
            <button className="ghost-button" type="button" onClick={() => setTheme(theme === 'light' ? 'dark' : 'light')}>
              {theme === 'light' ? 'Dark' : 'Light'} mode
            </button>
            <div className="account-menu">
              <button className="account-button" type="button" onClick={() => setAccountOpen((open) => !open)}>
                <span className="avatar">{(user?.name || 'A').slice(0, 1).toUpperCase()}</span>
                <span>
                  <strong>{user?.name ?? 'Local workspace'}</strong>
                  <small>{user?.role === 'admin' ? 'Admin' : 'User'}</small>
                </span>
              </button>
              {accountOpen && (
                <div className="account-dropdown">
                  <div>
                    <strong>{user?.name ?? 'Local workspace'}</strong>
                    <span>{user?.email ?? 'local@example.com'}</span>
                  </div>
                  <button type="button" onClick={openSettings}>Profile settings</button>
                  {!authDisabled && <button type="button" onClick={logout}>Log out</button>}
                </div>
              )}
            </div>
            <span className={`status ${status.toLowerCase()}`}>{status}</span>
          </div>
        </header>

        {currentView === 'settings' ? (
          <section className="settings-grid">
            <article className="panel profile-panel">
              <p className="eyebrow">Account</p>
              <h2>Profile settings</h2>
              <div className="profile-card">
                <span className="avatar large">{(user?.name || 'A').slice(0, 1).toUpperCase()}</span>
                <div>
                  <strong>{user?.name ?? 'Local workspace'}</strong>
                  <span>{user?.email ?? 'local@example.com'}</span>
                </div>
              </div>
              <dl className="settings-list">
                <div>
                  <dt>Workspace role</dt>
                  <dd>{user?.role === 'admin' ? 'Admin' : 'User'}</dd>
                </div>
                <div>
                  <dt>Access policy</dt>
                  <dd>{user?.role === 'admin' ? 'Can view all dashboards and reports.' : 'Can view only owned datasets, dashboards, and reports.'}</dd>
                </div>
                <div>
                  <dt>Session security</dt>
                  <dd>JWT session with server-side revocation.</dd>
                </div>
              </dl>
            </article>
            <article className="panel">
              <p className="eyebrow">Workspace</p>
              <h2>Enterprise controls</h2>
              <ul className="settings-notes">
                <li>Role is stored with the user profile in SQL Server.</li>
                <li>Dashboard and report history are filtered server-side.</li>
                <li>Protected admin routes reject non-admin users.</li>
              </ul>
            </article>
          </section>
        ) : (
          <>
        <section className="metrics-grid" aria-label="Performance metrics">
          {insights.metrics.map((metric) => (
            <article className="metric-card" key={metric.label}>
              <span>{metric.label}</span>
              <strong>{metric.value}</strong>
              <small>{metric.trend} this month</small>
            </article>
          ))}
        </section>

        <section className="csv-section" id="csv">
          <div className="section-heading">
            <div>
              <p className="eyebrow">CSV studio</p>
              <h2>Analyze business data in one clean view</h2>
            </div>
            <button className="ghost-button" type="button" disabled={!activeDataset} onClick={downloadPdfReport}>
              Download PDF
            </button>
            <button className="ghost-button" type="button" disabled={!activeDataset} onClick={saveCurrentDashboard}>
              Save dashboard
            </button>
          </div>

          <label
            className={`drop-zone ${isDragging ? 'dragging' : ''}`}
            onDragOver={(event) => {
              event.preventDefault();
              setIsDragging(true);
            }}
            onDragLeave={() => setIsDragging(false)}
            onDrop={handleDrop}
          >
            <input accept=".csv,text/csv" type="file" onChange={handleCsvUpload} />
            <strong>Drop CSV file here</strong>
            <span>{uploadState}</span>
          </label>

          <div className="dataset-strip">
            {datasets.map((dataset) => (
              <button
                className={activeDataset?.id === dataset.id ? 'selected' : ''}
                key={dataset.id}
                type="button"
                onClick={() => setActiveDataset(dataset)}
              >
                <strong>{dataset.fileName}</strong>
                <span>{dataset.rows} rows - {dataset.columns} columns</span>
              </button>
            ))}
          </div>

          <p className="persistence-note">{persistenceState}</p>

          <div className="csv-grid">
            <article className="panel data-panel">
              <div className="panel-header">
                <h3>{activeDataset?.fileName ?? 'Data preview'}</h3>
                <div className="count-strip">
                  <span>{activeDataset?.rows ?? 0} rows</span>
                  <span>{activeDataset?.columns ?? 0} columns</span>
                </div>
              </div>

              {activeDataset ? (
                <div className="table-wrap">
                  <table>
                    <thead>
                      <tr>
                        {activeDataset.headers.map((header) => (
                          <th key={header}>{header}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {activeDataset.preview.map((row, rowIndex) => (
                        <tr key={`${activeDataset.id}-${rowIndex}`}>
                          {activeDataset.headers.map((header) => (
                            <td key={header}>{row[header]}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="empty-state">No CSV uploaded yet.</div>
              )}
            </article>

            <aside className="side-stack">
              <article className="panel">
                <div className="panel-header">
                  <div>
                    <h3>Auto chart</h3>
                    <p className="muted">{activeDataset ? `Showing ${activeDataset.chartColumn}` : 'A chart appears after upload.'}</p>
                  </div>
                  <div className="segmented" aria-label="Chart type">
                    {(['bar', 'line', 'donut'] as ChartType[]).map((type) => (
                      <button className={chartType === type ? 'active' : ''} key={type} type="button" onClick={() => setChartType(type)}>
                        {type}
                      </button>
                    ))}
                  </div>
                </div>
                {renderChart(chartType, activeDataset, chartMax, linePoints)}
              </article>

              <article className="panel ai-panel">
                <h3>AI insights</h3>
                <ul>
                  {(activeDataset?.insights ?? ['Upload a CSV to generate clear, practical data insights.']).map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
              </article>
            </aside>
          </div>
        </section>

        <section className="history-grid">
          <article className="panel">
            <div className="panel-header">
              <h2>Saved dashboards</h2>
              <button className="ghost-button compact" type="button" onClick={refreshHistory}>Refresh</button>
            </div>
            <div className="history-list">
              {dashboards.length ? dashboards.map((dashboard) => (
                <button key={dashboard.id} type="button" onClick={() => openDashboard(dashboard)}>
                  <strong>{dashboard.name}</strong>
                  <span>
                    {dashboard.datasetName} - {dashboard.chartType} chart - {new Date(dashboard.updatedAt).toLocaleString()}
                    {user?.role === 'admin' && dashboard.ownerEmail ? ` - ${dashboard.ownerEmail}` : ''}
                  </span>
                </button>
              )) : <p className="muted">No dashboards saved yet.</p>}
            </div>
          </article>

          <article className="panel">
            <div className="panel-header">
              <h2>Report history</h2>
              <button className="ghost-button compact" type="button" onClick={refreshHistory}>Refresh</button>
            </div>
            <div className="history-list">
              {reports.length ? reports.map((report) => (
                <div className="history-item" key={report.id}>
                  <div>
                    <strong>{report.title}</strong>
                    <span>
                      {report.datasetName} - {new Date(report.createdAt).toLocaleString()}
                      {user?.role === 'admin' && report.ownerEmail ? ` - ${report.ownerEmail}` : ''}
                    </span>
                  </div>
                  <button className="ghost-button compact" type="button" onClick={() => downloadHistoricalReport(report)}>
                    Download
                  </button>
                </div>
              )) : <p className="muted">Downloaded reports will appear here.</p>}
            </div>
          </article>
        </section>

        <section className="assistant-grid" id="assistant">
          <article className="panel chat-panel">
            <div className="panel-header">
              <h2>AI data assistant</h2>
              <span>{activeDataset ? activeDataset.fileName : 'No dataset selected'}</span>
            </div>
            <div className="messages">
              {chat.map((message, index) => (
                <div className={`message ${message.role}`} key={`${message.role}-${index}`}>
                  {message.text}
                </div>
              ))}
            </div>
            <form className="chat-form" onSubmit={askAssistant}>
              <input
                disabled={!activeDataset}
                onChange={(event) => setQuestion(event.target.value)}
                placeholder="Ask about totals, averages, columns..."
                value={question}
              />
              <button disabled={!activeDataset || !question.trim()} type="submit">Ask</button>
            </form>
          </article>

          <article className="panel">
            <h2>Active workflows</h2>
            <div className="workflow-list">
              {workflows.map((workflow) => (
                <div className="workflow-row" key={workflow.name}>
                  <div>
                    <strong>{workflow.name}</strong>
                    <span>{workflow.owner} - {workflow.steps} steps</span>
                  </div>
                  <small>{workflow.status}</small>
                </div>
              ))}
            </div>
          </article>
        </section>
          </>
        )}
      </section>
    </main>
  );
}

function renderChart(chartType: ChartType, dataset: Dataset | null, chartMax: number, linePoints: string) {
  if (!dataset) {
    return <div className="empty-state chart-empty">Upload a CSV to build a chart.</div>;
  }

  if (chartType === 'line') {
    return (
      <div className="line-chart">
        <svg viewBox="0 0 100 100" role="img" aria-label="Line chart">
          <polyline points={linePoints} />
        </svg>
      </div>
    );
  }

  if (chartType === 'donut') {
    const total = dataset.chart.reduce((sum, point) => sum + point.value, 0) || 1;
    const topValue = dataset.chart[0]?.value ?? 0;
    const percentage = Math.round((topValue / total) * 100);
    return (
      <div className="donut-wrap">
        <div className="donut" style={{ '--slice': `${percentage}%` } as CSSProperties}>
          <strong>{percentage}%</strong>
          <span>top slice</span>
        </div>
      </div>
    );
  }

  return (
    <div className="bar-chart">
      {dataset.chart.map((point) => (
        <div className="bar-row" key={point.label}>
          <span>{point.label}</span>
          <div>
            <i style={{ width: `${Math.max((point.value / chartMax) * 100, 4)}%` }} />
          </div>
          <strong>{point.value}</strong>
        </div>
      ))}
    </div>
  );
}

function buildDashboardSnapshot(dataset: Dataset, chartType: ChartType) {
  return {
    dataset,
    chartType,
    savedAt: new Date().toISOString(),
    metrics: {
      rows: dataset.rows,
      columns: dataset.columns,
      chartColumn: dataset.chartColumn,
      labelColumn: dataset.labelColumn
    },
    chart: dataset.chart,
    insights: dataset.insights,
    numericSummary: dataset.numericSummary
  };
}

function buildReportLines(dataset: Dataset) {
  return [
    'Business AI Platform CSV Report',
    `Dataset: ${dataset.fileName}`,
    `Uploaded: ${new Date(dataset.uploadedAt).toLocaleString()}`,
    `Rows: ${dataset.rows}`,
    `Columns: ${dataset.columns}`,
    '',
    'AI Insights',
    ...dataset.insights.map((item) => `- ${item}`),
    '',
    'Numeric Summary',
    ...(dataset.numericSummary.length
      ? dataset.numericSummary.map((item) => `${item.column}: total ${item.total.toFixed(2)}, average ${item.average.toFixed(2)}, min ${item.min.toFixed(2)}, max ${item.max.toFixed(2)}`)
      : ['No numeric columns detected.'])
  ];
}

function downloadPdf(lines: string[], fileName: string) {
  const pdf = createSimplePdf(lines);
  const link = document.createElement('a');
  link.href = URL.createObjectURL(new Blob([pdf], { type: 'application/pdf' }));
  link.download = fileName;
  link.click();
  URL.revokeObjectURL(link.href);
}

function createSimplePdf(lines: string[]) {
  const escapePdf = (value: string) => value.replace(/\\/g, '\\\\').replace(/\(/g, '\\(').replace(/\)/g, '\\)');
  const content = [
    'BT',
    '/F1 16 Tf',
    '72 760 Td',
    ...lines.flatMap((line, index) => [
      index === 0 ? '' : '0 -20 Td',
      `(${escapePdf(line).slice(0, 92)}) Tj`
    ]),
    'ET'
  ].filter(Boolean).join('\n');
  const objects = [
    '1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj',
    '2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj',
    '3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >> endobj',
    '4 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj',
    `5 0 obj << /Length ${content.length} >> stream\n${content}\nendstream endobj`
  ];
  let pdf = '%PDF-1.4\n';
  const offsets = [0];
  objects.forEach((object) => {
    offsets.push(pdf.length);
    pdf += `${object}\n`;
  });
  const xref = pdf.length;
  pdf += `xref\n0 ${objects.length + 1}\n0000000000 65535 f \n`;
  offsets.slice(1).forEach((offset) => {
    pdf += `${String(offset).padStart(10, '0')} 00000 n \n`;
  });
  pdf += `trailer << /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF`;
  return pdf;
}
