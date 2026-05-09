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
  fileType?: string;
  worksheetName?: string | null;
  worksheets?: string[];
  warnings?: string[];
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
type AppView = 'dashboard' | 'settings' | 'adminUsers';

type User = {
  id: string;
  name: string;
  email: string;
  role: 'admin' | 'user';
  active?: boolean;
  createdAt?: string;
};

type AdminUser = User & {
  active: boolean;
  createdAt?: string;
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

type ConfigResponse = {
  authDisabled: boolean;
  storage: 'sql-server' | 'local-json' | string;
};

type AuthResponse = {
  token: string;
  user: User;
};

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
  const [uploadState, setUploadState] = useState('Drop a CSV or Excel file here to start analysis.');
  const [isDragging, setIsDragging] = useState(false);
  const [lastUploadedFile, setLastUploadedFile] = useState<File | null>(null);
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
  const [currentView, setCurrentView] = useState<AppView>('dashboard');
  const [accountOpen, setAccountOpen] = useState(false);
  const [question, setQuestion] = useState('');
  const [dashboards, setDashboards] = useState<SavedDashboard[]>([]);
  const [reports, setReports] = useState<ReportHistoryItem[]>([]);
  const [adminUsers, setAdminUsers] = useState<AdminUser[]>([]);
  const [adminLoading, setAdminLoading] = useState(false);
  const [adminMessage, setAdminMessage] = useState('Admin user controls are ready.');
  const [persistenceState, setPersistenceState] = useState('SQL Server storage ready for saved work.');
  const [chat, setChat] = useState<ChatMessage[]>([
    { role: 'assistant', text: 'Upload or select a dataset, then ask about rows, columns, totals, averages, or outliers.' }
  ]);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem('theme', theme);
  }, [theme]);

  useEffect(() => {
    fetch('/api/config', { credentials: 'include' })
      .then((response) => readJson<ConfigResponse>(response))
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

    loadWorkspace();
  }, [authDisabled, configLoaded, token]);

  useEffect(() => {
    if (currentView === 'adminUsers' && user?.role === 'admin') {
      loadAdminUsers();
    }
  }, [currentView, user?.role]);

  function apiFetch(path: string, options: RequestInit = {}) {
    const headers = new Headers(options.headers);

    if (token) {
      headers.set('Authorization', `Bearer ${token}`);
    }

    return fetch(path, {
      ...options,
      headers,
      credentials: 'include'
    }).then((response) => {
      if (response.status === 401) {
        logout();
        throw new Error('Authentication required.');
      }

      return response;
    });
  }

  async function loadWorkspace() {
    try {
      const [
        meResponse,
        insightsResponse,
        workflowsResponse,
        datasetsResponse,
        dashboardsResponse,
        reportsResponse
      ] = await Promise.all([
        apiFetch('/api/auth/me'),
        apiFetch('/api/insights'),
        apiFetch('/api/workflows'),
        apiFetch('/api/datasets'),
        apiFetch('/api/dashboards'),
        apiFetch('/api/reports')
      ]);

      const mePayload = await readJson<{ user: User }>(meResponse);
      const insightsPayload = await readJson<InsightResponse>(insightsResponse);
      const workflowsPayload = await readJson<{ workflows: Workflow[] }>(workflowsResponse);
      const datasetsPayload = await readJson<{ datasets: Dataset[] }>(datasetsResponse);
      const dashboardsPayload = await readJson<{ dashboards: SavedDashboard[] }>(dashboardsResponse);
      const reportsPayload = await readJson<{ reports: ReportHistoryItem[] }>(reportsResponse);

      const savedDatasets = datasetsPayload.datasets ?? [];
      const savedDashboards = dashboardsPayload.dashboards ?? [];
      const latestDashboard = savedDashboards[0];
      const latestDataset = latestDashboard
        ? savedDatasets.find((dataset) => dataset.id === latestDashboard.datasetId) ?? latestDashboard.snapshot?.dataset
        : undefined;

      setUser(mePayload.user);
      setInsights(insightsPayload);
      setWorkflows(workflowsPayload.workflows ?? []);
      setDatasets(savedDatasets);
      setActiveDataset(latestDataset ?? savedDatasets[0] ?? null);
      setDashboards(savedDashboards);
      setReports(reportsPayload.reports ?? []);
      setStatus('Live');

      if (latestDashboard?.chartType) {
        setChartType(latestDashboard.chartType);
      }
    } catch {
      if (authDisabled) {
        setStatus('Offline');
      }
    }
  }

  function logout() {
    localStorage.removeItem('authToken');
    setToken('');
    setUser(null);
    setDatasets([]);
    setActiveDataset(null);
    setDashboards([]);
    setReports([]);
    setAdminUsers([]);
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
      const response = await fetch(`/api/auth/${credentials.mode}`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: credentials.name,
          email: credentials.email,
          password: credentials.password
        })
      });

      const payload = await readJson<Partial<AuthResponse> & { error?: string }>(response);

      if (!response.ok || !payload.token || !payload.user) {
        throw new Error(payload.error || 'Authentication failed.');
      }

      localStorage.setItem('authToken', payload.token);
      setToken(payload.token);
      setUser(payload.user);
      setAuthPassword('');
      setAuthMessage('Welcome back.');
      setStatus('Live');
      setCurrentView('dashboard');
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

  async function uploadDataset(file: File, worksheetName?: string) {
    const extension = file.name.split('.').pop()?.toLowerCase();
    if (!extension || !['csv', 'xlsx', 'xls'].includes(extension)) {
      setUploadState('Upload a .csv, .xlsx, or .xls file.');
      return;
    }

    const formData = new FormData();
    formData.append('file', file);
    if (worksheetName) {
      formData.append('worksheetName', worksheetName);
    }
    setUploadState(`Analyzing ${file.name}${worksheetName ? ` - ${worksheetName}` : ''}...`);

    try {
      const response = await apiFetch('/api/files/upload', {
        method: 'POST',
        body: formData
      });

      const dataset = await readJson<Dataset & { error?: string }>(response);

      if (!response.ok) {
        throw new Error(dataset.error || 'Upload failed.');
      }

      setLastUploadedFile(file);
      setActiveDataset(dataset);
      setDatasets((current: Dataset[]) => [dataset, ...current.filter((item) => item.id !== dataset.id)]);
      setChat([{ role: 'assistant', text: `I loaded ${dataset.fileName}${dataset.worksheetName ? ` (${dataset.worksheetName})` : ''}. Ask me what changed, what stands out, or how many rows it has.` }]);
      const storageLabel = authDisabled ? 'saved locally' : 'saved to SQL Server';
      const warning = dataset.warnings?.[0] ? ` ${dataset.warnings[0]}` : '';
      setUploadState(`${(dataset.fileType ?? extension).toUpperCase()} analysis ready and ${storageLabel}.${warning}`);
    } catch (error) {
      setUploadState(error instanceof Error ? error.message : 'Upload failed.');
    }
  }

  function handleDatasetUpload(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    if (file) {
      uploadDataset(file);
    }
  }

  function handleDrop(event: DragEvent<HTMLLabelElement>) {
    event.preventDefault();
    setIsDragging(false);

    const file = event.dataTransfer.files[0];
    if (file) {
      uploadDataset(file);
    }
  }

  function selectWorksheet(worksheetName: string) {
    if (!lastUploadedFile || activeDataset?.worksheetName === worksheetName) {
      return;
    }

    uploadDataset(lastUploadedFile, worksheetName);
  }

  async function askAssistant(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!activeDataset || !question.trim()) {
      return;
    }

    const nextQuestion = question.trim();
    setQuestion('');
    setChat((current: ChatMessage[]) => [...current, { role: 'user', text: nextQuestion }]);

    try {
      const response = await apiFetch(`/api/datasets/${activeDataset.id}/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: nextQuestion })
      });
      const payload = await readJson<{ answer: string }>(response);
      setChat((current: ChatMessage[]) => [...current, { role: 'assistant', text: payload.answer }]);
    } catch {
      setChat((current: ChatMessage[]) => [...current, { role: 'assistant', text: 'I could not answer that yet. Check the backend connection and try again.' }]);
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
    const payload = await readJson<{ dashboard: SavedDashboard }>(response);
    setDashboards((current: SavedDashboard[]) => [payload.dashboard, ...current.filter((dashboard) => dashboard.id !== payload.dashboard.id)]);
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
    const dashboardsPayload = await readJson<{ dashboards: SavedDashboard[] }>(dashboardsResponse);
    const reportsPayload = await readJson<{ reports: ReportHistoryItem[] }>(reportsResponse);
    setDashboards(dashboardsPayload.dashboards ?? []);
    setReports(reportsPayload.reports ?? []);
  }

  async function loadAdminUsers() {
    if (user?.role !== 'admin') {
      setCurrentView('dashboard');
      return;
    }

    setAdminLoading(true);
    try {
      const response = await apiFetch('/api/admin/users');
      const payload = await readJson<{ users: AdminUser[] }>(response);
      setAdminUsers(payload.users ?? []);
      setAdminMessage('User directory refreshed.');
    } catch (error) {
      setAdminMessage(error instanceof Error ? error.message : 'Could not load users.');
    } finally {
      setAdminLoading(false);
    }
  }

  async function updateAdminUser(userId: string, updates: Partial<Pick<AdminUser, 'role' | 'active'>>) {
    try {
      const response = await apiFetch(`/api/admin/users/${userId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates)
      });
      const payload = await readJson<{ user: AdminUser; error?: string }>(response);

      if (!response.ok) {
        throw new Error(payload.error || 'User update failed.');
      }

      setAdminUsers((current: AdminUser[]) => current.map((entry) => entry.id === payload.user.id ? payload.user : entry));
      setAdminMessage('User access updated.');
    } catch (error) {
      setAdminMessage(error instanceof Error ? error.message : 'User update failed.');
    }
  }

  async function toggleAdminUser(userRecord: AdminUser) {
    const action = userRecord.active ? 'disable' : 'enable';
    if (!window.confirm(`Confirm ${action} for ${userRecord.email}?`)) {
      return;
    }

    await updateAdminUser(userRecord.id, { active: !userRecord.active });
  }

  async function deleteAdminUser(userRecord: AdminUser) {
    if (!window.confirm(`Delete ${userRecord.email}? This removes their saved dashboards, reports, and sessions.`)) {
      return;
    }

    try {
      const response = await apiFetch(`/api/admin/users/${userRecord.id}`, { method: 'DELETE' });

      if (!response.ok) {
        const payload = await readJson<{ error?: string }>(response);
        throw new Error(payload.error || 'Delete failed.');
      }

      setAdminUsers((current: AdminUser[]) => current.filter((entry) => entry.id !== userRecord.id));
      setAdminMessage('User deleted.');
    } catch (error) {
      setAdminMessage(error instanceof Error ? error.message : 'Delete failed.');
    }
  }

  async function revokeAdminUserSessions(userRecord: AdminUser) {
    if (!window.confirm(`Revoke active sessions for ${userRecord.email}?`)) {
      return;
    }

    try {
      const response = await apiFetch(`/api/admin/users/${userRecord.id}/revoke`, { method: 'POST' });
      const payload = await readJson<{ error?: string; revoked?: boolean }>(response);

      if (!response.ok || !payload.revoked) {
        throw new Error(payload.error || 'Session revoke failed.');
      }

      setAdminMessage('User sessions revoked.');
    } catch (error) {
      setAdminMessage(error instanceof Error ? error.message : 'Session revoke failed.');
    }
  }

  async function downloadPdfReport() {
    if (!activeDataset) {
      return;
    }

    const lines = buildReportLines(activeDataset);
    downloadPdf(lines, `${activeDataset.fileName.replace(/\.(csv|xlsx|xls)$/i, '')}-report.pdf`);

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
    const payload = await readJson<{ report: ReportHistoryItem }>(response);
    setReports((current: ReportHistoryItem[]) => [payload.report, ...current]);
    setPersistenceState('Report downloaded and added to history.');
  }

  function downloadHistoricalReport(report: ReportHistoryItem) {
    const lines = report.content?.lines
      ?? (report.content?.dataset ? buildReportLines(report.content.dataset) : [
        'Business AI Platform Data Report',
        `Report: ${report.title}`,
        `Dataset: ${report.datasetName}`,
        `Created: ${new Date(report.createdAt).toLocaleString()}`
      ]);
    const fileName = `${report.title.replace(/[^a-z0-9]+/gi, '-').replace(/^-|-$/g, '').toLowerCase() || 'business-ai-report'}.pdf`;
    downloadPdf(lines, fileName);
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

  if (!authDisabled && !user) {
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
          {user?.role === 'admin' && (
            <button className={currentView === 'adminUsers' ? 'active' : ''} type="button" onClick={() => setCurrentView('adminUsers')}>Admin Users</button>
          )}
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
        ) : currentView === 'adminUsers' && user?.role === 'admin' ? (
          <section className="admin-page">
            <article className="panel admin-panel">
              <div className="panel-header">
                <div>
                  <p className="eyebrow">Admin control center</p>
                  <h2>User management</h2>
                </div>
                <button className="ghost-button compact" type="button" disabled={adminLoading} onClick={loadAdminUsers}>
                  {adminLoading ? 'Refreshing' : 'Refresh'}
                </button>
              </div>
              <p className="persistence-note">{adminMessage}</p>
              <div className="table-wrap admin-table-wrap">
                <table className="admin-table">
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Email</th>
                      <th>Role</th>
                      <th>Status</th>
                      <th>Created</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {adminUsers.map((adminUser) => (
                      <tr key={adminUser.id}>
                        <td>{adminUser.name}</td>
                        <td>{adminUser.email}</td>
                        <td>
                          <select
                            aria-label={`Role for ${adminUser.email}`}
                            className="role-select"
                            disabled={adminUser.id === user.id}
                            value={adminUser.role}
                            onChange={(event) => updateAdminUser(adminUser.id, { role: event.target.value as AdminUser['role'] })}
                          >
                            <option value="user">User</option>
                            <option value="admin">Admin</option>
                          </select>
                        </td>
                        <td>
                          <span className={`status-pill ${adminUser.active ? 'active' : 'disabled'}`}>
                            {adminUser.active ? 'Active' : 'Disabled'}
                          </span>
                        </td>
                        <td>{adminUser.createdAt ? new Date(adminUser.createdAt).toLocaleDateString() : 'Unknown'}</td>
                        <td>
                          <div className="admin-actions">
                            {adminUser.role !== 'admin' && (
                              <button className="ghost-button compact" type="button" onClick={() => updateAdminUser(adminUser.id, { role: 'admin' })}>
                                Promote
                              </button>
                            )}
                            <button className="ghost-button compact" type="button" disabled={adminUser.id === user.id} onClick={() => toggleAdminUser(adminUser)}>
                              {adminUser.active ? 'Disable' : 'Enable'}
                            </button>
                            <button className="ghost-button compact" type="button" disabled={adminUser.id === user.id} onClick={() => revokeAdminUserSessions(adminUser)}>
                              Revoke
                            </button>
                            <button className="ghost-button compact danger" type="button" disabled={adminUser.id === user.id} onClick={() => deleteAdminUser(adminUser)}>
                              Delete
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                    {!adminUsers.length && (
                      <tr>
                        <td colSpan={6}>No users found.</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
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
                  <p className="eyebrow">Data studio</p>
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
                <input accept=".csv,.xlsx,.xls,text/csv,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" type="file" onChange={handleDatasetUpload} />
                <strong>Drop CSV or Excel file here</strong>
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
                    <span>{dataset.rows} rows - {dataset.columns} columns - {(dataset.fileType ?? 'csv').toUpperCase()}</span>
                    {dataset.worksheetName && <small>{dataset.worksheetName}</small>}
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
                      <span>{(activeDataset?.fileType ?? 'csv').toUpperCase()}</span>
                    </div>
                  </div>

                  {activeDataset ? (
                    <>
                      <div className="file-meta">
                        <span>File type: {(activeDataset.fileType ?? 'csv').toUpperCase()}</span>
                        {activeDataset.worksheetName && <span>Worksheet: {activeDataset.worksheetName}</span>}
                      </div>
                      {(activeDataset.worksheets?.length ?? 0) > 1 && (
                        <div className="sheet-tabs" aria-label="Worksheet tabs">
                          {activeDataset.worksheets?.map((sheetName) => (
                            <button
                              className={activeDataset.worksheetName === sheetName ? 'active' : ''}
                              key={sheetName}
                              type="button"
                              onClick={() => selectWorksheet(sheetName)}
                            >
                              {sheetName}
                            </button>
                          ))}
                        </div>
                      )}
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
                    </>
                  ) : (
                    <div className="empty-state">No dataset uploaded yet.</div>
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
                      {(activeDataset?.insights ?? ['Upload a CSV or Excel file to generate clear, practical data insights.']).map((item) => (
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
    return <div className="empty-state chart-empty">Upload a dataset to build a chart.</div>;
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

async function readJson<T>(response: Response): Promise<T> {
  const contentType = response.headers.get('content-type') || '';
  const text = await response.text();

  if (!contentType.includes('application/json')) {
    const preview = text.trim().slice(0, 80);
    throw new Error(preview.startsWith('<!doctype') || preview.startsWith('<html')
      ? 'API route returned HTML instead of JSON. Check the production /api rewrite.'
      : 'API route returned a non-JSON response.');
  }

  return JSON.parse(text) as T;
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
    'Business AI Platform Data Report',
    `Dataset: ${dataset.fileName}`,
    `File type: ${(dataset.fileType ?? 'csv').toUpperCase()}`,
    ...(dataset.worksheetName ? [`Worksheet: ${dataset.worksheetName}`] : []),
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
  pdf += `xref\n0 ${objects.length + 1}\n0000000000 65535 f\n`;
  offsets.slice(1).forEach((offset) => {
    pdf += `${String(offset).padStart(10, '0')} 00000 n\n`;
  });
  pdf += `trailer << /Size ${objects.length + 1} /Root 1 0 R >>\nstartxref\n${xref}\n%%EOF`;
  return pdf;
}
