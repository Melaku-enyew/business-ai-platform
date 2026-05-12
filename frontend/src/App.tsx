import { ChangeEvent, CSSProperties, DragEvent, FormEvent, ReactNode, useEffect, useMemo, useState } from 'react';
import { Navigate, Route, Routes, useLocation, useNavigate } from 'react-router-dom';

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
type UserRole = 'owner' | 'admin' | 'manager' | 'employee' | 'viewer';
type AppView =
  | 'dashboard'
  | 'assistant'
  | 'accounting'
  | 'engineering'
  | 'hr'
  | 'crm'
  | 'dataProcessing'
  | 'analytics'
  | 'reports'
  | 'adminUsers'
  | 'settings'
  | 'contact';

type User = {
  id: string;
  companyId?: string;
  name: string;
  email: string;
  role: UserRole;
  active?: boolean;
  emailVerified?: boolean;
  profilePhotoUrl?: string;
  notificationSettings?: Record<string, boolean>;
  preferences?: Record<string, string | boolean>;
  twoFactorEnabled?: boolean;
  lastLoginAt?: string;
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
  durableStorage?: boolean;
  emailConfigured?: boolean;
  sessionTimeoutMinutes?: number;
  sessionWarningSeconds?: number;
};

type AuditLog = {
  id: string;
  actorEmail?: string;
  action: string;
  targetType?: string;
  targetId?: string;
  createdAt: string;
};

type SystemStatus = {
  status: string;
  storage: string;
  auth: string;
  uptimeSeconds: number;
  uploadLimitMb: number;
  maxSpreadsheetRows: number;
};

type ModuleRecord = {
  id: string;
  module: string;
  recordType: string;
  title: string;
  status: string;
  amount?: number | null;
  createdAt: string;
  updatedAt: string;
};

type Invitation = {
  id: string;
  email: string;
  role: UserRole;
  status: string;
  expiresAt: string;
  createdAt: string;
};

type EmailLog = {
  id: string;
  emailType: string;
  recipient: string;
  subject: string;
  provider?: string;
  status: string;
  error?: string;
  attempts: number;
  createdAt: string;
};

type ModuleMetrics = Record<string, { total: number; open: number }>;
type ModuleAction = {
  title: string;
  copy: string;
  type: string;
  path: string;
};
type WorkspaceRoute = ModuleAction & {
  module: string;
  moduleLabel: string;
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

const ownerEmail = 'melakue@metenovaai.com';
const roleOptions: UserRole[] = ['viewer', 'employee', 'manager', 'admin', 'owner'];

const moduleNav: Array<{ view: AppView; label: string; adminOnly?: boolean }> = [
  { view: 'dashboard', label: 'Dashboard' },
  { view: 'assistant', label: 'AI Assistant' },
  { view: 'accounting', label: 'Accounting' },
  { view: 'engineering', label: 'Engineering' },
  { view: 'hr', label: 'HR' },
  { view: 'crm', label: 'CRM' },
  { view: 'dataProcessing', label: 'Data Processing' },
  { view: 'analytics', label: 'Analytics' },
  { view: 'reports', label: 'Reports' },
  { view: 'adminUsers', label: 'Admin Panel', adminOnly: true },
  { view: 'settings', label: 'Settings' },
  { view: 'contact', label: 'Contact Us' }
];

const moduleCards: Record<string, ModuleAction[]> = {
  accounting: [
    { title: 'Invoices', copy: 'Track billing status, aging, approvals, and payment readiness.', type: 'invoice', path: '/accounting/invoices' },
    { title: 'Expense Tracking', copy: 'Classify expenses, flag unusual spend, and prepare monthly close.', type: 'expense', path: '/accounting/expenses' },
    { title: 'Payroll', copy: 'Review payroll cycles, department allocations, and exception queues.', type: 'payroll', path: '/accounting/payroll' },
    { title: 'Financial Reports', copy: 'Generate budget, cash flow, tax, and executive finance reports.', type: 'financial_report', path: '/accounting/financial-reports' },
    { title: 'AI Financial Assistant', copy: 'Ask concise questions about trends, budget variance, and risks.', type: 'assistant', path: '/accounting/ai-financial-assistant' }
  ],
  engineering: [
    { title: 'Project Management', copy: 'Organize milestones, owners, blockers, and project health.', type: 'project', path: '/engineering/projects' },
    { title: 'Task Tracking', copy: 'Prioritize work, assign teams, and monitor delivery commitments.', type: 'task', path: '/engineering/tasks' },
    { title: 'Blueprint/File Management', copy: 'Prepare document upload, file versioning, and blueprint review workflows.', type: 'blueprint', path: '/engineering/blueprints' },
    { title: 'Progress Reports', copy: 'Summarize status, risks, dependencies, and next actions.', type: 'progress_report', path: '/engineering/progress-reports' }
  ],
  hr: [
    { title: 'Employee Records', copy: 'Centralize profiles, roles, onboarding status, and access policies.', type: 'employee', path: '/hr/employees' },
    { title: 'Attendance', copy: 'Prepare attendance, time, and shift tracking workflows.', type: 'attendance', path: '/hr/attendance' },
    { title: 'Hiring', copy: 'Track candidates, interviews, and onboarding steps.', type: 'hiring', path: '/hr/hiring' },
    { title: 'Leave Management', copy: 'Manage leave requests, approvals, and team coverage.', type: 'leave', path: '/hr/leave-management' }
  ],
  crm: [
    { title: 'Clients', copy: 'Track client accounts, contacts, and relationship health.', type: 'client', path: '/crm/clients' },
    { title: 'Leads', copy: 'Capture prospects and qualify opportunities.', type: 'lead', path: '/crm/leads' },
    { title: 'Sales Pipeline', copy: 'Manage stages, value, and next actions.', type: 'pipeline', path: '/crm/sales-pipeline' },
    { title: 'Customer Notes', copy: 'Log interaction history and follow-ups.', type: 'note', path: '/crm/customer-notes' }
  ],
  dataProcessing: [
    { title: 'Data Cleanup', copy: 'Normalize messy columns, repair values, and prepare trusted datasets.', type: 'cleanup', path: '/data-processing/cleanup' },
    { title: 'Duplicate Detection', copy: 'Find repeated records and review merge candidates.', type: 'dedupe', path: '/data-processing/duplicates' },
    { title: 'Validation', copy: 'Check completeness, types, outliers, and required fields.', type: 'validation', path: '/data-processing/validation' },
    { title: 'Import/Export Tools', copy: 'Prepare batch import/export pipelines for enterprise systems.', type: 'import_export', path: '/data-processing/import-export' },
    { title: 'Data Quality Reports', copy: 'Generate executive quality and cleanup recommendations.', type: 'quality_report', path: '/data-processing/quality-reports' }
  ]
};

const moduleLabels: Record<string, string> = {
  accounting: 'Accounting',
  engineering: 'Engineering',
  hr: 'HR',
  crm: 'CRM',
  dataProcessing: 'Data Processing'
};

const workspaceRoutes: WorkspaceRoute[] = Object.entries(moduleCards).flatMap(([module, cards]) =>
  cards.map((card) => ({ ...card, module, moduleLabel: moduleLabels[module] ?? module }))
);

export function App() {
  const navigate = useNavigate();
  const location = useLocation();
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
  const [inviteToken, setInviteToken] = useState('');
  const [inviteName, setInviteName] = useState('');
  const [invitePassword, setInvitePassword] = useState('');
  const [recoveryEmail, setRecoveryEmail] = useState('');
  const [recoveryMessage, setRecoveryMessage] = useState('');
  const [profileName, setProfileName] = useState('');
  const [profilePhotoUrl, setProfilePhotoUrl] = useState('');
  const [securityMessage, setSecurityMessage] = useState('');
  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [twoFactorEnabled, setTwoFactorEnabled] = useState(false);
  const [contactName, setContactName] = useState('');
  const [contactEmail, setContactEmail] = useState('');
  const [contactMessage, setContactMessage] = useState('');
  const [contactContext, setContactContext] = useState('');
  const [supportMessage, setSupportMessage] = useState('');
  const [supportSending, setSupportSending] = useState(false);
  const [currentView, setCurrentView] = useState<AppView>('dashboard');
  const [accountOpen, setAccountOpen] = useState(false);
  const [question, setQuestion] = useState('');
  const [dashboards, setDashboards] = useState<SavedDashboard[]>([]);
  const [reports, setReports] = useState<ReportHistoryItem[]>([]);
  const [adminUsers, setAdminUsers] = useState<AdminUser[]>([]);
  const [adminLoading, setAdminLoading] = useState(false);
  const [adminMessage, setAdminMessage] = useState('Admin user controls are ready.');
  const [auditLogs, setAuditLogs] = useState<AuditLog[]>([]);
  const [systemStatus, setSystemStatus] = useState<SystemStatus | null>(null);
  const [invitations, setInvitations] = useState<Invitation[]>([]);
  const [emailLogs, setEmailLogs] = useState<EmailLog[]>([]);
  const [inviteEmail, setInviteEmail] = useState('');
  const [inviteRole, setInviteRole] = useState<UserRole>('employee');
  const [moduleMetrics, setModuleMetrics] = useState<ModuleMetrics>({});
  const [moduleRecords, setModuleRecords] = useState<ModuleRecord[]>([]);
  const [moduleMessage, setModuleMessage] = useState('Select a tool to create a workspace record.');
  const [moduleForm, setModuleForm] = useState({ title: '', recordType: 'item', amount: '' });
  const [recordSearch, setRecordSearch] = useState('');
  const [recordStatusFilter, setRecordStatusFilter] = useState('all');
  const [selectedRecord, setSelectedRecord] = useState<ModuleRecord | null>(null);
  const [persistenceState, setPersistenceState] = useState('Enterprise-grade secure cloud storage ready.');
  const [durableStorage, setDurableStorage] = useState(true);
  const [emailConfigured, setEmailConfigured] = useState(false);
  const [sessionTimeoutMs, setSessionTimeoutMs] = useState(15 * 60 * 1000);
  const [sessionWarningSeconds, setSessionWarningSeconds] = useState(60);
  const [showSessionWarning, setShowSessionWarning] = useState(false);
  const [sessionSecondsLeft, setSessionSecondsLeft] = useState(60);
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
        setDurableStorage(config.durableStorage !== false);
        setEmailConfigured(Boolean(config.emailConfigured));
        setSessionTimeoutMs(Math.max(config.sessionTimeoutMinutes ?? 15, 5) * 60 * 1000);
        setSessionWarningSeconds(Math.max(config.sessionWarningSeconds ?? 60, 15));
        setPersistenceState(config.durableStorage === false ? 'Protected workspace storage needs to be connected before workspace changes can be saved permanently.' : 'Enterprise-grade secure cloud storage ready.');
      })
      .catch(() => {
        setAuthDisabled(false);
        setPersistenceState('Protected workspace infrastructure ready.');
      })
      .finally(() => setConfigLoaded(true));
  }, []);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    const tokenParam = params.get('token');
    if (window.location.pathname.includes('accept-invite') && tokenParam) {
      setInviteToken(tokenParam);
      setAuthMessage('Complete your invitation to join this workspace.');
    }
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
    if (currentView === 'adminUsers' && canManageUsers(user)) {
      loadAdminUsers();
    }
  }, [currentView, user?.role]);

  useEffect(() => {
    if (!user) {
      return;
    }

    setProfileName(user.name);
    setProfilePhotoUrl(user.profilePhotoUrl ?? '');
    setTwoFactorEnabled(Boolean(user.twoFactorEnabled));
    setContactName(user.name);
    setContactEmail(user.email);
  }, [user]);

  useEffect(() => {
    if (['accounting', 'engineering', 'hr', 'crm', 'dataProcessing'].includes(currentView)) {
      loadModuleRecords(currentView);
    }
  }, [currentView]);

  useEffect(() => {
    const workspaceRoute = workspaceRoutes.find((route) => route.path === location.pathname);
    if (workspaceRoute) {
      setCurrentView(workspaceRoute.module as AppView);
      return;
    }
    if (location.pathname === '/analytics/dashboard') {
      setCurrentView('analytics');
    } else if (location.pathname === '/reports/history') {
      setCurrentView('reports');
    } else if (location.pathname.startsWith('/admin/')) {
      setCurrentView('adminUsers');
    }
  }, [location.pathname]);

  useEffect(() => {
    const syncAuth = (event: StorageEvent) => {
      if (event.key === 'authToken') {
        const nextToken = event.newValue || '';
        if (!nextToken) {
          logout('Signed out in another tab.');
          return;
        }
        setToken(nextToken);
      }
      if (event.key === 'metenovaLogoutAt' && event.newValue) {
        logout('Session ended in another tab.');
      }
    };

    window.addEventListener('storage', syncAuth);
    return () => window.removeEventListener('storage', syncAuth);
  }, []);

  useEffect(() => {
    if (!user || authDisabled) {
      setShowSessionWarning(false);
      return;
    }

    const activityEvents = ['mousemove', 'keydown', 'click', 'scroll', 'touchstart', 'visibilitychange'];
    const markActive = () => {
      if (document.visibilityState !== 'hidden') {
        localStorage.setItem('metenovaLastActivityAt', String(Date.now()));
        setShowSessionWarning(false);
      }
    };
    const checkSession = () => {
      const lastActivity = Number(localStorage.getItem('metenovaLastActivityAt') || Date.now());
      const elapsed = Date.now() - lastActivity;
      const remainingMs = sessionTimeoutMs - elapsed;
      const nextSeconds = Math.max(Math.ceil(remainingMs / 1000), 0);
      setSessionSecondsLeft(nextSeconds);

      if (remainingMs <= 0) {
        localStorage.setItem('metenovaLogoutAt', String(Date.now()));
        logoutRemote('Session expired after inactivity.');
        return;
      }

      setShowSessionWarning(nextSeconds <= sessionWarningSeconds);
    };

    markActive();
    activityEvents.forEach((eventName) => window.addEventListener(eventName, markActive, { passive: true }));
    const interval = window.setInterval(checkSession, 1000);
    return () => {
      activityEvents.forEach((eventName) => window.removeEventListener(eventName, markActive));
      window.clearInterval(interval);
    };
  }, [user?.id, authDisabled, sessionTimeoutMs, sessionWarningSeconds]);

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
        reportsResponse,
        moduleMetricsResponse
      ] = await Promise.all([
        apiFetch('/api/auth/me'),
        apiFetch('/api/insights'),
        apiFetch('/api/workflows'),
        apiFetch('/api/datasets'),
        apiFetch('/api/dashboards'),
        apiFetch('/api/reports'),
        apiFetch('/api/modules/metrics')
      ]);

      const mePayload = await readJson<{ user: User }>(meResponse);
      const insightsPayload = await readJson<InsightResponse>(insightsResponse);
      const workflowsPayload = await readJson<{ workflows: Workflow[] }>(workflowsResponse);
      const datasetsPayload = await readJson<{ datasets: Dataset[] }>(datasetsResponse);
      const dashboardsPayload = await readJson<{ dashboards: SavedDashboard[] }>(dashboardsResponse);
      const reportsPayload = await readJson<{ reports: ReportHistoryItem[] }>(reportsResponse);
      const moduleMetricsPayload = await readJson<{ metrics: ModuleMetrics }>(moduleMetricsResponse);

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
      setModuleMetrics(moduleMetricsPayload.metrics ?? {});
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

  function logout(message = 'Signed out') {
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
    setAuthMessage(message);
    setShowSessionWarning(false);
  }

  async function logoutRemote(message = 'Signed out') {
    try {
      if (token) {
        await apiFetch('/api/auth/logout', { method: 'POST' });
      }
    } finally {
      localStorage.setItem('metenovaLogoutAt', String(Date.now()));
      logout(message);
    }
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
      localStorage.setItem('metenovaLastActivityAt', String(Date.now()));
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

  async function acceptInvite(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setAuthMessage('Activating your workspace invitation...');

    try {
      const response = await fetch('/api/auth/accept-invite', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          token: inviteToken,
          name: inviteName,
          password: invitePassword
        })
      });
      const payload = await readJson<Partial<AuthResponse> & { error?: string }>(response);
      if (!response.ok || !payload.token || !payload.user) {
        throw new Error(payload.error || 'Invitation could not be accepted.');
      }

      localStorage.setItem('authToken', payload.token);
      localStorage.setItem('metenovaLastActivityAt', String(Date.now()));
      window.history.replaceState({}, '', '/');
      setToken(payload.token);
      setUser(payload.user);
      setInviteToken('');
      setInvitePassword('');
      setAuthMessage('Invitation accepted.');
      setStatus('Live');
      setCurrentView('dashboard');
    } catch (error) {
      setAuthMessage(error instanceof Error ? error.message : 'Invitation could not be accepted.');
    }
  }

  async function requestPasswordReset() {
    if (!recoveryEmail.trim()) {
      setRecoveryMessage('Enter your account email first.');
      return;
    }

    try {
      const response = await fetch('/api/auth/forgot-password', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: recoveryEmail })
      });
      const payload = await readJson<{ message: string; resetUrl?: string }>(response);
      setRecoveryMessage(payload.resetUrl ? `${payload.message} Secure reset link prepared for this environment.` : payload.message);
    } catch (error) {
      setRecoveryMessage(error instanceof Error ? error.message : 'Recovery request failed.');
    }
  }

  async function requestUsernameRecovery() {
    if (!recoveryEmail.trim()) {
      setRecoveryMessage('Enter your account email first.');
      return;
    }

    try {
      const response = await fetch('/api/auth/recover-username', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: recoveryEmail })
      });
      const payload = await readJson<{ message: string; username?: string }>(response);
      setRecoveryMessage(payload.username ? `${payload.message} Username: ${payload.username}` : payload.message);
    } catch (error) {
      setRecoveryMessage(error instanceof Error ? error.message : 'Recovery request failed.');
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
      const storageLabel = 'saved to your protected workspace';
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
    setPersistenceState('Dashboard saved to your protected workspace.');
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
    if (!canManageUsers(user)) {
      setCurrentView('dashboard');
      return;
    }

    setAdminLoading(true);
    try {
      const [usersResponse, auditResponse, systemResponse, invitationsResponse, emailLogsResponse] = await Promise.all([
        apiFetch('/api/admin/users'),
        apiFetch('/api/admin/audit-logs'),
        apiFetch('/api/admin/system'),
        apiFetch('/api/admin/invitations'),
        apiFetch('/api/admin/email-logs')
      ]);
      const payload = await readJson<{ users: AdminUser[] }>(usersResponse);
      const auditPayload = await readJson<{ auditLogs: AuditLog[] }>(auditResponse);
      const systemPayload = await readJson<SystemStatus>(systemResponse);
      const invitationPayload = await readJson<{ invitations: Invitation[] }>(invitationsResponse);
      const emailLogPayload = await readJson<{ emailLogs: EmailLog[] }>(emailLogsResponse);
      setAdminUsers(payload.users ?? []);
      setAuditLogs(auditPayload.auditLogs ?? []);
      setSystemStatus(systemPayload);
      setInvitations(invitationPayload.invitations ?? []);
      setEmailLogs(emailLogPayload.emailLogs ?? []);
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
      if (payload.user.id === user?.id) {
        setUser(payload.user);
        const refreshResponse = await apiFetch('/api/auth/refresh', { method: 'POST' });
        const refreshPayload = await readJson<AuthResponse>(refreshResponse);
        localStorage.setItem('authToken', refreshPayload.token);
        setToken(refreshPayload.token);
      }
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

  async function inviteWorkspaceUser(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!inviteEmail.trim()) {
      setAdminMessage('Enter an email address before sending an invite.');
      return;
    }

    try {
      const response = await apiFetch('/api/admin/invitations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: inviteEmail, role: inviteRole })
      });
      const payload = await readJson<{ invitation: Invitation; message: string; error?: string; acceptUrl?: string }>(response);
      if (!response.ok) {
        throw new Error(payload.error || 'Invitation failed.');
      }

      setInvitations((current: Invitation[]) => [payload.invitation, ...current]);
      setInviteEmail('');
      setInviteRole('employee');
      setAdminMessage(payload.acceptUrl ? `${payload.message} Secure invite link prepared for this environment.` : payload.message);
    } catch (error) {
      setAdminMessage(error instanceof Error ? error.message : 'Invitation failed.');
    }
  }

  async function retryEmailLog(log: EmailLog) {
    try {
      const response = await apiFetch(`/api/admin/email-logs/${log.id}/retry`, { method: 'POST' });
      const payload = await readJson<{ emailLog: EmailLog; message?: string; error?: string }>(response);
      if (!response.ok) {
        throw new Error(payload.error || 'Email retry failed.');
      }
      setEmailLogs((current: EmailLog[]) => current.map((entry) => entry.id === log.id ? payload.emailLog : entry));
      setAdminMessage(payload.message ?? 'Email retry complete.');
    } catch (error) {
      setAdminMessage(error instanceof Error ? error.message : 'Email retry failed.');
    }
  }

  async function saveProfile(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    try {
      const response = await apiFetch('/api/profile', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: profileName,
          profilePhotoUrl,
          twoFactorEnabled,
          notificationSettings: {
            emailReports: true,
            securityAlerts: true,
            productUpdates: false
          },
          preferences: {
            theme,
            executiveMode: true
          }
        })
      });
      const payload = await readJson<{ user: User }>(response);
      setUser(payload.user);
      setSecurityMessage('Profile and preferences saved.');
    } catch (error) {
      setSecurityMessage(error instanceof Error ? error.message : 'Profile update failed.');
    }
  }

  async function changePassword(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    try {
      const response = await apiFetch('/api/profile/change-password', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ currentPassword, newPassword })
      });
      const payload = await readJson<{ message: string; error?: string }>(response);

      if (!response.ok) {
        throw new Error(payload.error || 'Password change failed.');
      }

      setCurrentPassword('');
      setNewPassword('');
      setSecurityMessage(payload.message);
    } catch (error) {
      setSecurityMessage(error instanceof Error ? error.message : 'Password change failed.');
    }
  }

  async function requestEmailVerification() {
    try {
      const response = await apiFetch('/api/auth/request-verification', { method: 'POST' });
      const payload = await readJson<{ message: string; verificationUrl?: string }>(response);
      setSecurityMessage(payload.verificationUrl ? `${payload.message} Secure verification link prepared for this environment.` : payload.message);
    } catch (error) {
      setSecurityMessage(error instanceof Error ? error.message : 'Verification request failed.');
    }
  }

  async function submitContact(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = event.currentTarget;
    const attachments = form.querySelector<HTMLInputElement>('input[name="attachments"]')?.files;
    const body = new FormData();
    body.append('name', contactName);
    body.append('email', contactEmail);
    body.append('message', contactMessage);
    body.append('pageContext', contactContext || currentView);
    Array.from(attachments ?? []).slice(0, 3).forEach((file) => body.append('attachments', file));
    setSupportSending(true);

    try {
      const response = await apiFetch('/api/contact', {
        method: 'POST',
        body
      });
      const payload = await readJson<{ message: string; error?: string; delivery?: { status?: string; error?: string } }>(response);

      if (!response.ok) {
        throw new Error(payload.error || 'Support request failed.');
      }

      setContactMessage('');
      setContactContext('');
      form.reset();
      setSupportMessage(payload.delivery?.status === 'failed'
        ? `${payload.message} Delivery detail: ${payload.delivery.error ?? 'Email delivery is not configured.'}`
        : payload.message);
    } catch (error) {
      setSupportMessage(error instanceof Error ? error.message : 'Support request failed.');
    } finally {
      setSupportSending(false);
    }
  }

  async function loadModuleRecords(module: string) {
    try {
      const response = await apiFetch(`/api/modules/${module}/records`);
      const payload = await readJson<{ records: ModuleRecord[] }>(response);
      setModuleRecords(payload.records ?? []);
      setModuleMessage(payload.records?.length ? 'Workspace records loaded.' : 'No records yet. Create the first item for this module.');
    } catch (error) {
      setModuleMessage(error instanceof Error ? error.message : 'Could not load module records.');
    }
  }

  async function createModuleItem(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (!moduleForm.title.trim()) {
      setModuleMessage('Enter a title before saving.');
      return;
    }

    try {
      const response = await apiFetch(`/api/modules/${currentView}/records`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          title: moduleForm.title,
          recordType: moduleForm.recordType,
          amount: moduleForm.amount,
          metadata: { source: 'module-ui' }
        })
      });
      const payload = await readJson<{ record: ModuleRecord; error?: string }>(response);
      if (!response.ok) {
        throw new Error(payload.error || 'Could not save module record.');
      }
      setModuleRecords((current: ModuleRecord[]) => [payload.record, ...current]);
      setModuleMetrics((current) => ({
        ...current,
        [currentView]: {
          total: (current[currentView]?.total ?? 0) + 1,
          open: (current[currentView]?.open ?? 0) + 1
        }
      }));
      setModuleForm({ title: '', recordType: moduleCards[currentView]?.[0]?.type ?? 'item', amount: '' });
      setModuleMessage('Record saved to this company workspace.');
    } catch (error) {
      setModuleMessage(error instanceof Error ? error.message : 'Could not save module record.');
    }
  }

  async function updateModuleItem(record: ModuleRecord, updates: Partial<Pick<ModuleRecord, 'status' | 'title'>>) {
    try {
      const response = await apiFetch(`/api/modules/${record.module}/records/${record.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates)
      });
      const payload = await readJson<{ record: ModuleRecord; error?: string }>(response);
      if (!response.ok) {
        throw new Error(payload.error || 'Could not update module record.');
      }
      setModuleRecords((current: ModuleRecord[]) => current.map((entry) => entry.id === record.id ? payload.record : entry));
      setSelectedRecord((current: ModuleRecord | null) => current?.id === record.id ? payload.record : current);
      setModuleMessage('Record updated.');
    } catch (error) {
      setModuleMessage(error instanceof Error ? error.message : 'Could not update module record.');
    }
  }

  async function editModuleItem(record: ModuleRecord) {
    const title = window.prompt('Update record title', record.title);
    if (title == null || !title.trim() || title.trim() === record.title) {
      return;
    }
    await updateModuleItem(record, { title: title.trim() });
  }

  async function deleteModuleItem(record: ModuleRecord) {
    if (!window.confirm(`Delete ${record.title}?`)) {
      return;
    }

    try {
      const response = await apiFetch(`/api/modules/${record.module}/records/${record.id}`, { method: 'DELETE' });
      if (!response.ok) {
        const payload = await readJson<{ error?: string }>(response);
        throw new Error(payload.error || 'Could not delete module record.');
      }
      setModuleRecords((current: ModuleRecord[]) => current.filter((entry) => entry.id !== record.id));
      setSelectedRecord((current: ModuleRecord | null) => current?.id === record.id ? null : current);
      setModuleMetrics((current) => ({
        ...current,
        [record.module]: {
          total: Math.max((current[record.module]?.total ?? 1) - 1, 0),
          open: Math.max((current[record.module]?.open ?? 1) - (record.status === 'closed' ? 0 : 1), 0)
        }
      }));
      setModuleMessage('Record deleted.');
    } catch (error) {
      setModuleMessage(error instanceof Error ? error.message : 'Could not delete module record.');
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
        'Metenova AI Data Report',
        `Report: ${report.title}`,
        `Dataset: ${report.datasetName}`,
        `Created: ${new Date(report.createdAt).toLocaleString()}`
      ]);
    const fileName = `${report.title.replace(/[^a-z0-9]+/gi, '-').replace(/^-|-$/g, '').toLowerCase() || 'business-ai-report'}.pdf`;
    downloadPdf(lines, fileName);
    setPersistenceState(`Downloaded ${report.title}.`);
  }

  function openSettings() {
    navigate('/');
    setCurrentView('settings');
    setAccountOpen(false);
  }

  function openView(view: AppView) {
    if (view === 'analytics') {
      navigate('/analytics/dashboard');
      return;
    }
    if (view === 'reports') {
      navigate('/reports/history');
      return;
    }
    if (view === 'adminUsers') {
      navigate('/admin/users');
      return;
    }
    navigate('/');
    setCurrentView(view);
  }

  if (!configLoaded) {
    return (
      <main className="auth-shell">
        <section className="auth-card">
          <div className="brand auth-brand">
            <span className="brand-icon">AI</span>
            <span>Metenova AI</span>
          </div>
          <p className="auth-copy">Loading secure workspace...</p>
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
            <span>Metenova AI</span>
          </div>
          <p className="eyebrow">Secure workspace</p>
          <h1>{inviteToken ? 'Accept invitation' : authMode === 'login' ? 'Welcome back' : 'Create your account'}</h1>
          <p className="auth-copy">{authMessage}</p>
          {inviteToken ? (
            <form className="auth-form" onSubmit={acceptInvite}>
              <label>
                Name
                <input autoComplete="name" value={inviteName} onChange={(event) => setInviteName(event.target.value)} />
              </label>
              <label>
                Password
                <input autoComplete="new-password" minLength={8} type="password" value={invitePassword} onChange={(event) => setInvitePassword(event.target.value)} />
              </label>
              <button type="submit">Join workspace</button>
            </form>
          ) : (
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
          )}
          {!inviteToken && (
            <>
              <div className="recovery-panel">
                <input
                  autoComplete="email"
                  placeholder="Email for recovery"
                  type="email"
                  value={recoveryEmail}
                  onChange={(event) => setRecoveryEmail(event.target.value)}
                />
                <div>
                  <button type="button" onClick={requestPasswordReset}>Forgot password</button>
                  <button type="button" onClick={requestUsernameRecovery}>Recover username</button>
                </div>
                {recoveryMessage && <p>{recoveryMessage}</p>}
              </div>
              <button className="link-button" type="button" onClick={() => setAuthMode(authMode === 'login' ? 'signup' : 'login')}>
                {authMode === 'login' ? 'Need an account? Sign up' : 'Already have an account? Log in'}
              </button>
            </>
          )}
        </section>
      </main>
    );
  }

  return (
    <main className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <span className="brand-icon">AI</span>
          <span>Metenova AI</span>
        </div>
        <nav aria-label="Primary">
          {moduleNav
            .filter((item) => !item.adminOnly || canManageUsers(user))
            .map((item) => (
              <button
              className={currentView === item.view ? 'active' : ''}
              key={item.view}
              type="button"
              onClick={() => item.view === 'settings' ? openSettings() : openView(item.view)}
            >
                {item.label}
              </button>
            ))}
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
            <button className="ghost-button" type="button" onClick={() => {
              setContactContext(currentView);
              openView('contact');
            }}>
              Support
            </button>
            <div className="account-menu">
              <button className="account-button" type="button" onClick={() => setAccountOpen((open) => !open)}>
                <span className="avatar">{(user?.name || 'A').slice(0, 1).toUpperCase()}</span>
                <span>
                  <strong>{user?.name ?? 'Metenova workspace'}</strong>
                  <small>{roleLabel(user?.role)}</small>
                </span>
              </button>
              {accountOpen && (
                <div className="account-dropdown">
                  <div>
                    <strong>{user?.name ?? 'Metenova workspace'}</strong>
                    <span>{user?.email ?? 'workspace@metenovaai.com'}</span>
                  </div>
                  <button type="button" onClick={openSettings}>Profile settings</button>
                  {!authDisabled && <button type="button" onClick={() => logoutRemote()}>Log out</button>}
                </div>
              )}
            </div>
            <span className={`status ${status.toLowerCase()}`}>{status}</span>
          </div>
        </header>

        {location.pathname !== '/' ? (
          <RoutedPages
            apiFetch={apiFetch}
            auditLogs={auditLogs}
            canManage={canManageUsers(user)}
            dashboards={dashboards}
            deleteAdminUser={deleteAdminUser}
            downloadHistoricalReport={downloadHistoricalReport}
            reports={reports}
            systemStatus={systemStatus}
            updateAdminUser={updateAdminUser}
            users={adminUsers}
          />
        ) : currentView === 'settings' ? (
          <section className="settings-grid">
            <article className="panel profile-panel">
              <p className="eyebrow">Account</p>
              <h2>Profile settings</h2>
              <div className="profile-card">
                {user?.profilePhotoUrl ? (
                  <img className="avatar large photo-avatar" src={user.profilePhotoUrl} alt="" />
                ) : (
                  <span className="avatar large">{(user?.name || 'A').slice(0, 1).toUpperCase()}</span>
                )}
                <div>
                  <strong>{user?.name ?? 'Metenova workspace'}</strong>
                  <span>{user?.email ?? 'workspace@metenovaai.com'}</span>
                </div>
              </div>
              <form className="settings-form" onSubmit={saveProfile}>
                <label>
                  Display name
                  <input value={profileName} onChange={(event) => setProfileName(event.target.value)} />
                </label>
                <label>
                  Profile picture URL
                  <input placeholder="https://..." value={profilePhotoUrl} onChange={(event) => setProfilePhotoUrl(event.target.value)} />
                </label>
                <label className="toggle-row">
                  <input checked={twoFactorEnabled} type="checkbox" onChange={(event) => setTwoFactorEnabled(event.target.checked)} />
                  Optional 2FA enabled
                </label>
                <button type="submit">Save profile</button>
              </form>
              <dl className="settings-list">
                <div>
                  <dt>Workspace role</dt>
                  <dd>{roleLabel(user?.role)}</dd>
                </div>
                <div>
                  <dt>Access policy</dt>
                  <dd>{canManageUsers(user) ? 'Can manage workspace users, reports, and security controls.' : 'Can access modules based on assigned permissions.'}</dd>
                </div>
                <div>
                  <dt>Email verification</dt>
                  <dd>{user?.emailVerified ? 'Verified' : 'Not verified'}</dd>
                </div>
                <div>
                  <dt>Session security</dt>
                  <dd>Protected sessions with revocation and account lockout protection.</dd>
                </div>
              </dl>
            </article>
            <article className="panel">
              <p className="eyebrow">Security</p>
              <h2>Account protection</h2>
              <form className="settings-form" onSubmit={changePassword}>
                <label>
                  Current password
                  <input type="password" value={currentPassword} onChange={(event) => setCurrentPassword(event.target.value)} />
                </label>
                <label>
                  New password
                  <input minLength={8} type="password" value={newPassword} onChange={(event) => setNewPassword(event.target.value)} />
                </label>
                <button type="submit">Change password</button>
              </form>
              <button className="ghost-button support-button" type="button" onClick={requestEmailVerification}>
                Send verification email
              </button>
              {securityMessage && <p className="persistence-note">{securityMessage}</p>}
              <ul className="settings-notes">
                <li>Owner permissions are permanently reserved for {ownerEmail}.</li>
                <li>Roles, active status, settings, and preferences persist in protected workspace infrastructure.</li>
                <li>Failed login attempts trigger temporary account lockout.</li>
              </ul>
            </article>
          </section>
        ) : currentView === 'adminUsers' && canManageUsers(user) ? (
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
                            disabled={adminUser.email === ownerEmail && user.email !== ownerEmail}
                            value={adminUser.role}
                            onChange={(event) => updateAdminUser(adminUser.id, { role: event.target.value as AdminUser['role'] })}
                          >
                            {roleOptions.map((role) => (
                              <option key={role} value={role}>{roleLabel(role)}</option>
                            ))}
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
                            {adminUser.role !== 'admin' && adminUser.role !== 'owner' && (
                              <button className="ghost-button compact" type="button" onClick={() => updateAdminUser(adminUser.id, { role: 'admin' })}>
                                Promote
                              </button>
                            )}
                            <button className="ghost-button compact" type="button" disabled={adminUser.id === user.id || adminUser.email === ownerEmail} onClick={() => toggleAdminUser(adminUser)}>
                              {adminUser.active ? 'Disable' : 'Enable'}
                            </button>
                            <button className="ghost-button compact" type="button" disabled={adminUser.id === user.id} onClick={() => revokeAdminUserSessions(adminUser)}>
                              Revoke
                            </button>
                            <button className="ghost-button compact danger" type="button" disabled={adminUser.id === user.id || adminUser.email === ownerEmail} onClick={() => deleteAdminUser(adminUser)}>
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
              <div className="admin-ops-grid">
                <article>
                  <h3>Invite workspace user</h3>
                  <form className="module-form invite-form" onSubmit={inviteWorkspaceUser}>
                    <input placeholder="employee@company.com" type="email" value={inviteEmail} onChange={(event) => setInviteEmail(event.target.value)} />
                    <select value={inviteRole} onChange={(event) => setInviteRole(event.target.value as UserRole)}>
                      {roleOptions.filter((role) => role !== 'owner' || user.email === ownerEmail).map((role) => (
                        <option key={role} value={role}>{roleLabel(role)}</option>
                      ))}
                    </select>
                    <button type="submit">Send invite</button>
                  </form>
                  <div className="audit-list">
                    {invitations.slice(0, 4).map((invite) => (
                      <div key={invite.id}>
                        <strong>{invite.email}</strong>
                        <span>{roleLabel(invite.role)} - {invite.status} - expires {new Date(invite.expiresAt).toLocaleDateString()}</span>
                      </div>
                    ))}
                    {!invitations.length && <p className="muted">No pending invitations yet.</p>}
                  </div>
                </article>
                <article>
                  <h3>Email delivery</h3>
                  <div className="audit-list">
                    {emailLogs.slice(0, 4).map((log) => (
                      <div key={log.id}>
                        <strong>{log.emailType} - {log.status}</strong>
                        <span>{log.recipient} - {log.provider ?? 'secure email'}{log.error ? ` - ${log.error}` : ''}</span>
                        {log.status !== 'sent' && (
                          <button className="ghost-button compact" type="button" onClick={() => retryEmailLog(log)}>
                            Retry
                          </button>
                        )}
                      </div>
                    ))}
                    {!emailLogs.length && <p className="muted">No email delivery events yet.</p>}
                  </div>
                </article>
              </div>
              <div className="admin-insights">
                <article>
                  <h3>System monitoring</h3>
                  <dl className="settings-list compact-list">
                    <div><dt>Status</dt><dd>{systemStatus?.status ?? 'Unknown'}</dd></div>
                    <div><dt>Storage</dt><dd>Enterprise-grade secure cloud storage</dd></div>
                    <div><dt>Authentication</dt><dd>Protected workspace sessions</dd></div>
                    <div><dt>Upload limit</dt><dd>{systemStatus ? `${systemStatus.uploadLimitMb} MB` : 'Unknown'}</dd></div>
                  </dl>
                </article>
                <article>
                  <h3>Audit logs</h3>
                  <div className="audit-list">
                    {auditLogs.slice(0, 6).map((log) => (
                      <div key={log.id}>
                        <strong>{log.action}</strong>
                        <span>{log.actorEmail ?? 'System'} - {new Date(log.createdAt).toLocaleString()}</span>
                      </div>
                    ))}
                    {!auditLogs.length && <p className="muted">No audit events yet.</p>}
                  </div>
                </article>
              </div>
            </article>
          </section>
        ) : currentView === 'contact' ? (
          <section className="module-page">
            <article className="panel company-panel">
              <p className="eyebrow">Metenova AI</p>
              <h2>Contact & Support</h2>
              <p className="module-copy">
                Metenova AI is a modern AI-powered business operations and analytics platform designed to help companies manage data,
                analytics, business workflows, reporting, AI automation, data cleanup, user management, reports, and enterprise operations
                in a scalable modular system.
              </p>
              <div className="support-grid">
                <div><strong>Owner</strong><span>Melaku</span></div>
                <div><strong>Email</strong><span>{ownerEmail}</span></div>
                <div><strong>Phone</strong><span>202-607-1255</span></div>
              </div>
            </article>
            <article className="panel">
              <div className="panel-header">
                <div>
                  <p className="eyebrow">Need help?</p>
                  <h2>Send a support request</h2>
                </div>
                <button className="ghost-button compact" type="button" onClick={() => {
                  setContactContext(currentView);
                  setContactMessage('I need help with ');
                }}>
                  Quick support
                </button>
              </div>
              {!emailConfigured && (
                <p className="persistence-note warning-note">Support request tracking is active. Direct email delivery is being configured for this workspace.</p>
              )}
              <form className="contact-form" onSubmit={submitContact}>
                <input placeholder="Name" value={contactName} onChange={(event) => setContactName(event.target.value)} />
                <input placeholder="Email" type="email" value={contactEmail} onChange={(event) => setContactEmail(event.target.value)} />
                <input placeholder="Page or module context" value={contactContext} onChange={(event) => setContactContext(event.target.value)} />
                <textarea placeholder="How can we help?" value={contactMessage} onChange={(event) => setContactMessage(event.target.value)} />
                <label className="file-input-label">
                  Optional screenshot or file
                  <input name="attachments" type="file" multiple />
                </label>
                <button type="submit" disabled={supportSending}>{supportSending ? 'Sending...' : 'Submit request'}</button>
              </form>
              {supportMessage && <p className="persistence-note">{supportMessage}</p>}
              <div className="faq-grid">
                <div><strong>Where is my data?</strong><span>Enterprise-grade secure cloud storage with encrypted company data management.</span></div>
                <div><strong>How do I get help?</strong><span>Use this form or email {ownerEmail}.</span></div>
                <div><strong>What integrations are planned?</strong><span>QuickBooks, Excel, Microsoft 365, Google Workspace, Stripe, PayPal, Slack, and external APIs.</span></div>
              </div>
            </article>
          </section>
        ) : ['accounting', 'engineering', 'hr', 'crm', 'dataProcessing'].includes(currentView) ? (
          <section className="module-page">
            {renderModulePage(
              currentView,
              setCurrentView,
              moduleMetrics[currentView],
              moduleRecords,
              moduleForm,
              setModuleForm,
              createModuleItem,
              moduleMessage,
              updateModuleItem,
              deleteModuleItem,
              editModuleItem,
              recordSearch,
              setRecordSearch,
              recordStatusFilter,
              setRecordStatusFilter,
              selectedRecord,
              setSelectedRecord,
              navigate
            )}
          </section>
        ) : currentView === 'analytics' ? (
          <section className="module-page">
            <article className="panel">
              <p className="eyebrow">Analytics</p>
              <h2>Executive analytics dashboard</h2>
              <div className="module-grid">
                <div><strong>Dataset intelligence</strong><span>{datasets.length} saved datasets available for analysis.</span></div>
                <div><strong>Dashboards</strong><span>{dashboards.length} saved dashboards across the workspace.</span></div>
                <div><strong>Reports</strong><span>{reports.length} generated reports in history.</span></div>
                <div><strong>AI recommendations</strong><span>Use uploaded data to generate trends, variance explanations, and executive summaries.</span></div>
              </div>
            </article>
          </section>
        ) : currentView === 'reports' ? (
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
                    <span>{dashboard.datasetName} - {dashboard.chartType} chart - {new Date(dashboard.updatedAt).toLocaleString()}</span>
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
                      <span>{report.datasetName} - {new Date(report.createdAt).toLocaleString()}</span>
                    </div>
                    <button className="ghost-button compact" type="button" onClick={() => downloadHistoricalReport(report)}>Download</button>
                  </div>
                )) : <p className="muted">Downloaded reports will appear here.</p>}
              </div>
            </article>
          </section>
        ) : currentView === 'assistant' ? (
          <section className="assistant-grid" id="assistant">
            {renderAssistantPanel(activeDataset, chat, question, setQuestion, askAssistant)}
            <article className="panel">
              <h2>Assistant capabilities</h2>
              <div className="workflow-list">
                {['Business trend summaries', 'Increase and decrease explanations', 'Highest and lowest value detection', 'Executive-ready insights'].map((item) => (
                  <div className="workflow-row" key={item}><strong>{item}</strong><small>Ready</small></div>
                ))}
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
                        {canManageUsers(user) && dashboard.ownerEmail ? ` - ${dashboard.ownerEmail}` : ''}
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
                          {canManageUsers(user) && report.ownerEmail ? ` - ${report.ownerEmail}` : ''}
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
      <button className="floating-help" type="button" onClick={() => {
        setContactContext(currentView);
        openView('contact');
        setContactMessage('I need help with ');
      }}>
        Help
      </button>
      {showSessionWarning && (
        <div className="session-overlay" role="alertdialog" aria-modal="true" aria-labelledby="session-warning-title">
          <div className="session-modal">
            <p className="eyebrow">Session security</p>
            <h2 id="session-warning-title">You will be signed out soon</h2>
            <p>Your session will expire in {sessionSecondsLeft} seconds because of inactivity.</p>
            <div className="session-actions">
              <button type="button" onClick={() => {
                localStorage.setItem('metenovaLastActivityAt', String(Date.now()));
                setShowSessionWarning(false);
              }}>
                Continue session
              </button>
              <button className="ghost-button" type="button" onClick={() => logoutRemote('Signed out for security.')}>
                Log out now
              </button>
            </div>
          </div>
        </div>
      )}
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

function canManageUsers(user: User | null) {
  return user?.role === 'owner' || user?.role === 'admin';
}

function roleLabel(role?: string) {
  const labels: Record<string, string> = {
    owner: 'Owner / Super Admin',
    admin: 'Admin',
    manager: 'Manager',
    employee: 'Employee',
    viewer: 'Viewer / Client'
  };
  return labels[role ?? ''] ?? 'Employee';
}

function pipelineStepPath(stage: string) {
  const normalized = stage.toLowerCase();
  if (normalized.includes('duplicate')) {
    return '/data-processing/duplicates';
  }
  if (normalized.includes('validate')) {
    return '/data-processing/validation';
  }
  if (normalized.includes('normalize') || normalized.includes('clean') || normalized.includes('approve')) {
    return '/data-processing/cleanup';
  }
  return '/data-processing/import-export';
}

function PageLayout({ children }: { children: ReactNode }) {
  return <section className="module-page routed-page">{children}</section>;
}

function BackButton() {
  const navigate = useNavigate();
  return <button className="back-button" type="button" onClick={() => navigate(-1)}>Back</button>;
}

function PageHeader({ title, eyebrow, copy }: { title: string; eyebrow: string; copy?: string }) {
  return (
    <article className="panel routed-header">
      <BackButton />
      <div>
        <p className="eyebrow">{eyebrow}</p>
        <h2>{title}</h2>
        {copy && <p className="module-copy">{copy}</p>}
      </div>
    </article>
  );
}

function EmptyState({ title, copy }: { title: string; copy: string }) {
  return (
    <div className="empty-state routed-empty">
      <strong>{title}</strong>
      <span>{copy}</span>
    </div>
  );
}

function LoadingCard({ label = 'Loading workspace...' }: { label?: string }) {
  return <article className="panel loading-card">{label}</article>;
}

function RecordTable({
  records,
  onEdit,
  onDelete
}: {
  records: ModuleRecord[];
  onEdit: (record: ModuleRecord) => void;
  onDelete: (record: ModuleRecord) => void;
}) {
  if (!records.length) {
    return <EmptyState title="No records yet" copy="Create the first workspace item to begin tracking this workflow." />;
  }

  return (
    <div className="table-wrap routed-table">
      <table>
        <thead>
          <tr>
            <th>Title</th>
            <th>Status</th>
            <th>Amount</th>
            <th>Updated</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {records.map((record) => (
            <tr key={record.id}>
              <td>{record.title}</td>
              <td>{record.status}</td>
              <td>{record.amount == null ? '-' : Number(record.amount).toLocaleString()}</td>
              <td>{new Date(record.updatedAt).toLocaleString()}</td>
              <td>
                <div className="record-actions table-actions">
                  <button className="ghost-button compact" type="button" onClick={() => onEdit(record)}>Edit</button>
                  <button className="ghost-button compact danger" type="button" onClick={() => onDelete(record)}>Delete</button>
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function RoutedPages(props: {
  apiFetch: (path: string, options?: RequestInit) => Promise<Response>;
  auditLogs: AuditLog[];
  canManage: boolean;
  dashboards: SavedDashboard[];
  deleteAdminUser: (user: AdminUser) => void;
  downloadHistoricalReport: (report: ReportHistoryItem) => void;
  reports: ReportHistoryItem[];
  systemStatus: SystemStatus | null;
  updateAdminUser: (userId: string, updates: Partial<AdminUser>) => void;
  users: AdminUser[];
}) {
  return (
    <Routes>
      {workspaceRoutes.map((route) => (
        <Route
          element={<ModuleWorkspacePage apiFetch={props.apiFetch} route={route} />}
          key={route.path}
          path={route.path}
        />
      ))}
      <Route path="/analytics/dashboard" element={<AnalyticsWorkspace dashboards={props.dashboards} reports={props.reports} />} />
      <Route path="/reports/history" element={<ReportsHistoryWorkspace downloadHistoricalReport={props.downloadHistoricalReport} reports={props.reports} />} />
      <Route path="/admin/users" element={props.canManage ? <AdminUsersWorkspace deleteAdminUser={props.deleteAdminUser} updateAdminUser={props.updateAdminUser} users={props.users} /> : <Navigate to="/" replace />} />
      <Route path="/admin/audit-logs" element={props.canManage ? <AuditLogsWorkspace auditLogs={props.auditLogs} /> : <Navigate to="/" replace />} />
      <Route path="/admin/system-monitoring" element={props.canManage ? <SystemMonitoringWorkspace status={props.systemStatus} /> : <Navigate to="/" replace />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}

function ModuleWorkspacePage({ apiFetch, route }: { apiFetch: (path: string, options?: RequestInit) => Promise<Response>; route: WorkspaceRoute }) {
  const [records, setRecords] = useState<ModuleRecord[]>([]);
  const [title, setTitle] = useState('');
  const [amount, setAmount] = useState('');
  const [status, setStatus] = useState('open');
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState('all');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');

  async function loadRecords() {
    setLoading(true);
    setError('');
    try {
      const response = await apiFetch(`/api/modules/${route.module}/records`);
      const payload = await readJson<{ records?: ModuleRecord[]; error?: string }>(response);
      if (!response.ok) {
        throw new Error(payload.error || 'Could not load records.');
      }
      setRecords((payload.records ?? []).filter((record) => record.recordType === route.type));
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : 'Could not load records.');
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadRecords();
  }, [route.path]);

  async function createRecord(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!title.trim()) {
      setError('Enter a title before saving.');
      return;
    }
    setSaving(true);
    setError('');
    try {
      const response = await apiFetch(`/api/modules/${route.module}/records`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title, amount, status, recordType: route.type, metadata: { workspacePath: route.path } })
      });
      const payload = await readJson<{ record?: ModuleRecord; error?: string }>(response);
      if (!response.ok || !payload.record) {
        throw new Error(payload.error || 'Could not save record.');
      }
      setRecords((current) => [payload.record as ModuleRecord, ...current]);
      setTitle('');
      setAmount('');
      setStatus('open');
    } catch (saveError) {
      setError(saveError instanceof Error ? saveError.message : 'Could not save record.');
    } finally {
      setSaving(false);
    }
  }

  async function editRecord(record: ModuleRecord) {
    const nextTitle = window.prompt('Update title', record.title);
    if (nextTitle == null || !nextTitle.trim()) {
      return;
    }
    setError('');
    try {
      const response = await apiFetch(`/api/modules/${route.module}/records/${record.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: nextTitle.trim() })
      });
      const payload = await readJson<{ record?: ModuleRecord; error?: string }>(response);
      if (!response.ok || !payload.record) {
        throw new Error(payload.error || 'Could not update record.');
      }
      setRecords((current) => current.map((entry) => entry.id === record.id ? payload.record as ModuleRecord : entry));
    } catch (editError) {
      setError(editError instanceof Error ? editError.message : 'Could not update record.');
    }
  }

  async function deleteRecord(record: ModuleRecord) {
    if (!window.confirm(`Delete ${record.title}?`)) {
      return;
    }
    setError('');
    try {
      const response = await apiFetch(`/api/modules/${route.module}/records/${record.id}`, { method: 'DELETE' });
      if (!response.ok) {
        const payload = await readJson<{ error?: string }>(response);
        throw new Error(payload.error || 'Could not delete record.');
      }
      setRecords((current) => current.filter((entry) => entry.id !== record.id));
    } catch (deleteError) {
      setError(deleteError instanceof Error ? deleteError.message : 'Could not delete record.');
    }
  }

  const filteredRecords = records
    .filter((record) => filter === 'all' || record.status === filter)
    .filter((record) => !search.trim() || [record.title, record.status].some((value) => value.toLowerCase().includes(search.toLowerCase())));

  return (
    <PageLayout>
      <PageHeader title={route.title} eyebrow={route.moduleLabel} copy={route.copy} />
      <article className="panel routed-workspace">
        <form className="module-form routed-form" onSubmit={createRecord}>
          <input placeholder={`${route.title} title`} value={title} onChange={(event) => setTitle(event.target.value)} />
          <input placeholder="Amount or value" value={amount} onChange={(event) => setAmount(event.target.value)} />
          <select value={status} onChange={(event) => setStatus(event.target.value)}>
            <option value="open">Open</option>
            <option value="in_progress">In progress</option>
            <option value="closed">Closed</option>
          </select>
          <button type="submit" disabled={saving}>{saving ? 'Saving...' : 'Create record'}</button>
        </form>
        <div className="record-toolbar">
          <input placeholder="Search this workspace" value={search} onChange={(event) => setSearch(event.target.value)} />
          <select value={filter} onChange={(event) => setFilter(event.target.value)}>
            <option value="all">All statuses</option>
            <option value="open">Open</option>
            <option value="in_progress">In progress</option>
            <option value="closed">Closed</option>
          </select>
        </div>
        {error && <p className="persistence-note warning-note">{error}</p>}
        {loading ? <LoadingCard /> : <RecordTable records={filteredRecords} onDelete={deleteRecord} onEdit={editRecord} />}
      </article>
    </PageLayout>
  );
}

function AnalyticsWorkspace({ dashboards, reports }: { dashboards: SavedDashboard[]; reports: ReportHistoryItem[] }) {
  return (
    <PageLayout>
      <PageHeader title="Analytics Dashboard" eyebrow="Analytics" copy="Executive KPI workspace for dashboards, reports, and AI-ready analysis." />
      <article className="panel">
        <div className="module-grid">
          <div><strong>{dashboards.length}</strong><span>Saved dashboards</span></div>
          <div><strong>{reports.length}</strong><span>Generated reports</span></div>
          <div><strong>Live</strong><span>AI analytics workspace</span></div>
        </div>
      </article>
    </PageLayout>
  );
}

function ReportsHistoryWorkspace({ reports, downloadHistoricalReport }: { reports: ReportHistoryItem[]; downloadHistoricalReport: (report: ReportHistoryItem) => void }) {
  return (
    <PageLayout>
      <PageHeader title="Report History" eyebrow="Reports" copy="Download generated reports and review saved analysis output." />
      <article className="panel">
        <div className="history-list">
          {reports.length ? reports.map((report) => (
            <div className="history-item" key={report.id}>
              <div>
                <strong>{report.title}</strong>
                <span>{report.datasetName} - {new Date(report.createdAt).toLocaleString()}</span>
              </div>
              <button className="ghost-button compact" type="button" onClick={() => downloadHistoricalReport(report)}>Download</button>
            </div>
          )) : <EmptyState title="No reports yet" copy="Generated PDF and analysis reports will appear here." />}
        </div>
      </article>
    </PageLayout>
  );
}

function AdminUsersWorkspace({ users, updateAdminUser, deleteAdminUser }: { users: AdminUser[]; updateAdminUser: (userId: string, updates: Partial<AdminUser>) => void; deleteAdminUser: (user: AdminUser) => void }) {
  const [search, setSearch] = useState('');
  const filteredUsers = users.filter((user) => [user.name, user.email, user.role].some((value) => value.toLowerCase().includes(search.toLowerCase())));
  return (
    <PageLayout>
      <PageHeader title="User Management" eyebrow="Admin" copy="Manage workspace roles, active status, and company access." />
      <AdminSubnav />
      <article className="panel routed-workspace">
        <div className="record-toolbar"><input placeholder="Search users" value={search} onChange={(event) => setSearch(event.target.value)} /></div>
        <div className="table-wrap routed-table">
          <table>
            <thead><tr><th>Name</th><th>Email</th><th>Role</th><th>Status</th><th>Actions</th></tr></thead>
            <tbody>
              {filteredUsers.map((adminUser) => (
                <tr key={adminUser.id}>
                  <td>{adminUser.name}</td>
                  <td>{adminUser.email}</td>
                  <td>
                    <select className="role-select" value={adminUser.role} onChange={(event) => updateAdminUser(adminUser.id, { role: event.target.value as UserRole })}>
                      {roleOptions.map((role) => <option key={role} value={role}>{roleLabel(role)}</option>)}
                    </select>
                  </td>
                  <td>{adminUser.active ? 'Active' : 'Disabled'}</td>
                  <td><button className="ghost-button compact danger" type="button" onClick={() => deleteAdminUser(adminUser)}>Delete</button></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {!filteredUsers.length && <EmptyState title="No users found" copy="Invite users or adjust the search filter." />}
      </article>
    </PageLayout>
  );
}

function AuditLogsWorkspace({ auditLogs }: { auditLogs: AuditLog[] }) {
  return (
    <PageLayout>
      <PageHeader title="Audit Logs" eyebrow="Admin" copy="Review role, security, support, and workspace activity." />
      <AdminSubnav />
      <article className="panel">
        <div className="audit-list">
          {auditLogs.length ? auditLogs.map((log) => (
            <div key={log.id}><strong>{log.action}</strong><span>{log.actorEmail ?? 'System'} - {new Date(log.createdAt).toLocaleString()}</span></div>
          )) : <EmptyState title="No audit events" copy="Security and workspace activity will appear here." />}
        </div>
      </article>
    </PageLayout>
  );
}

function SystemMonitoringWorkspace({ status }: { status: SystemStatus | null }) {
  return (
    <PageLayout>
      <PageHeader title="System Monitoring" eyebrow="Admin" copy="Operational health, protected storage readiness, and upload capacity." />
      <AdminSubnav />
      <article className="panel">
        {status ? (
          <dl className="settings-list">
            <div><dt>Status</dt><dd>{status.status}</dd></div>
            <div><dt>Storage</dt><dd>Protected workspace infrastructure</dd></div>
            <div><dt>Sessions</dt><dd>Secure workspace sessions</dd></div>
            <div><dt>Upload limit</dt><dd>{status.uploadLimitMb} MB</dd></div>
          </dl>
        ) : <LoadingCard label="Loading system status..." />}
      </article>
    </PageLayout>
  );
}

function AdminSubnav() {
  const navigate = useNavigate();
  const location = useLocation();
  const links = [
    { label: 'Users', path: '/admin/users' },
    { label: 'Audit logs', path: '/admin/audit-logs' },
    { label: 'System monitoring', path: '/admin/system-monitoring' }
  ];
  return (
    <nav className="workspace-subnav" aria-label="Admin workspace navigation">
      {links.map((link) => (
        <button className={location.pathname === link.path ? 'active' : ''} key={link.path} type="button" onClick={() => navigate(link.path)}>
          {link.label}
        </button>
      ))}
    </nav>
  );
}

function renderModulePage(
  view: AppView,
  setCurrentView: (view: AppView) => void,
  metrics: { total: number; open: number } | undefined,
  records: ModuleRecord[],
  moduleForm: { title: string; recordType: string; amount: string },
  setModuleForm: (value: { title: string; recordType: string; amount: string }) => void,
  createModuleItem: (event: FormEvent<HTMLFormElement>) => void,
  moduleMessage: string,
  updateModuleItem: (record: ModuleRecord, updates: Partial<Pick<ModuleRecord, 'status' | 'title'>>) => void,
  deleteModuleItem: (record: ModuleRecord) => void,
  editModuleItem: (record: ModuleRecord) => void,
  recordSearch: string,
  setRecordSearch: (value: string) => void,
  recordStatusFilter: string,
  setRecordStatusFilter: (value: string) => void,
  selectedRecord: ModuleRecord | null,
  setSelectedRecord: (record: ModuleRecord | null) => void,
  navigate: (path: string) => void
) {
  const titles: Record<string, string> = {
    accounting: 'Accounting command center',
    engineering: 'Engineering & projects',
    hr: 'HR workspace',
    crm: 'CRM workspace',
    dataProcessing: 'Data processing & cleanup'
  };
  const descriptions: Record<string, string> = {
    accounting: 'Invoices, expenses, payroll, financial reports, budgets, tax tracking, and an AI financial assistant.',
    engineering: 'Project management, task tracking, team assignments, document uploads, blueprint management, and workflow reports.',
    hr: 'People operations, roles, onboarding, team assignments, and HR-ready AI support.',
    crm: 'Client records, opportunity tracking, support history, and AI-assisted relationship management.',
    dataProcessing: 'Data cleanup, duplicate detection, validation, normalization, import/export, batch processing, and data quality reports.'
  };
  const cards = moduleCards[view] ?? [];
  const pipelineStages = ['Upload', 'Validate', 'Detect duplicates', 'Normalize', 'Clean', 'Approve', 'Export'];
  const pipelineRecords = records.filter((record) => ['pipeline_job', 'cleanup', 'dedupe', 'validation', 'import_export'].includes(record.recordType));
  const normalizedSearch = recordSearch.trim().toLowerCase();
  const filteredRecords = records
    .filter((record) => recordStatusFilter === 'all' || record.status === recordStatusFilter)
    .filter((record) => {
      if (!normalizedSearch) {
        return true;
      }
      return [record.title, record.recordType, record.status, record.ownerEmail]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(normalizedSearch));
    });

  return (
    <>
      <article className="panel module-hero">
        <p className="eyebrow">Modular AI ERP</p>
        <h2>{titles[view]}</h2>
        <p className="module-copy">{descriptions[view]}</p>
        <div className="module-stat-row">
          <span>{metrics?.total ?? 0} total records</span>
          <span>{metrics?.open ?? 0} open items</span>
        </div>
        <button className="ghost-button support-button" type="button" onClick={() => setCurrentView('contact')}>
          Need help?
        </button>
      </article>
      <div className="module-grid">
        {cards.map((card) => (
          <article className="module-card" key={card.title}>
            <span>{records.filter((record) => record.recordType === card.type).length} records</span>
            <strong>{card.title}</strong>
            <p>{card.copy}</p>
            <button type="button" onClick={() => navigate(card.path)}>
              Open {card.title}
            </button>
          </article>
        ))}
      </div>
      {view === 'dataProcessing' && (
        <article className="panel pipeline-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Processing pipeline</p>
              <h2>Enterprise cleanup workflow</h2>
            </div>
            <button type="button" className="ghost-button compact" onClick={() => navigate('/data-processing/import-export')}>
              Start pipeline
            </button>
          </div>
          <div className="pipeline-stages">
            {pipelineStages.map((stage, index) => (
              <button
                className={index < Math.min(pipelineRecords.length + 1, pipelineStages.length) ? 'active' : ''}
                key={stage}
                onClick={() => navigate(pipelineStepPath(stage))}
                type="button"
              >
                <strong>{index + 1}</strong>
                <span>{stage}</span>
              </button>
            ))}
          </div>
          <div className="workflow-list">
            {pipelineRecords.slice(0, 4).map((record) => (
              <div className="workflow-row" key={record.id}>
                <div>
                  <strong>{record.title}</strong>
                  <span>{record.recordType} - queued with AI cleanup recommendations</span>
                </div>
                <small>{record.status}</small>
              </div>
            ))}
            {!pipelineRecords.length && (
              <div className="empty-state compact-empty">Start a pipeline to track validation, cleanup, approvals, exports, and processing history.</div>
            )}
          </div>
        </article>
      )}
      <article className="panel module-workbench">
        <div>
          <p className="eyebrow">Workspace records</p>
          <h2>Create a real module item</h2>
          <p className="persistence-note">{moduleMessage}</p>
        </div>
        <div className="record-toolbar">
          <input placeholder="Search records" value={recordSearch} onChange={(event) => setRecordSearch(event.target.value)} />
          <select value={recordStatusFilter} onChange={(event) => setRecordStatusFilter(event.target.value)}>
            <option value="all">All statuses</option>
            <option value="open">Open</option>
            <option value="closed">Closed</option>
          </select>
        </div>
        <form className="module-form" onSubmit={createModuleItem}>
          <select value={moduleForm.recordType} onChange={(event) => setModuleForm({ ...moduleForm, recordType: event.target.value })}>
            {cards.map((card) => <option key={card.type} value={card.type}>{card.title}</option>)}
          </select>
          <input placeholder="Title or reference" value={moduleForm.title} onChange={(event) => setModuleForm({ ...moduleForm, title: event.target.value })} />
          <input placeholder="Amount (optional)" value={moduleForm.amount} onChange={(event) => setModuleForm({ ...moduleForm, amount: event.target.value })} />
          <button type="submit">Save record</button>
        </form>
        {selectedRecord && (
          <div className="record-detail">
            <div>
              <p className="eyebrow">Selected record</p>
              <h3>{selectedRecord.title}</h3>
              <span>{selectedRecord.recordType} - {selectedRecord.status}</span>
              <span>Updated {new Date(selectedRecord.updatedAt).toLocaleString()}</span>
              {selectedRecord.ownerEmail && <span>Owner {selectedRecord.ownerEmail}</span>}
            </div>
            <button className="ghost-button compact" type="button" onClick={() => setSelectedRecord(null)}>Close</button>
          </div>
        )}
        <div className="record-list">
          {filteredRecords.map((record) => (
            <div key={record.id}>
              <strong>{record.title}</strong>
              <span>{record.recordType} - {record.status} - {new Date(record.updatedAt).toLocaleString()}{record.ownerEmail ? ` - ${record.ownerEmail}` : ''}</span>
              <div className="record-actions">
                <button className="ghost-button compact" type="button" onClick={() => setSelectedRecord(record)}>
                  View
                </button>
                <button className="ghost-button compact" type="button" onClick={() => editModuleItem(record)}>
                  Edit
                </button>
                <button className="ghost-button compact" type="button" onClick={() => updateModuleItem(record, { status: record.status === 'closed' ? 'open' : 'closed' })}>
                  {record.status === 'closed' ? 'Reopen' : 'Close'}
                </button>
                <button className="ghost-button compact danger" type="button" onClick={() => deleteModuleItem(record)}>
                  Delete
                </button>
              </div>
            </div>
          ))}
          {!filteredRecords.length && <div className="empty-state compact-empty">{records.length ? 'No records match the current filters.' : 'No records yet. Add the first operational item.'}</div>}
        </div>
      </article>
    </>
  );
}

function renderAssistantPanel(
  activeDataset: Dataset | null,
  chat: ChatMessage[],
  question: string,
  setQuestion: (question: string) => void,
  askAssistant: (event: FormEvent<HTMLFormElement>) => void
) {
  return (
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
          placeholder="Ask about totals, averages, trends, outliers..."
          value={question}
        />
        <button disabled={!activeDataset || !question.trim()} type="submit">Ask</button>
      </form>
    </article>
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
    'Metenova AI Data Report',
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
