import { ChangeEvent, Component, CSSProperties, Dispatch, DragEvent, ErrorInfo, FormEvent, ReactNode, SetStateAction, useEffect, useMemo, useState } from 'react';
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
  companyId?: string;
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
  originalDatasetId?: string | null;
  cleanedDatasetId?: string | null;
  cleanupStatus?: string;
  cleanupLogs?: string[];
  cleanupMetrics?: CleanupMetrics;
  cleanupPreview?: {
    before: Record<string, string>[];
    after: Record<string, string>[];
  } | null;
  cleanupOperations?: string[];
  futureAiReady?: boolean;
  ownerName?: string;
  ownerEmail?: string;
};

type CleanupMetrics = {
  duplicatesRemoved?: number;
  rowsFixed?: number;
  invalidValuesDetected?: number;
  columnsStandardized?: number;
  totalCleanedRows?: number;
  failedRows?: number;
  processingDurationMs?: number;
  anomaliesDetected?: number;
};

type CleanupJob = {
  id: string;
  companyId?: string;
  originalDatasetId: string;
  cleanedDatasetId?: string | null;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  metrics?: CleanupMetrics;
  logs?: string[];
  error?: string | null;
  createdAt: string;
  updatedAt: string;
};

type ChartType = 'bar' | 'line' | 'donut';
type Theme = 'light' | 'dark';
type ChatMessage = { role: 'assistant' | 'user'; text: string };
type AuthMode = 'login' | 'signup';
type UserRole = 'owner' | 'admin' | 'manager' | 'employee' | 'viewer';
type PreviewMode = 'upload' | 'validation' | 'duplicates' | 'normalization' | 'cleanup' | 'approval' | 'export' | 'compare' | 'history';
type AppView =
  | 'dashboard'
  | 'assistant'
  | 'accounting'
  | 'engineering'
  | 'hr'
  | 'crm'
  | 'dataProcessing'
  | 'companies'
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
  assignedCompanies?: CompanyAssignment[];
};

type AdminUser = User & {
  active: boolean;
  createdAt?: string;
};

type CompanyAssignment = {
  id: string;
  userId: string;
  companyId: string;
  companyName: string;
  role: UserRole;
  assignedAt?: string;
  createdAt?: string;
};

type SavedDashboard = {
  id: string;
  companyId?: string;
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
  companyId?: string;
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
  postgresqlConnected?: boolean;
  database?: DatabaseStatus;
  emailConfigured?: boolean;
  sessionTimeoutMinutes?: number;
  sessionWarningSeconds?: number;
  csrfToken?: string;
};

type DatabaseStatus = {
  usingPostgres?: boolean;
  hostConfigured?: boolean;
  database?: string | null;
  host?: string | null;
  port?: string | null;
  connected?: boolean;
  tablesInitialized?: boolean;
  connectionError?: string | null;
  retries?: number;
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
  ownerEmail?: string;
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

type NotificationItem = {
  id: string;
  companyId?: string;
  type: string;
  title: string;
  message: string;
  status: string;
  createdAt: string;
};

type EnterpriseConnector = {
  id: string;
  companyId?: string;
  name: string;
  connectorType: string;
  status: string;
  healthStatus: string;
  schedule?: Record<string, string | boolean | number>;
  metadata?: Record<string, string | boolean | number>;
  credentialEncrypted?: boolean;
  lastSyncAt?: string | null;
  nextSyncAt?: string | null;
  updatedAt?: string;
};

type ConnectorSyncLog = {
  id: string;
  connectorId: string;
  companyId?: string;
  status: string;
  recordsProcessed: number;
  failedRows: number;
  retries: number;
  durationMs: number;
  error?: string;
  createdAt: string;
};

type PipelineSchedule = {
  id: string;
  companyId?: string;
  name: string;
  scheduleType: string;
  cronExpression?: string;
  eventTrigger?: string;
  priority: number;
  slaMinutes: number;
  retryPolicy?: Record<string, string | number | boolean>;
  dependencies?: string[];
  status: string;
  nextRunAt?: string | null;
  lastRunAt?: string | null;
  metadata?: Record<string, string | number | boolean>;
};

type WorkflowInsight = {
  id: string;
  companyId?: string;
  datasetId?: string | null;
  module: string;
  insightType: string;
  severity: string;
  title: string;
  summary: string;
  confidence: number;
  recommendations: string[];
  explainability?: Record<string, unknown>;
  status: string;
  createdAt: string;
};

type AccessRequest = {
  id: string;
  companyId?: string;
  department?: string;
  requestedRole: UserRole;
  reason: string;
  status: string;
  expiresAt?: string | null;
  createdAt: string;
};

type EnterpriseOperations = {
  connectors: EnterpriseConnector[];
  syncLogs: ConnectorSyncLog[];
  schedules: PipelineSchedule[];
  intelligence: WorkflowInsight[];
  accessRequests: AccessRequest[];
};

type Company = {
  id: string;
  name: string;
  industry: string;
  ownerName: string;
  email: string;
  phone: string;
  status: string;
  createdAt: string;
  updatedAt?: string;
};

type CompanyFormValues = {
  name: string;
  industry: string;
  ownerName: string;
  email: string;
  phone: string;
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
type PipelineStageState = 'queued' | 'running' | 'blocked' | 'waiting_approval' | 'completed' | 'failed' | 'archived';
type ModulePipelineConfig = {
  uploadTitle: string;
  uploadCopy: string;
  stages: string[];
  rules: string[];
  qualitySignals: string[];
  exportLabel: string;
  emptyState: string;
  module: string;
};

type AuthResponse = {
  token: string;
  user: User;
};

type ViteImportMeta = ImportMeta & {
  env: {
    VITE_API_URL?: string;
    MODE?: string;
    PROD?: boolean;
  };
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
const viteEnv = (import.meta as ViteImportMeta).env;
const API_BASE = (viteEnv.VITE_API_URL || window.location.origin).replace(/\/$/, '');
const AUTH_TOKEN_KEY = 'metenovaSessionToken';
const LEGACY_AUTH_TOKEN_KEY = 'authToken';
const SESSION_ACTIVITY_KEY = 'metenovaLastActivityAt';
const LOGOUT_BROADCAST_KEY = 'metenovaLogoutAt';
const STARTUP_ERROR_KEY = 'metenovaStartupError';
const DEFAULT_AUTH_MESSAGE = 'Sign in to access your dashboards and datasets.';

function apiUrl(path: string) {
  if (/^https?:\/\//i.test(path)) {
    return path;
  }
  return `${API_BASE}${path.startsWith('/') ? path : `/${path}`}`;
}

async function fetchWithRetry(path: string, options: RequestInit = {}, attempts = 2) {
  const requestUrl = apiUrl(path);
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await fetch(requestUrl, {
        ...options,
        credentials: options.credentials ?? 'include',
        headers: {
          'X-Request-Id': `web-${Date.now()}-${attempt}`,
          ...(options.headers instanceof Headers ? Object.fromEntries(options.headers.entries()) : options.headers ?? {})
        }
      });
      if (attempt > 1) {
        console.info('[Metenova API] retry recovered', { path, requestUrl, attempt, status: response.status });
      }
      return response;
    } catch (error) {
      lastError = error;
      console.error('[Metenova API] fetch failed', {
        path,
        requestUrl,
        attempt,
        apiBase: API_BASE,
        origin: window.location.origin,
        message: error instanceof Error ? error.message : String(error)
      });
      if (attempt < attempts) await new Promise((resolve) => window.setTimeout(resolve, 600 * attempt));
    }
  }
  throw lastError instanceof Error ? lastError : new Error('Failed to fetch.');
}

function startupErrorMessage(payload: { error?: string; code?: string; database?: DatabaseStatus; requestId?: string }) {
  if (payload.code !== 'STARTUP_NOT_READY' && payload.code !== 'POSTGRESQL_UNAVAILABLE') return payload.error || 'Authentication failed.';
  const database = payload.database;
  const details = [
    database?.host ? `database host: ${database.host}` : '',
    database?.connectionError ? `error: ${database.connectionError}` : '',
    payload.requestId ? `request: ${payload.requestId}` : ''
  ].filter(Boolean).join(' | ');
  return `${payload.error || 'Database is reconnecting. Please retry in a moment.'}${details ? ` ${details}` : ''}`;
}

function isStartupRecoveryMessage(message: string) {
  return message.startsWith('Backend startup is not ready') || message.startsWith('PostgreSQL storage is reconnecting') || message.startsWith('Database is reconnecting');
}

const moduleNav: Array<{ view: AppView; label: string; icon: string; adminOnly?: boolean }> = [
  { view: 'dashboard', label: 'Dashboard', icon: 'DB' },
  { view: 'assistant', label: 'Business Assistant', icon: 'BA' },
  { view: 'companies', label: 'Companies', icon: 'CO' },
  { view: 'accounting', label: 'Accounting', icon: 'AC' },
  { view: 'engineering', label: 'Engineering', icon: 'EN' },
  { view: 'hr', label: 'HR', icon: 'HR' },
  { view: 'crm', label: 'CRM', icon: 'CR' },
  { view: 'dataProcessing', label: 'Data Processing', icon: 'DP' },
  { view: 'analytics', label: 'Analytics', icon: 'AN' },
  { view: 'reports', label: 'Reports', icon: 'RP' },
  { view: 'adminUsers', label: 'Admin Panel', icon: 'AD', adminOnly: true },
  { view: 'settings', label: 'Settings', icon: 'ST' },
  { view: 'contact', label: 'Contact Us', icon: 'CS' }
];

const sampleCompanies: Company[] = [
  {
    id: 'company-brightpath-logistics',
    name: 'BrightPath Logistics LLC',
    industry: 'Logistics',
    ownerName: 'Operations Team',
    email: 'ops@brightpath.example',
    phone: '202-555-0141',
    status: 'Active',
    createdAt: new Date('2026-01-12T14:00:00Z').toISOString()
  },
  {
    id: 'company-metrocare-health',
    name: 'MetroCare Health',
    industry: 'Healthcare',
    ownerName: 'Care Administration',
    email: 'admin@metrocare.example',
    phone: '202-555-0186',
    status: 'Active',
    createdAt: new Date('2026-02-04T16:30:00Z').toISOString()
  },
  {
    id: 'company-apex-accounting',
    name: 'Apex Accounting Group',
    industry: 'Finance',
    ownerName: 'Client Services',
    email: 'hello@apexaccounting.example',
    phone: '202-555-0198',
    status: 'Active',
    createdAt: new Date('2026-03-18T13:15:00Z').toISOString()
  }
];

function buildCompanyQuery(companyId: string) {
  return companyId ? `?companyId=${encodeURIComponent(companyId)}` : '';
}

function formatUploadError(payload: { error?: string; code?: string; uploadStage?: string; requestId?: string }, status: number) {
  const codeLabels: Record<string, string> = {
    UPLOAD_INVALID_MIME: 'Invalid MIME',
    UPLOAD_INVALID_SCHEMA: 'Invalid schema',
    UPLOAD_COMPANY_REQUIRED: 'Unauthorized company',
    UPLOAD_COMPANY_NOT_FOUND: 'Unauthorized company',
    UPLOAD_PARSER_FAILURE: 'Parser failure',
    UPLOAD_DATABASE_FAILURE: 'Database persistence failure',
    UPLOAD_FILE_MISSING: 'Multipart/form-data handling failed',
    CSRF_INVALID: 'CSRF/session failure',
    SESSION_EXPIRED: 'CSRF/session failure'
  };
  const label = payload.code ? codeLabels[payload.code] ?? payload.code : status === 401 || status === 403 ? 'CSRF/session failure' : 'Upload failure';
  const stage = payload.uploadStage ? ` Stage: ${payload.uploadStage}.` : '';
  const request = payload.requestId ? ` Request ID: ${payload.requestId}.` : '';
  return `${label}: ${payload.error || 'The dataset could not be uploaded.'}${stage}${request}`;
}

const moduleCards: Record<string, ModuleAction[]> = {
  accounting: [
    { title: 'Invoices', copy: 'Track billing status, aging, approvals, and payment readiness.', type: 'invoice', path: '/accounting/invoices' },
    { title: 'Expense Tracking', copy: 'Classify expenses, flag unusual spend, and prepare monthly close.', type: 'expense', path: '/accounting/expenses' },
    { title: 'Payroll', copy: 'Review payroll cycles, department allocations, and exception queues.', type: 'payroll', path: '/accounting/payroll' },
    { title: 'Financial Reports', copy: 'Generate budget, cash flow, tax, and executive finance reports.', type: 'financial_report', path: '/accounting/financial-reports' },
    { title: 'Business Financial Assistant', copy: 'Ask concise questions about trends, budget variance, and risks.', type: 'assistant', path: '/accounting/ai-financial-assistant' }
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
    { title: 'Dataset Workspace', copy: 'Upload once, then validate, dedupe, normalize, clean, score, approve, and export from one operational workspace.', type: 'dataset_workspace', path: '/data-processing/workspace' }
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
const dataProcessingWorkspaceRoute = workspaceRoutes.find((route) => route.path === '/data-processing/workspace') ?? {
  title: 'Dataset Workspace',
  copy: 'Upload once, then validate, dedupe, normalize, clean, score, approve, and export from one operational workspace.',
  type: 'dataset_workspace',
  path: '/data-processing/workspace',
  module: 'dataProcessing',
  moduleLabel: 'Data Processing'
};

const defaultPipelineStages = ['Upload', 'Validate', 'Detect Duplicates', 'Normalize', 'Clean', 'Approval', 'Export'];

const modulePipelineConfigs: Record<string, ModulePipelineConfig> = {
  accounting: {
    module: 'accounting',
    uploadTitle: 'Upload invoice files here',
    uploadCopy: 'CSV, XLSX, or JSON invoice files are checked for required invoice fields, vendor matching, tax readiness, payment status, and ERP export quality.',
    stages: ['Upload', 'Validate Invoices', 'Duplicate Detection', 'Vendor Matching', 'Tax Validation', 'Payment Validation', 'Approval', 'Export ERP File'],
    rules: [
      'invoice_number is required',
      'vendor_name is required',
      'amount must be numeric and cannot be negative',
      'duplicate invoice detection runs before approval',
      'payment status, tax, currency, aging, PO matching, and invoice date checks are reviewed'
    ],
    qualitySignals: ['Duplicate invoice risk', 'Tax validation', 'Payment aging', 'Currency normalization'],
    exportLabel: 'Export ERP File',
    emptyState: 'Upload invoice, vendor payment, or expense files to start the Accounting pipeline.'
  },
  engineering: {
    module: 'engineering',
    uploadTitle: 'Upload project schedules, engineering plans, or resource files',
    uploadCopy: 'Project files are validated as an operational dependency chain with milestone, owner, sequencing, resource, and schedule consistency checks.',
    stages: ['Upload', 'Validate Project Structure', 'Dependency Analysis', 'Resource Conflict Detection', 'Schedule Analysis', 'Risk Detection', 'Approval', 'Export Project Report'],
    rules: [
      'project_id is required',
      'milestone dependencies must be valid',
      'predecessor and successor sequencing is checked',
      'resource conflicts and missing owners are flagged',
      'schedule overlaps and project status consistency are reviewed'
    ],
    qualitySignals: ['Dependency health', 'Resource conflicts', 'Schedule overlap', 'Delivery risk'],
    exportLabel: 'Export Project Report',
    emptyState: 'Upload schedules, resources, milestones, or engineering files to start the project lifecycle pipeline.'
  },
  dataProcessing: {
    module: 'dataProcessing',
    uploadTitle: 'Upload dataset once',
    uploadCopy: 'Centralize CSV, XLSX, XLS, or JSON uploads, then reuse the same dataset across validation, duplicate detection, cleanup, reporting, and export tools.',
    stages: ['Upload', 'Validate', 'Detect Duplicates', 'Normalize', 'Clean', 'Quality Score', 'Approve', 'Export'],
    rules: [
      'required columns must exist',
      'schema drift is detected before cleanup',
      'duplicate records are isolated',
      'failed rows and anomalies are separated for review',
      'data quality score is calculated before approval'
    ],
    qualitySignals: ['Data quality score', 'Anomaly score', 'Failed row isolation', 'Schema drift'],
    exportLabel: 'Export Clean Dataset',
    emptyState: 'Upload business datasets to run schema validation, cleanup, quality scoring, approval, and export.'
  },
  default: {
    module: 'operations',
    uploadTitle: 'Upload company workflow files',
    uploadCopy: 'CSV, XLSX, or JSON files are validated, processed, approved, and exported inside this company workspace.',
    stages: defaultPipelineStages,
    rules: ['company_id access is enforced', 'records are validated before approval', 'exports stay scoped to the selected company'],
    qualitySignals: ['Workflow status', 'Validation output', 'Approval readiness', 'Export history'],
    exportLabel: 'Export',
    emptyState: 'Upload a company file to start this workflow.'
  }
};

const emptyEnterpriseOperations: EnterpriseOperations = {
  connectors: [],
  syncLogs: [],
  schedules: [],
  intelligence: [],
  accessRequests: []
};

export function App() {
  const navigate = useNavigate();
  const location = useLocation();
  const [insights, setInsights] = useState<InsightResponse>(fallbackInsights);
  const [workflows, setWorkflows] = useState<Workflow[]>([]);
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [activeDataset, setActiveDataset] = useState<Dataset | null>(null);
  const [cleanupJobs, setCleanupJobs] = useState<CleanupJob[]>([]);
  const [cleanupMessage, setCleanupMessage] = useState('Cleanup pipeline ready.');
  const [cleanupRunning, setCleanupRunning] = useState(false);
  const [deletingDatasetId, setDeletingDatasetId] = useState('');
  const [status, setStatus] = useState('Connecting');
  const [uploadState, setUploadState] = useState('Drop a CSV or Excel file here to start analysis.');
  const [isDragging, setIsDragging] = useState(false);
  const [lastUploadedFile, setLastUploadedFile] = useState<File | null>(null);
  const [chartType, setChartType] = useState<ChartType>('bar');
  const [theme, setTheme] = useState<Theme>(() => (localStorage.getItem('theme') as Theme) || 'light');
  const [token, setToken] = useState(() => {
    localStorage.removeItem(LEGACY_AUTH_TOKEN_KEY);
    return sessionStorage.getItem(AUTH_TOKEN_KEY) || '';
  });
  const [user, setUser] = useState<User | null>(null);
  const [authDisabled, setAuthDisabled] = useState(false);
  const [configLoaded, setConfigLoaded] = useState(false);
  const [authMode, setAuthMode] = useState<AuthMode>('login');
  const [authName, setAuthName] = useState('');
  const [authEmail, setAuthEmail] = useState('');
  const [authPassword, setAuthPassword] = useState('');
  const [authMessage, setAuthMessage] = useState(DEFAULT_AUTH_MESSAGE);
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
  const [accessUser, setAccessUser] = useState<AdminUser | null>(null);
  const [accessCompanyIds, setAccessCompanyIds] = useState<string[]>([]);
  const [accessRole, setAccessRole] = useState<UserRole>('employee');
  const [accessSaving, setAccessSaving] = useState(false);
  const [auditLogs, setAuditLogs] = useState<AuditLog[]>([]);
  const [systemStatus, setSystemStatus] = useState<SystemStatus | null>(null);
  const [invitations, setInvitations] = useState<Invitation[]>([]);
  const [emailLogs, setEmailLogs] = useState<EmailLog[]>([]);
  const [notifications, setNotifications] = useState<NotificationItem[]>([]);
  const [enterpriseOps, setEnterpriseOps] = useState<EnterpriseOperations>(emptyEnterpriseOperations);
  const [enterpriseOpsMessage, setEnterpriseOpsMessage] = useState('Enterprise operations fabric ready.');
  const [syncingConnectorId, setSyncingConnectorId] = useState('');
  const [companies, setCompanies] = useState<Company[]>(sampleCompanies);
  const [companiesLoading, setCompaniesLoading] = useState(false);
  const [companiesError, setCompaniesError] = useState('');
  const [companySaving, setCompanySaving] = useState(false);
  const [selectedCompanyId, setSelectedCompanyId] = useState(sampleCompanies[0]?.id ?? '');
  const [companyFormOpen, setCompanyFormOpen] = useState(false);
  const [companyForm, setCompanyForm] = useState<CompanyFormValues>({
    name: '',
    industry: '',
    ownerName: '',
    email: '',
    phone: ''
  });
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
  const [workspaceAction, setWorkspaceAction] = useState('');
  const [csrfToken, setCsrfToken] = useState('');
  const [durableStorage, setDurableStorage] = useState(true);
  const [emailConfigured, setEmailConfigured] = useState(false);
  const [sessionTimeoutMs, setSessionTimeoutMs] = useState(30 * 60 * 1000);
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
    console.info('[Metenova API]', {
      apiBase: API_BASE,
      origin: window.location.origin,
      mode: viteEnv.MODE,
      mobile: /Mobi|Android|iPhone|iPad/i.test(navigator.userAgent)
    });
  }, []);

  function applyRuntimeConfig(config: ConfigResponse) {
    setAuthDisabled(Boolean(config.authDisabled));
    setDurableStorage(config.durableStorage !== false);
    setEmailConfigured(Boolean(config.emailConfigured));
    setCsrfToken(config.csrfToken ?? '');
    setSessionTimeoutMs((config.sessionTimeoutMinutes ?? 30) * 60 * 1000);
    setSessionWarningSeconds(Math.max(config.sessionWarningSeconds ?? 60, 15));
    setPersistenceState(config.durableStorage === false ? 'Protected workspace storage needs to be connected before workspace changes can be saved permanently.' : 'Enterprise-grade secure cloud storage ready.');

    if (config.durableStorage !== false && config.postgresqlConnected !== false) {
      localStorage.removeItem(STARTUP_ERROR_KEY);
      sessionStorage.removeItem(STARTUP_ERROR_KEY);
      setAuthMessage((message) => isStartupRecoveryMessage(message) ? DEFAULT_AUTH_MESSAGE : message);
    }
  }

  useEffect(() => {
    fetchWithRetry('/api/config', { credentials: 'include' }, 3)
      .then((response) => readJson<ConfigResponse>(response))
      .then(applyRuntimeConfig)
      .catch((error) => {
        console.error('[Metenova API] Config request failed', error);
        setAuthDisabled(false);
        setCsrfToken('');
        setAuthMessage('Login is available. Secure configuration is retrying in the background.');
        setPersistenceState('Protected workspace infrastructure ready.');
      })
      .finally(() => setConfigLoaded(true));
  }, []);

  useEffect(() => {
    if (user) return undefined;
    const shouldPoll = isStartupRecoveryMessage(authMessage) || durableStorage === false;
    if (!shouldPoll) return undefined;

    const interval = window.setInterval(() => {
      fetchWithRetry('/api/config', { credentials: 'include' }, 1)
        .then((response) => readJson<ConfigResponse>(response))
        .then(applyRuntimeConfig)
        .catch((error) => console.warn('[Metenova API] Startup recovery check failed', error));
    }, 5000);

    return () => window.clearInterval(interval);
  }, [authMessage, durableStorage, user]);

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
    if (user && selectedCompanyId) {
      void loadCompanyWorkspace(selectedCompanyId);
    }
  }, [selectedCompanyId, user?.id]);

  useEffect(() => {
    if (activeDataset) {
      void loadCleanupHistory(activeDataset);
    } else {
      setCleanupJobs([]);
    }
  }, [activeDataset?.id]);

  useEffect(() => {
    const workspaceRoute = workspaceRoutes.find((route) => route.path === location.pathname)
      ?? (location.pathname.startsWith('/data-processing/') ? dataProcessingWorkspaceRoute : undefined);
    if (workspaceRoute) {
      setCurrentView(workspaceRoute.module as AppView);
      return;
    }
    if (location.pathname === '/analytics/dashboard') {
      setCurrentView('analytics');
    } else if (location.pathname === '/reports/history') {
      setCurrentView('reports');
    } else if (location.pathname === '/companies') {
      setCurrentView('companies');
    } else if (location.pathname.startsWith('/admin/')) {
      setCurrentView('adminUsers');
    }
  }, [location.pathname]);

  useEffect(() => {
    const syncAuth = (event: StorageEvent) => {
      if (event.key === LOGOUT_BROADCAST_KEY && event.newValue) {
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
        sessionStorage.setItem(SESSION_ACTIVITY_KEY, String(Date.now()));
        setShowSessionWarning(false);
      }
    };
    const checkSession = () => {
      const lastActivity = Number(sessionStorage.getItem(SESSION_ACTIVITY_KEY) || Date.now());
      const elapsed = Date.now() - lastActivity;
      const remainingMs = sessionTimeoutMs - elapsed;
      const nextSeconds = Math.max(Math.ceil(remainingMs / 1000), 0);
      setSessionSecondsLeft(nextSeconds);

      if (remainingMs <= 0) {
        localStorage.setItem(LOGOUT_BROADCAST_KEY, String(Date.now()));
        logoutRemote('Session expired after 30 minutes of inactivity.');
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

  async function refreshCsrfToken() {
    const response = await fetchWithRetry('/api/config', { credentials: 'include' }, 3);
    const config = await readJson<ConfigResponse>(response);
    if (!response.ok || !config.csrfToken) {
      throw new Error('Could not refresh secure request token.');
    }
    setCsrfToken(config.csrfToken);
    return config.csrfToken;
  }

  async function apiFetch(path: string, options: RequestInit = {}) {
    const method = String(options.method || 'GET').toUpperCase();
    const isMutation = !['GET', 'HEAD'].includes(method);
    const tokenForRequest = isMutation && !csrfToken ? await refreshCsrfToken() : csrfToken;
    const headers = new Headers(options.headers);

    if (token) {
      headers.set('Authorization', `Bearer ${token}`);
    }
    if (isMutation && tokenForRequest) {
      headers.set('X-CSRF-Token', tokenForRequest);
    }

    const requestUrl = apiUrl(path);
    console.info('[Metenova API] request', { path, requestUrl });

    const requestInit = {
      ...options,
      headers,
      credentials: 'include'
    };

      const response = await fetchWithRetry(path, requestInit, 2);
    if (response.status === 403 && isMutation) {
      const payload = await response.clone().json().catch(() => ({} as { code?: string }));
      if (payload.code === 'CSRF_INVALID') {
        const refreshed = await refreshCsrfToken();
        headers.set('X-CSRF-Token', refreshed);
        return fetchWithRetry(path, requestInit, 2);
      }
    }

    if (response.status === 401) {
      const payload = await readJson<{ error?: string; code?: string }>(response);
      const message = payload.code === 'SESSION_EXPIRED'
        ? 'Session expired. Please sign in again.'
        : payload.error || 'Authentication required.';
      logout(message);
      throw new Error(message);
    }

    return response;
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
        moduleMetricsResponse,
        enterpriseOpsResponse
      ] = await Promise.all([
        apiFetch('/api/auth/me'),
        apiFetch('/api/insights'),
        apiFetch('/api/workflows'),
        apiFetch('/api/datasets'),
        apiFetch('/api/dashboards'),
        apiFetch('/api/reports'),
        apiFetch('/api/modules/metrics'),
        apiFetch('/api/enterprise-operations')
      ]);

      const mePayload = await readJson<{ user: User }>(meResponse);
      const insightsPayload = await readJson<InsightResponse>(insightsResponse);
      const workflowsPayload = await readJson<{ workflows: Workflow[] }>(workflowsResponse);
      const datasetsPayload = await readJson<{ datasets: Dataset[] }>(datasetsResponse);
      const dashboardsPayload = await readJson<{ dashboards: SavedDashboard[] }>(dashboardsResponse);
      const reportsPayload = await readJson<{ reports: ReportHistoryItem[] }>(reportsResponse);
      const moduleMetricsPayload = await readJson<{ metrics: ModuleMetrics }>(moduleMetricsResponse);
      const enterpriseOpsPayload = await readJson<EnterpriseOperations>(enterpriseOpsResponse);

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
      setEnterpriseOps(enterpriseOpsPayload ?? emptyEnterpriseOperations);
      setStatus('Live');
      void loadCompanies();

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
    sessionStorage.removeItem(AUTH_TOKEN_KEY);
    sessionStorage.removeItem(SESSION_ACTIVITY_KEY);
    localStorage.removeItem(LEGACY_AUTH_TOKEN_KEY);
    setToken('');
    setUser(null);
    setDatasets([]);
    setActiveDataset(null);
    setCleanupJobs([]);
    setDashboards([]);
    setReports([]);
    setNotifications([]);
    setEnterpriseOps(emptyEnterpriseOperations);
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
      localStorage.setItem(LOGOUT_BROADCAST_KEY, String(Date.now()));
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
      const response = await fetchWithRetry(`/api/auth/${credentials.mode}`, {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: credentials.name,
          email: credentials.email,
          password: credentials.password
        })
      });

      const payload = await readJson<Partial<AuthResponse> & { error?: string; code?: string; database?: DatabaseStatus; requestId?: string }>(response);

      if (!response.ok || !payload.token || !payload.user) {
        const message = startupErrorMessage(payload);
        if (payload.code === 'STARTUP_NOT_READY' || payload.code === 'POSTGRESQL_UNAVAILABLE') sessionStorage.setItem(STARTUP_ERROR_KEY, message);
        throw new Error(message);
      }

      sessionStorage.setItem(AUTH_TOKEN_KEY, payload.token);
      sessionStorage.setItem(SESSION_ACTIVITY_KEY, String(Date.now()));
      localStorage.removeItem(STARTUP_ERROR_KEY);
      sessionStorage.removeItem(STARTUP_ERROR_KEY);
      setToken(payload.token);
      setUser(payload.user);
      setAuthPassword('');
      setAuthMessage('Welcome back.');
      setStatus('Live');
      setCurrentView('dashboard');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Authentication failed.';
      console.error('[Metenova Auth] Authentication request failed', {
        apiBase: API_BASE,
        origin: window.location.origin,
        mode: credentials.mode,
        message
      });
      setAuthMessage(message === 'Failed to fetch' ? 'Could not reach the authentication service. Please retry; production connectivity has been logged.' : message);
    }
  }

  async function acceptInvite(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setAuthMessage('Activating your workspace invitation...');

    try {
      const response = await fetchWithRetry('/api/auth/accept-invite', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          token: inviteToken,
          name: inviteName,
          password: invitePassword
        })
      });
      const payload = await readJson<Partial<AuthResponse> & { error?: string; code?: string; database?: DatabaseStatus; requestId?: string }>(response);
      if (!response.ok || !payload.token || !payload.user) {
        const message = startupErrorMessage(payload);
        if (payload.code === 'STARTUP_NOT_READY' || payload.code === 'POSTGRESQL_UNAVAILABLE') sessionStorage.setItem(STARTUP_ERROR_KEY, message);
        throw new Error(message);
      }

      sessionStorage.setItem(AUTH_TOKEN_KEY, payload.token);
      sessionStorage.setItem(SESSION_ACTIVITY_KEY, String(Date.now()));
      localStorage.removeItem(STARTUP_ERROR_KEY);
      sessionStorage.removeItem(STARTUP_ERROR_KEY);
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
      const response = await fetch(apiUrl('/api/auth/forgot-password'), {
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
      const response = await fetch(apiUrl('/api/auth/recover-username'), {
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

  const selectedCompany = useMemo(
    () => companies.find((company) => company.id === selectedCompanyId) ?? companies[0] ?? sampleCompanies[0],
    [companies, selectedCompanyId]
  );
  const companyDatasets = useMemo(
    () => selectedCompanyId ? datasets.filter((dataset) => dataset.companyId === selectedCompanyId) : datasets,
    [datasets, selectedCompanyId]
  );
  const activeCleanedDataset = useMemo(() => {
    if (!activeDataset) return null;
    if (activeDataset.originalDatasetId) return activeDataset;
    return datasets.find((dataset) => dataset.id === activeDataset.cleanedDatasetId) ?? null;
  }, [activeDataset, datasets]);
  const companyDashboards = useMemo(
    () => selectedCompanyId ? dashboards.filter((dashboard) => dashboard.companyId === selectedCompanyId) : dashboards,
    [dashboards, selectedCompanyId]
  );
  const companyReports = useMemo(
    () => selectedCompanyId ? reports.filter((report) => report.companyId === selectedCompanyId) : reports,
    [reports, selectedCompanyId]
  );
  const companyCleanupJobs = useMemo(
    () => selectedCompanyId ? cleanupJobs.filter((job) => job.companyId === selectedCompanyId) : cleanupJobs,
    [cleanupJobs, selectedCompanyId]
  );
  const datasetHealthScore = useMemo(() => {
    if (!companyDatasets.length) return 100;
    const issues = companyDatasets.reduce((total, dataset) => total
      + (dataset.cleanupMetrics?.invalidValuesDetected ?? 0)
      + (dataset.cleanupMetrics?.failedRows ?? 0)
      + (dataset.cleanupMetrics?.duplicatesRemoved ?? 0), 0);
    return Math.max(40, Math.round(100 - issues / Math.max(companyDatasets.length, 1)));
  }, [companyDatasets]);
  const failedRecordCount = useMemo(
    () => companyDatasets.reduce((total, dataset) => total + (dataset.cleanupMetrics?.failedRows ?? 0), 0),
    [companyDatasets]
  );
  const connectorHealth = useMemo(() => {
    const total = enterpriseOps.connectors.length;
    const healthy = enterpriseOps.connectors.filter((connector) => ['healthy', 'ready'].includes(connector.healthStatus) || connector.status === 'ready').length;
    return { total, healthy, failed: enterpriseOps.connectors.filter((connector) => connector.status === 'failed').length };
  }, [enterpriseOps.connectors]);

  async function uploadDataset(file: File, worksheetName?: string, companyIdOverride?: string) {
    const targetCompanyId = companyIdOverride || selectedCompanyId;
    if (!targetCompanyId) {
      setUploadState('Select a company before uploading a dataset.');
      return undefined;
    }

    const extension = file.name.split('.').pop()?.toLowerCase();
    if (!extension || !['csv', 'xlsx', 'xls', 'json'].includes(extension)) {
      setUploadState('Upload a .csv, .xlsx, .xls, or .json file.');
      return undefined;
    }

    const formData = new FormData();
    formData.append('file', file);
    formData.append('companyId', targetCompanyId);
    formData.append('module', currentView);
    if (worksheetName) {
      formData.append('worksheetName', worksheetName);
    }
    setUploadState(`Analyzing ${file.name}${worksheetName ? ` - ${worksheetName}` : ''}...`);

    try {
      const response = await apiFetch('/api/files/upload', {
        method: 'POST',
        body: formData
      });

      const dataset = await readJson<Dataset & { error?: string; code?: string; uploadStage?: string; requestId?: string }>(response);

      if (!response.ok) {
        throw new Error(formatUploadError(dataset, response.status));
      }

      setLastUploadedFile(file);
      setActiveDataset(dataset);
      setDatasets((current: Dataset[]) => [dataset, ...current.filter((item) => item.id !== dataset.id)]);
      setChat([{ role: 'assistant', text: `I loaded ${dataset.fileName}${dataset.worksheetName ? ` (${dataset.worksheetName})` : ''}. Ask me what changed, what stands out, or how many rows it has.` }]);
      const storageLabel = 'saved to your protected workspace';
      const warning = dataset.warnings?.[0] ? ` ${dataset.warnings[0]}` : '';
      setUploadState(`${(dataset.fileType ?? extension).toUpperCase()} analysis ready and ${storageLabel}.${warning}`);
      return dataset;
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Upload failed.';
      console.error('[Metenova Upload] Upload failed', {
        companyId: targetCompanyId,
        fileName: file.name,
        fileType: file.type || extension,
        message
      });
      setUploadState(message);
      return undefined;
    }
  }

  function handleDatasetUpload(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    if (file) {
      uploadDataset(file);
    }
  }

  async function loadCleanupHistory(dataset: Dataset) {
    try {
      const sourceDatasetId = dataset.originalDatasetId ?? dataset.id;
      const response = await apiFetch(`/api/datasets/${sourceDatasetId}/cleanup-jobs`);
      const payload = await readJson<{ cleanupJobs?: CleanupJob[]; error?: string }>(response);
      if (!response.ok) {
        throw new Error(payload.error || 'Cleanup history could not be loaded.');
      }
      setCleanupJobs(payload.cleanupJobs ?? []);
    } catch (error) {
      setCleanupMessage(error instanceof Error ? error.message : 'Cleanup history could not be loaded.');
    }
  }

  async function cleanActiveDataset() {
    if (!activeDataset || activeDataset.originalDatasetId) {
      setCleanupMessage('Select an original uploaded dataset before running cleanup.');
      return;
    }

    setCleanupRunning(true);
    setCleanupMessage('Cleanup queued and processing...');
    try {
      const response = await apiFetch(`/api/datasets/${activeDataset.id}/cleanup`, { method: 'POST' });
      const payload = await readJson<{ job?: CleanupJob; originalDataset?: Dataset; cleanedDataset?: Dataset; error?: string }>(response);
      if (!response.ok || !payload.cleanedDataset || !payload.originalDataset) {
        throw new Error(payload.error || 'Cleanup failed.');
      }

      setDatasets((current) => [
        payload.cleanedDataset as Dataset,
        payload.originalDataset as Dataset,
        ...current.filter((item) => item.id !== payload.cleanedDataset?.id && item.id !== payload.originalDataset?.id)
      ]);
      setActiveDataset(payload.cleanedDataset);
      setCleanupJobs((current) => payload.job ? [payload.job, ...current.filter((job) => job.id !== payload.job?.id)] : current);
      setCleanupMessage('Cleaned dataset created and ready for business analytics.');
      setPersistenceState('Original preserved. Cleaned dataset saved to the company workspace.');
    } catch (error) {
      setCleanupMessage(error instanceof Error ? error.message : 'Cleanup failed.');
    } finally {
      setCleanupRunning(false);
    }
  }

  async function downloadDatasetExport(dataset: Dataset | null) {
    if (!dataset) {
      return;
    }

    try {
      const response = await apiFetch(`/api/datasets/${dataset.id}/export`);
      if (!response.ok) {
        const payload = await readJson<{ error?: string }>(response);
        throw new Error(payload.error || 'Export failed.');
      }
      const blob = await response.blob();
      const link = document.createElement('a');
      link.href = URL.createObjectURL(blob);
      link.download = dataset.fileName.replace(/\.(csv|xlsx|xls)$/i, '.csv');
      link.click();
      URL.revokeObjectURL(link.href);
      setCleanupMessage('Dataset export downloaded.');
    } catch (error) {
      setCleanupMessage(error instanceof Error ? error.message : 'Export failed.');
    }
  }

  async function deleteDatasetRecord(dataset: Dataset | null = activeDataset) {
    if (!dataset) return;
    if (!canManageWorkspaceData(user)) {
      setPersistenceState('You need manager access to delete datasets.');
      return;
    }
    const versionLabel = dataset.originalDatasetId ? 'this cleaned version' : 'this original dataset and its cleaned versions';
    if (!window.confirm(`Delete ${dataset.fileName}? This removes ${versionLabel} from active datasets and archives linked cleanup/report assets.`)) {
      return;
    }
    setDeletingDatasetId(dataset.id);
    setWorkspaceAction(`Deleting ${dataset.fileName}...`);
    try {
      const response = await apiFetch(`/api/datasets/${dataset.id}`, { method: 'DELETE' });
      const payload = await readJson<{ success?: boolean; datasetId?: string; deleted?: boolean; deletedIds?: string[]; error?: string }>(response);
      if (!response.ok || !payload.success || !payload.deleted) {
        console.error('[Metenova API] Dataset delete failed', { datasetId: dataset.id, status: response.status, payload });
        throw new Error(payload.error || 'Dataset delete failed.');
      }
      const deletedIds = new Set(payload.deletedIds?.length ? payload.deletedIds : [payload.datasetId ?? dataset.id]);
      setDatasets((current) => current.filter((item) => !deletedIds.has(item.id)));
      setDashboards((current) => current.filter((dashboard) => !deletedIds.has(dashboard.datasetId)));
      setReports((current) => current.filter((report) => !deletedIds.has(report.datasetId)));
      setCleanupJobs((current) => current.filter((job) => !deletedIds.has(job.originalDatasetId) && !(job.cleanedDatasetId && deletedIds.has(job.cleanedDatasetId))));
      setActiveDataset((current) => {
        if (!current || deletedIds.has(current.id)) {
          return datasets.find((item) => !deletedIds.has(item.id) && item.companyId === dataset.companyId) ?? null;
        }
        return current;
      });
      if (dataset.companyId) {
        await loadCompanyWorkspace(dataset.companyId);
      } else {
        await loadWorkspace();
      }
      setCleanupMessage(`${dataset.fileName} deleted from active datasets.`);
      setPersistenceState('Dataset deleted and workspace refreshed.');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Dataset delete failed.';
      setCleanupMessage(message);
      setPersistenceState(message);
    } finally {
      setDeletingDatasetId('');
      setWorkspaceAction('');
    }
  }

  async function archiveDatasetRecord(dataset: Dataset | null = activeDataset) {
    if (!dataset) return;
    if (!canManageWorkspaceData(user)) {
      setPersistenceState('You need manager access to archive datasets.');
      return;
    }
    setWorkspaceAction(`Archiving ${dataset.fileName}...`);
    try {
      const response = await apiFetch(`/api/datasets/${dataset.id}/archive`, { method: 'POST' });
      const payload = await readJson<{ dataset?: Dataset; error?: string }>(response);
      if (!response.ok || !payload.dataset) throw new Error(payload.error || 'Dataset archive failed.');
      setDatasets((current) => current.map((entry) => entry.id === payload.dataset?.id ? payload.dataset as Dataset : entry));
      setActiveDataset(payload.dataset);
      setPersistenceState('Dataset archived.');
    } catch (error) {
      setPersistenceState(error instanceof Error ? error.message : 'Dataset archive failed.');
    } finally {
      setWorkspaceAction('');
    }
  }

  async function deleteCleanupJobRecord(job: CleanupJob) {
    if (!canManageWorkspaceData(user)) {
      setCleanupMessage('You need manager access to delete cleanup jobs.');
      return;
    }
    if (!window.confirm('Delete this cleanup job from history?')) return;
    try {
      const response = await apiFetch(`/api/cleanup-jobs/${job.id}`, { method: 'DELETE' });
      const payload = await readJson<{ error?: string }>(response);
      if (!response.ok) throw new Error(payload.error || 'Cleanup job delete failed.');
      setCleanupJobs((current) => current.filter((entry) => entry.id !== job.id));
      setCleanupMessage('Cleanup job deleted.');
    } catch (error) {
      setCleanupMessage(error instanceof Error ? error.message : 'Cleanup job delete failed.');
    }
  }

  async function deleteReportRecord(report: ReportHistoryItem) {
    if (!canManageWorkspaceData(user)) {
      setPersistenceState('You need manager access to delete reports.');
      return;
    }
    if (!window.confirm(`Delete report ${report.title}?`)) return;
    try {
      const response = await apiFetch(`/api/reports/${report.id}`, { method: 'DELETE' });
      const payload = await readJson<{ error?: string }>(response);
      if (!response.ok) throw new Error(payload.error || 'Report delete failed.');
      setReports((current) => current.filter((entry) => entry.id !== report.id));
      setPersistenceState('Report deleted.');
    } catch (error) {
      setPersistenceState(error instanceof Error ? error.message : 'Report delete failed.');
    }
  }

  async function updateNotificationRecord(notification: NotificationItem, updates: { status?: string; archive?: boolean }) {
    try {
      const response = await apiFetch(`/api/notifications/${notification.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates)
      });
      const payload = await readJson<{ notification?: NotificationItem; error?: string }>(response);
      if (!response.ok || !payload.notification) throw new Error(payload.error || 'Notification update failed.');
      setNotifications((current) => updates.archive
        ? current.filter((entry) => entry.id !== notification.id)
        : current.map((entry) => entry.id === notification.id ? payload.notification as NotificationItem : entry));
    } catch (error) {
      setPersistenceState(error instanceof Error ? error.message : 'Notification update failed.');
    }
  }

  async function syncEnterpriseConnector(connector: EnterpriseConnector) {
    if (!selectedCompanyId) return;
    setSyncingConnectorId(connector.id);
    setEnterpriseOpsMessage(`Syncing ${connector.name}...`);
    try {
      const response = await apiFetch(`/api/connectors/${connector.id}/sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ companyId: selectedCompanyId, recordsProcessed: Math.max(companyDatasets.length * 125, 250) })
      });
      const payload = await readJson<{ syncLog?: ConnectorSyncLog; error?: string }>(response);
      if (!response.ok || !payload.syncLog) throw new Error(payload.error || 'Connector sync failed.');
      setEnterpriseOps((current) => ({
        ...current,
        syncLogs: [payload.syncLog as ConnectorSyncLog, ...current.syncLogs],
        connectors: current.connectors.map((entry) => entry.id === connector.id ? {
          ...entry,
          status: payload.syncLog?.status === 'failed' ? 'failed' : 'ready',
          healthStatus: payload.syncLog?.status === 'failed' ? 'degraded' : 'healthy',
          lastSyncAt: payload.syncLog?.createdAt ?? new Date().toISOString()
        } : entry)
      }));
      setEnterpriseOpsMessage(`${connector.name} sync completed. ${payload.syncLog.recordsProcessed.toLocaleString()} records processed.`);
    } catch (error) {
      setEnterpriseOpsMessage(error instanceof Error ? error.message : 'Connector sync failed.');
    } finally {
      setSyncingConnectorId('');
    }
  }

  async function createNightlySchedule() {
    if (!selectedCompanyId) return;
    try {
      const response = await apiFetch('/api/pipeline-schedules', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          companyId: selectedCompanyId,
          name: 'Nightly data operations run',
          scheduleType: 'cron',
          cronExpression: '0 2 * * *',
          priority: 2,
          slaMinutes: 60,
          retryPolicy: { attempts: 3, backoffMinutes: 15 },
          dependencies: ['connector:ready', 'approval:not_required'],
          metadata: { module: currentView, eventDriven: false }
        })
      });
      const payload = await readJson<{ schedule?: PipelineSchedule; error?: string }>(response);
      if (!response.ok || !payload.schedule) throw new Error(payload.error || 'Schedule could not be created.');
      setEnterpriseOps((current) => ({ ...current, schedules: [payload.schedule as PipelineSchedule, ...current.schedules] }));
      setEnterpriseOpsMessage('Nightly operations schedule created with retry and SLA tracking.');
    } catch (error) {
      setEnterpriseOpsMessage(error instanceof Error ? error.message : 'Schedule could not be created.');
    }
  }

  async function requestWorkflowAccess() {
    if (!selectedCompanyId) return;
    try {
      const response = await apiFetch('/api/access-requests', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          companyId: selectedCompanyId,
          department: currentView,
          requestedRole: 'manager',
          reason: 'Temporary access requested for enterprise workflow operations.'
        })
      });
      const payload = await readJson<{ accessRequest?: AccessRequest; error?: string }>(response);
      if (!response.ok || !payload.accessRequest) throw new Error(payload.error || 'Access request failed.');
      setEnterpriseOps((current) => ({ ...current, accessRequests: [payload.accessRequest as AccessRequest, ...current.accessRequests] }));
      setEnterpriseOpsMessage('Access request routed for approval.');
    } catch (error) {
      setEnterpriseOpsMessage(error instanceof Error ? error.message : 'Access request failed.');
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
        companyId: activeDataset.companyId,
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
      apiFetch(`/api/dashboards${buildCompanyQuery(selectedCompanyId)}`),
      apiFetch(`/api/reports${buildCompanyQuery(selectedCompanyId)}`)
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
      const platformAdmin = user?.role === 'owner' || user?.role === 'admin';
      const usersResponse = await apiFetch('/api/admin/users');
      const payload = await readJson<{ users: AdminUser[] }>(usersResponse);
      if (!usersResponse.ok) {
        throw new Error((payload as { error?: string }).error || 'Could not load users.');
      }
      setAdminUsers(payload.users ?? []);
      if (platformAdmin) {
        const [auditResponse, systemResponse, invitationsResponse, emailLogsResponse] = await Promise.all([
          apiFetch('/api/admin/audit-logs'),
          apiFetch('/api/admin/system'),
          apiFetch('/api/admin/invitations'),
          apiFetch('/api/admin/email-logs')
        ]);
        const auditPayload = await readJson<{ auditLogs: AuditLog[] }>(auditResponse);
        const systemPayload = await readJson<SystemStatus>(systemResponse);
        const invitationPayload = await readJson<{ invitations: Invitation[] }>(invitationsResponse);
        const emailLogPayload = await readJson<{ emailLogs: EmailLog[] }>(emailLogsResponse);
        setAuditLogs(auditPayload.auditLogs ?? []);
        setSystemStatus(systemPayload);
        setInvitations(invitationPayload.invitations ?? []);
        setEmailLogs(emailLogPayload.emailLogs ?? []);
      } else {
        setAuditLogs([]);
        setSystemStatus(null);
        setInvitations([]);
        setEmailLogs([]);
      }
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
        sessionStorage.setItem(AUTH_TOKEN_KEY, refreshPayload.token);
        sessionStorage.setItem(SESSION_ACTIVITY_KEY, String(Date.now()));
        setToken(refreshPayload.token);
      }
      setAdminMessage('User access updated.');
    } catch (error) {
      setAdminMessage(error instanceof Error ? error.message : 'User update failed.');
    }
  }

  function openCompanyAccess(userRecord: AdminUser) {
    setAccessUser(userRecord);
    setAccessCompanyIds((userRecord.assignedCompanies ?? []).map((assignment) => assignment.companyId));
    setAccessRole((userRecord.assignedCompanies?.[0]?.role as UserRole | undefined) ?? userRecord.role ?? 'employee');
  }

  function toggleAccessCompany(companyId: string) {
    setAccessCompanyIds((current) => (
      current.includes(companyId)
        ? current.filter((entry) => entry !== companyId)
        : [...current, companyId]
    ));
  }

  async function saveCompanyAccess() {
    if (!accessUser) return;
    setAccessSaving(true);
    try {
      const assignments = accessCompanyIds.map((companyId) => ({ companyId, role: accessRole }));
      const response = await apiFetch(`/api/admin/users/${accessUser.id}/company-assignments`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ assignments })
      });
      const payload = await readJson<{ user?: AdminUser; assignments?: CompanyAssignment[]; error?: string }>(response);
      if (!response.ok || !payload.user) {
        throw new Error(payload.error || 'Could not update company access.');
      }
      setAdminUsers((current) => current.map((entry) => entry.id === payload.user?.id ? payload.user : entry));
      setAccessUser(null);
      setAdminMessage('Company access updated.');
    } catch (error) {
      setAdminMessage(error instanceof Error ? error.message : 'Could not update company access.');
    } finally {
      setAccessSaving(false);
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
      const payload = await readJson<{ success?: boolean; message?: string; error?: string; delivery?: { status?: string; error?: string } }>(response);

      if (!response.ok || payload.success === false) {
        throw new Error(payload.error || 'Support request failed.');
      }

      setContactMessage('');
      setContactContext('');
      form.reset();
      setSupportMessage(payload.message || 'Message sent successfully');
    } catch (error) {
      setSupportMessage(error instanceof Error ? error.message : 'Support request failed.');
    } finally {
      setSupportSending(false);
    }
  }

  function updateCompanyForm(field: keyof CompanyFormValues, value: string) {
    setCompanyForm((current) => ({ ...current, [field]: value }));
  }

  function resetCompanyForm() {
    setCompanyForm({
      name: '',
      industry: '',
      ownerName: '',
      email: '',
      phone: ''
    });
  }

  async function loadCompanies() {
    setCompaniesLoading(true);
    setCompaniesError('');
    try {
      const response = await apiFetch('/api/companies');
      const payload = await readJson<{ companies?: Company[]; error?: string }>(response);
      if (!response.ok) {
        throw new Error(payload.error || 'Could not load companies.');
      }

      const nextCompanies = payload.companies?.length ? payload.companies : sampleCompanies;
      setCompanies(nextCompanies);
      setSelectedCompanyId((current) => nextCompanies.some((company) => company.id === current) ? current : nextCompanies[0]?.id ?? '');
    } catch (error) {
      setCompanies(sampleCompanies);
      setCompaniesError(error instanceof Error ? error.message : 'Could not load companies.');
    } finally {
      setCompaniesLoading(false);
    }
  }

  async function loadCompanyWorkspace(companyId: string) {
    if (!companyId) return;

    try {
      const query = buildCompanyQuery(companyId);
      const [datasetsResponse, dashboardsResponse, reportsResponse, notificationsResponse, enterpriseOpsResponse] = await Promise.all([
        apiFetch(`/api/datasets${query}`),
        apiFetch(`/api/dashboards${query}`),
        apiFetch(`/api/reports${query}`),
        apiFetch(`/api/notifications${query}`),
        apiFetch(`/api/enterprise-operations${query}`)
      ]);
      const datasetsPayload = await readJson<{ datasets?: Dataset[]; error?: string }>(datasetsResponse);
      const dashboardsPayload = await readJson<{ dashboards?: SavedDashboard[]; error?: string }>(dashboardsResponse);
      const reportsPayload = await readJson<{ reports?: ReportHistoryItem[]; error?: string }>(reportsResponse);
      const notificationsPayload = await readJson<{ notifications?: NotificationItem[]; error?: string }>(notificationsResponse);
      const enterpriseOpsPayload = await readJson<EnterpriseOperations & { error?: string }>(enterpriseOpsResponse);

      if (!datasetsResponse.ok) throw new Error(datasetsPayload.error || 'Could not load company datasets.');
      if (!dashboardsResponse.ok) throw new Error(dashboardsPayload.error || 'Could not load company dashboards.');
      if (!reportsResponse.ok) throw new Error(reportsPayload.error || 'Could not load company reports.');
      if (!notificationsResponse.ok) throw new Error(notificationsPayload.error || 'Could not load notifications.');
      if (!enterpriseOpsResponse.ok) throw new Error(enterpriseOpsPayload.error || 'Could not load enterprise operations.');

      const nextDatasets = datasetsPayload.datasets ?? [];
      setDatasets(nextDatasets);
      setDashboards(dashboardsPayload.dashboards ?? []);
      setReports(reportsPayload.reports ?? []);
      setNotifications(notificationsPayload.notifications ?? []);
      setEnterpriseOps({
        connectors: enterpriseOpsPayload.connectors ?? [],
        syncLogs: enterpriseOpsPayload.syncLogs ?? [],
        schedules: enterpriseOpsPayload.schedules ?? [],
        intelligence: enterpriseOpsPayload.intelligence ?? [],
        accessRequests: enterpriseOpsPayload.accessRequests ?? []
      });
      setActiveDataset((current) => current?.companyId === companyId ? current : nextDatasets[0] ?? null);
      setPersistenceState(nextDatasets.length ? 'Company workspace data loaded.' : 'No datasets uploaded for this company yet.');
    } catch (error) {
      setPersistenceState(error instanceof Error ? error.message : 'Could not load company workspace.');
    }
  }

  async function saveCompany(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const nextCompany = {
      name: companyForm.name.trim(),
      industry: companyForm.industry.trim(),
      ownerName: companyForm.ownerName.trim(),
      email: companyForm.email.trim(),
      phone: companyForm.phone.trim()
    };

    if (!nextCompany.name || !nextCompany.industry || !nextCompany.ownerName || !nextCompany.email || !nextCompany.phone) {
      setPersistenceState('Complete all company fields before saving.');
      setCompaniesError('Complete all company fields before saving.');
      return;
    }

    setCompanySaving(true);
    setCompaniesError('');
    try {
      const response = await apiFetch('/api/companies', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(nextCompany)
      });
      const payload = await readJson<{ company?: Company; error?: string }>(response);
      if (!response.ok || !payload.company) {
        throw new Error(payload.error || 'Company could not be created.');
      }

      resetCompanyForm();
      setCompanyFormOpen(false);
      setPersistenceState(`${payload.company.name} workspace saved.`);
      await loadCompanies();
    } catch (error) {
      setCompaniesError(error instanceof Error ? error.message : 'Company could not be created.');
    } finally {
      setCompanySaving(false);
    }
  }

  async function updateCompanyName(company: Company) {
    const nextName = window.prompt('Company name', company.name)?.trim();
    if (!nextName || nextName === company.name) return;
    setWorkspaceAction(`Renaming ${company.name}...`);
    try {
      const response = await apiFetch(`/api/companies/${company.id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: nextName })
      });
      const payload = await readJson<{ company?: Company; error?: string }>(response);
      if (!response.ok || !payload.company) throw new Error(payload.error || 'Company update failed.');
      setCompanies((current) => current.map((entry) => entry.id === company.id ? payload.company as Company : entry));
      setPersistenceState('Company renamed.');
    } catch (error) {
      setPersistenceState(error instanceof Error ? error.message : 'Company update failed.');
    } finally {
      setWorkspaceAction('');
    }
  }

  async function deleteCompanyWorkspace(company: Company) {
    if (user?.role !== 'owner') {
      setPersistenceState('Only the owner role can delete a company workspace.');
      return;
    }
    if (!window.confirm(`Delete ${company.name}? This removes company datasets, cleanup jobs, reports, dashboards, pipelines, and users.`)) return;
    setWorkspaceAction(`Deleting ${company.name}...`);
    try {
      const response = await apiFetch(`/api/companies/${company.id}`, { method: 'DELETE' });
      if (!response.ok) {
        const payload = await readJson<{ error?: string }>(response);
        throw new Error(payload.error || 'Company delete failed.');
      }
      setCompanies((current) => current.filter((entry) => entry.id !== company.id));
      if (selectedCompanyId === company.id) {
        const fallbackCompany = companies.find((entry) => entry.id !== company.id);
        setSelectedCompanyId(fallbackCompany?.id ?? '');
        setDatasets([]);
        setDashboards([]);
        setReports([]);
        setNotifications([]);
        setActiveDataset(null);
      }
      setPersistenceState('Company workspace deleted.');
    } catch (error) {
      setPersistenceState(error instanceof Error ? error.message : 'Company delete failed.');
    } finally {
      setWorkspaceAction('');
    }
  }

  async function runCompanyAction(company: Company, action: string) {
    setSelectedCompanyId(company.id);
    setWorkspaceAction(`${action} for ${company.name}...`);
    try {
      await loadCompanyWorkspace(company.id);
      const query = buildCompanyQuery(company.id);

      if (action === 'Upload Data') {
        navigate('/');
        setCurrentView('dashboard');
        setUploadState(`${company.name}: choose a CSV or Excel file to upload.`);
      } else if (action === 'Clean Data') {
        const response = await apiFetch(`/api/datasets${query}`);
        const payload = await readJson<{ datasets?: Dataset[]; error?: string }>(response);
        if (!response.ok) throw new Error(payload.error || 'Could not load datasets.');
        const candidate = (payload.datasets ?? []).find((dataset) => !dataset.originalDatasetId);
        if (!candidate) throw new Error('Upload an original dataset before running cleanup.');
        setActiveDataset(candidate);
        const cleanupResponse = await apiFetch(`/api/datasets/${candidate.id}/cleanup`, { method: 'POST' });
        const cleanupPayload = await readJson<{ cleanedDataset?: Dataset; originalDataset?: Dataset; job?: CleanupJob; error?: string }>(cleanupResponse);
        if (!cleanupResponse.ok || !cleanupPayload.cleanedDataset || !cleanupPayload.originalDataset) {
          throw new Error(cleanupPayload.error || 'Cleanup failed.');
        }
        setDatasets((current) => [
          cleanupPayload.cleanedDataset as Dataset,
          cleanupPayload.originalDataset as Dataset,
          ...current.filter((item) => item.id !== cleanupPayload.cleanedDataset?.id && item.id !== cleanupPayload.originalDataset?.id)
        ]);
        setActiveDataset(cleanupPayload.cleanedDataset);
        if (cleanupPayload.job) setCleanupJobs((current) => [cleanupPayload.job as CleanupJob, ...current]);
        navigate('/');
        setCurrentView('dashboard');
        setCleanupMessage('Company cleanup completed.');
      } else if (action === 'View Dashboard') {
        navigate('/');
        setCurrentView('dashboard');
      } else if (action === 'Reports') {
        navigate('/reports/history');
      } else if (action === 'Analytics') {
        navigate('/analytics/dashboard');
      } else if (action === 'Pipelines') {
        navigate('/data-processing/workspace');
      } else if (action === 'Export Data') {
        const response = await apiFetch(`/api/datasets${query}`);
        const payload = await readJson<{ datasets?: Dataset[]; error?: string }>(response);
        if (!response.ok) throw new Error(payload.error || 'Could not load datasets.');
        const dataset = (payload.datasets ?? []).find((item) => item.cleanupStatus === 'completed') ?? payload.datasets?.[0] ?? null;
        if (!dataset) throw new Error('No datasets are available to export.');
        await downloadDatasetExport(dataset);
      } else if (action === 'Delete Dataset') {
        const response = await apiFetch(`/api/datasets${query}`);
        const payload = await readJson<{ datasets?: Dataset[]; error?: string }>(response);
        if (!response.ok) throw new Error(payload.error || 'Could not load datasets.');
        const dataset = payload.datasets?.[0] ?? null;
        if (!dataset) throw new Error('No datasets are available to delete.');
        await deleteDatasetRecord(dataset);
      } else if (action === 'Delete Cleanup Job') {
        const response = await apiFetch('/api/cleanup-jobs');
        const payload = await readJson<{ cleanupJobs?: CleanupJob[]; error?: string }>(response);
        if (!response.ok) throw new Error(payload.error || 'Could not load cleanup jobs.');
        const job = (payload.cleanupJobs ?? []).find((entry) => entry.companyId === company.id);
        if (!job) throw new Error('No cleanup jobs are available to delete.');
        await deleteCleanupJobRecord(job);
      }
      setPersistenceState(`${action} completed for ${company.name}.`);
    } catch (error) {
      setPersistenceState(error instanceof Error ? error.message : `${action} failed.`);
    } finally {
      setWorkspaceAction('');
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
        companyId: activeDataset.companyId,
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
    if (view === 'companies') {
      navigate('/companies');
      return;
    }
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
                <span className="nav-icon">{item.icon}</span>
                {item.label}
              </button>
            ))}
        </nav>
      </aside>

      <section className="content" id="overview">
        <header className="topbar">
          <div>
            <p className="eyebrow">Operations command center</p>
            <h1>Business workflow performance</h1>
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

        {accessUser && (
          <CompanyAccessModal
            accessCompanyIds={accessCompanyIds}
            accessRole={accessRole}
            companies={companies}
            onClose={() => setAccessUser(null)}
            onRoleChange={setAccessRole}
            onSave={saveCompanyAccess}
            onToggleCompany={toggleAccessCompany}
            saving={accessSaving}
            user={accessUser}
          />
        )}

        {location.pathname !== '/' ? (
          <RoutedPages
            apiFetch={apiFetch}
            auditLogs={auditLogs}
            canManage={canManageUsers(user)}
            companies={companies}
            companiesError={companiesError}
            companiesLoading={companiesLoading}
            companyForm={companyForm}
            companyFormOpen={companyFormOpen}
            companySaving={companySaving}
            dashboards={companyDashboards}
            datasets={datasets}
            archiveDatasetRecord={archiveDatasetRecord}
            deleteAdminUser={deleteAdminUser}
            deleteCompanyWorkspace={deleteCompanyWorkspace}
            deleteDatasetRecord={deleteDatasetRecord}
            deletingDatasetId={deletingDatasetId}
            downloadHistoricalReport={downloadHistoricalReport}
            downloadDatasetExport={downloadDatasetExport}
            openCompanyAccess={openCompanyAccess}
            reports={companyReports}
            resetCompanyForm={resetCompanyForm}
            runCompanyAction={runCompanyAction}
            saveCompany={saveCompany}
            setCompanyFormOpen={setCompanyFormOpen}
            selectedCompanyId={selectedCompanyId}
            setActiveDataset={setActiveDataset}
            setCleanupJobs={setCleanupJobs}
            setDatasets={setDatasets}
            setSelectedCompanyId={setSelectedCompanyId}
            loadCompanies={loadCompanies}
            systemStatus={systemStatus}
            updateCompanyName={updateCompanyName}
            updateAdminUser={updateAdminUser}
            updateCompanyForm={updateCompanyForm}
            users={adminUsers}
            user={user}
            uploadDataset={uploadDataset}
            workspaceAction={workspaceAction}
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
                      <th>Assigned Companies</th>
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
                            disabled={adminUser.email === ownerEmail && user?.email !== ownerEmail}
                            value={adminUser.role}
                            onChange={(event) => updateAdminUser(adminUser.id, { role: event.target.value as AdminUser['role'] })}
                          >
                            {roleOptions.map((role) => (
                              <option key={role} value={role}>{roleLabel(role)}</option>
                            ))}
                          </select>
                        </td>
                        <td>
                          <AssignedCompaniesList assignments={adminUser.assignedCompanies ?? []} />
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
                            <button className="ghost-button compact" type="button" disabled={adminUser.id === user?.id || adminUser.email === ownerEmail} onClick={() => toggleAdminUser(adminUser)}>
                              {adminUser.active ? 'Disable' : 'Enable'}
                            </button>
                            <button className="ghost-button compact" type="button" disabled={adminUser.id === user?.id} onClick={() => revokeAdminUserSessions(adminUser)}>
                              Revoke
                            </button>
                            <button className="ghost-button compact" type="button" onClick={() => openCompanyAccess(adminUser)}>
                              Assign Companies
                            </button>
                            <button className="ghost-button compact danger" type="button" disabled={adminUser.id === user?.id || adminUser.email === ownerEmail} onClick={() => deleteAdminUser(adminUser)}>
                              Delete
                            </button>
                          </div>
                        </td>
                      </tr>
                    ))}
                    {!adminUsers.length && (
                      <tr>
                        <td colSpan={7}>No users found.</td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
              {(user?.role === 'owner' || user?.role === 'admin') && <div className="admin-ops-grid">
                <article>
                  <h3>Invite workspace user</h3>
                  <form className="module-form invite-form" onSubmit={inviteWorkspaceUser}>
                    <input placeholder="employee@company.com" type="email" value={inviteEmail} onChange={(event) => setInviteEmail(event.target.value)} />
                    <select value={inviteRole} onChange={(event) => setInviteRole(event.target.value as UserRole)}>
                      {roleOptions.filter((role) => role !== 'owner' || user?.email === ownerEmail).map((role) => (
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
              </div>}
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
                Metenova AI is a modern business operations and analytics platform designed to help companies manage data,
                analytics, business workflows, reporting, business automation, data cleanup, user management, reports, and enterprise operations
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
                <div><strong>Dataset intelligence</strong><span>{companyDatasets.length} saved datasets for {selectedCompany?.name ?? 'this company'}.</span></div>
                <div><strong>Dashboards</strong><span>{companyDashboards.length} saved dashboards across this company workspace.</span></div>
                <div><strong>Reports</strong><span>{companyReports.length} generated reports in company history.</span></div>
                <div><strong>Business recommendations</strong><span>Use uploaded data to generate trends, variance explanations, and executive summaries.</span></div>
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
                {companyDashboards.length ? companyDashboards.map((dashboard) => (
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
                {companyReports.length ? companyReports.map((report) => (
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
            <EnterpriseCockpit
              activeCleanedDataset={activeCleanedDataset}
              activeDataset={activeDataset}
              canManageUsers={canManageUsers(user)}
              chartMax={chartMax}
              chartType={chartType}
              cleanupJobs={companyCleanupJobs}
              companies={companies}
              companyDashboards={companyDashboards}
              companyDatasets={companyDatasets}
              companyReports={companyReports}
              connectorHealth={connectorHealth}
              datasetHealthScore={datasetHealthScore}
              downloadDatasetExport={downloadDatasetExport}
              downloadPdfReport={downloadPdfReport}
              enterpriseOps={enterpriseOps}
              enterpriseOpsMessage={enterpriseOpsMessage}
              failedRecordCount={failedRecordCount}
              insights={insights}
              linePoints={linePoints}
              navigate={navigate}
              notifications={notifications}
              onArchiveDataset={archiveDatasetRecord}
              onCleanDataset={cleanActiveDataset}
              onCreateNightlySchedule={createNightlySchedule}
              onOpenView={openView}
              onRequestWorkflowAccess={requestWorkflowAccess}
              onSaveDashboard={saveCurrentDashboard}
              onSelectCompany={setSelectedCompanyId}
              onSelectDataset={setActiveDataset}
              onSyncConnector={syncEnterpriseConnector}
              onToggleTheme={() => setTheme(theme === 'light' ? 'dark' : 'light')}
              persistenceState={persistenceState}
              roleLabel={roleLabel(user?.role)}
              selectedCompany={selectedCompany}
              selectedCompanyId={selectedCompanyId}
              syncingConnectorId={syncingConnectorId}
              theme={theme}
              updateNotificationRecord={updateNotificationRecord}
              uploadState={uploadState}
              user={user}
              workflows={workflows}
            />
            <div className="legacy-dashboard-hidden">
            <section className="panel ops-command-center">
              <div className="panel-header">
                <div>
                  <p className="eyebrow">Interactive operations command center</p>
                  <h2>{selectedCompany?.name ?? 'Company'} workspace</h2>
                </div>
                <span className="status-pill active">{roleLabel(user?.role)} visibility</span>
              </div>
              <div className="command-center-grid">
                <button type="button" onClick={() => openView('companies')}>
                  <span>Company dashboard</span>
                  <strong>{canManageUsers(user) ? `${companies.length} companies` : selectedCompany?.name ?? 'Assigned workspace'}</strong>
                  <small>{canManageUsers(user) ? 'Owner/Admin global view' : 'Scoped to assigned company data'}</small>
                </button>
                  <button type="button" onClick={() => navigate('/data-processing/workspace')}>
                  <span>Pipeline progress</span>
                  <strong>{companyCleanupJobs.filter((job) => job.status === 'completed').length}/{companyCleanupJobs.length}</strong>
                  <small>Completed cleanup jobs</small>
                </button>
                <button type="button" onClick={() => openView('reports')}>
                  <span>Approval and export queue</span>
                  <strong>{companyReports.length}</strong>
                  <small>Reports and export-ready outputs</small>
                </button>
                <button type="button" onClick={() => openView('analytics')}>
                  <span>Dataset health score</span>
                  <strong>{datasetHealthScore}%</strong>
                  <small>{failedRecordCount.toLocaleString()} failed records isolated</small>
                </button>
                <button type="button" onClick={createNightlySchedule}>
                  <span>Enterprise scheduling</span>
                  <strong>{enterpriseOps.schedules.length}</strong>
                  <small>Nightly, hourly, event, SLA, and retry orchestration</small>
                </button>
                <button type="button" onClick={requestWorkflowAccess}>
                  <span>Access governance</span>
                  <strong>{enterpriseOps.accessRequests.filter((request) => request.status === 'pending').length}</strong>
                  <small>Approval routing, temporary access, and security logs</small>
                </button>
              </div>
              <div className="approval-queue">
                <div>
                  <strong>Workflow status</strong>
                  <span>{companyCleanupJobs.filter((job) => job.status === 'processing' || job.status === 'pending').length} active or queued jobs</span>
                </div>
                <div>
                  <strong>Export history</strong>
                  <span>{companyReports.length + companyDashboards.length} saved reports and dashboards</span>
                </div>
                <div>
                  <strong>Notifications</strong>
                  <span>{notifications.filter((notification) => notification.status !== 'read').length} unread alerts</span>
                </div>
                <div>
                  <strong>Connector health</strong>
                  <span>{connectorHealth.healthy}/{connectorHealth.total} healthy, {connectorHealth.failed} failed</span>
                </div>
              </div>
              <p className="persistence-note">{enterpriseOpsMessage}</p>
              <div className="enterprise-ops-grid">
                <article>
                  <div className="panel-header compact-header">
                    <div>
                      <p className="eyebrow">Enterprise connectors</p>
                      <h3>Automatic data ingestion</h3>
                    </div>
                    <span className="status-pill active">{connectorHealth.total} sources</span>
                  </div>
                  <div className="connector-list">
                    {enterpriseOps.connectors.slice(0, 12).map((connector) => (
                      <div className="connector-row" key={connector.id}>
                        <div>
                          <strong>{connector.name}</strong>
                          <span>{connector.connectorType.replaceAll('_', ' ')} | {connector.credentialEncrypted ? 'encrypted credentials' : 'credential setup pending'}</span>
                          <small>Last sync: {connector.lastSyncAt ? new Date(connector.lastSyncAt).toLocaleString() : 'Not synced'} | Next: {connector.nextSyncAt ? new Date(connector.nextSyncAt).toLocaleString() : 'Unscheduled'}</small>
                        </div>
                        <div>
                          <span className={`status-pill ${connector.healthStatus === 'healthy' ? 'active' : 'disabled'}`}>{connector.healthStatus}</span>
                          <button className="ghost-button compact" type="button" disabled={syncingConnectorId === connector.id} onClick={() => syncEnterpriseConnector(connector)}>
                            {syncingConnectorId === connector.id ? 'Syncing' : 'Sync'}
                          </button>
                        </div>
                      </div>
                    ))}
                  </div>
                </article>
                <article>
                  <div className="panel-header compact-header">
                    <div>
                      <p className="eyebrow">Workflow intelligence</p>
                      <h3>AI operations layer</h3>
                    </div>
                    <span className="status-pill active">{enterpriseOps.intelligence.length} insights</span>
                  </div>
                  <div className="insight-list">
                    {enterpriseOps.intelligence.slice(0, 5).map((insight) => (
                      <div className="insight-row" key={insight.id}>
                        <div>
                          <strong>{insight.title}</strong>
                          <span>{insight.summary}</span>
                        </div>
                        <small>{Math.round(insight.confidence * 100)}% confidence | {insight.module}</small>
                      </div>
                    ))}
                    {!enterpriseOps.intelligence.length && <p className="muted">AI recommendations, anomaly scoring, fraud detection, and risk forecasting will appear here.</p>}
                  </div>
                </article>
                <article>
                  <div className="panel-header compact-header">
                    <div>
                      <p className="eyebrow">Scheduled pipelines</p>
                      <h3>Orchestration engine</h3>
                    </div>
                    <button className="ghost-button compact" type="button" onClick={createNightlySchedule}>Add nightly</button>
                  </div>
                  <div className="schedule-list">
                    {enterpriseOps.schedules.slice(0, 5).map((schedule) => (
                      <div className="schedule-row" key={schedule.id}>
                        <strong>{schedule.name}</strong>
                        <span>{schedule.scheduleType} {schedule.cronExpression || schedule.eventTrigger || 'manual'} | SLA {schedule.slaMinutes}m | priority {schedule.priority}</span>
                        <small>{schedule.status} | next run {schedule.nextRunAt ? new Date(schedule.nextRunAt).toLocaleString() : 'not scheduled'}</small>
                      </div>
                    ))}
                  </div>
                </article>
                <article>
                  <div className="panel-header compact-header">
                    <div>
                      <p className="eyebrow">Sync logs</p>
                      <h3>Connector observability</h3>
                    </div>
                    <span className="status-pill active">{enterpriseOps.syncLogs.length} events</span>
                  </div>
                  <div className="sync-log-list">
                    {enterpriseOps.syncLogs.slice(0, 6).map((log) => (
                      <div className="sync-log-row" key={log.id}>
                        <strong>{log.status}</strong>
                        <span>{log.recordsProcessed.toLocaleString()} processed | {log.failedRows.toLocaleString()} failed | {log.retries} retries</span>
                        <small>{(log.durationMs / 1000).toFixed(1)}s | {new Date(log.createdAt).toLocaleString()}</small>
                      </div>
                    ))}
                    {!enterpriseOps.syncLogs.length && <p className="muted">Sync duration, failed rows, retry attempts, and incremental sync logs will appear here.</p>}
                  </div>
                </article>
              </div>
            </section>
            <section className="metrics-grid" aria-label="Performance metrics">
              {insights.metrics.map((metric) => (
                <article className="metric-card" key={metric.label}>
                  <span>{metric.label}</span>
                  <strong>{metric.value}</strong>
                  <small>{metric.trend} this month</small>
                </article>
              ))}
            </section>

            <section className="panel notification-center" aria-label="Notification center">
              <div className="panel-header">
                <div>
                  <p className="eyebrow">Notifications</p>
                  <h2>Company activity feed</h2>
                </div>
                <span className="status-pill active">{notifications.length} updates</span>
              </div>
              <div className="notification-list">
                {notifications.slice(0, 5).map((notification) => (
                  <div key={notification.id}>
                    <div>
                      <strong>{notification.title}</strong>
                      <span>{notification.message} - {new Date(notification.createdAt).toLocaleString()}</span>
                    </div>
                    <div className="notification-actions">
                      <button className="ghost-button compact" type="button" onClick={() => updateNotificationRecord(notification, { status: 'read' })}>Mark Read</button>
                      <button className="ghost-button compact" type="button" onClick={() => updateNotificationRecord(notification, { archive: true })}>Archive</button>
                    </div>
                  </div>
                ))}
                {!notifications.length && <p className="muted">Upload, cleanup, export, report, and failed pipeline alerts will appear here.</p>}
              </div>
            </section>

            <section className="csv-section" id="csv">
              <div className="section-heading">
                <div>
                  <p className="eyebrow">Data studio</p>
                  <h2>Analyze business data in one clean view</h2>
                </div>
                <label className="company-selector">
                  Company
                  <select value={selectedCompanyId} onChange={(event) => setSelectedCompanyId(event.target.value)}>
                    {companies.map((company) => (
                      <option key={company.id} value={company.id}>{company.name}</option>
                    ))}
                  </select>
                </label>
                <button className="ghost-button" type="button" disabled={!activeDataset} onClick={downloadPdfReport}>
                  Download PDF
                </button>
                <button className="ghost-button" type="button" disabled={!activeDataset || cleanupRunning || Boolean(activeDataset?.originalDatasetId)} onClick={cleanActiveDataset}>
                  {cleanupRunning ? 'Cleaning...' : 'Clean Data'}
                </button>
                <button className="ghost-button" type="button" disabled={!activeCleanedDataset} onClick={() => downloadDatasetExport(activeCleanedDataset)}>
                  Download Cleaned CSV
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
                <input accept=".csv,.xlsx,.xls,.json,text/csv,application/json,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" type="file" onChange={handleDatasetUpload} />
                <strong>Drop CSV, Excel, or JSON file here</strong>
                <span>{selectedCompany ? `${selectedCompany.name}: ${uploadState}` : uploadState}</span>
              </label>

              <div className="dataset-strip">
                {companyDatasets.map((dataset) => (
                  <button
                    className={activeDataset?.id === dataset.id ? 'selected' : ''}
                    key={dataset.id}
                    type="button"
                    onClick={() => setActiveDataset(dataset)}
                  >
                    <strong>{dataset.fileName}</strong>
                    <span>{dataset.rows} rows - {dataset.columns} columns - {(dataset.fileType ?? 'csv').toUpperCase()}</span>
                    {dataset.worksheetName && <small>{dataset.worksheetName}</small>}
                    <small>{dataset.originalDatasetId ? 'Cleaned dataset' : `Cleanup ${dataset.cleanupStatus ?? 'pending'}`}</small>
                  </button>
                ))}
                {!companyDatasets.length && <span className="muted">No datasets for this company yet.</span>}
              </div>

              <p className="persistence-note">{persistenceState}</p>
              {renderCleanupPanel(activeDataset, cleanupJobs, cleanupMessage, deleteCleanupJobRecord)}

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
                        <span>Cleanup status: {activeDataset.cleanupStatus ?? 'original'}</span>
                        {activeDataset.worksheetName && <span>Worksheet: {activeDataset.worksheetName}</span>}
                      </div>
                      <div className="dataset-actions-row">
                        <button className="ghost-button compact" type="button" onClick={() => downloadDatasetExport(activeDataset)}>
                          Export CSV
                        </button>
                        <button className="ghost-button compact" type="button" onClick={cleanActiveDataset} disabled={cleanupRunning || Boolean(activeDataset.originalDatasetId)}>
                          Reprocess
                        </button>
                        <button className="ghost-button compact" type="button" onClick={() => archiveDatasetRecord(activeDataset)}>
                          Archive Dataset
                        </button>
                        <button className="ghost-button compact danger" type="button" disabled={deletingDatasetId === activeDataset.id} onClick={() => deleteDatasetRecord(activeDataset)}>
                          {deletingDatasetId === activeDataset.id ? 'Deleting...' : 'Delete Dataset'}
                        </button>
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
                    <h3>Business insights</h3>
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
                  {companyDashboards.length ? companyDashboards.map((dashboard) => (
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
                  {companyReports.length ? companyReports.map((report) => (
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
                      <button className="ghost-button compact danger" type="button" onClick={() => deleteReportRecord(report)}>
                        Delete Report
                      </button>
                    </div>
                  )) : <p className="muted">Downloaded reports will appear here.</p>}
                </div>
              </article>
            </section>

            <section className="assistant-grid" id="assistant">
              <article className="panel chat-panel">
                <div className="panel-header">
                  <h2>Business data assistant</h2>
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
            </div>
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
                sessionStorage.setItem(SESSION_ACTIVITY_KEY, String(Date.now()));
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

function EnterpriseCockpit({
  activeCleanedDataset,
  activeDataset,
  canManageUsers,
  chartMax,
  chartType,
  cleanupJobs,
  companies,
  companyDashboards,
  companyDatasets,
  companyReports,
  connectorHealth,
  datasetHealthScore,
  downloadDatasetExport,
  downloadPdfReport,
  enterpriseOps,
  enterpriseOpsMessage,
  failedRecordCount,
  insights,
  linePoints,
  navigate,
  notifications,
  onArchiveDataset,
  onCleanDataset,
  onCreateNightlySchedule,
  onOpenView,
  onRequestWorkflowAccess,
  onSaveDashboard,
  onSelectCompany,
  onSelectDataset,
  onSyncConnector,
  onToggleTheme,
  persistenceState,
  roleLabel,
  selectedCompany,
  selectedCompanyId,
  syncingConnectorId,
  theme,
  updateNotificationRecord,
  uploadState,
  user,
  workflows
}: {
  activeCleanedDataset: Dataset | null;
  activeDataset: Dataset | null;
  canManageUsers: boolean;
  chartMax: number;
  chartType: ChartType;
  cleanupJobs: CleanupJob[];
  companies: Company[];
  companyDashboards: SavedDashboard[];
  companyDatasets: Dataset[];
  companyReports: ReportHistoryItem[];
  connectorHealth: { total: number; healthy: number; failed: number };
  datasetHealthScore: number;
  downloadDatasetExport: (dataset: Dataset | null) => void;
  downloadPdfReport: () => void;
  enterpriseOps: EnterpriseOperations;
  enterpriseOpsMessage: string;
  failedRecordCount: number;
  insights: InsightResponse;
  linePoints: string;
  navigate: (path: string) => void;
  notifications: NotificationItem[];
  onArchiveDataset: (dataset: Dataset | null) => void;
  onCleanDataset: () => void;
  onCreateNightlySchedule: () => void;
  onOpenView: (view: AppView) => void;
  onRequestWorkflowAccess: () => void;
  onSaveDashboard: () => void;
  onSelectCompany: (companyId: string) => void;
  onSelectDataset: (dataset: Dataset | null) => void;
  onSyncConnector: (connector: EnterpriseConnector) => void;
  onToggleTheme: () => void;
  persistenceState: string;
  roleLabel: string;
  selectedCompany?: Company;
  selectedCompanyId: string;
  syncingConnectorId: string;
  theme: Theme;
  updateNotificationRecord: (notification: NotificationItem, updates: { status?: string; archive?: boolean }) => void;
  uploadState: string;
  user: User | null;
  workflows: Workflow[];
}) {
  const tabs = ['Overview', 'Operations', 'Pipelines', 'Connectors', 'Approvals', 'Analytics', 'Reports', 'Governance'];
  const [activeTab, setActiveTab] = useState('Overview');
  const [drillPanel, setDrillPanel] = useState<{ title: string; kind: string } | null>(null);
  const [search, setSearch] = useState('');
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>(() => {
    try {
      return JSON.parse(localStorage.getItem('metenovaCockpitCollapsed') || '{}');
    } catch {
      return {};
    }
  });
  const pendingApprovals = cleanupJobs.filter((job) => job.status === 'pending' || job.status === 'processing').length;
  const failedJobs = cleanupJobs.filter((job) => job.status === 'failed').length + connectorHealth.failed;
  const unreadNotifications = notifications.filter((notification) => notification.status !== 'read').length;
  const lastUpload = companyDatasets[0]?.uploadedAt ? new Date(companyDatasets[0].uploadedAt).toLocaleString() : 'No uploads yet';
  const latestExport = companyReports[0]?.createdAt ? new Date(companyReports[0].createdAt).toLocaleString() : 'No exports yet';
  const visibleConnectors = enterpriseOps.connectors.filter((connector) => !search || [connector.name, connector.connectorType, connector.status].join(' ').toLowerCase().includes(search.toLowerCase()));
  const toggleSection = (section: string) => {
    setCollapsed((current) => {
      const next = { ...current, [section]: !current[section] };
      localStorage.setItem('metenovaCockpitCollapsed', JSON.stringify(next));
      return next;
    });
  };
  const kpis = [
    { key: 'workflows', label: 'Active workflows', value: workflows.length, detail: `${cleanupJobs.length} cleanup jobs tracked`, kind: 'workflow' },
    { key: 'failed', label: 'Failed jobs', value: failedJobs, detail: 'Connector and pipeline failures', kind: 'timeline' },
    { key: 'approvals', label: 'Pending approvals', value: pendingApprovals, detail: `${enterpriseOps.accessRequests.filter((request) => request.status === 'pending').length} access requests`, kind: 'approval' },
    { key: 'connectors', label: 'Connector health', value: `${connectorHealth.healthy}/${connectorHealth.total}`, detail: 'Healthy enterprise sources', kind: 'connectors' },
    { key: 'exports', label: 'Exports today', value: companyReports.length, detail: `Latest: ${latestExport}`, kind: 'reports' },
    { key: 'savings', label: 'Automation savings', value: insights.metrics[1]?.value ?? 0, detail: `${insights.metrics[1]?.trend ?? '+0%'} this month`, kind: 'analytics' },
    { key: 'quality', label: 'Data quality score', value: `${datasetHealthScore}%`, detail: `${failedRecordCount} failed rows isolated`, kind: 'datasets' },
    { key: 'ai', label: 'AI alerts', value: enterpriseOps.intelligence.length, detail: 'Anomaly and risk insights', kind: 'ai' }
  ];
  const showSection = (section: string) => !collapsed[section] && (activeTab === 'Overview' || activeTab === section);

  return (
    <section className="dashboard-cockpit" aria-label="Enterprise operations cockpit">
      <div className="cockpit-topbar">
        <label>
          Company
          <select value={selectedCompanyId} onChange={(event) => onSelectCompany(event.target.value)}>
            {companies.map((company) => <option key={company.id} value={company.id}>{company.name}</option>)}
          </select>
        </label>
        <input aria-label="Global search" placeholder="Search workflows, connectors, datasets..." value={search} onChange={(event) => setSearch(event.target.value)} />
        <button type="button" onClick={() => setDrillPanel({ title: 'Notifications', kind: 'notifications' })}>Alerts {unreadNotifications}</button>
        <button type="button" onClick={() => setDrillPanel({ title: 'Approval Queue', kind: 'approval' })}>Approvals {pendingApprovals}</button>
        <button type="button" onClick={() => setDrillPanel({ title: 'Connector Health', kind: 'connectors' })}>Connectors {connectorHealth.healthy}/{connectorHealth.total}</button>
        <button type="button" onClick={onToggleTheme}>{theme === 'light' ? 'Dark' : 'Light'}</button>
        <div className="cockpit-profile">
          <strong>{user?.name ?? 'Workspace'}</strong>
          <span>{roleLabel}</span>
        </div>
      </div>

      <div className="cockpit-tabs" role="tablist" aria-label="Dashboard views">
        {tabs.map((tab) => (
          <button className={activeTab === tab ? 'active' : ''} key={tab} type="button" onClick={() => setActiveTab(tab)}>{tab}</button>
        ))}
      </div>

      <div className="cockpit-kpis">
        {kpis.map((kpi) => (
          <button className="cockpit-kpi" key={kpi.key} type="button" onClick={() => setDrillPanel({ title: kpi.label, kind: kpi.kind })}>
            <span>{kpi.label}</span>
            <strong>{kpi.value}</strong>
            <small>{kpi.detail}</small>
            <i style={{ width: `${Math.min(100, Math.max(18, Number.parseInt(String(kpi.value), 10) || 72))}%` }} />
          </button>
        ))}
      </div>

      <div className="cockpit-layout">
        <div className="cockpit-main">
          {showSection('Operations') && (
            <CockpitSection title="Real-time operations" count={`${cleanupJobs.length} jobs`} collapsed={collapsed.Operations} onToggle={() => toggleSection('Operations')}>
              <div className="workflow-map">
                {['Connector Trigger', 'Queued', 'Running', 'Waiting Approval', 'Export', 'Audit'].map((node, index) => (
                  <button className={index <= 2 ? 'completed' : index === 3 ? 'waiting' : 'queued'} key={node} type="button" onClick={() => setDrillPanel({ title: node, kind: 'timeline' })}>
                    <strong>{node}</strong>
                    <span>{index < 5 ? '->' : 'done'}</span>
                  </button>
                ))}
              </div>
              <div className="cockpit-mini-grid">
                <div><strong>{lastUpload}</strong><span>Recent upload</span></div>
                <div><strong>{enterpriseOps.schedules.length}</strong><span>Scheduled workflows</span></div>
                <div><strong>{enterpriseOps.syncLogs.length}</strong><span>Sync events</span></div>
                <div><strong>{enterpriseOpsMessage}</strong><span>Operations status</span></div>
              </div>
            </CockpitSection>
          )}

          {showSection('Connectors') && (
            <CockpitSection title="Connectors" count={`${visibleConnectors.length} sources`} collapsed={collapsed.Connectors} onToggle={() => toggleSection('Connectors')}>
              <div className="compact-table">
                {visibleConnectors.slice(0, 8).map((connector) => (
                  <div key={connector.id}>
                    <span><strong>{connector.name}</strong><small>{connector.connectorType.replaceAll('_', ' ')}</small></span>
                    <span>{connector.healthStatus}</span>
                    <span>{connector.lastSyncAt ? new Date(connector.lastSyncAt).toLocaleDateString() : 'No sync'}</span>
                    <button type="button" disabled={syncingConnectorId === connector.id} onClick={() => onSyncConnector(connector)}>{syncingConnectorId === connector.id ? 'Syncing' : 'Sync'}</button>
                  </div>
                ))}
              </div>
            </CockpitSection>
          )}

          {showSection('Pipelines') && (
            <CockpitSection title="Pipeline queue" count={`${enterpriseOps.schedules.length} schedules`} collapsed={collapsed.Pipelines} onToggle={() => toggleSection('Pipelines')}>
              <div className="timeline-list">
                {enterpriseOps.schedules.slice(0, 5).map((schedule) => (
                  <button key={schedule.id} type="button" onClick={() => setDrillPanel({ title: schedule.name, kind: 'timeline' })}>
                    <strong>{schedule.name}</strong>
                    <span>{schedule.status} | SLA {schedule.slaMinutes}m | {schedule.cronExpression || schedule.eventTrigger || 'manual'}</span>
                  </button>
                ))}
                <button type="button" onClick={onCreateNightlySchedule}>Create nightly workflow schedule</button>
              </div>
            </CockpitSection>
          )}

          {showSection('Analytics') && (
            <CockpitSection title="Compact data studio" count={`${companyDatasets.length} datasets`} collapsed={collapsed.Analytics} onToggle={() => toggleSection('Analytics')}>
              <div className="data-studio-compact">
                <div>
                  <span>Datasets</span>
                  <strong>{companyDatasets.length}</strong>
                  <small>Last upload: {lastUpload}</small>
                </div>
                <div>
                  <span>Quality</span>
                  <strong>{datasetHealthScore}%</strong>
                  <small>Trust score based on failed rows and cleanup signals</small>
                </div>
                <div className="studio-chart-preview">
                  {renderChart(chartType, activeDataset, chartMax, linePoints)}
                </div>
                <div className="studio-actions">
                  <button type="button" onClick={() => navigate('/data-processing/workspace')}>Open analytics studio</button>
                  <button type="button" disabled={!activeDataset} onClick={downloadPdfReport}>PDF</button>
                  <button type="button" disabled={!activeCleanedDataset} onClick={() => downloadDatasetExport(activeCleanedDataset)}>Clean CSV</button>
                  <button type="button" disabled={!activeDataset} onClick={onSaveDashboard}>Save</button>
                </div>
              </div>
            </CockpitSection>
          )}

          {showSection('Reports') && (
            <CockpitSection title="Reports and exports" count={`${companyReports.length + companyDashboards.length} assets`} collapsed={collapsed.Reports} onToggle={() => toggleSection('Reports')}>
              <div className="compact-table">
                {[...companyReports.slice(0, 4).map((report) => ({ id: report.id, name: report.title, meta: report.datasetName, date: report.createdAt })), ...companyDashboards.slice(0, 3).map((dashboard) => ({ id: dashboard.id, name: dashboard.name, meta: dashboard.datasetName, date: dashboard.updatedAt }))].map((item) => (
                  <div key={item.id}><span><strong>{item.name}</strong><small>{item.meta}</small></span><span>{new Date(item.date).toLocaleDateString()}</span><button type="button" onClick={() => onOpenView('reports')}>Open</button></div>
                ))}
              </div>
            </CockpitSection>
          )}

          {showSection('Governance') && (
            <CockpitSection title="Governance" count={`${enterpriseOps.accessRequests.length} access requests`} collapsed={collapsed.Governance} onToggle={() => toggleSection('Governance')}>
              <div className="cockpit-mini-grid">
                <div><strong>{canManageUsers ? companies.length : 1}</strong><span>Visible companies</span></div>
                <div><strong>{enterpriseOps.accessRequests.filter((request) => request.status === 'pending').length}</strong><span>Pending access requests</span></div>
                <div><strong>{roleLabel}</strong><span>Current access level</span></div>
                <button type="button" onClick={onRequestWorkflowAccess}>Request workflow access</button>
              </div>
            </CockpitSection>
          )}
        </div>

        <aside className="live-rail" aria-label="Live activity rail">
          <div>
            <p className="eyebrow">Live activity</p>
            <strong>{selectedCompany?.name ?? 'Company'}</strong>
          </div>
          {notifications.slice(0, 8).map((notification) => (
            <button key={notification.id} type="button" onClick={() => updateNotificationRecord(notification, { status: 'read' })}>
              <strong>{notification.title}</strong>
              <span>{notification.message}</span>
            </button>
          ))}
          {!notifications.length && <p className="muted">No live alerts yet.</p>}
        </aside>
      </div>

      {drillPanel && (
        <div className="drill-overlay" role="dialog" aria-modal="true" aria-label={drillPanel.title}>
          <section className="drill-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Drill-through</p>
                <h2>{drillPanel.title}</h2>
              </div>
              <button className="ghost-button compact" type="button" onClick={() => setDrillPanel(null)}>Close</button>
            </div>
            <div className="drill-tabs">
              {['Summary', 'Timeline', 'Logs', 'Actions'].map((tab) => <button key={tab} type="button">{tab}</button>)}
            </div>
            <div className="drill-content">
              {renderDrillContent(drillPanel.kind, {
                activeDataset,
                cleanupJobs,
                companyDatasets,
                enterpriseOps,
                notifications,
                onArchiveDataset,
                onCleanDataset,
                onSelectDataset
              })}
            </div>
          </section>
        </div>
      )}
    </section>
  );
}

function CockpitSection({ children, collapsed, count, onToggle, title }: { children: ReactNode; collapsed?: boolean; count: string; onToggle: () => void; title: string }) {
  return (
    <article className="cockpit-section">
      <button className="cockpit-section-header" type="button" onClick={onToggle}>
        <span><strong>{title}</strong><small>{count}</small></span>
        <i>{collapsed ? 'Expand' : 'Collapse'}</i>
      </button>
      {!collapsed && children}
    </article>
  );
}

function renderDrillContent(kind: string, context: {
  activeDataset: Dataset | null;
  cleanupJobs: CleanupJob[];
  companyDatasets: Dataset[];
  enterpriseOps: EnterpriseOperations;
  notifications: NotificationItem[];
  onArchiveDataset: (dataset: Dataset | null) => void;
  onCleanDataset: () => void;
  onSelectDataset: (dataset: Dataset | null) => void;
}) {
  if (kind === 'connectors') {
    return <div className="compact-table">{context.enterpriseOps.connectors.map((connector) => <div key={connector.id}><span><strong>{connector.name}</strong><small>{connector.connectorType}</small></span><span>{connector.status}</span><span>{connector.healthStatus}</span></div>)}</div>;
  }
  if (kind === 'datasets' || kind === 'analytics') {
    return <div className="compact-table">{context.companyDatasets.map((dataset) => <div key={dataset.id}><span><strong>{dataset.fileName}</strong><small>{dataset.rows} rows | {dataset.cleanupStatus}</small></span><button type="button" onClick={() => context.onSelectDataset(dataset)}>Select</button><button type="button" onClick={context.onCleanDataset}>Clean</button><button type="button" onClick={() => context.onArchiveDataset(dataset)}>Archive</button></div>)}</div>;
  }
  if (kind === 'ai') {
    return <div className="timeline-list">{context.enterpriseOps.intelligence.map((insight) => <button key={insight.id} type="button"><strong>{insight.title}</strong><span>{insight.summary} | {Math.round(insight.confidence * 100)}% confidence</span></button>)}</div>;
  }
  if (kind === 'notifications') {
    return <div className="timeline-list">{context.notifications.map((notification) => <button key={notification.id} type="button"><strong>{notification.title}</strong><span>{notification.message}</span></button>)}</div>;
  }
  return <div className="timeline-list">{context.cleanupJobs.map((job) => <button key={job.id} type="button"><strong>{job.status}</strong><span>{job.logs?.[0] ?? 'Pipeline execution'} | {new Date(job.updatedAt).toLocaleString()}</span></button>)}</div>;
}

function renderCleanupPanel(
  dataset: Dataset | null,
  cleanupJobs: CleanupJob[],
  cleanupMessage: string,
  deleteCleanupJobRecord: (job: CleanupJob) => void
) {
  const metrics = dataset?.cleanupMetrics ?? {};
  const preview = dataset?.cleanupPreview;
  const previewHeaders = Array.from(new Set([
    ...Object.keys(preview?.before?.[0] ?? {}),
    ...Object.keys(preview?.after?.[0] ?? {})
  ])).slice(0, 5);
  const metricItems = [
    ['Duplicates removed', metrics.duplicatesRemoved ?? 0],
    ['Rows fixed', metrics.rowsFixed ?? 0],
    ['Invalid values', metrics.invalidValuesDetected ?? 0],
    ['Columns standardized', metrics.columnsStandardized ?? 0],
    ['Cleaned rows', metrics.totalCleanedRows ?? dataset?.rows ?? 0],
    ['Failed rows', metrics.failedRows ?? 0],
    ['Anomalies', metrics.anomaliesDetected ?? 0]
  ];

  return (
    <section className="cleanup-grid" aria-label="Data cleanup pipeline">
      <article className="panel cleanup-summary">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Business-ready cleanup</p>
            <h3>Cleanup summary</h3>
          </div>
          <span className={`cleanup-status ${dataset?.cleanupStatus ?? 'pending'}`}>{dataset?.cleanupStatus ?? 'pending'}</span>
        </div>
        <p className="persistence-note">{cleanupMessage}</p>
        <div className="cleanup-metrics">
          {metricItems.map(([label, value]) => (
            <div key={label}>
              <strong>{value}</strong>
              <span>{label}</span>
            </div>
          ))}
        </div>
        <ul className="cleanup-logs">
          {(dataset?.cleanupLogs?.length ? dataset.cleanupLogs : ['Upload a dataset, then run Clean Data to create a preserved original plus cleaned version.']).slice(0, 5).map((log) => (
            <li key={log}>{log}</li>
          ))}
        </ul>
      </article>

      <article className="panel cleanup-preview">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Before / after</p>
            <h3>Preview</h3>
          </div>
          {dataset?.futureAiReady && <span className="status-pill active">Business-ready</span>}
        </div>
        {preview && previewHeaders.length ? (
          <div className="before-after-grid">
            {(['before', 'after'] as const).map((side) => (
              <div key={side}>
                <strong>{side === 'before' ? 'Original' : 'Cleaned'}</strong>
                <div className="mini-table-wrap">
                  <table>
                    <thead><tr>{previewHeaders.map((header) => <th key={header}>{header}</th>)}</tr></thead>
                    <tbody>
                      {(preview[side] ?? []).slice(0, 3).map((row, index) => (
                        <tr key={`${side}-${index}`}>{previewHeaders.map((header) => <td key={header}>{row[header] ?? ''}</td>)}</tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="empty-state compact-empty">Run cleanup to compare original and cleaned rows.</div>
        )}
      </article>

      <article className="panel cleanup-history">
        <div className="panel-header">
          <div>
            <p className="eyebrow">History</p>
            <h3>Cleanup jobs</h3>
          </div>
        </div>
        <div className="history-list compact-history">
          {cleanupJobs.length ? cleanupJobs.map((job) => (
            <div className="history-item" key={job.id}>
              <div>
                <strong>{job.status}</strong>
                <span>{new Date(job.updatedAt).toLocaleString()} - {(job.metrics?.totalCleanedRows ?? 0)} cleaned rows - {(job.metrics?.processingDurationMs ?? 0)} ms</span>
              </div>
              <div className="cleanup-job-actions">
                <span className={`cleanup-status ${job.status}`}>{job.status}</span>
                <button className="ghost-button compact danger" type="button" onClick={() => deleteCleanupJobRecord(job)}>Delete Cleanup Job</button>
              </div>
            </div>
          )) : <p className="muted">Cleanup jobs for the selected dataset will appear here.</p>}
        </div>
      </article>
    </section>
  );
}

function canManageUsers(user: User | null) {
  return user?.role === 'owner' || user?.role === 'admin' || user?.role === 'manager';
}

function canManageWorkspaceData(user: User | null) {
  return user?.role === 'owner' || user?.role === 'admin' || user?.role === 'manager';
}

const EMPTY_ARRAY: never[] = [];

function asArray<T>(value: T[] | null | undefined): T[] {
  return Array.isArray(value) ? value : EMPTY_ARRAY;
}

function displayNumber(value: number | null | undefined) {
  return Number(value ?? 0).toLocaleString();
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

function getPreviewRows(dataset: Dataset, mode: PreviewMode) {
  const preview = asArray(dataset.preview);
  const cleanupAfter = asArray(dataset.cleanupPreview?.after);
  if (mode === 'cleanup' || mode === 'compare') {
    return cleanupAfter.length ? cleanupAfter.slice(0, 25) : preview.slice(0, 25);
  }
  if (mode === 'normalization') {
    return preview.slice(0, 25).map((row) => {
      const normalized = normalizePreviewRow(row);
      return Object.fromEntries(Object.keys(row).map((key) => [key, `${String(row[key] ?? '')} -> ${String(normalized[key] ?? '')}`]));
    });
  }
  return preview.slice(0, 25);
}

function getPreviewHeaders(dataset: Dataset, rows?: Record<string, string>[]) {
  const headers = asArray(dataset.headers);
  const previewRows = asArray(rows);
  return headers.length ? headers : Array.from(new Set(previewRows.flatMap((row) => Object.keys(row ?? {}))));
}

function inferColumnTypes(rows: Record<string, string>[] | undefined, headers: string[] | undefined) {
  const previewRows = asArray(rows);
  return asArray(headers).reduce<Record<string, string>>((types, header) => {
    const values = previewRows.map((row) => String(row?.[header] ?? '').trim()).filter(Boolean);
    if (!values.length) {
      types[header] = 'empty';
    } else if (values.every((value) => Number.isFinite(Number(value.replace(/[$,%]/g, ''))))) {
      types[header] = 'number';
    } else if (values.every((value) => !Number.isNaN(Date.parse(value)))) {
      types[header] = 'date';
    } else if (values.every((value) => ['yes', 'no', 'true', 'false', 'y', 'n'].includes(value.toLowerCase()))) {
      types[header] = 'boolean';
    } else {
      types[header] = 'text';
    }
    return types;
  }, {});
}

function summarizeValidation(dataset: Dataset) {
  const rows = asArray(dataset.preview);
  const headers = asArray(dataset.headers);
  let missingValues = 0;
  let invalidTypes = 0;
  const failedRows = new Set<number>();
  rows.forEach((row, rowIndex) => {
    headers.forEach((header) => {
      const value = String(row[header] ?? '').trim();
      if (!value || ['null', 'n/a', 'na', 'undefined'].includes(value.toLowerCase())) {
        missingValues += 1;
        failedRows.add(rowIndex);
      }
      if (/amount|total|price|cost|revenue|sales|qty|quantity|count|number|rate|percent|score|hours|balance/i.test(header) && value && !Number.isFinite(Number(value.replace(/[$,%]/g, '')))) {
        invalidTypes += 1;
        failedRows.add(rowIndex);
      }
    });
  });
  return { missingValues, invalidTypes, failedRows: failedRows.size };
}

function findDuplicateRows(rows: Record<string, string>[]) {
  const seen = new Map<string, number>();
  asArray(rows).forEach((row) => {
    const key = JSON.stringify(row);
    seen.set(key, (seen.get(key) ?? 0) + 1);
  });
  return [...seen.entries()]
    .filter(([, count]) => count > 1)
    .map(([key, count]) => ({ key, count, confidence: 98 }));
}

function normalizePreviewRow(row: Record<string, string>) {
  return Object.fromEntries(Object.entries(row).map(([key, value]) => [standardizeColumnName(key), normalizePreviewValue(value)]));
}

function standardizeColumnName(key: string) {
  return key.trim().replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, '').toLowerCase() || key;
}

function normalizePreviewValue(value: string) {
  const compact = String(value ?? '').trim().replace(/\s+/g, ' ');
  if (!compact || ['null', 'n/a', 'na', 'undefined', 'invalid'].includes(compact.toLowerCase())) return '';
  const parsedDate = Date.parse(compact);
  if (/^\d{1,4}[/-]\d{1,2}[/-]\d{1,4}$/.test(compact) && !Number.isNaN(parsedDate)) return new Date(parsedDate).toISOString().slice(0, 10);
  const numeric = Number(compact.replace(/[$,%]/g, ''));
  if (/^[($-]?\d/.test(compact) && Number.isFinite(numeric)) return String(numeric);
  return compact.replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function buildPreviewWarnings(dataset: Dataset, mode: PreviewMode, validation: { missingValues: number; invalidTypes: number; failedRows: number }, duplicates: Array<{ count: number }>) {
  const warnings = [
    `${validation.failedRows} failed preview rows detected.`,
    `${validation.missingValues} missing values found in preview.`,
    `${validation.invalidTypes} invalid column type values found.`,
    `${duplicates.reduce((sum, duplicate) => sum + duplicate.count, 0)} duplicate preview rows detected.`
  ];
  if (dataset.cleanupMetrics?.duplicatesRemoved) warnings.push(`${dataset.cleanupMetrics.duplicatesRemoved} duplicates removed during cleanup.`);
  if (dataset.cleanupMetrics?.rowsFixed) warnings.push(`${dataset.cleanupMetrics.rowsFixed} rows fixed during cleanup.`);
  if (mode === 'export') warnings.push(`Export filename: ${dataset.fileName.replace(/\.(csv|xlsx|xls|json)$/i, '.csv')}`);
  return warnings;
}

function getDatasetVersions(datasets: Dataset[], dataset: Dataset) {
  const rootId = dataset.originalDatasetId ?? dataset.id;
  return asArray(datasets)
    .filter((entry) => entry.id === rootId || entry.originalDatasetId === rootId || entry.cleanedDatasetId === dataset.id)
    .sort((a, b) => new Date(b.uploadedAt).getTime() - new Date(a.uploadedAt).getTime());
}

function stageStatusForMode(mode: PreviewMode, dataset: Dataset) {
  if (dataset.cleanupStatus === 'failed') return 'failed';
  if (mode === 'upload' || mode === 'validation' || mode === 'duplicates' || mode === 'normalization') return 'completed';
  if (mode === 'cleanup' && dataset.cleanupStatus !== 'completed') return 'running';
  if (mode === 'export' || mode === 'approval' || mode === 'cleanup' || mode === 'compare') return 'completed';
  return 'queued';
}

function cellClass(value: unknown, header: string) {
  const text = String(value ?? '').trim();
  if (!text || ['null', 'n/a', 'na', 'undefined'].includes(text.toLowerCase())) return 'cell-warning';
  if (text.toLowerCase().includes('invalid')) return 'cell-error';
  if (/amount|total|price|cost|revenue|sales|qty|quantity|count|number|rate|percent|score|hours|balance/i.test(header) && !Number.isFinite(Number(text.replace(/[$,%]/g, '')))) return 'cell-error';
  return '';
}

function AssignedCompaniesList({ assignments }: { assignments: CompanyAssignment[] }) {
  if (!assignments.length) {
    return <span className="muted">No company access</span>;
  }

  return (
    <div className="assigned-companies">
      {assignments.slice(0, 3).map((assignment) => (
        <span className="assignment-chip" key={assignment.id || assignment.companyId}>
          {assignment.companyName || assignment.companyId}
        </span>
      ))}
      {assignments.length > 3 && <span className="assignment-chip muted-chip">+{assignments.length - 3}</span>}
    </div>
  );
}

function CompanyAccessModal({
  accessCompanyIds,
  accessRole,
  companies,
  onClose,
  onRoleChange,
  onSave,
  onToggleCompany,
  saving,
  user
}: {
  accessCompanyIds: string[];
  accessRole: UserRole;
  companies: Company[];
  onClose: () => void;
  onRoleChange: (role: UserRole) => void;
  onSave: () => void;
  onToggleCompany: (companyId: string) => void;
  saving: boolean;
  user: AdminUser;
}) {
  return (
    <div className="modal-backdrop" role="presentation">
      <section className="access-modal" aria-modal="true" role="dialog" aria-label={`Company access for ${user.name}`}>
        <div className="panel-header">
          <div>
            <p className="eyebrow">Company access</p>
            <h2>{user.name}</h2>
            <p className="muted">{user.email}</p>
          </div>
          <button className="ghost-button compact" type="button" onClick={onClose}>Close</button>
        </div>
        <label className="access-role-picker">
          Company role
          <select value={accessRole} onChange={(event) => onRoleChange(event.target.value as UserRole)}>
            {roleOptions.filter((role) => role !== 'owner').map((role) => (
              <option key={role} value={role}>{roleLabel(role)}</option>
            ))}
          </select>
        </label>
        <div className="company-access-list">
          {companies.map((company) => (
            <label className="company-access-row" key={company.id}>
              <input
                checked={accessCompanyIds.includes(company.id)}
                type="checkbox"
                onChange={() => onToggleCompany(company.id)}
              />
              <span>
                <strong>{company.name}</strong>
                <small>{company.industry}</small>
              </span>
            </label>
          ))}
          {!companies.length && <p className="muted">No companies are available to assign.</p>}
        </div>
        <div className="modal-actions">
          <button className="ghost-button" type="button" onClick={onClose}>Cancel</button>
          <button type="button" disabled={saving} onClick={onSave}>{saving ? 'Saving...' : 'Save Access'}</button>
        </div>
      </section>
    </div>
  );
}

function pipelineStepPath(stage: string) {
  return '/data-processing/workspace';
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

class WorkspaceErrorBoundary extends Component<{ children: ReactNode; label: string }, { error?: Error; info?: ErrorInfo }> {
  state: { error?: Error; info?: ErrorInfo } = {};

  static getDerivedStateFromError(error: Error) {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('[Metenova Workspace] Render failure', { label: this.props.label, error, info });
    this.setState({ info });
  }

  render() {
    if (!this.state.error) return this.props.children;
    return (
      <PageLayout>
        <article className="panel workspace-error-panel">
          <p className="eyebrow">Runtime fallback</p>
          <h2>{this.props.label} could not render</h2>
          <p className="persistence-note warning-note">{this.state.error.message || 'A rendering error occurred while mounting this workspace.'}</p>
          <div className="dataset-detail-grid">
            <div><span>Route</span><strong>{window.location.pathname}</strong></div>
            <div><span>Recovery</span><strong>Reload workspace</strong></div>
            <div><span>Component</span><strong>{this.props.label}</strong></div>
            <div><span>Status</span><strong>Fallback active</strong></div>
          </div>
          <div className="modal-actions">
            <button className="ghost-button" type="button" onClick={() => this.setState({ error: undefined, info: undefined })}>Retry render</button>
            <button type="button" onClick={() => window.location.assign('/data-processing/workspace')}>Reload workspace</button>
          </div>
        </article>
      </PageLayout>
    );
  }
}

function RoutedPages(props: {
  apiFetch: (path: string, options?: RequestInit) => Promise<Response>;
  archiveDatasetRecord: (dataset: Dataset | null) => void;
  auditLogs: AuditLog[];
  canManage: boolean;
  companies: Company[];
  companiesError: string;
  companiesLoading: boolean;
  companyForm: CompanyFormValues;
  companyFormOpen: boolean;
  companySaving: boolean;
  dashboards: SavedDashboard[];
  datasets: Dataset[];
  deleteAdminUser: (user: AdminUser) => void;
  deleteCompanyWorkspace: (company: Company) => void;
  deleteDatasetRecord: (dataset: Dataset | null) => void;
  deletingDatasetId: string;
  downloadDatasetExport: (dataset: Dataset | null) => void;
  downloadHistoricalReport: (report: ReportHistoryItem) => void;
  openCompanyAccess: (user: AdminUser) => void;
  reports: ReportHistoryItem[];
  loadCompanies: () => void;
  resetCompanyForm: () => void;
  runCompanyAction: (company: Company, action: string) => void;
  saveCompany: (event: FormEvent<HTMLFormElement>) => Promise<void>;
  setCompanyFormOpen: (open: boolean) => void;
  selectedCompanyId: string;
  setActiveDataset: (dataset: Dataset | null) => void;
  setCleanupJobs: Dispatch<SetStateAction<CleanupJob[]>>;
  setDatasets: Dispatch<SetStateAction<Dataset[]>>;
  setSelectedCompanyId: (companyId: string) => void;
  systemStatus: SystemStatus | null;
  updateAdminUser: (userId: string, updates: Partial<AdminUser>) => void;
  updateCompanyName: (company: Company) => void;
  updateCompanyForm: (field: keyof CompanyFormValues, value: string) => void;
  users: AdminUser[];
  user: User | null;
  uploadDataset: (file: File, worksheetName?: string, companyIdOverride?: string) => Promise<Dataset | undefined>;
  workspaceAction: string;
}) {
  return (
    <Routes>
      {workspaceRoutes.map((route) => (
        <Route
          element={(
            <WorkspaceErrorBoundary label={route.title}>
              <ModuleWorkspacePage
                apiFetch={props.apiFetch}
                archiveDatasetRecord={props.archiveDatasetRecord}
                companies={props.companies}
                datasets={props.datasets}
                deleteDatasetRecord={props.deleteDatasetRecord}
                deletingDatasetId={props.deletingDatasetId}
                downloadDatasetExport={props.downloadDatasetExport}
                route={route}
                selectedCompanyId={props.selectedCompanyId}
                setActiveDataset={props.setActiveDataset}
                setCleanupJobs={props.setCleanupJobs}
                setDatasets={props.setDatasets}
                setSelectedCompanyId={props.setSelectedCompanyId}
                uploadDataset={props.uploadDataset}
              />
            </WorkspaceErrorBoundary>
          )}
          key={route.path}
          path={route.path}
        />
      ))}
      <Route
        path="/data-processing/*"
        element={(
          <WorkspaceErrorBoundary label="Data Processing Workspace">
            <ModuleWorkspacePage
              apiFetch={props.apiFetch}
              archiveDatasetRecord={props.archiveDatasetRecord}
              companies={props.companies}
              datasets={props.datasets}
              deleteDatasetRecord={props.deleteDatasetRecord}
              deletingDatasetId={props.deletingDatasetId}
              downloadDatasetExport={props.downloadDatasetExport}
              route={dataProcessingWorkspaceRoute}
              selectedCompanyId={props.selectedCompanyId}
              setActiveDataset={props.setActiveDataset}
              setCleanupJobs={props.setCleanupJobs}
              setDatasets={props.setDatasets}
              setSelectedCompanyId={props.setSelectedCompanyId}
              uploadDataset={props.uploadDataset}
            />
          </WorkspaceErrorBoundary>
        )}
      />
      <Route
        path="/companies"
        element={(
          <CompaniesWorkspace
            companies={props.companies}
            error={props.companiesError}
            companyForm={props.companyForm}
            formOpen={props.companyFormOpen}
            loading={props.companiesLoading}
            saving={props.companySaving}
            loadCompanies={props.loadCompanies}
            resetCompanyForm={props.resetCompanyForm}
            saveCompany={props.saveCompany}
            setFormOpen={props.setCompanyFormOpen}
            runCompanyAction={props.runCompanyAction}
            deleteCompanyWorkspace={props.deleteCompanyWorkspace}
            updateCompanyName={props.updateCompanyName}
            updateCompanyForm={props.updateCompanyForm}
            user={props.user}
            workspaceAction={props.workspaceAction}
          />
        )}
      />
      <Route path="/analytics/dashboard" element={<AnalyticsWorkspace dashboards={props.dashboards} reports={props.reports} />} />
      <Route path="/reports/history" element={<ReportsHistoryWorkspace downloadHistoricalReport={props.downloadHistoricalReport} reports={props.reports} />} />
      <Route path="/admin/users" element={props.canManage ? <AdminUsersWorkspace deleteAdminUser={props.deleteAdminUser} openCompanyAccess={props.openCompanyAccess} updateAdminUser={props.updateAdminUser} users={props.users} /> : <Navigate to="/" replace />} />
      <Route path="/admin/audit-logs" element={props.canManage ? <AuditLogsWorkspace auditLogs={props.auditLogs} /> : <Navigate to="/" replace />} />
      <Route path="/admin/system-monitoring" element={props.canManage ? <SystemMonitoringWorkspace status={props.systemStatus} /> : <Navigate to="/" replace />} />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}

function CompaniesWorkspace({
  companies,
  companyForm,
  error,
  formOpen,
  loading,
  loadCompanies,
  resetCompanyForm,
  runCompanyAction,
  saveCompany,
  saving,
  setFormOpen,
  updateCompanyForm,
  updateCompanyName,
  deleteCompanyWorkspace,
  user,
  workspaceAction
}: {
  companies: Company[];
  companyForm: CompanyFormValues;
  error: string;
  formOpen: boolean;
  loading: boolean;
  loadCompanies: () => void;
  resetCompanyForm: () => void;
  runCompanyAction: (company: Company, action: string) => void;
  saveCompany: (event: FormEvent<HTMLFormElement>) => Promise<void>;
  saving: boolean;
  setFormOpen: (open: boolean) => void;
  updateCompanyForm: (field: keyof CompanyFormValues, value: string) => void;
  updateCompanyName: (company: Company) => void;
  deleteCompanyWorkspace: (company: Company) => void;
  user: User | null;
  workspaceAction: string;
}) {
  const closeForm = () => {
    resetCompanyForm();
    setFormOpen(false);
  };
  const canCreateCompanies = user?.role === 'owner';
  const canManageCompanyOps = user?.role === 'owner' || user?.role === 'admin' || user?.role === 'manager';
  const canViewPipelines = canManageCompanyOps || user?.role === 'employee';
  const visibleActions = [
    ...(canManageCompanyOps ? ['Upload Data', 'Clean Data'] : []),
    'View Dashboard',
    'Reports',
    'Analytics',
    ...(canViewPipelines ? ['Pipelines'] : []),
    ...(canManageCompanyOps ? ['Export Data', 'Delete Dataset', 'Delete Cleanup Job'] : [])
  ];

  return (
    <section className="routed-page companies-page">
      <PageHeader
        copy="Create and manage company workspaces for uploads, reports, dashboards, and business data cleanup."
        eyebrow="Companies"
        title="Company Management"
      />

      <article className="panel company-management-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Workspace setup</p>
            <h2>Company workspaces</h2>
          </div>
          {canCreateCompanies && (
            <button className="ghost-button" type="button" onClick={() => setFormOpen(true)}>
              Create Company
            </button>
          )}
          <button className="ghost-button compact" type="button" disabled={loading} onClick={loadCompanies}>
            {loading ? 'Refreshing' : 'Refresh'}
          </button>
        </div>
        <p className="module-copy">
          Each company workspace has isolated uploads, reports, dashboards, cleaned datasets, and business cleanup pipelines.
        </p>
        {error && <p className="persistence-note warning-note">{error}</p>}
        <div className="company-summary-grid">
          <div><strong>{companies.length}</strong><span>Total workspaces</span></div>
          <div><strong>{companies.filter((company) => company.status === 'Active').length}</strong><span>Active companies</span></div>
          <div><strong>Business-ready</strong><span>Pipeline operations</span></div>
        </div>
      </article>

      {formOpen && (
        <article className="panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">New workspace</p>
              <h2>Create Company</h2>
            </div>
            <button className="ghost-button compact" type="button" onClick={closeForm}>
              Close
            </button>
          </div>
          <form className="company-form" onSubmit={saveCompany}>
            <label>
              Company Name
              <input value={companyForm.name} onChange={(event) => updateCompanyForm('name', event.target.value)} />
            </label>
            <label>
              Industry
              <input value={companyForm.industry} onChange={(event) => updateCompanyForm('industry', event.target.value)} />
            </label>
            <label>
              Owner Name
              <input value={companyForm.ownerName} onChange={(event) => updateCompanyForm('ownerName', event.target.value)} />
            </label>
            <label>
              Email
              <input type="email" value={companyForm.email} onChange={(event) => updateCompanyForm('email', event.target.value)} />
            </label>
            <label>
              Phone
              <input value={companyForm.phone} onChange={(event) => updateCompanyForm('phone', event.target.value)} />
            </label>
            <div className="company-form-actions">
              <button type="submit" disabled={saving}>{saving ? 'Saving...' : 'Save Company'}</button>
              <button className="ghost-button compact" type="button" onClick={closeForm}>Cancel</button>
            </div>
          </form>
        </article>
      )}

      <article className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Workspace directory</p>
            <h2>Companies</h2>
          </div>
        </div>
        <div className="table-wrap company-table-wrap">
          <table className="company-table">
            <thead>
              <tr>
                <th>Company Name</th>
                <th>Industry</th>
                <th>Owner</th>
                <th>Email</th>
                <th>Phone</th>
                <th>Status</th>
                <th>Created Date</th>
                <th>Workspace Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={8}>Loading companies...</td>
                </tr>
              ) : companies.map((company) => (
                <tr key={company.id}>
                  <td>{company.name}</td>
                  <td>{company.industry}</td>
                  <td>{company.ownerName}</td>
                  <td>{company.email}</td>
                  <td>{company.phone}</td>
                  <td><span className="status-pill active">{company.status}</span></td>
                  <td>{new Date(company.createdAt).toLocaleDateString()}</td>
                  <td>
                    <div className="company-actions">
                      {visibleActions.map((action) => (
                        <button
                          className={action.startsWith('Delete') ? 'danger-action' : ''}
                          disabled={Boolean(workspaceAction)}
                          key={action}
                          type="button"
                          onClick={() => runCompanyAction(company, action)}
                        >
                          {workspaceAction.includes(company.name) && workspaceAction.startsWith(action) ? 'Working...' : action}
                        </button>
                      ))}
                      {canManageCompanyOps && <button type="button" disabled={Boolean(workspaceAction)} onClick={() => updateCompanyName(company)}>Rename</button>}
                      {user?.role === 'owner' && <button className="danger-action" type="button" disabled={Boolean(workspaceAction)} onClick={() => deleteCompanyWorkspace(company)}>Delete Company</button>}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="persistence-note">
          {workspaceAction || 'Each company action is company-scoped and loads isolated uploads, reports, dashboards, cleaned datasets, and cleanup jobs.'}
        </p>
      </article>
    </section>
  );
}

function ModuleWorkspacePage({
  apiFetch,
  archiveDatasetRecord,
  companies,
  datasets,
  deleteDatasetRecord,
  deletingDatasetId,
  downloadDatasetExport,
  route,
  selectedCompanyId,
  setActiveDataset,
  setCleanupJobs,
  setDatasets,
  setSelectedCompanyId,
  uploadDataset
}: {
  apiFetch: (path: string, options?: RequestInit) => Promise<Response>;
  archiveDatasetRecord: (dataset: Dataset | null) => void;
  companies: Company[];
  datasets: Dataset[];
  deleteDatasetRecord: (dataset: Dataset | null) => void;
  deletingDatasetId: string;
  downloadDatasetExport: (dataset: Dataset | null) => void;
  route: WorkspaceRoute;
  selectedCompanyId: string;
  setActiveDataset: (dataset: Dataset | null) => void;
  setCleanupJobs: Dispatch<SetStateAction<CleanupJob[]>>;
  setDatasets: Dispatch<SetStateAction<Dataset[]>>;
  setSelectedCompanyId: (companyId: string) => void;
  uploadDataset: (file: File, worksheetName?: string, companyIdOverride?: string) => Promise<Dataset | undefined>;
}) {
  const safeCompanies = asArray(companies);
  const safeDatasets = asArray(datasets);
  const [records, setRecords] = useState<ModuleRecord[]>([]);
  const [title, setTitle] = useState('');
  const [amount, setAmount] = useState('');
  const [status, setStatus] = useState('open');
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState('all');
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const pipelineConfig = modulePipelineConfigs[route?.module] ?? modulePipelineConfigs.default;
  const workflowStages = asArray(pipelineConfig.stages);
  const pipelineRules = asArray(pipelineConfig.rules);
  const qualitySignals = asArray(pipelineConfig.qualitySignals);
  const isDataProcessingWorkspace = route.module === 'dataProcessing';
  const [workflowMessage, setWorkflowMessage] = useState(pipelineConfig.emptyState);
  const [workflowStage, setWorkflowStage] = useState(workflowStages[0] ?? 'Upload');
  const [stageDetail, setStageDetail] = useState('');
  const [toolPanel, setToolPanel] = useState<'validation' | 'duplicates' | 'cleanup' | 'reports'>('validation');
  const [dragActive, setDragActive] = useState(false);
  const [previewState, setPreviewState] = useState<{ dataset: Dataset; mode: PreviewMode } | null>(null);
  const [expandedDatasetId, setExpandedDatasetId] = useState('');
  const [lastModuleFile, setLastModuleFile] = useState<File | null>(null);
  const [workspaceDatasets, setWorkspaceDatasets] = useState<Dataset[]>([]);
  const moduleDatasets = useMemo(() => {
    const merged = [...asArray(workspaceDatasets), ...safeDatasets];
    const unique = new Map<string, Dataset>();
    merged.forEach((dataset) => {
      if (dataset?.id && (!selectedCompanyId || dataset.companyId === selectedCompanyId) && !unique.has(dataset.id)) {
        unique.set(dataset.id, dataset);
      }
    });
    return [...unique.values()].sort((a, b) => new Date(b.uploadedAt).getTime() - new Date(a.uploadedAt).getTime());
  }, [safeDatasets, selectedCompanyId, workspaceDatasets]);
  const activeStageIndex = Math.max(workflowStages.indexOf(workflowStage), 0);
  const selectedCompany = safeCompanies.find((company) => company.id === selectedCompanyId) ?? safeCompanies[0] ?? null;
  const selectedCompanyName = selectedCompany?.name ?? 'No company selected';

  useEffect(() => {
    setWorkflowStage(workflowStages[0] ?? 'Upload');
    setWorkflowMessage(pipelineConfig.emptyState);
    setExpandedDatasetId('');
    setStageDetail('');
  }, [pipelineConfig.emptyState, route.module, route.path, workflowStages]);

  useEffect(() => {
    setWorkspaceDatasets((current) => current.filter((dataset) => !safeDatasets.some((entry) => entry.id === dataset.id)));
  }, [safeDatasets]);

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
        body: JSON.stringify({ title, amount, status, recordType: route.type, companyId: selectedCompanyId, metadata: { workspacePath: route.path } })
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

  async function handleModuleUpload(file: File) {
    setUploading(true);
    setUploadProgress(12);
    setError('');
    setLastModuleFile(file);
    setWorkflowMessage(`Uploading ${file.name}...`);
    setWorkflowStage(workflowStages[0]);
    try {
      window.setTimeout(() => setUploadProgress(48), 120);
      const dataset = await uploadDataset(file, undefined, selectedCompanyId);
      if (!dataset) {
        throw new Error(`Upload failed for ${file.name}. Review the upload status above for the exact validation, parser, authorization, CSRF/session, or persistence error.`);
      }
      setUploadProgress(100);
      setWorkflowStage(workflowStages[1] ?? workflowStages[0]);
      setWorkflowMessage(`${dataset.fileName} uploaded to ${selectedCompanyName}. ${workflowStages[1] ?? 'Validation'} is ready.`);
      setActiveDataset(dataset);
      setWorkspaceDatasets((current) => [dataset, ...current.filter((entry) => entry.id !== dataset.id)]);
      setExpandedDatasetId(dataset.id);
      setPreviewState({ dataset, mode: 'upload' });
    } catch (uploadError) {
      const message = uploadError instanceof Error ? uploadError.message : 'Upload failed.';
      console.error('[Metenova Module Upload] Workflow upload failed', {
        module: route.module,
        path: route.path,
        companyId: selectedCompanyId,
        fileName: file.name,
        mimeType: file.type,
        message
      });
      setWorkflowStage(workflowStages[0]);
      setWorkflowMessage(message);
      setError(`${message} The file was not added to this workflow.`);
    } finally {
      window.setTimeout(() => {
        setUploading(false);
        setUploadProgress(0);
      }, 500);
    }
  }

  function handleModuleFileChange(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    if (file) void handleModuleUpload(file);
    event.target.value = '';
  }

  function handleDrop(event: DragEvent<HTMLDivElement>) {
    event.preventDefault();
    setDragActive(false);
    const file = event.dataTransfer.files?.[0];
    if (file) void handleModuleUpload(file);
  }

  async function cleanDatasetFromModule(dataset: Dataset) {
    setWorkflowStage(workflowStages.find((stage) => /clean|cleanup/i.test(stage)) ?? workflowStages[4] ?? workflowStages[0]);
    setWorkflowMessage(`Cleaning ${dataset.fileName}...`);
    try {
      const response = await apiFetch(`/api/datasets/${dataset.id}/cleanup`, { method: 'POST' });
      const payload = await readJson<{ job?: CleanupJob; originalDataset?: Dataset; cleanedDataset?: Dataset; error?: string }>(response);
      if (!response.ok || !payload.cleanedDataset || !payload.originalDataset) {
        throw new Error(payload.error || 'Cleanup failed.');
      }
      setDatasets((current) => [
        payload.cleanedDataset as Dataset,
        payload.originalDataset as Dataset,
        ...current.filter((entry) => entry.id !== payload.cleanedDataset?.id && entry.id !== payload.originalDataset?.id)
      ]);
      setCleanupJobs((current) => payload.job ? [payload.job, ...current.filter((job) => job.id !== payload.job?.id)] : current);
      setActiveDataset(payload.cleanedDataset);
      setWorkflowStage(workflowStages.find((stage) => /approval|approve/i.test(stage)) ?? workflowStages.at(-2) ?? workflowStages[0]);
      setWorkflowMessage(`Cleanup completed. ${payload.job?.metrics?.totalCleanedRows ?? payload.cleanedDataset.rows} rows ready for approval.`);
    } catch (cleanupError) {
      setWorkflowStage(workflowStages.find((stage) => /clean|cleanup/i.test(stage)) ?? workflowStages[4] ?? workflowStages[0]);
      setWorkflowMessage(cleanupError instanceof Error ? cleanupError.message : 'Cleanup failed.');
      setError(cleanupError instanceof Error ? cleanupError.message : 'Cleanup failed.');
    }
  }

  function validateDataset(dataset: Dataset) {
    setActiveDataset(dataset);
    setExpandedDatasetId(dataset.id);
    setWorkflowStage(workflowStages.find((stage) => /duplicate|dependencies|project structure|invoice/i.test(stage)) ?? workflowStages[2] ?? workflowStages[0]);
    setWorkflowMessage(`${dataset.fileName} validated. ${displayNumber(dataset.rows)} rows and ${displayNumber(dataset.columns)} columns detected.`);
    setPreviewState({ dataset, mode: 'validation' });
  }

  function normalizeDataset(dataset: Dataset) {
    setActiveDataset(dataset);
    setExpandedDatasetId(dataset.id);
    setWorkflowStage(workflowStages.find((stage) => /normalize|schedule|resource|vendor/i.test(stage)) ?? workflowStages[3] ?? workflowStages[0]);
    setWorkflowMessage(`${dataset.fileName} queued for normalization and value standardization.`);
    setPreviewState({ dataset, mode: 'normalization' });
  }

  function approveDataset(dataset: Dataset) {
    setActiveDataset(dataset);
    setExpandedDatasetId(dataset.id);
    setWorkflowStage(workflowStages.find((stage) => /export/i.test(stage)) ?? workflowStages.at(-1) ?? workflowStages[0]);
    setWorkflowMessage(`${dataset.fileName} approved for export and reporting.`);
    setPreviewState({ dataset, mode: 'approval' });
  }

  const filteredRecords = records
    .filter((record) => filter === 'all' || record.status === filter)
    .filter((record) => !search.trim() || [record.title, record.status].some((value) => value.toLowerCase().includes(search.toLowerCase())));

  function runDatasetAction(dataset: Dataset, action: string) {
    setError('');
    setExpandedDatasetId(dataset.id);
    if (action === 'preview') setPreviewState({ dataset, mode: 'upload' });
    if (action === 'edit') setPreviewState({ dataset, mode: 'normalization' });
    if (action === 'deleteRows') setWorkflowMessage(`${dataset.fileName} row deletion queued. Row-level editing history will be tracked in the next version checkpoint.`);
    if (action === 'newVersion') setWorkflowMessage(`Upload a replacement file to create the next version for ${dataset.fileName}.`);
    if (action === 'restore') setPreviewState({ dataset, mode: 'history' });
    if (action === 'validate') validateDataset(dataset);
    if (action === 'results') setPreviewState({ dataset, mode: 'duplicates' });
    if (action === 'normalize') normalizeDataset(dataset);
    if (action === 'clean') void cleanDatasetFromModule(dataset);
    if (action === 'compare') setPreviewState({ dataset, mode: 'compare' });
    if (action === 'approve') approveDataset(dataset);
    if (action === 'export') setPreviewState({ dataset, mode: 'export' });
    if (action === 'download') downloadDatasetExport(dataset);
    if (action === 'reprocess') void cleanDatasetFromModule(dataset.originalDatasetId ? safeDatasets.find((item) => item.id === dataset.originalDatasetId) ?? dataset : dataset);
    if (action === 'archive') archiveDatasetRecord(dataset);
    if (action === 'delete') deleteDatasetRecord(dataset);
  }

  function stageState(stage: string, index: number): PipelineStageState {
    if (/approval/i.test(stage) && index === activeStageIndex) return 'waiting_approval';
    if (/blocked/i.test(workflowMessage) && index === activeStageIndex) return 'blocked';
    if (/failed|could not|error/i.test(workflowMessage) && index === activeStageIndex) return 'failed';
    if (index < activeStageIndex) return 'completed';
    if (index === activeStageIndex) return 'running';
    return 'queued';
  }

  const stageDetailIndex = Math.max(workflowStages.indexOf(stageDetail), 0);

  return (
    <PageLayout>
      <PageHeader title={route.title} eyebrow={route.moduleLabel} copy={route.copy} />
      {previewState && (
        <DatasetPreviewModal
          allDatasets={safeDatasets}
          company={safeCompanies.find((company) => company.id === previewState.dataset.companyId)}
          dataset={previewState.dataset}
          mode={previewState.mode}
          onApprove={approveDataset}
          onClose={() => setPreviewState(null)}
          onDownload={downloadDatasetExport}
          onReprocess={cleanDatasetFromModule}
          onRestore={(dataset) => {
            setActiveDataset(dataset);
            setPreviewState({ dataset, mode: 'upload' });
            setWorkflowMessage(`${dataset.fileName} restored as active preview version.`);
          }}
        />
      )}
      <article className="panel module-upload-panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">{route.moduleLabel} dataset workspace</p>
            <h2>{pipelineConfig.uploadTitle}</h2>
          </div>
          <select value={selectedCompanyId} onChange={(event) => setSelectedCompanyId(event.target.value)}>
            {!safeCompanies.some((company) => company.id === selectedCompanyId) && <option value="">Select company</option>}
            {safeCompanies.map((company) => <option key={company.id} value={company.id}>{company.name}</option>)}
          </select>
        </div>
        <div
          className={`dropzone ${dragActive ? 'active' : ''}`}
          onDragLeave={() => setDragActive(false)}
          onDragOver={(event) => {
            event.preventDefault();
            setDragActive(true);
          }}
          onDrop={handleDrop}
        >
          <strong>{pipelineConfig.uploadTitle}</strong>
          <span>{pipelineConfig.uploadCopy}</span>
          <input accept=".csv,.xlsx,.xls,.json" type="file" onChange={handleModuleFileChange} />
        </div>
        {uploading && (
          <div className="progress-track" aria-label="Upload progress">
            <span style={{ width: `${uploadProgress}%` }} />
          </div>
        )}
        <div className="workflow-rule-grid" aria-label="Pipeline rules">
          {pipelineRules.map((rule) => <span key={rule}>{rule}</span>)}
        </div>
        <div className="pipeline-stages workflow-stages connected-pipeline-rail sticky-workflow-header">
          {workflowStages.map((stage, index) => (
            <button
              className={`stage-${stageState(stage, index)} ${index <= activeStageIndex ? 'active' : ''}`}
              key={stage}
              type="button"
              onClick={() => {
                setWorkflowStage(stage);
                setStageDetail(stage);
              }}
            >
              <strong>{index + 1}</strong>
              <span>{stage}</span>
              <small>{stageState(stage, index).replace('_', ' ')}</small>
            </button>
          ))}
        </div>
        {stageDetail && (
          <div className="stage-detail-drawer">
            <div>
              <p className="eyebrow">Stage detail</p>
              <strong>{stageDetail}</strong>
              <span>{workflowMessage}</span>
            </div>
            <div className="dataset-detail-grid compact">
              <div><span>Operator</span><strong>Current user</strong></div>
              <div><span>Status</span><strong>{stageState(stageDetail, stageDetailIndex).replace('_', ' ')}</strong></div>
              <div><span>Metrics</span><strong>{moduleDatasets.length} datasets</strong></div>
              <div><span>Retry history</span><strong>0 retries</strong></div>
            </div>
          </div>
        )}
        <p className="persistence-note">{workflowMessage}</p>
        {!safeCompanies.length && (
          <p className="persistence-note warning-note">No company context is available yet. Retry after workspace access loads, or ask an owner to assign a company.</p>
        )}
        {error && lastModuleFile && (
          <button className="ghost-button compact" type="button" onClick={() => void handleModuleUpload(lastModuleFile)}>
            Retry upload
          </button>
        )}
      </article>
      <article className="panel">
        <div className="panel-header">
          <div>
            <p className="eyebrow">Single dataset workspace</p>
            <h2>Enterprise dataset grid</h2>
          </div>
          <span className="dataset-count">{moduleDatasets.length.toLocaleString()} active</span>
        </div>
        {isDataProcessingWorkspace && (
          <div className="sticky-action-toolbar" role="tablist" aria-label="Data processing tools">
            {[
              ['validation', 'Validation'],
              ['duplicates', 'Duplicate Detection'],
              ['cleanup', 'Cleanup'],
              ['reports', 'Quality Reports']
            ].map(([key, label]) => (
              <button className={toolPanel === key ? 'active' : ''} key={key} type="button" onClick={() => setToolPanel(key as typeof toolPanel)}>{label}</button>
            ))}
          </div>
        )}
        <div className="enterprise-dataset-list">
          <div className="dataset-table-head">
            <span>Dataset name</span>
            <span>Pipeline status</span>
            <span>Record count</span>
            <span>Company</span>
            <span>Uploaded by</span>
          </div>
          {moduleDatasets.map((dataset) => (
            <article className={`enterprise-dataset-row ${expandedDatasetId === dataset.id ? 'expanded' : ''}`} key={dataset.id}>
              <button className="dataset-row-summary" type="button" onClick={() => setExpandedDatasetId(expandedDatasetId === dataset.id ? '' : dataset.id)}>
                <span>
                  <strong>{dataset.fileName}</strong>
                  <small>{new Date(dataset.uploadedAt).toLocaleString()} | {dataset.cleanupStatus ?? 'original'}</small>
                </span>
                <span>{workflowStage}</span>
                <span>{displayNumber(dataset.rows)} rows</span>
                <span>{safeCompanies.find((company) => company.id === dataset.companyId)?.name ?? selectedCompanyName}</span>
                <span>{dataset.ownerName ?? dataset.ownerEmail ?? 'Workspace user'}</span>
              </button>
              {expandedDatasetId === dataset.id && (
                <div className="dataset-row-details">
                  <div className="dataset-detail-grid">
                    <div>
                      <span>Status</span>
                      <strong>{dataset.cleanupStatus ?? 'original'}</strong>
                    </div>
                    <div>
                      <span>Columns</span>
                      <strong>{displayNumber(dataset.columns)}</strong>
                    </div>
                    <div>
                      <span>Quality score</span>
                      <strong>{Math.max(45, 100 - ((dataset.cleanupMetrics?.invalidValuesDetected ?? 0) + (dataset.cleanupMetrics?.failedRows ?? 0))).toLocaleString()}%</strong>
                    </div>
                    <div>
                      <span>Anomaly score</span>
                      <strong>{dataset.cleanupMetrics?.anomaliesDetected ?? summarizeValidation(dataset).warnings.length}</strong>
                    </div>
                  </div>
                  <div className="dataset-preview-strip">
                    {getPreviewRows(dataset, 'upload').slice(0, 3).map((row, index) => (
                      <span key={`${dataset.id}-preview-${index}`}>{getPreviewHeaders(dataset).slice(0, 3).map((header) => row[header] || 'null').join(' | ')}</span>
                    ))}
                  </div>
                  <div className="workflow-history-strip">
                    {(asArray(dataset.cleanupLogs).length ? asArray(dataset.cleanupLogs) : ['Uploaded', ...qualitySignals]).slice(0, 5).map((entry) => <span key={entry}>{entry}</span>)}
                  </div>
                  <div className="dataset-row-footer">
                    <details className="dataset-action-menu">
                      <summary>{deletingDatasetId === dataset.id ? 'Deleting...' : 'Actions'}</summary>
                      <button type="button" onClick={() => runDatasetAction(dataset, 'preview')}>Preview</button>
                      <button type="button" onClick={() => runDatasetAction(dataset, 'edit')}>Edit rows</button>
                      <button type="button" onClick={() => runDatasetAction(dataset, 'validate')}>Validate</button>
                      <button type="button" onClick={() => runDatasetAction(dataset, 'normalize')}>Normalize</button>
                      {!dataset.originalDatasetId && <button type="button" onClick={() => runDatasetAction(dataset, 'clean')}>Clean</button>}
                      <button type="button" onClick={() => runDatasetAction(dataset, 'deleteRows')}>Delete rows</button>
                      <button type="button" onClick={() => runDatasetAction(dataset, 'results')}>View Results</button>
                      <button type="button" onClick={() => runDatasetAction(dataset, 'compare')}>Compare</button>
                      <button type="button" onClick={() => runDatasetAction(dataset, 'approve')}>Approve</button>
                      <button type="button" onClick={() => runDatasetAction(dataset, 'export')}>{pipelineConfig.exportLabel}</button>
                      <button type="button" onClick={() => runDatasetAction(dataset, 'download')}>Download</button>
                      <button type="button" onClick={() => runDatasetAction(dataset, 'reprocess')}>Reprocess</button>
                      <button type="button" onClick={() => runDatasetAction(dataset, 'newVersion')}>Upload new version</button>
                      <button type="button" onClick={() => runDatasetAction(dataset, 'restore')}>Restore version</button>
                      <button type="button" onClick={() => runDatasetAction(dataset, 'archive')}>Archive</button>
                      <button className="danger-action" disabled={deletingDatasetId === dataset.id} type="button" onClick={() => runDatasetAction(dataset, 'delete')}>Delete</button>
                    </details>
                    <button className="ghost-button compact" type="button" onClick={() => setPreviewState({ dataset, mode: 'history' })}>Version history</button>
                  </div>
                </div>
              )}
            </article>
          ))}
          {!moduleDatasets.length && <EmptyState title="No datasets uploaded" copy={pipelineConfig.emptyState} />}
        </div>
      </article>
      {isDataProcessingWorkspace ? (
        <article className="panel routed-workspace compact-tool-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Tool panel</p>
              <h2>{toolPanel === 'validation' ? 'Validation results' : toolPanel === 'duplicates' ? 'Duplicate detection' : toolPanel === 'cleanup' ? 'Cleanup operations' : 'Quality reports'}</h2>
            </div>
            <button className="ghost-button compact" type="button" disabled={!moduleDatasets.length} onClick={() => moduleDatasets[0] && setPreviewState({ dataset: moduleDatasets[0], mode: toolPanel === 'duplicates' ? 'duplicates' : toolPanel === 'cleanup' ? 'cleanup' : toolPanel === 'reports' ? 'export' : 'validation' })}>
              Open drawer
            </button>
          </div>
          <div className="dataset-detail-grid">
            <div><span>Datasets</span><strong>{moduleDatasets.length}</strong></div>
            <div><span>Invalid values</span><strong>{moduleDatasets.reduce((sum, dataset) => sum + (dataset.cleanupMetrics?.invalidValuesDetected ?? summarizeValidation(dataset).warnings.length), 0)}</strong></div>
            <div><span>Duplicates</span><strong>{moduleDatasets.reduce((sum, dataset) => sum + findDuplicateRows(dataset.preview).length, 0)}</strong></div>
            <div><span>Exports ready</span><strong>{moduleDatasets.filter((dataset) => dataset.cleanupStatus === 'completed' || dataset.originalDatasetId).length}</strong></div>
          </div>
          <p className="persistence-note">
            {toolPanel === 'validation' && 'Validation, missing values, schema drift, and type issues are available from each dataset action drawer.'}
            {toolPanel === 'duplicates' && 'Duplicate review reuses the selected dataset and opens side-by-side duplicate evidence without another upload.'}
            {toolPanel === 'cleanup' && 'Cleanup, normalization, reprocessing, versioning, and change-log actions are dataset-centered.'}
            {toolPanel === 'reports' && 'Quality reports and exports are generated from approved dataset versions in this workspace.'}
          </p>
          {error && <p className="persistence-note warning-note">{error}</p>}
        </article>
      ) : (
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
      )}
    </PageLayout>
  );
}

function DatasetPreviewModal({
  allDatasets,
  company,
  dataset,
  mode,
  onApprove,
  onClose,
  onDownload,
  onReprocess,
  onRestore
}: {
  allDatasets: Dataset[];
  company?: Company;
  dataset: Dataset;
  mode: PreviewMode;
  onApprove: (dataset: Dataset) => void;
  onClose: () => void;
  onDownload: (dataset: Dataset | null) => void;
  onReprocess: (dataset: Dataset) => void;
  onRestore: (dataset: Dataset) => void;
}) {
  const [page, setPage] = useState(1);
  const [filter, setFilter] = useState('');
  const [sortColumn, setSortColumn] = useState(dataset.headers[0] ?? '');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');
  const pageSize = 10;
  const previewRows = getPreviewRows(dataset, mode);
  const headers = getPreviewHeaders(dataset, previewRows);
  const columnTypes = inferColumnTypes(dataset.preview, dataset.headers);
  const validation = summarizeValidation(dataset);
  const duplicates = findDuplicateRows(dataset.preview);
  const versionHistory = getDatasetVersions(allDatasets, dataset);
  const filteredRows = previewRows
    .filter((row) => !filter.trim() || Object.values(row).some((value) => String(value ?? '').toLowerCase().includes(filter.toLowerCase())))
    .sort((a, b) => {
      const left = String(a[sortColumn] ?? '');
      const right = String(b[sortColumn] ?? '');
      return sortDirection === 'asc' ? left.localeCompare(right) : right.localeCompare(left);
    });
  const pageCount = Math.max(Math.ceil(filteredRows.length / pageSize), 1);
  const pagedRows = filteredRows.slice((page - 1) * pageSize, page * pageSize);
  const modeTitle = {
    upload: 'Upload Preview',
    validation: 'Validation Preview',
    duplicates: 'Duplicate Detection',
    normalization: 'Normalization Preview',
    cleanup: 'Cleanup Preview',
    approval: 'Approval Preview',
    export: 'Export Preview',
    compare: 'Before / After Compare',
    history: 'Version History'
  }[mode];

  function sortBy(header: string) {
    setSortDirection((current) => sortColumn === header && current === 'asc' ? 'desc' : 'asc');
    setSortColumn(header);
    setPage(1);
  }

  return (
    <div className="modal-backdrop" role="presentation">
      <section className="preview-modal" aria-modal="true" role="dialog" aria-label={modeTitle}>
        <div className="panel-header">
          <div>
            <p className="eyebrow">{modeTitle}</p>
            <h2>{dataset.fileName}</h2>
            <p className="muted">{company?.name ?? 'Company workspace'} - {dataset.fileType?.toUpperCase() ?? 'DATA'} - uploaded {new Date(dataset.uploadedAt).toLocaleString()}</p>
          </div>
          <button className="ghost-button compact" type="button" onClick={onClose}>Close</button>
        </div>

        <div className="preview-summary-grid">
          <div><strong>{displayNumber(dataset.rows)}</strong><span>Rows</span></div>
          <div><strong>{displayNumber(dataset.columns)}</strong><span>Columns</span></div>
          <div><strong>{validation.missingValues}</strong><span>Missing values</span></div>
          <div><strong>{duplicates.length}</strong><span>Duplicates</span></div>
          <div><strong>{dataset.cleanupMetrics?.failedRows ?? validation.failedRows}</strong><span>Failed rows</span></div>
          <div><strong>{dataset.cleanupStatus ?? 'pending'}</strong><span>Status</span></div>
        </div>

        <div className="preview-status-row">
          {['queued', 'running', 'completed', 'failed'].map((state) => (
            <span className={`pipeline-state ${state} ${state === stageStatusForMode(mode, dataset) ? 'current' : ''}`} key={state}>{state}</span>
          ))}
        </div>

        <div className="preview-panels">
          <article>
            <h3>Column Types</h3>
            <div className="column-type-list">
              {Object.entries(columnTypes).map(([column, type]) => <span key={column}>{column}: {type}</span>)}
            </div>
          </article>
          <article>
            <h3>Warnings</h3>
            <ul className="preview-warnings">
              {buildPreviewWarnings(dataset, mode, validation, duplicates).map((warning) => <li key={warning}>{warning}</li>)}
            </ul>
          </article>
        </div>

        {(mode === 'normalization' || mode === 'cleanup' || mode === 'compare') && (
          <BeforeAfterPreview dataset={dataset} />
        )}

        {mode === 'duplicates' && (
          <div className="duplicate-list">
            {duplicates.slice(0, 5).map((duplicate) => (
              <div key={duplicate.key}>
                <strong>Duplicate confidence {duplicate.confidence}%</strong>
                <span>{duplicate.count} matching rows. Recommendation: merge duplicate records after source-system review.</span>
              </div>
            ))}
            {!duplicates.length && <p className="muted">No duplicate rows found in the available preview window.</p>}
          </div>
        )}

        <div className="record-toolbar preview-toolbar">
          <input placeholder="Filter preview rows" value={filter} onChange={(event) => { setFilter(event.target.value); setPage(1); }} />
          <button className="ghost-button compact" type="button" disabled={page <= 1} onClick={() => setPage((current) => Math.max(current - 1, 1))}>Previous</button>
          <span>Page {page} of {pageCount}</span>
          <button className="ghost-button compact" type="button" disabled={page >= pageCount} onClick={() => setPage((current) => Math.min(current + 1, pageCount))}>Next</button>
        </div>

        <div className="table-wrap preview-table-wrap">
          <table className="preview-table">
            <thead>
              <tr>{headers.map((header) => <th key={header}><button type="button" onClick={() => sortBy(header)}>{header}{sortColumn === header ? ` ${sortDirection === 'asc' ? 'up' : 'down'}` : ''}</button></th>)}</tr>
            </thead>
            <tbody>
              {pagedRows.map((row, rowIndex) => (
                <tr key={`${rowIndex}-${page}`}>{headers.map((header) => <td className={cellClass(row[header], header)} key={header}>{String(row[header] ?? '')}</td>)}</tr>
              ))}
              {!pagedRows.length && <tr><td colSpan={headers.length || 1}>No preview rows match this filter.</td></tr>}
            </tbody>
          </table>
        </div>

        <div className="version-history">
          <h3>Version History</h3>
          {versionHistory.map((version) => (
            <button className={version.id === dataset.id ? 'active' : ''} key={version.id} type="button" onClick={() => onRestore(version)}>
              {version.fileName} - {version.cleanupStatus ?? 'original'} - {new Date(version.uploadedAt).toLocaleDateString()}
            </button>
          ))}
        </div>

        <div className="modal-actions">
          <button className="ghost-button" type="button" onClick={() => onReprocess(dataset.originalDatasetId ? allDatasets.find((entry) => entry.id === dataset.originalDatasetId) ?? dataset : dataset)}>Reprocess</button>
          <button className="ghost-button" type="button" onClick={() => onApprove(dataset)}>Approve</button>
          <button type="button" onClick={() => onDownload(dataset)}>Download</button>
        </div>
      </section>
    </div>
  );
}

function BeforeAfterPreview({ dataset }: { dataset: Dataset }) {
  const before = asArray(dataset.cleanupPreview?.before).length ? asArray(dataset.cleanupPreview?.before).slice(0, 5) : asArray(dataset.preview).slice(0, 5);
  const after = asArray(dataset.cleanupPreview?.after).length ? asArray(dataset.cleanupPreview?.after).slice(0, 5) : before.map(normalizePreviewRow);
  const headers = Array.from(new Set([...Object.keys(before[0] ?? {}), ...Object.keys(after[0] ?? {})]));
  return (
    <div className="before-after-grid">
      {(['Before', 'After'] as const).map((label) => (
        <article key={label}>
          <h3>{label}</h3>
          <div className="mini-table-wrap">
            <table>
              <thead><tr>{headers.map((header) => <th key={header}>{header}</th>)}</tr></thead>
              <tbody>
                {(label === 'Before' ? before : after).slice(0, 5).map((row, index) => (
                  <tr key={`${label}-${index}`}>{headers.map((header) => <td className={cellClass(row[header], header)} key={header}>{String(row[header] ?? '')}</td>)}</tr>
                ))}
              </tbody>
            </table>
          </div>
        </article>
      ))}
    </div>
  );
}

function AnalyticsWorkspace({ dashboards, reports }: { dashboards: SavedDashboard[]; reports: ReportHistoryItem[] }) {
  return (
    <PageLayout>
      <PageHeader title="Analytics Dashboard" eyebrow="Analytics" copy="Executive KPI workspace for dashboards, reports, and business-ready analysis." />
      <article className="panel">
        <div className="module-grid">
          <div><strong>{dashboards.length}</strong><span>Saved dashboards</span></div>
          <div><strong>{reports.length}</strong><span>Generated reports</span></div>
          <div><strong>Live</strong><span>Business analytics workspace</span></div>
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

function AdminUsersWorkspace({
  users,
  updateAdminUser,
  deleteAdminUser,
  openCompanyAccess
}: {
  users: AdminUser[];
  updateAdminUser: (userId: string, updates: Partial<AdminUser>) => void;
  deleteAdminUser: (user: AdminUser) => void;
  openCompanyAccess: (user: AdminUser) => void;
}) {
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
            <thead><tr><th>Name</th><th>Email</th><th>Role</th><th>Assigned Companies</th><th>Status</th><th>Actions</th></tr></thead>
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
                  <td><AssignedCompaniesList assignments={adminUser.assignedCompanies ?? []} /></td>
                  <td>{adminUser.active ? 'Active' : 'Disabled'}</td>
                  <td>
                    <div className="admin-actions">
                      <button className="ghost-button compact" type="button" onClick={() => openCompanyAccess(adminUser)}>Manage Access</button>
                      <button className="ghost-button compact danger" type="button" onClick={() => deleteAdminUser(adminUser)}>Delete</button>
                    </div>
                  </td>
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
    accounting: 'Invoices, expenses, payroll, financial reports, budgets, tax tracking, and a business financial assistant.',
    engineering: 'Project management, task tracking, team assignments, document uploads, blueprint management, and workflow reports.',
    hr: 'People operations, roles, onboarding, team assignments, and HR-ready business support.',
    crm: 'Client records, opportunity tracking, support history, and business-assisted relationship management.',
    dataProcessing: 'Data cleanup, duplicate detection, validation, normalization, import/export, batch processing, and data quality reports.'
  };
  const cards = moduleCards[view] ?? [];
  const pipelineStages = ['Upload', 'Validate', 'Detect Duplicates', 'Normalize', 'Clean', 'Quality Score', 'Approve', 'Export'];
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
        <p className="eyebrow">Modular Business ERP</p>
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
            <button type="button" className="ghost-button compact" onClick={() => navigate('/data-processing/workspace')}>
              Open workspace
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
                  <span>{record.recordType} - queued with business cleanup recommendations</span>
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
        <h2>Business data assistant</h2>
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
    'Business Insights',
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
