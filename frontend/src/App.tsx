import { ChangeEvent, Component, CSSProperties, Dispatch, DragEvent, ErrorInfo, FormEvent, ReactNode, SetStateAction, useEffect, useMemo, useState } from 'react';
import { Navigate, Route, Routes, useLocation, useNavigate } from 'react-router-dom';
import { DatasetGrid, HrDatasetSummary } from './components/hr/DatasetGrid';
import { DatasetToolbar } from './components/hr/DatasetToolbar';
import { EmployeeGrid, EmployeeGridRow } from './components/hr/EmployeeGrid';
import { HRWorkspaceFrame } from './components/hr/HRWorkspace';
import { TimesheetWorkspaceSummary } from './components/hr/TimesheetWorkspace';
import { UploadCenter, UploadStep } from './components/hr/UploadCenter';

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
  name?: string;
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
  records?: Record<string, string>[];
  previewRows?: Record<string, string>[];
  validationResults?: unknown[];
  duplicates?: unknown[];
  pipeline?: unknown[];
  exports?: unknown[];
  approvals?: unknown[];
  qualityResults?: unknown[];
  qualityScore?: number;
  status?: string;
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
type PreviewMode = 'upload' | 'validation' | 'duplicates' | 'normalization' | 'cleanup' | 'approval' | 'export' | 'compare' | 'history' | 'edit' | 'query';
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
    metrics?: Record<string, number>;
    trends?: Array<Record<string, string | number>>;
    executiveSummary?: string[];
    aiInsights?: Array<{
      type?: string;
      severity?: string;
      title: string;
      summary: string;
      confidence?: number;
    }>;
    recommendations?: string[];
    approvalStatus?: string;
    generatedAt?: string;
    trigger?: string;
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
  companyId?: string;
  userId?: string;
  module: string;
  recordType: string;
  title: string;
  status: string;
  amount?: number | null;
  metadata?: Record<string, unknown>;
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

type UploadErrorPayload = {
  success?: boolean;
  error?: string;
  code?: string;
  uploadStage?: string;
  stage?: string;
  requestId?: string;
  details?: Record<string, unknown>;
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
const SELECTED_COMPANY_STORAGE_KEY = 'metenovaSelectedCompanyId';
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

const moduleNav: Array<{ group: string; view: AppView; label: string; icon: string; adminOnly?: boolean }> = [
  { group: 'Core Workspace', view: 'dashboard', label: 'Dashboard', icon: 'DB' },
  { group: 'Core Workspace', view: 'companies', label: 'Companies', icon: 'CO' },
  { group: 'Core Workspace', view: 'assistant', label: 'Business Assistant', icon: 'BA' },
  { group: 'Operations', view: 'hr', label: 'HR & Workforce', icon: 'HR' },
  { group: 'Operations', view: 'accounting', label: 'Finance & Accounting', icon: 'FA' },
  { group: 'Operations', view: 'engineering', label: 'Engineering & Projects', icon: 'EP' },
  { group: 'Operations', view: 'crm', label: 'CRM & Sales', icon: 'CS' },
  { group: 'Data & Intelligence', view: 'dataProcessing', label: 'Enterprise Data Hub', icon: 'DH' },
  { group: 'Data & Intelligence', view: 'analytics', label: 'Analytics', icon: 'AN' },
  { group: 'Administration', view: 'adminUsers', label: 'Admin Center', icon: 'AD', adminOnly: true },
  { group: 'Administration', view: 'settings', label: 'Settings', icon: 'ST' },
  { group: 'Administration', view: 'contact', label: 'Contact Us', icon: 'CU' }
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

function companyFromAssignment(assignment: CompanyAssignment): Company {
  return {
    id: assignment.companyId,
    name: assignment.companyName || 'Assigned workspace',
    industry: 'Business operations',
    ownerName: 'Assigned team',
    email: '',
    phone: '',
    status: 'Active'
  };
}

function userScopedCompanyFallback(user?: User | null): Company[] {
  if (!user) return [];
  const assigned = asArray(user.assignedCompanies)
    .filter((assignment) => Boolean(assignment.companyId))
    .map(companyFromAssignment);

  if (assigned.length) return assigned;
  if (!user.companyId) return [];

  return [{
    id: user.companyId,
    name: 'Assigned workspace',
    industry: 'Business operations',
    ownerName: user.name || 'Assigned user',
    email: user.email || '',
    phone: '',
    status: 'Active'
  }];
}

function buildCompanyQuery(companyId: string) {
  return companyId ? `?companyId=${encodeURIComponent(companyId)}` : '';
}

function formatUploadError(payload: UploadErrorPayload, status: number) {
  const codeLabels: Record<string, string> = {
    UPLOAD_INVALID_MIME: 'Invalid MIME',
    UPLOAD_INVALID_SCHEMA: 'Invalid schema',
    UPLOAD_COMPANY_REQUIRED: 'Unauthorized company',
    UPLOAD_COMPANY_NOT_FOUND: 'Unauthorized company',
    COMPANY_FORBIDDEN: 'Unauthorized company',
    UPLOAD_PARSER_FAILURE: 'Parser failure',
    UPLOAD_DATABASE_FAILURE: 'Database persistence failure',
    UPLOAD_FILE_MISSING: 'Multipart/form-data handling failed',
    CSRF_INVALID: 'CSRF/session failure',
    SESSION_EXPIRED: 'CSRF/session failure'
  };
  const label = payload.code ? codeLabels[payload.code] ?? payload.code : status === 401 || status === 403 ? 'CSRF/session failure' : 'Upload failure';
  const stageName = payload.stage ?? payload.uploadStage;
  const stage = stageName ? ` Stage: ${stageName}.` : '';
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
    { title: 'HR Overview', copy: 'Compact HR command center for company workforce, datasets, approvals, and AI insights.', type: 'overview', path: '/hr' },
    { title: 'Employee Records', copy: 'Centralize profiles, roles, onboarding status, and access policies.', type: 'employee', path: '/hr/employees' },
    { title: 'Timesheets', copy: 'Manage daily entries, weekly approvals, project hours, PTO, overtime, and payroll-ready exports.', type: 'timesheets', path: '/hr/timesheets' },
    { title: 'Payroll Workspace', copy: 'Run payroll preparation, paystub review, dataset-backed payroll exports, and exception checks.', type: 'payroll', path: '/hr/payroll' },
    { title: 'Hiring', copy: 'Track candidates, interviews, and onboarding steps.', type: 'hiring', path: '/hr/hiring' },
    { title: 'Leave Management', copy: 'Manage leave requests, approvals, and team coverage.', type: 'leave', path: '/hr/leave-management' },
    { title: 'HR Dataset Workspace', copy: 'Select one HR dataset at a time, edit rows and columns, approve changes, and publish exports.', type: 'datasets', path: '/hr/datasets' },
    { title: 'HR Reports Workspace', copy: 'Generate HR, payroll, PTO, timesheet, and workforce reports inside the company workspace.', type: 'reports', path: '/hr/reports' },
    { title: 'HR Approval Workspace', copy: 'Review PTO, timesheet, payroll, and dataset approval queues.', type: 'approvals', path: '/hr/approvals' },
    { title: 'HR AI Insights Workspace', copy: 'Review workforce risk, payroll anomalies, missing data, and operational recommendations.', type: 'ai_insights', path: '/hr/ai-insights' }
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
  accounting: 'Finance & Accounting',
  engineering: 'Engineering & Projects',
  hr: 'HR & Workforce',
  crm: 'CRM & Sales',
  dataProcessing: 'Enterprise Data Hub'
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
  const [selectedCompanyId, setSelectedCompanyId] = useState(() => localStorage.getItem(SELECTED_COMPANY_STORAGE_KEY) || sampleCompanies[0]?.id || '');
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
    if (selectedCompanyId) {
      localStorage.setItem(SELECTED_COMPANY_STORAGE_KEY, selectedCompanyId);
    }
  }, [selectedCompanyId]);

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

      const savedDatasets = asArray(datasetsPayload.datasets).map(normalizeDatasetForClient);
      const savedDashboards = dashboardsPayload.dashboards ?? [];
      const latestDashboard = savedDashboards[0];
      const latestDataset = latestDashboard
        ? savedDatasets.find((dataset) => dataset.id === latestDashboard.datasetId) ?? latestDashboard.snapshot?.dataset
        : undefined;
      const normalizedLatestDataset = latestDataset ? normalizeDatasetForClient(latestDataset) : null;

      setUser(mePayload.user);
      setInsights(insightsPayload);
      setWorkflows(workflowsPayload.workflows ?? []);
      setDatasets(savedDatasets);
      setActiveDataset(normalizedLatestDataset ?? savedDatasets[0] ?? null);
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
    const chart = datasetChart(activeDataset);
    if (!chart.length) {
      return 1;
    }

    return Math.max(...chart.map((point) => point.value), 1);
  }, [activeDataset]);

  const linePoints = useMemo(() => {
    const chart = datasetChart(activeDataset);
    if (!chart.length) {
      return '';
    }

    return chart
      .map((point, index) => {
        const x = chart.length === 1 ? 50 : (index / (chart.length - 1)) * 100;
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
  const assignedCompanyIds = useMemo(() => asArray(user?.assignedCompanies).map((assignment) => assignment.companyId), [user?.assignedCompanies]);
  const uploadCompanies = useMemo(() => {
    if (!user || canManageUsers(user)) return companies;
    const assigned = companies.filter((company) => assignedCompanyIds.includes(company.id));
    const primaryCompany = companies.filter((company) => company.id === user.companyId);
    return assigned.length ? assigned : primaryCompany.length ? primaryCompany : userScopedCompanyFallback(user);
  }, [assignedCompanyIds, companies, user]);
  const effectiveCompanyId = useMemo(() => {
    if (uploadCompanies.some((company) => company.id === selectedCompanyId)) return selectedCompanyId;
    return uploadCompanies[0]?.id ?? selectedCompanyId;
  }, [selectedCompanyId, uploadCompanies]);

  async function uploadDataset(file: File, worksheetName?: string, companyIdOverride?: string) {
    const targetCompanyId = companyIdOverride || effectiveCompanyId;
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

      const uploadPayload = await readJson<(Dataset & UploadErrorPayload) & { dataset?: Dataset }>(response);
      console.info('[Metenova Upload] Dataset upload response', {
        requestId: uploadPayload?.requestId,
        keys: Object.keys(asObject(uploadPayload)),
        dataset: uploadPayload
      });

      if (!response.ok) {
        throw new Error(formatUploadError(uploadPayload, response.status));
      }

      const dataset = normalizeDatasetForClient(uploadPayload.dataset ?? uploadPayload);
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
        normalizeDatasetForClient(payload.cleanedDataset as Dataset),
        normalizeDatasetForClient(payload.originalDataset as Dataset),
        ...current.filter((item) => item.id !== payload.cleanedDataset?.id && item.id !== payload.originalDataset?.id)
      ]);
      setActiveDataset(normalizeDatasetForClient(payload.cleanedDataset));
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
      const archivedDataset = normalizeDatasetForClient(payload.dataset);
      setDatasets((current) => current.map((entry) => entry.id === archivedDataset.id ? archivedDataset : entry));
      setActiveDataset(archivedDataset);
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
          chartColumn: activeDataset.chartColumn ?? '',
          labelColumn: activeDataset.labelColumn ?? ''
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
      const userAssignedIds = asArray(user?.assignedCompanies).map((assignment) => assignment.companyId);
      const visibleCompanies = !user || canManageUsers(user)
        ? nextCompanies
        : nextCompanies.filter((company) => userAssignedIds.includes(company.id) || company.id === user.companyId);
      const companyOptions = visibleCompanies.length ? visibleCompanies : userScopedCompanyFallback(user);
      setCompanies(companyOptions);
      setSelectedCompanyId((current) => companyOptions.some((company) => company.id === current) ? current : companyOptions[0]?.id ?? '');
    } catch (error) {
      setCompanies(!user || canManageUsers(user) ? sampleCompanies : userScopedCompanyFallback(user));
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

      const nextDatasets = asArray(datasetsPayload.datasets).map(normalizeDatasetForClient);
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
        const companyActionDatasets = asArray(payload.datasets).map(normalizeDatasetForClient);
        const candidate = companyActionDatasets.find((dataset) => !dataset.originalDatasetId);
        if (!candidate) throw new Error('Upload an original dataset before running cleanup.');
        setActiveDataset(candidate);
        const cleanupResponse = await apiFetch(`/api/datasets/${candidate.id}/cleanup`, { method: 'POST' });
        const cleanupPayload = await readJson<{ cleanedDataset?: Dataset; originalDataset?: Dataset; job?: CleanupJob; error?: string }>(cleanupResponse);
        if (!cleanupResponse.ok || !cleanupPayload.cleanedDataset || !cleanupPayload.originalDataset) {
          throw new Error(cleanupPayload.error || 'Cleanup failed.');
        }
        const cleanedDataset = normalizeDatasetForClient(cleanupPayload.cleanedDataset as Dataset);
        const originalDataset = normalizeDatasetForClient(cleanupPayload.originalDataset as Dataset);
        setDatasets((current) => [
          cleanedDataset,
          originalDataset,
          ...current.filter((item) => item.id !== cleanupPayload.cleanedDataset?.id && item.id !== cleanupPayload.originalDataset?.id)
        ]);
        setActiveDataset(cleanedDataset);
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
        const companyActionDatasets = asArray(payload.datasets).map(normalizeDatasetForClient);
        const dataset = companyActionDatasets.find((item) => item.cleanupStatus === 'completed') ?? companyActionDatasets[0] ?? null;
        if (!dataset) throw new Error('No datasets are available to export.');
        await downloadDatasetExport(dataset);
      } else if (action === 'Delete Dataset') {
        const response = await apiFetch(`/api/datasets${query}`);
        const payload = await readJson<{ datasets?: Dataset[]; error?: string }>(response);
        if (!response.ok) throw new Error(payload.error || 'Could not load datasets.');
        const dataset = asArray(payload.datasets).map(normalizeDatasetForClient)[0] ?? null;
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
          insights: datasetInsights(activeDataset),
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
    if (view === 'dataProcessing') {
      navigate('/data-processing/workspace');
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
          {['Core Workspace', 'Operations', 'Data & Intelligence', 'Administration'].map((group) => {
            const items = moduleNav.filter((item) => item.group === group && (!item.adminOnly || canManageUsers(user)));
            if (!items.length) return null;
            return (
              <div className="nav-group" key={group}>
                <span>{group}</span>
                {items.map((item) => (
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
              </div>
            );
          })}
        </nav>
      </aside>

      <section className="content" id="overview">
        <header className="topbar">
          <div>
            <p className="eyebrow">Operations command center</p>
            <h1>{selectedCompany?.name ?? 'Company'} business operating system</h1>
          </div>
          <div className="top-actions">
            <label className="global-company-selector">
              Company
              <select value={effectiveCompanyId} onChange={(event) => setSelectedCompanyId(event.target.value)}>
                {uploadCompanies.map((company) => <option key={company.id} value={company.id}>{company.name}</option>)}
              </select>
            </label>
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
            companies={uploadCompanies}
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
            companies={uploadCompanies}
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
            selectedCompanyId={effectiveCompanyId}
            setActiveDataset={setActiveDataset}
            setCleanupJobs={setCleanupJobs}
            setDatasets={setDatasets}
            setReports={setReports}
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
              companies={uploadCompanies}
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
                  <select value={effectiveCompanyId} onChange={(event) => setSelectedCompanyId(event.target.value)}>
                    {uploadCompanies.map((company) => (
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
                      {asArray(activeDataset.worksheets).length > 1 && (
                        <div className="sheet-tabs" aria-label="Worksheet tabs">
                          {asArray(activeDataset.worksheets).map((sheetName) => (
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
                              {datasetHeaders(activeDataset).map((header) => (
                                <th key={header}>{header}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {datasetPreview(activeDataset).map((row, rowIndex) => (
                              <tr key={`${activeDataset.id}-${rowIndex}`}>
                                {datasetHeaders(activeDataset).map((header) => (
                                  <td key={header}>{row[header]}</td>
                                ))}
                              </tr>
                            ))}
                            {!datasetPreview(activeDataset).length && (
                              <tr><td colSpan={Math.max(datasetHeaders(activeDataset).length, 1)}>No preview rows are available for this dataset yet.</td></tr>
                            )}
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
                        <p className="muted">{activeDataset ? `Showing ${activeDataset.chartColumn ?? 'dataset values'}` : 'A chart appears after upload.'}</p>
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
                      {(datasetInsights(activeDataset).length ? datasetInsights(activeDataset) : ['Upload a CSV or Excel file to generate clear, practical data insights.']).map((item) => (
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
  const chart = datasetChart(dataset);
  if (!chart.length) {
    return <div className="empty-state chart-empty">Upload your first dataset to begin processing.</div>;
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
    const total = chart.reduce((sum, point) => sum + point.value, 0) || 1;
    const topValue = chart[0]?.value ?? 0;
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
      {chart.map((point) => (
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
  const tabs = ['Overview', 'Operations', 'Pipelines', 'Approvals', 'Analytics', 'Reports', 'Governance'];
  const [activeTab, setActiveTab] = useState('Overview');
  const [drillPanel, setDrillPanel] = useState<{ title: string; kind: string } | null>(null);
  const [search, setSearch] = useState('');
  const [datasetRegistryQuery, setDatasetRegistryQuery] = useState('');
  const [datasetRegistryModule, setDatasetRegistryModule] = useState('all');
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
  const moduleGroups = [
    ['HR', companyDatasets.filter((dataset) => classifyDatasetModule(dataset) === 'HR')],
    ['Finance', companyDatasets.filter((dataset) => classifyDatasetModule(dataset) === 'Finance')],
    ['Engineering', companyDatasets.filter((dataset) => classifyDatasetModule(dataset) === 'Engineering')],
    ['CRM', companyDatasets.filter((dataset) => classifyDatasetModule(dataset) === 'CRM')]
  ] as Array<[string, Dataset[]]>;
  const registryDatasets = companyDatasets
    .filter((dataset) => datasetRegistryModule === 'all' || classifyDatasetModule(dataset) === datasetRegistryModule)
    .filter((dataset) => !datasetRegistryQuery.trim() || [
      dataset.fileName,
      dataset.ownerEmail,
      dataset.cleanupStatus,
      ...asArray(dataset.headers)
    ].join(' ').toLowerCase().includes(datasetRegistryQuery.toLowerCase()));

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

      <div className="company-dataset-selector">
        <div>
          <p className="eyebrow">Company dataset registry</p>
          <strong>{selectedCompany?.name ?? 'Selected company'} datasets</strong>
          <span>{companyDatasets.length} datasets connected to dashboard, analytics, reports, pipelines, and governance.</span>
        </div>
        <select value={activeDataset?.id ?? ''} onChange={(event) => onSelectDataset(companyDatasets.find((dataset) => dataset.id === event.target.value) ?? null)}>
          <option value="">Company overview</option>
          {companyDatasets.map((dataset) => <option key={dataset.id} value={dataset.id}>{dataset.fileName}</option>)}
        </select>
        <button type="button" onClick={() => navigate('/data-processing/workspace')}>Open Enterprise Data Hub</button>
      </div>

      <div className="company-module-dataset-registry">
        {moduleGroups.map(([label, datasets]) => (
          <div key={String(label)}>
            <span>{label} datasets</span>
            <strong>{datasets.length}</strong>
            <small>{datasets[0]?.fileName ?? 'No module dataset yet'}</small>
          </div>
        ))}
      </div>

      <div className="dashboard-dataset-browser">
        <div className="record-toolbar">
          <input placeholder="Search datasets, columns, owners, status..." value={datasetRegistryQuery} onChange={(event) => setDatasetRegistryQuery(event.target.value)} />
          <select value={datasetRegistryModule} onChange={(event) => setDatasetRegistryModule(event.target.value)}>
            <option value="all">All modules</option>
            <option value="HR">HR</option>
            <option value="Finance">Finance</option>
            <option value="Engineering">Engineering</option>
            <option value="CRM">CRM</option>
          </select>
        </div>
        <div className="compact-table">
          {registryDatasets.slice(0, 8).map((dataset) => (
            <div key={dataset.id}>
              <span><strong>{dataset.fileName}</strong><small>{classifyDatasetModule(dataset)} | {dataset.rows} rows | {dataset.cleanupStatus ?? dataset.status ?? 'uploaded'}</small></span>
              <span>{dataset.ownerEmail ?? dataset.ownerName ?? 'Workspace'}</span>
              <button type="button" onClick={() => onSelectDataset(dataset)}>Preview rows</button>
              <button type="button" onClick={() => navigate(moduleWorkspacePathForDataset(dataset))}>Open module</button>
            </div>
          ))}
          {!registryDatasets.length && <div><span><strong>No datasets match the current filters.</strong><small>Try another module, status, owner, or column query.</small></span></div>}
        </div>
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
              <CompanyOperationsMatrix
                companyDatasets={companyDatasets}
                companyReports={companyReports}
                selectedCompany={selectedCompany}
              />
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
              <CompanyPipelineRegistry cleanupJobs={cleanupJobs} companyDatasets={companyDatasets} />
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
              <CompanyGovernancePanel
                companyDatasets={companyDatasets}
                companyReports={companyReports}
                roleLabel={roleLabel}
              />
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

function CompanyOperationsMatrix({
  companyDatasets,
  companyReports,
  selectedCompany
}: {
  companyDatasets: Dataset[];
  companyReports: ReportHistoryItem[];
  selectedCompany?: Company;
}) {
  const operations = [
    ['Employees', 'HR & Workforce', 'Employee profiles, documents, paystubs, PTO, onboarding, performance reviews'],
    ['Invoices', 'Finance & Accounting', 'Invoices, expenses, vendor payments, taxes, budgets, reconciliation'],
    ['Projects', 'Engineering & Projects', 'Projects, milestones, dependencies, resources, work orders'],
    ['Customers', 'CRM & Sales', 'Customers, leads, opportunities, contracts, support tickets, follow-ups'],
    ['Datasets', 'Enterprise Data Hub', `${companyDatasets.length} active datasets connected to modules`],
    ['Reports', 'Company reports', `${companyReports.length} generated reports and exports`]
  ];
  return (
    <div className="company-ops-matrix">
      {operations.map(([title, module, copy]) => (
        <article key={title}>
          <span>{module}</span>
          <strong>{title}</strong>
          <p>{copy}</p>
          <small>{selectedCompany?.name ?? 'Company'} scoped</small>
        </article>
      ))}
    </div>
  );
}

function CompanyPipelineRegistry({ cleanupJobs, companyDatasets }: { cleanupJobs: CleanupJob[]; companyDatasets: Dataset[] }) {
  const rows = companyDatasets.slice(0, 6).map((dataset) => ({
    id: dataset.id,
    name: dataset.fileName,
    status: dataset.pipelineStatus ?? dataset.cleanupStatus ?? 'uploaded',
    detail: `${dataset.rows} rows | quality ${dataset.qualityScore ?? 0}%`
  }));
  return (
    <div className="compact-table">
      {rows.map((row) => (
        <div key={row.id}>
          <span><strong>{row.name}</strong><small>{row.detail}</small></span>
          <span>{row.status}</span>
          <span>{cleanupJobs.filter((job) => job.originalDatasetId === row.id || job.cleanedDatasetId === row.id).length} jobs</span>
        </div>
      ))}
      {!rows.length && <p className="muted">Upload a dataset to create company pipeline history.</p>}
    </div>
  );
}

function CompanyGovernancePanel({
  companyDatasets,
  companyReports,
  roleLabel
}: {
  companyDatasets: Dataset[];
  companyReports: ReportHistoryItem[];
  roleLabel: string;
}) {
  return (
    <div className="governance-grid">
      <div><span>Data lineage</span><strong>{companyDatasets.length}</strong><small>datasets tracked</small></div>
      <div><span>Report history</span><strong>{companyReports.length}</strong><small>exports and reports</small></div>
      <div><span>Access role</span><strong>{roleLabel}</strong><small>company-scoped permissions</small></div>
      <div><span>Change history</span><strong>Active</strong><small>upload, cleanup, archive, delete logs</small></div>
    </div>
  );
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

function asObject(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' ? value as Record<string, unknown> : {};
}

function asRecordArray(value: unknown): Record<string, string>[] {
  return Array.isArray(value)
    ? value.filter((entry): entry is Record<string, string> => Boolean(entry) && typeof entry === 'object')
    : [];
}

function finiteNumber(value: unknown, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function displayNumber(value: number | null | undefined) {
  return finiteNumber(value).toLocaleString();
}

function datasetChart(dataset: Dataset | null | undefined) {
  return asArray(dataset?.chart);
}

function datasetHeaders(dataset: Dataset | null | undefined) {
  return asArray(dataset?.headers);
}

function datasetPreview(dataset: Dataset | null | undefined) {
  return asRecordArray(dataset?.preview);
}

function datasetRecords(dataset: Dataset | null | undefined) {
  return asRecordArray(dataset?.records);
}

function datasetPreviewRows(dataset: Dataset | null | undefined) {
  return asRecordArray(dataset?.previewRows);
}

function datasetInsights(dataset: Dataset | null | undefined) {
  return asArray(dataset?.insights);
}

function datasetNumericSummary(dataset: Dataset | null | undefined) {
  return asArray(dataset?.numericSummary);
}

function normalizeDatasetForClient(dataset: Dataset): Dataset {
  const preview = datasetPreview(dataset);
  const previewRows = datasetPreviewRows(dataset);
  const records = datasetRecords(dataset);
  const resolvedPreview = preview.length ? preview : previewRows.length ? previewRows : records;
  const resolvedHeaders = datasetHeaders(dataset).length
    ? datasetHeaders(dataset)
    : Array.from(new Set(resolvedPreview.flatMap((row) => Object.keys(row))));
  const rawRows = Array.isArray((dataset as unknown as { rows?: unknown }).rows)
    ? ((dataset as unknown as { rows?: unknown[] }).rows?.length ?? 0)
    : dataset.rows;
  return {
    ...dataset,
    id: dataset?.id ?? crypto.randomUUID(),
    name: dataset.name ?? dataset.fileName ?? 'Untitled Dataset',
    fileName: dataset.fileName ?? dataset.name ?? 'Untitled Dataset',
    uploadedAt: dataset.uploadedAt ?? new Date().toISOString(),
    rows: finiteNumber(rawRows, resolvedPreview.length),
    columns: finiteNumber(dataset.columns, resolvedHeaders.length),
    headers: resolvedHeaders,
    preview: resolvedPreview,
    records,
    previewRows: previewRows.length ? previewRows : resolvedPreview,
    validationResults: asArray(dataset.validationResults),
    duplicates: asArray(dataset.duplicates),
    pipeline: asArray(dataset.pipeline),
    exports: asArray(dataset.exports),
    approvals: asArray(dataset.approvals),
    qualityResults: asArray(dataset.qualityResults),
    qualityScore: finiteNumber(dataset.qualityScore, 0),
    status: dataset.status ?? dataset.cleanupStatus ?? 'uploaded',
    chart: datasetChart(dataset),
    insights: datasetInsights(dataset),
    numericSummary: datasetNumericSummary(dataset),
    warnings: asArray(dataset.warnings),
    worksheets: asArray(dataset.worksheets),
    cleanupLogs: asArray(dataset.cleanupLogs),
    cleanupPreview: dataset.cleanupPreview ? {
      before: asArray(dataset.cleanupPreview.before),
      after: asArray(dataset.cleanupPreview.after)
    } : dataset.cleanupPreview
  };
}

function classifyDatasetModule(dataset: Dataset | null | undefined) {
  const fileName = String(dataset?.fileName ?? dataset?.name ?? '').toLowerCase();
  const fileType = String(dataset?.fileType ?? '').toLowerCase();
  const headers = datasetHeaders(dataset).join(' ').toLowerCase();
  const haystack = `${fileName} ${fileType} ${headers}`;
  const exactHrDatasets = [
    'employee records dataset',
    'timesheet dataset',
    'pto dataset',
    'sick leave dataset',
    'payroll dataset',
    'hiring dataset',
    'performance dataset'
  ];

  if (exactHrDatasets.some((name) => fileName === name || fileName.startsWith(`${name} `))) return 'HR';
  if (/\b(invoice|expense|payment|tax|gl|ledger|accounting|finance|budget|vendor|reconciliation)\b/.test(haystack)) return 'Finance';
  if (/\b(project|task|ticket|deployment|engineering|sprint|milestone|predecessor|successor|work order|resource planning)\b/.test(haystack)) return 'Engineering';
  if (/\b(lead|customer|opportunit|contract|sales|crm|pipeline|account)\b/.test(haystack)) return 'CRM';
  if (/\b(employee|attendance|pto|sick|payroll|hiring|performance|paystub|benefits|onboarding)\b/.test(haystack)) return 'HR';
  return 'Enterprise Data Hub';
}

function moduleLabelForRoute(moduleName: string) {
  if (moduleName === 'hr') return 'HR';
  if (moduleName === 'accounting') return 'Finance';
  if (moduleName === 'engineering') return 'Engineering';
  if (moduleName === 'crm') return 'CRM';
  return 'Enterprise Data Hub';
}

function moduleWorkspacePathForDataset(dataset: Dataset | null | undefined) {
  const module = classifyDatasetModule(dataset);
  const fileName = String(dataset?.fileName ?? '').toLowerCase();
  if (module === 'HR') {
    if (/attendance|timesheet|shift/.test(fileName)) return '/hr/timesheets';
    if (/pto|sick|leave/.test(fileName)) return '/hr/leave';
    if (/hiring|onboarding/.test(fileName)) return '/hr/hiring';
    return '/hr/employees';
  }
  if (module === 'Finance') return '/accounting/invoices';
  if (module === 'Engineering') return '/engineering/projects';
  if (module === 'CRM') return '/crm/clients';
  return '/data-processing/workspace';
}

function analyzeUploadedDataset(dataset: Dataset | null | undefined, moduleName: string) {
  const headers = datasetHeaders(dataset).map((header) => header.toLowerCase().replace(/\s+/g, '_'));
  const has = (candidates: string[]) => candidates.some((candidate) => headers.includes(candidate));
  const type = has(['employee_id', 'employeeid']) && has(['employee_name', 'name', 'full_name'])
    ? 'Employee dataset'
    : has(['payroll_period', 'gross_pay', 'net_pay', 'pay_rate'])
      ? 'Payroll dataset'
      : has(['work_date', 'date', 'start_time', 'end_time', 'hours']) && has(['employee_id', 'employeeid'])
        ? 'Timesheet dataset'
        : has(['customer', 'customer_name', 'lead', 'opportunity'])
          ? 'CRM/customer dataset'
          : has(['invoice_number', 'vendor', 'amount'])
            ? 'Finance dataset'
            : has(['project_id', 'task', 'milestone'])
              ? 'Engineering dataset'
              : 'Unknown dataset';
  const expected = moduleName === 'hr'
    ? ['employeeId', 'employeeName', 'workDate', 'startTime', 'endTime', 'totalHours', 'overtimeHours', 'department', 'manager', 'approvalStatus', 'payrollPeriod']
    : ['id', 'name', 'status', 'owner', 'updatedAt'];
  const normalizedExpected = expected.map((header) => header.toLowerCase());
  const missingColumns = expected.filter((header) => !headers.includes(header.toLowerCase()) && !headers.includes(header.toLowerCase().replace(/[A-Z]/g, (match) => `_${match.toLowerCase()}`)));
  const extraColumns = datasetHeaders(dataset).filter((header) => !normalizedExpected.includes(header.toLowerCase()));
  const invalidTypes = datasetPreview(dataset).slice(0, 25).flatMap((row, rowIndex) => (
    Object.entries(row).flatMap(([header, value]) => {
      if (/date/i.test(header) && value && Number.isNaN(Date.parse(String(value)))) return [`Row ${rowIndex + 1}: ${header} is not a valid date`];
      if (/hours|amount|pay|rate|total/i.test(header) && value && !Number.isFinite(Number(String(value).replace(/[$,%]/g, '')))) return [`Row ${rowIndex + 1}: ${header} is not numeric`];
      if (/email/i.test(header) && value && !String(value).includes('@')) return [`Row ${rowIndex + 1}: ${header} is not a valid email`];
      return [];
    })
  ));
  return {
    detectedType: type,
    missingColumns,
    extraColumns,
    invalidTypes: invalidTypes.slice(0, 6),
    duplicateKeys: findRepeatedValues(datasetPreview(dataset).map((row) => String(row.employeeId ?? row.employee_id ?? row.id ?? '').trim()).filter(Boolean)),
    recommendedAction: moduleName === 'hr' && type.includes('CRM') ? 'Cancel upload or send to CRM workspace' : missingColumns.length ? 'Map columns before merge' : 'Append rows or merge into existing dataset'
  };
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

function normalizeEditableRow(row: Record<string, string>) {
  return Object.fromEntries(Object.entries(row ?? {}).map(([key, value]) => [key, String(value ?? '')]));
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
  const warnings = [
    missingValues ? `${missingValues} missing values found in preview.` : '',
    invalidTypes ? `${invalidTypes} invalid column type values found.` : '',
    failedRows.size ? `${failedRows.size} failed preview rows detected.` : ''
  ].filter(Boolean);
  return { missingValues, invalidTypes, failedRows: failedRows.size, warnings };
}

function validationWarningCount(dataset: Dataset) {
  return asArray(summarizeValidation(dataset).warnings).length;
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

function findRepeatedValues(values: string[]) {
  const seen = new Set<string>();
  const duplicates = new Set<string>();
  values.forEach((value) => {
    if (seen.has(value)) duplicates.add(value);
    seen.add(value);
  });
  return [...duplicates];
}

function normalizePreviewRow(row: Record<string, string>) {
  return Object.fromEntries(Object.entries(asObject(row)).map(([key, value]) => [standardizeColumnName(key), normalizePreviewValue(String(value ?? ''))]));
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
  if (mode === 'export' || mode === 'approval' || mode === 'cleanup' || mode === 'compare' || mode === 'edit' || mode === 'query') return 'completed';
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
  setReports: Dispatch<SetStateAction<ReportHistoryItem[]>>;
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
                reports={props.reports}
                route={route}
                selectedCompanyId={props.selectedCompanyId}
                setActiveDataset={props.setActiveDataset}
                setCleanupJobs={props.setCleanupJobs}
                setDatasets={props.setDatasets}
                setReports={props.setReports}
                setSelectedCompanyId={props.setSelectedCompanyId}
                user={props.user}
                uploadDataset={props.uploadDataset}
              />
            </WorkspaceErrorBoundary>
          )}
          key={route.path}
          path={route.path}
        />
      ))}
      <Route
        path="/hr/attendance"
        element={(
          <WorkspaceErrorBoundary label="Timesheets">
            <ModuleWorkspacePage
              apiFetch={props.apiFetch}
              archiveDatasetRecord={props.archiveDatasetRecord}
              companies={props.companies}
              datasets={props.datasets}
              deleteDatasetRecord={props.deleteDatasetRecord}
              deletingDatasetId={props.deletingDatasetId}
              downloadDatasetExport={props.downloadDatasetExport}
              reports={props.reports}
              route={workspaceRoutes.find((route) => route.path === '/hr/timesheets') ?? { title: 'Timesheets', copy: 'Manage daily entries, weekly approvals, project hours, PTO, overtime, and payroll-ready exports.', type: 'timesheets', path: '/hr/timesheets', module: 'hr', moduleLabel: 'HR & Workforce' }}
              selectedCompanyId={props.selectedCompanyId}
              setActiveDataset={props.setActiveDataset}
              setCleanupJobs={props.setCleanupJobs}
              setDatasets={props.setDatasets}
              setReports={props.setReports}
              setSelectedCompanyId={props.setSelectedCompanyId}
              user={props.user}
              uploadDataset={props.uploadDataset}
            />
          </WorkspaceErrorBoundary>
        )}
      />
      <Route
        path="/hr/leave"
        element={(
          <WorkspaceErrorBoundary label="Leave Management">
            <ModuleWorkspacePage
              apiFetch={props.apiFetch}
              archiveDatasetRecord={props.archiveDatasetRecord}
              companies={props.companies}
              datasets={props.datasets}
              deleteDatasetRecord={props.deleteDatasetRecord}
              deletingDatasetId={props.deletingDatasetId}
              downloadDatasetExport={props.downloadDatasetExport}
              reports={props.reports}
              route={workspaceRoutes.find((route) => route.path === '/hr/leave-management') ?? { title: 'Leave Management', copy: 'Manage leave requests, approvals, and team coverage.', type: 'leave', path: '/hr/leave', module: 'hr', moduleLabel: 'HR & Workforce' }}
              selectedCompanyId={props.selectedCompanyId}
              setActiveDataset={props.setActiveDataset}
              setCleanupJobs={props.setCleanupJobs}
              setDatasets={props.setDatasets}
              setReports={props.setReports}
              setSelectedCompanyId={props.setSelectedCompanyId}
              user={props.user}
              uploadDataset={props.uploadDataset}
            />
          </WorkspaceErrorBoundary>
        )}
      />
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
              reports={props.reports}
              route={dataProcessingWorkspaceRoute}
              selectedCompanyId={props.selectedCompanyId}
              setActiveDataset={props.setActiveDataset}
              setCleanupJobs={props.setCleanupJobs}
              setDatasets={props.setDatasets}
              setReports={props.setReports}
              setSelectedCompanyId={props.setSelectedCompanyId}
              user={props.user}
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
  reports,
  route,
  selectedCompanyId,
  setActiveDataset,
  setCleanupJobs,
  setDatasets,
  setReports,
  setSelectedCompanyId,
  user,
  uploadDataset
}: {
  apiFetch: (path: string, options?: RequestInit) => Promise<Response>;
  archiveDatasetRecord: (dataset: Dataset | null) => void;
  companies: Company[];
  datasets: Dataset[];
  deleteDatasetRecord: (dataset: Dataset | null) => void;
  deletingDatasetId: string;
  downloadDatasetExport: (dataset: Dataset | null) => void;
  reports: ReportHistoryItem[];
  route: WorkspaceRoute;
  selectedCompanyId: string;
  setActiveDataset: (dataset: Dataset | null) => void;
  setCleanupJobs: Dispatch<SetStateAction<CleanupJob[]>>;
  setDatasets: Dispatch<SetStateAction<Dataset[]>>;
  setReports: Dispatch<SetStateAction<ReportHistoryItem[]>>;
  setSelectedCompanyId: (companyId: string) => void;
  user: User | null;
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
  const [updateMode, setUpdateMode] = useState('new_dataset');
  const [updateTargetDatasetId, setUpdateTargetDatasetId] = useState('');
  const [uploadAnalysis, setUploadAnalysis] = useState<ReturnType<typeof analyzeUploadedDataset> | null>(null);
  const [workspaceDatasets, setWorkspaceDatasets] = useState<Dataset[]>([]);
  const [generatingReportId, setGeneratingReportId] = useState('');
  const [openActionDatasetId, setOpenActionDatasetId] = useState('');
  const moduleDatasets = useMemo(() => {
    const moduleLabel = moduleLabelForRoute(route.module);
    const merged = [...asArray(workspaceDatasets), ...safeDatasets];
    const unique = new Map<string, Dataset>();
    merged.forEach((dataset) => {
      const belongsToModule = isDataProcessingWorkspace || classifyDatasetModule(dataset) === moduleLabel;
      if (dataset?.id && belongsToModule && (!selectedCompanyId || dataset.companyId === selectedCompanyId) && !unique.has(dataset.id)) {
        unique.set(dataset.id, dataset);
      }
    });
    return [...unique.values()].sort((a, b) => new Date(b.uploadedAt).getTime() - new Date(a.uploadedAt).getTime());
  }, [isDataProcessingWorkspace, route.module, safeDatasets, selectedCompanyId, workspaceDatasets]);
  const activeStageIndex = Math.max(workflowStages.indexOf(workflowStage), 0);
  const selectedCompany = safeCompanies.find((company) => company.id === selectedCompanyId) ?? safeCompanies[0] ?? null;
  const selectedCompanyName = selectedCompany?.name ?? 'No company selected';
  const safeReports = asArray(reports);

  useEffect(() => {
    setWorkflowStage(workflowStages[0] ?? 'Upload');
    setWorkflowMessage(pipelineConfig.emptyState);
    setExpandedDatasetId('');
    setStageDetail('');
  }, [pipelineConfig.emptyState, route.module, route.path, workflowStages]);

  useEffect(() => {
    setWorkspaceDatasets((current) => current.filter((dataset) => !safeDatasets.some((entry) => entry.id === dataset.id)));
  }, [safeDatasets]);

  useEffect(() => {
    const closeMenus = () => setOpenActionDatasetId('');
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setOpenActionDatasetId('');
    };
    document.addEventListener('click', closeMenus);
    document.addEventListener('keydown', closeOnEscape);
    return () => {
      document.removeEventListener('click', closeMenus);
      document.removeEventListener('keydown', closeOnEscape);
    };
  }, []);

  useEffect(() => {
    setOpenActionDatasetId('');
  }, [toolPanel, route.path, previewState?.mode]);

  async function loadRecords() {
    setLoading(true);
    setError('');
    try {
      const response = await apiFetch(`/api/modules/${route.module}/records`);
      const payload = await readJson<{ records?: ModuleRecord[]; error?: string }>(response);
      if (!response.ok) {
        throw new Error(payload.error || 'Could not load records.');
      }
      setRecords((payload.records ?? []).filter((record) => route.module === 'hr' || record.recordType === route.type));
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
      const analysis = analyzeUploadedDataset(dataset, route.module);
      setUploadAnalysis(analysis);
      setUploadProgress(100);
      setWorkflowStage(workflowStages[1] ?? workflowStages[0]);
      const updateContext = updateMode === 'new_dataset'
        ? 'created as a new dataset'
        : `${updateMode.replaceAll('_', ' ')} queued against ${moduleDatasets.find((item) => item.id === updateTargetDatasetId)?.fileName ?? 'selected dataset'}`;
      setWorkflowMessage(`${dataset.fileName} uploaded to ${selectedCompanyName}, ${updateContext}. Detected ${analysis.detectedType}; recommendation: ${analysis.recommendedAction}.`);
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
      const cleanedDataset = normalizeDatasetForClient(payload.cleanedDataset as Dataset);
      const originalDataset = normalizeDatasetForClient(payload.originalDataset as Dataset);
      setDatasets((current) => [
        cleanedDataset,
        originalDataset,
        ...current.filter((entry) => entry.id !== payload.cleanedDataset?.id && entry.id !== payload.originalDataset?.id)
      ]);
      setCleanupJobs((current) => payload.job ? [payload.job, ...current.filter((job) => job.id !== payload.job?.id)] : current);
      setActiveDataset(cleanedDataset);
      setWorkflowStage(workflowStages.find((stage) => /approval|approve/i.test(stage)) ?? workflowStages.at(-2) ?? workflowStages[0]);
      setWorkflowMessage(`Cleanup completed. ${payload.job?.metrics?.totalCleanedRows ?? cleanedDataset.rows} rows ready for approval.`);
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

  async function saveDatasetRows(dataset: Dataset, records: Record<string, string>[]) {
    const headers = getPreviewHeaders(dataset, records);
    const response = await apiFetch(`/api/datasets/${dataset.id}/records`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        headers,
        records,
        status: dataset.cleanupStatus ?? dataset.status ?? 'uploaded',
        pipelineStatus: dataset.cleanupStatus ?? dataset.pipelineStatus ?? 'uploaded'
      })
    });
    const payload = await readJson<{ dataset?: Dataset; error?: string }>(response);
    if (!response.ok || !payload.dataset) {
      throw new Error(payload.error || 'Dataset rows could not be saved.');
    }
    const normalized = normalizeDatasetForClient(payload.dataset);
    setDatasets((current) => [normalized, ...current.filter((entry) => entry.id !== normalized.id)]);
    setWorkspaceDatasets((current) => [normalized, ...current.filter((entry) => entry.id !== normalized.id)]);
    setActiveDataset(normalized);
    setExpandedDatasetId(normalized.id);
    setPreviewState({ dataset: normalized, mode: 'edit' });
    setWorkflowMessage(`${normalized.fileName} rows saved and synced to the company dataset registry.`);
  }

  async function approveDataset(dataset: Dataset) {
    const localApprovedDataset = { ...dataset, cleanupStatus: 'completed', pipelineStatus: 'completed', status: 'completed' };
    setDatasets((current) => [localApprovedDataset, ...current.filter((entry) => entry.id !== dataset.id)]);
    setWorkspaceDatasets((current) => [localApprovedDataset, ...current.filter((entry) => entry.id !== dataset.id)]);
    setActiveDataset(localApprovedDataset);
    setExpandedDatasetId(dataset.id);
    setWorkflowStage(workflowStages.find((stage) => /export/i.test(stage)) ?? workflowStages.at(-1) ?? workflowStages[0]);
    setWorkflowMessage(`${dataset.fileName} approved. Dataset status is COMPLETED and ready for export.`);
    setPreviewState({ dataset: localApprovedDataset, mode: 'approval' });
    try {
      const response = await apiFetch(`/api/datasets/${dataset.id}/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: 'completed' })
      });
      const payload = await readJson<{ dataset?: Dataset; error?: string }>(response);
      if (!response.ok || !payload.dataset) {
        throw new Error(payload.error || 'Approval status could not be saved.');
      }
      const approvedDataset = normalizeDatasetForClient(payload.dataset);
      setDatasets((current) => [approvedDataset, ...current.filter((entry) => entry.id !== approvedDataset.id)]);
      setWorkspaceDatasets((current) => [approvedDataset, ...current.filter((entry) => entry.id !== approvedDataset.id)]);
      setActiveDataset(approvedDataset);
      setPreviewState({ dataset: approvedDataset, mode: 'approval' });
    } catch (approvalError) {
      const message = approvalError instanceof Error ? approvalError.message : 'Approval status could not be saved.';
      setWorkflowMessage(`${dataset.fileName} approved locally. ${message}`);
    }
  }

  async function updateDatasetWorkflowStatus(dataset: Dataset, status: string, message: string, mode: PreviewMode) {
    const stagedDataset = { ...dataset, cleanupStatus: status, pipelineStatus: status, status };
    setDatasets((current) => [stagedDataset, ...current.filter((entry) => entry.id !== dataset.id)]);
    setWorkspaceDatasets((current) => [stagedDataset, ...current.filter((entry) => entry.id !== dataset.id)]);
    setActiveDataset(stagedDataset);
    setPreviewState({ dataset: stagedDataset, mode });
    setWorkflowMessage(message);
    try {
      const response = await apiFetch(`/api/datasets/${dataset.id}/status`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status })
      });
      const payload = await readJson<{ dataset?: Dataset; error?: string }>(response);
      if (response.ok && payload.dataset) {
        const normalized = normalizeDatasetForClient(payload.dataset);
        setDatasets((current) => [normalized, ...current.filter((entry) => entry.id !== normalized.id)]);
        setWorkspaceDatasets((current) => [normalized, ...current.filter((entry) => entry.id !== normalized.id)]);
        setPreviewState({ dataset: normalized, mode });
      }
    } catch {
      setWorkflowMessage(`${message} Status is staged locally and will resync on the next successful API request.`);
    }
  }

  function reportsForDataset(dataset: Dataset) {
    return safeReports.filter((report) => report.datasetId === dataset.id);
  }

  function latestReportForDataset(dataset: Dataset) {
    return reportsForDataset(dataset)[0] ?? null;
  }

  async function generateDatasetReport(dataset: Dataset) {
    setGeneratingReportId(dataset.id);
    setError('');
    setWorkflowStage(workflowStages.find((stage) => /quality|approval|export/i.test(stage)) ?? workflowStages.at(-2) ?? workflowStages[0]);
    setWorkflowMessage(`Generating executive, quality, and audit reports for ${dataset.fileName}...`);
    try {
      const response = await apiFetch(`/api/datasets/${dataset.id}/reports/generate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ trigger: 'workspace_action' })
      });
      const payload = await readJson<{ reports?: ReportHistoryItem[]; error?: string }>(response);
      if (!response.ok || !payload.reports?.length) {
        throw new Error(payload.error || 'Report generation failed.');
      }
      setReports((current) => [
        ...asArray(payload.reports),
        ...current.filter((report) => !asArray(payload.reports).some((next) => next.id === report.id))
      ]);
      setWorkflowMessage(`Generated ${payload.reports.length} linked BI reports for ${dataset.fileName}.`);
      setToolPanel('reports');
    } catch (reportError) {
      const message = reportError instanceof Error ? reportError.message : 'Report generation failed.';
      setWorkflowMessage(message);
      setError(message);
    } finally {
      setGeneratingReportId('');
    }
  }

  function exportReportPdf(dataset: Dataset) {
    const report = latestReportForDataset(dataset);
    const lines = report?.content?.lines ?? buildReportLines(dataset);
    downloadPdf(lines, `${dataset.fileName.replace(/\.(csv|xlsx|xls|json)$/i, '')}-executive-report.pdf`);
    setWorkflowMessage(`PDF report exported for ${dataset.fileName}.`);
  }

  function exportReportExcel(dataset: Dataset) {
    const report = latestReportForDataset(dataset);
    const metrics = report?.content?.metrics ?? {};
    const rows = [
      ['Metric', 'Value'],
      ['Dataset', dataset.fileName],
      ['Rows', String(dataset.rows)],
      ['Columns', String(dataset.columns)],
      ...Object.entries(metrics).map(([key, value]) => [key, String(value)]),
      ['', ''],
      ['Recommendation', 'Summary'],
      ...(report?.content?.recommendations ?? datasetInsights(dataset)).map((item) => ['Recommendation', item])
    ];
    downloadText(rows.map((row) => row.map(csvEscape).join(',')).join('\n'), `${dataset.fileName.replace(/\.(csv|xlsx|xls|json)$/i, '')}-bi-report.csv`, 'text/csv');
    setWorkflowMessage(`Excel-compatible report export created for ${dataset.fileName}.`);
  }

  function sendDatasetReport(dataset: Dataset) {
    setWorkflowMessage(`Report send queued for ${dataset.fileName}. Email delivery will use workspace notification settings when SMTP is configured.`);
  }

  function scheduleDatasetReport(dataset: Dataset) {
    setWorkflowMessage(`Scheduled weekly executive report placeholder created for ${dataset.fileName}.`);
  }

  const filteredRecords = records
    .filter((record) => !selectedCompanyId || record.companyId === selectedCompanyId)
    .filter((record) => filter === 'all' || record.status === filter)
    .filter((record) => !search.trim() || [record.title, record.status].some((value) => value.toLowerCase().includes(search.toLowerCase())));

  function runDatasetAction(dataset: Dataset, action: string) {
    setError('');
    setExpandedDatasetId(dataset.id);
    if (action === 'preview') setPreviewState({ dataset, mode: 'upload' });
    if (action === 'edit') setPreviewState({ dataset, mode: 'edit' });
    if (action === 'query') setPreviewState({ dataset, mode: 'query' });
    if (action === 'deleteRows') {
      setPreviewState({ dataset, mode: 'edit' });
      setWorkflowMessage(`${dataset.fileName} opened in row edit mode. Clear row values or remove data before saving a versioned edit.`);
    }
    if (action === 'newVersion') {
      setUploadAnalysis(analyzeUploadedDataset(dataset, route.module));
      setWorkflowMessage(`Upload a replacement file to create the next version for ${dataset.fileName}. Column mapping and merge preview are ready.`);
    }
    if (action === 'restore') setPreviewState({ dataset, mode: 'history' });
    if (action === 'validate') {
      validateDataset(dataset);
      void updateDatasetWorkflowStatus(dataset, 'validating', `${dataset.fileName} validation is running with schema, duplicate key, and type checks.`, 'validation');
    }
    if (action === 'results') setPreviewState({ dataset, mode: 'duplicates' });
    if (action === 'normalize') {
      normalizeDataset(dataset);
      void updateDatasetWorkflowStatus(dataset, 'mapping columns', `${dataset.fileName} is in column mapping and normalization review.`, 'normalization');
    }
    if (action === 'clean') void cleanDatasetFromModule(dataset);
    if (action === 'compare') setPreviewState({ dataset, mode: 'compare' });
    if (action === 'approve') void approveDataset(dataset);
    if (action === 'generateReport') void generateDatasetReport(dataset);
    if (action === 'exportPdf') exportReportPdf(dataset);
    if (action === 'exportExcel') exportReportExcel(dataset);
    if (action === 'sendReport') sendDatasetReport(dataset);
    if (action === 'scheduleReport') scheduleDatasetReport(dataset);
    if (action === 'export') void updateDatasetWorkflowStatus(dataset, 'exported', `${dataset.fileName} export preview opened and status marked exported.`, 'export');
    if (action === 'download') downloadDatasetExport(dataset);
    if (action === 'reprocess') void cleanDatasetFromModule(dataset.originalDatasetId ? safeDatasets.find((item) => item.id === dataset.originalDatasetId) ?? dataset : dataset);
    if (action === 'archive') {
      void updateDatasetWorkflowStatus(dataset, 'archived', `${dataset.fileName} archive status saved.`, 'history');
      archiveDatasetRecord(dataset);
    }
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
          onSaveRows={saveDatasetRows}
          onRestore={(dataset) => {
            setActiveDataset(dataset);
            setPreviewState({ dataset, mode: 'upload' });
            setWorkflowMessage(`${dataset.fileName} restored as active preview version.`);
          }}
        />
      )}
      {isDataProcessingWorkspace ? <article className="panel module-upload-panel">
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
        {isDataProcessingWorkspace && (
          <div className="incremental-update-panel">
            <div>
              <p className="eyebrow">Incremental dataset updates</p>
              <strong>How do you want to process this upload?</strong>
              <span>Compare before applying, append rows, merge matching records, replace, or create a version checkpoint.</span>
            </div>
            <select value={updateMode} onChange={(event) => setUpdateMode(event.target.value)}>
              <option value="new_dataset">Create new dataset</option>
              <option value="append_rows">Append rows</option>
              <option value="merge_matching_records">Merge/update matching records</option>
              <option value="replace_existing">Replace existing dataset</option>
              <option value="new_version">Upload as new version</option>
              <option value="compare_before_apply">Compare before applying</option>
              <option value="ignore_duplicate_rows">Ignore duplicate rows</option>
              <option value="replace_duplicate_rows">Replace duplicate rows</option>
            </select>
            <select value={updateTargetDatasetId} onChange={(event) => setUpdateTargetDatasetId(event.target.value)} disabled={updateMode === 'new_dataset' || !moduleDatasets.length}>
              <option value="">Select target dataset</option>
              {moduleDatasets.map((dataset) => <option key={dataset.id} value={dataset.id}>{dataset.fileName}</option>)}
            </select>
          </div>
        )}
        {uploadAnalysis && (
          <div className="dataset-merge-preview">
            <div>
              <p className="eyebrow">Smart dataset analysis</p>
              <strong>Detected dataset type: {uploadAnalysis.detectedType}</strong>
              <span>{uploadAnalysis.recommendedAction}</span>
            </div>
            <div className="dataset-detail-grid compact">
              <div><span>Missing columns</span><strong>{uploadAnalysis.missingColumns.length || 'None'}</strong></div>
              <div><span>Extra columns</span><strong>{uploadAnalysis.extraColumns.length || 'None'}</strong></div>
              <div><span>Invalid types</span><strong>{uploadAnalysis.invalidTypes.length || 'None'}</strong></div>
              <div><span>Duplicate keys</span><strong>{uploadAnalysis.duplicateKeys.length || 'None'}</strong></div>
            </div>
            <div className="workflow-history-strip">
              {[...uploadAnalysis.missingColumns.map((column) => `Missing: ${column}`), ...uploadAnalysis.invalidTypes, ...uploadAnalysis.duplicateKeys.map((key) => `Duplicate key: ${key}`)].slice(0, 8).map((item) => <span key={item}>{item}</span>)}
              {!uploadAnalysis.missingColumns.length && !uploadAnalysis.invalidTypes.length && !uploadAnalysis.duplicateKeys.length && <span>Columns are ready for append, merge, replace, or new version workflow.</span>}
            </div>
            <div className="inline-actions">
              {['Create new dataset', 'Merge into existing dataset', 'Append rows', 'Replace records', 'Ignore duplicates', 'Cancel upload'].map((action) => (
                <button className={action === 'Cancel upload' ? 'ghost-button compact danger' : 'ghost-button compact'} key={action} type="button" onClick={() => setWorkflowMessage(`${action} selected. ${uploadAnalysis.detectedType} is staged for review before persistence changes.`)}>
                  {action}
                </button>
              ))}
            </div>
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
      </article> : route.module !== 'hr' ? (
        <article className="panel module-dataset-ribbon">
          <div>
            <p className="eyebrow">{route.moduleLabel} datasets</p>
            <h2>{route.title} writes into module-owned datasets</h2>
            <span>Large uploads live in Enterprise Data Hub. This workspace uses quick actions, operational records, and synced module datasets.</span>
          </div>
          <div className="dataset-preview-strip">
            <span>{moduleDatasets.length} linked datasets</span>
            <span>{filteredRecords.length} operational records</span>
            <span>{selectedCompanyName}</span>
          </div>
        </article>
      ) : null}
      {route.module !== 'hr' && <article className="panel">
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
                      <strong>{dataset.cleanupMetrics?.anomaliesDetected ?? validationWarningCount(dataset)}</strong>
                    </div>
                  </div>
                  <EnterpriseReportPanel
                    dataset={dataset}
                    generating={generatingReportId === dataset.id}
                    onExportExcel={() => runDatasetAction(dataset, 'exportExcel')}
                    onExportPdf={() => runDatasetAction(dataset, 'exportPdf')}
                    onGenerate={() => runDatasetAction(dataset, 'generateReport')}
                    onSchedule={() => runDatasetAction(dataset, 'scheduleReport')}
                    onSend={() => runDatasetAction(dataset, 'sendReport')}
                    reports={reportsForDataset(dataset)}
                  />
                  <div className="dataset-preview-strip">
                    {getPreviewRows(dataset, 'upload').slice(0, 3).map((row, index) => (
                      <span key={`${dataset.id}-preview-${index}`}>{getPreviewHeaders(dataset).slice(0, 3).map((header) => row[header] || 'null').join(' | ')}</span>
                    ))}
                  </div>
                  <div className="workflow-history-strip">
                    {(asArray(dataset.cleanupLogs).length ? asArray(dataset.cleanupLogs) : ['Uploaded', ...qualitySignals]).slice(0, 5).map((entry) => <span key={entry}>{entry}</span>)}
                  </div>
                  <div className="dataset-row-footer">
                    <div className={`dataset-action-menu ${openActionDatasetId === dataset.id ? 'open' : ''}`} onClick={(event) => event.stopPropagation()}>
                      <button
                        className="dataset-action-trigger"
                        type="button"
                        aria-expanded={openActionDatasetId === dataset.id}
                        onClick={(event) => {
                          event.stopPropagation();
                          setOpenActionDatasetId((current) => current === dataset.id ? '' : dataset.id);
                        }}
                      >
                        {deletingDatasetId === dataset.id ? 'Deleting...' : 'Select Action'}
                      </button>
                      {openActionDatasetId === dataset.id && (
                        <div className="dataset-action-dropdown">
                          {[
                            ['preview', 'Preview Rows'],
                            ['edit', 'Edit Rows'],
                            ['query', 'Query Dataset'],
                            ['compare', 'Compare Versions'],
                            ['validate', 'Validate'],
                            ['normalize', 'Normalize'],
                            ...(!dataset.originalDatasetId ? [['clean', 'Clean']] : []),
                            ['approve', 'Approve'],
                            ['export', 'Export'],
                            ['reprocess', 'Reprocess'],
                            ['archive', 'Archive'],
                            ['delete', 'Delete'],
                            ['results', 'View Results'],
                            ['deleteRows', 'Delete Rows'],
                            ['generateReport', generatingReportId === dataset.id ? 'Generating...' : 'Generate Report'],
                            ['exportPdf', 'Export PDF'],
                            ['exportExcel', 'Export Excel'],
                            ['sendReport', 'Send Report'],
                            ['scheduleReport', 'Schedule Report'],
                            ['download', 'Download'],
                            ['newVersion', 'Upload New Version'],
                            ['restore', 'Restore Version']
                          ].map(([action, label]) => (
                            <button
                              className={action === 'delete' ? 'danger-action' : ''}
                              disabled={(action === 'delete' && deletingDatasetId === dataset.id) || (action === 'generateReport' && generatingReportId === dataset.id)}
                              key={action}
                              type="button"
                              onClick={() => {
                                setOpenActionDatasetId('');
                                runDatasetAction(dataset, action);
                              }}
                            >
                              {label}
                            </button>
                          ))}
                        </div>
                      )}
                    </div>
                    <button className="ghost-button compact" type="button" onClick={() => setPreviewState({ dataset, mode: 'history' })}>Version history</button>
                  </div>
                </div>
              )}
            </article>
          ))}
          {!moduleDatasets.length && <EmptyState title="Upload your first dataset to begin processing." copy={pipelineConfig.emptyState} />}
        </div>
      </article>}
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
            <div><span>Invalid values</span><strong>{moduleDatasets.reduce((sum, dataset) => sum + (dataset.cleanupMetrics?.invalidValuesDetected ?? validationWarningCount(dataset)), 0)}</strong></div>
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
      ) : route.module === 'hr' ? (
        <HrWorkforceWorkspace
          apiFetch={apiFetch}
          company={selectedCompany}
          datasets={moduleDatasets}
          onDatasetAction={runDatasetAction}
          onSaveDatasetRows={saveDatasetRows}
          records={filteredRecords}
          route={route}
          selectedCompanyId={selectedCompanyId}
          setError={setError}
          setDatasets={setDatasets}
          setRecords={setRecords}
          user={user}
        />
      ) : (
        <article className="panel routed-workspace">
          <ModuleBusinessDashboard
            records={filteredRecords}
            route={route}
            selectedCompany={selectedCompany}
          />
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

function EnterpriseReportPanel({
  dataset,
  generating,
  onExportExcel,
  onExportPdf,
  onGenerate,
  onSchedule,
  onSend,
  reports
}: {
  dataset: Dataset;
  generating: boolean;
  onExportExcel: () => void;
  onExportPdf: () => void;
  onGenerate: () => void;
  onSchedule: () => void;
  onSend: () => void;
  reports: ReportHistoryItem[];
}) {
  const latestReport = asArray(reports)[0] ?? null;
  const intelligence = latestReport?.content ?? buildLocalBusinessIntelligence(dataset);
  const metrics = intelligence.metrics ?? {};
  const aiInsights = asArray(intelligence.aiInsights);
  const recommendations = asArray(intelligence.recommendations);
  const executiveSummary = asArray(intelligence.executiveSummary);
  const approvalStatus = String(intelligence.approvalStatus ?? (Number(metrics.qualityScore ?? 0) >= 85 ? 'approved_ready' : 'waiting_approval'));

  return (
    <section className="enterprise-report-panel">
      <div className="report-panel-header">
        <div>
          <p className="eyebrow">Enterprise BI layer</p>
          <h3>Executive report intelligence</h3>
          <span>{reports.length ? `${reports.length} linked report${reports.length === 1 ? '' : 's'}` : 'Auto-generate quality, audit, and executive summaries'}</span>
        </div>
        <span className={`approval-status ${approvalStatus}`}>{approvalStatus.replace('_', ' ')}</span>
      </div>
      <div className="report-kpi-grid">
        <div><span>Total rows</span><strong>{displayNumber(Number(metrics.rowCount ?? dataset.rows))}</strong></div>
        <div><span>Quality score</span><strong>{Number(metrics.qualityScore ?? dataset.qualityScore ?? 0)}%</strong></div>
        <div><span>Anomaly score</span><strong>{Number(metrics.anomalyScore ?? 0)}</strong></div>
        <div><span>Duplicates</span><strong>{Number(metrics.duplicates ?? findDuplicateRows(datasetPreview(dataset)).length)}</strong></div>
        <div><span>Failed rows</span><strong>{Number(metrics.failedRows ?? dataset.cleanupMetrics?.failedRows ?? 0)}</strong></div>
      </div>
      <div className="report-intelligence-grid">
        <article>
          <h4>Executive summary</h4>
          {(executiveSummary.length ? executiveSummary : datasetInsights(dataset)).slice(0, 3).map((item) => <p key={item}>{item}</p>)}
        </article>
        <article>
          <h4>AI insight panel</h4>
          {(aiInsights.length ? aiInsights : buildLocalBusinessIntelligence(dataset).aiInsights).slice(0, 4).map((insight) => (
            <div className="ai-insight-chip" key={insight.title}>
              <strong>{insight.title}</strong>
              <span>{insight.summary}</span>
            </div>
          ))}
        </article>
        <article>
          <h4>Operational recommendations</h4>
          {(recommendations.length ? recommendations : ['Generate a linked report to unlock workflow recommendations.']).slice(0, 4).map((item) => <p key={item}>{item}</p>)}
        </article>
      </div>
      <div className="report-action-row">
        <button type="button" onClick={onGenerate} disabled={generating}>{generating ? 'Generating...' : 'Generate Report'}</button>
        <button className="ghost-button compact" type="button" onClick={onExportPdf}>Export PDF</button>
        <button className="ghost-button compact" type="button" onClick={onExportExcel}>Export Excel</button>
        <button className="ghost-button compact" type="button" onClick={onSend}>Send Report</button>
        <button className="ghost-button compact" type="button" onClick={onSchedule}>Schedule Report</button>
      </div>
      {latestReport && <p className="persistence-note">Latest report: {latestReport.title} - {new Date(latestReport.createdAt).toLocaleString()}</p>}
    </section>
  );
}

function ModuleBusinessDashboard({ records, route, selectedCompany }: { records: ModuleRecord[]; route: WorkspaceRoute; selectedCompany: Company | null }) {
  const [calculatorInput, setCalculatorInput] = useState('1000');
  const moduleFeatures: Record<string, string[]> = {
    hr: ['Employee directory', 'Paystubs', 'Timesheets', 'PTO and benefits', 'Onboarding', 'Org chart', 'Performance reviews', 'HR reports'],
    accounting: ['Invoices', 'Expenses', 'Vendor payments', 'Payroll accounting', 'Taxes', 'Budgets', 'Reconciliation', 'Financial reports'],
    engineering: ['Projects', 'Milestones', 'Tasks', 'Dependencies', 'Resource planning', 'Schedules', 'Work orders', 'Project reports'],
    crm: ['Customer profiles', 'Leads', 'Opportunities', 'Contracts', 'Communication history', 'Support tickets', 'Follow-up reminders', 'Sales reports']
  };
  const aiExamples: Record<string, string[]> = {
    hr: ['Missing employee IDs', 'Duplicate employees', 'Payroll inconsistencies', 'Missing paystubs', 'Overtime risk', 'Turnover risk'],
    accounting: ['Duplicate invoices', 'Negative invoice amounts', 'Missing PO', 'Late payments', 'Tax risk', 'Unusual expenses'],
    engineering: ['Project delay risk', 'Resource conflict', 'Missing owner', 'Schedule overlap', 'Dependency conflict', 'Milestone risk'],
    crm: ['Sales forecast', 'Customer churn risk', 'Stalled opportunities', 'Missing follow-ups', 'Contract risk', 'Revenue scoring']
  };
  const amount = Number(calculatorInput || 0);
  const calculatorRows = route.module === 'accounting'
    ? [
      ['Invoice total', amount * 1.06],
      ['Tax estimate', amount * 0.06],
      ['Payroll cost', amount * 1.18],
      ['Margin at 35%', amount * 0.35],
      ['Budget variance at 8%', amount * 0.08],
      ['Cash flow reserve', amount * 0.22]
    ]
    : [];
  return (
    <section className="module-business-dashboard">
      <div className="module-business-header">
        <div>
          <p className="eyebrow">{route.moduleLabel}</p>
          <h2>{route.title} operations</h2>
          <span>{selectedCompany?.name ?? 'Selected company'} workspace: records, approvals, reports, AI insights, and operational history.</span>
        </div>
        <strong>{records.length} records</strong>
      </div>
      <div className="module-feature-grid">
        {(moduleFeatures[route.module] ?? []).map((feature) => <span key={feature}>{feature}</span>)}
      </div>
      <div className="module-intelligence-grid">
        <article>
          <h3>AI intelligence</h3>
          {(aiExamples[route.module] ?? ['Operational summary', 'Anomaly alerts', 'Approval recommendations']).map((item) => (
            <div className="ai-insight-chip" key={item}>
              <strong>{item}</strong>
              <span>Ready for company-scoped analysis as module data grows.</span>
            </div>
          ))}
        </article>
        <article>
          <h3>Approval queue</h3>
          <p>{records.filter((record) => record.status !== 'closed').length} open items are available for manager review.</p>
          <p>Reports, approvals, pipelines, and exports live inside this module and the company dashboard.</p>
        </article>
        {route.module === 'accounting' && (
          <article>
            <h3>Finance calculators</h3>
            <input value={calculatorInput} onChange={(event) => setCalculatorInput(event.target.value)} aria-label="Calculator amount" />
            {calculatorRows.map(([label, value]) => (
              <p key={label}><strong>{label}:</strong> ${Number(value).toLocaleString(undefined, { maximumFractionDigits: 2 })}</p>
            ))}
          </article>
        )}
      </div>
    </section>
  );
}

function HrWorkforceWorkspace({
  apiFetch,
  company,
  datasets,
  onDatasetAction,
  onSaveDatasetRows,
  records,
  route,
  selectedCompanyId,
  setError,
  setDatasets,
  setRecords,
  user
}: {
  apiFetch: (path: string, options?: RequestInit) => Promise<Response>;
  company: Company | null;
  datasets: Dataset[];
  onDatasetAction: (dataset: Dataset, action: string) => void;
  onSaveDatasetRows: (dataset: Dataset, records: Record<string, string>[]) => Promise<void>;
  records: ModuleRecord[];
  route: WorkspaceRoute;
  selectedCompanyId: string;
  setError: (message: string) => void;
  setDatasets: Dispatch<SetStateAction<Dataset[]>>;
  setRecords: Dispatch<SetStateAction<ModuleRecord[]>>;
  user: User | null;
}) {
  const [employeeForm, setEmployeeForm] = useState({
    employeeId: '',
    name: '',
    email: '',
    phone: '',
    department: '',
    title: '',
    manager: '',
    hireDate: '',
    salary: '',
    employmentType: 'Full-time',
    taxDetails: '',
    benefits: '',
    notes: ''
  });
  const [selectedEmployeeId, setSelectedEmployeeId] = useState('');
  const [employeeWorkspaceId, setEmployeeWorkspaceId] = useState('');
  const [employeeTab, setEmployeeTab] = useState('Overview');
  const [savingEmployee, setSavingEmployee] = useState(false);
  const [employeeFormOpen, setEmployeeFormOpen] = useState(false);
  const [riskMonitorOpen, setRiskMonitorOpen] = useState(false);
  const [uploadCenterOpen, setUploadCenterOpen] = useState(false);
  const [aiDrawerOpen, setAiDrawerOpen] = useState(false);
  const [activityCleared, setActivityCleared] = useState(false);
  const [hrUploadType, setHrUploadType] = useState('Employee');
  const [hrUploadAction, setHrUploadAction] = useState('Create new dataset');
  const [stagedHrFile, setStagedHrFile] = useState<File | null>(null);
  const [hrUploadStep, setHrUploadStep] = useState<UploadStep>('upload');
  const [selectedHrDatasetId, setSelectedHrDatasetId] = useState('');
  const [payrollFileName, setPayrollFileName] = useState('');
  const [leaveWorkflow, setLeaveWorkflow] = useState<'pto' | 'sick' | null>(null);
  const [leaveSaving, setLeaveSaving] = useState(false);
  const [attendanceTab, setAttendanceTab] = useState(route.type === 'timesheets' ? 'Live Workforce' : 'Dashboard');
  const [attendanceDate, setAttendanceDate] = useState(() => new Date().toISOString().slice(0, 10));
  const [payPeriod, setPayPeriod] = useState(() => new Date().toISOString().slice(0, 7));
  const [timeEntryForm, setTimeEntryForm] = useState({ employeeRecordId: '', startTime: '09:00', endTime: '17:00', breakMinutes: '30', status: 'draft', shift: 'Day shift', location: 'On-site', projectCode: '', taskCode: '', workType: 'Regular', notes: '' });
  const [leaveForm, setLeaveForm] = useState({
    employeeRecordId: '',
    startDate: '',
    endDate: '',
    reason: '',
    attachmentName: '',
    manager: '',
    emergency: false
  });
  const canManageHr = user?.role === 'owner' || user?.role === 'admin' || user?.role === 'manager';
  const canViewSensitiveHr = canManageHr;
  const navigate = useNavigate();
  const employees = records.filter((record) => record.recordType === 'employee' && record.status !== 'archived');
  const archivedEmployees = records.filter((record) => record.recordType === 'employee' && record.status === 'archived');
  const payrollRecords = records.filter((record) => record.recordType === 'payroll');
  const attendanceRecords = records.filter((record) => record.recordType === 'attendance' || record.recordType === 'timesheet');
  const shiftRecords = records.filter((record) => record.recordType === 'shift');
  const leaveRequests = records.filter((record) => record.recordType === 'leave_request');
  const documents = records.filter((record) => record.recordType === 'document' || record.recordType === 'paystub');
  const selectedEmployee = employees.find((employee) => employee.id === selectedEmployeeId) ?? employees[0] ?? null;
  const workspaceEmployee = employees.find((employee) => employee.id === employeeWorkspaceId) ?? null;
  const selectedEmployeePayroll = payrollRecords.filter((record) => record.metadata?.employeeRecordId === selectedEmployee?.id || record.metadata?.employeeId === selectedEmployee?.metadata?.employeeId);
  const selectedEmployeeAttendance = attendanceRecords.filter((record) => record.metadata?.employeeRecordId === selectedEmployee?.id || record.metadata?.employeeId === selectedEmployee?.metadata?.employeeId);
  const selectedEmployeeDocs = documents.filter((record) => record.metadata?.employeeRecordId === selectedEmployee?.id || record.metadata?.employeeId === selectedEmployee?.metadata?.employeeId);
  const selectedEmployeeLeave = leaveRequests.filter((record) => record.metadata?.employeeRecordId === selectedEmployee?.id || record.metadata?.employeeId === selectedEmployee?.metadata?.employeeId);
  const workspaceEmployeePayroll = payrollRecords.filter((record) => record.metadata?.employeeRecordId === workspaceEmployee?.id || record.metadata?.employeeId === workspaceEmployee?.metadata?.employeeId);
  const workspaceEmployeeAttendance = attendanceRecords.filter((record) => record.metadata?.employeeRecordId === workspaceEmployee?.id || record.metadata?.employeeId === workspaceEmployee?.metadata?.employeeId);
  const workspaceEmployeeDocs = documents.filter((record) => record.metadata?.employeeRecordId === workspaceEmployee?.id || record.metadata?.employeeId === workspaceEmployee?.metadata?.employeeId);
  const workspaceEmployeeLeave = leaveRequests.filter((record) => record.metadata?.employeeRecordId === workspaceEmployee?.id || record.metadata?.employeeId === workspaceEmployee?.metadata?.employeeId);
  const duplicateEmployeeIds = findRepeatedValues(employees.map((employee) => String(employee.metadata?.employeeId ?? '').trim()).filter(Boolean));
  const employeesMissingIds = employees.filter((employee) => !String(employee.metadata?.employeeId ?? '').trim()).length;
  const missingPaystubs = employees.filter((employee) => !documents.some((doc) => doc.recordType === 'paystub' && (doc.metadata?.employeeRecordId === employee.id || doc.metadata?.employeeId === employee.metadata?.employeeId))).length;
  const overtimeWarnings = attendanceRecords.filter((record) => Number(record.metadata?.overtimeHours ?? 0) > 8).length;
  const pendingOnboarding = employees.filter((employee) => employee.status === 'onboarding').length;
  const ptoRequests = leaveRequests.filter((record) => record.status === 'pending_approval' && record.metadata?.leaveType === 'pto').length;
  const sickRequests = leaveRequests.filter((record) => record.status === 'pending_approval' && record.metadata?.leaveType === 'sick').length;
  const pendingHrApprovals = leaveRequests.filter((record) => record.status === 'pending_approval');
  function hrDatasetFromRows(label: string, path: string, rows: Record<string, string>[]): Dataset {
    const headers = Array.from(new Set(rows.flatMap((row) => Object.keys(row))));
    const uploadedAt = new Date(Math.max(0, ...records.map((record) => new Date(record.updatedAt).getTime())) || Date.now()).toISOString();
    return normalizeDatasetForClient({
      id: `hr-virtual-${label.toLowerCase().replace(/\s+/g, '-')}`,
      companyId: selectedCompanyId,
      fileName: `${label} Dataset`,
      fileType: 'dataset',
      uploadedAt,
      rows: rows.length,
      columns: headers.length,
      headers,
      preview: rows,
      records: rows,
      previewRows: rows,
      chartColumn: headers[0] ?? 'status',
      labelColumn: headers[0] ?? 'name',
      chart: [],
      numericSummary: [],
      insights: [`${label} workspace dataset is linked to ${path}.`],
      cleanupStatus: rows.length ? 'active' : 'empty',
      status: rows.length ? 'active' : 'empty',
      ownerName: user?.name ?? 'HR workspace',
      ownerEmail: user?.email
    } as Dataset);
  }

  const virtualHrDatasets = [
    hrDatasetFromRows('Employee', '/hr/employees', employees.map((employee) => ({
      recordId: employee.id,
      employeeId: String(employee.metadata?.employeeId ?? ''),
      name: employee.title,
      department: String(employee.metadata?.department ?? ''),
      title: String(employee.metadata?.title ?? ''),
      status: employee.status,
      manager: String(employee.metadata?.manager ?? ''),
      email: String(employee.metadata?.email ?? ''),
      phone: String(employee.metadata?.phone ?? ''),
      hireDate: String(employee.metadata?.hireDate ?? ''),
      employmentType: String(employee.metadata?.employmentType ?? '')
    }))),
    hrDatasetFromRows('Timesheet', '/hr/timesheets', attendanceRecords.map((record) => ({
      employeeId: String(record.metadata?.employeeId ?? ''),
      employeeName: String(record.metadata?.employeeName ?? record.title),
      workDate: String(record.metadata?.workDate ?? record.metadata?.date ?? ''),
      projectCode: String(record.metadata?.projectCode ?? ''),
      taskCode: String(record.metadata?.taskCode ?? ''),
      totalHours: String(record.metadata?.totalHours ?? record.metadata?.hours ?? ''),
      overtimeHours: String(record.metadata?.overtimeHours ?? ''),
      approvalStatus: String(record.metadata?.approvalStatus ?? record.status),
      payrollPeriod: String(record.metadata?.payrollPeriod ?? '')
    }))),
    hrDatasetFromRows('Payroll', '/hr/payroll', payrollRecords.map((record) => ({
      employeeId: String(record.metadata?.employeeId ?? ''),
      employeeName: String(record.metadata?.employeeName ?? record.title),
      payrollPeriod: String(record.metadata?.payrollPeriod ?? ''),
      fileName: String(record.metadata?.fileName ?? ''),
      status: record.status,
      amount: String(record.amount ?? '')
    }))),
    hrDatasetFromRows('PTO', '/hr/leave-management', leaveRequests.filter((record) => record.metadata?.leaveType === 'pto').map((record) => ({
      employeeId: String(record.metadata?.employeeId ?? ''),
      employeeName: String(record.metadata?.employeeName ?? record.title),
      startDate: String(record.metadata?.startDate ?? ''),
      endDate: String(record.metadata?.endDate ?? ''),
      requestedHours: String(record.metadata?.requestedHours ?? ''),
      manager: String(record.metadata?.manager ?? ''),
      status: record.status
    }))),
    hrDatasetFromRows('Sick Leave', '/hr/leave-management', leaveRequests.filter((record) => record.metadata?.leaveType === 'sick').map((record) => ({
      employeeId: String(record.metadata?.employeeId ?? ''),
      employeeName: String(record.metadata?.employeeName ?? record.title),
      startDate: String(record.metadata?.startDate ?? ''),
      endDate: String(record.metadata?.endDate ?? ''),
      requestedHours: String(record.metadata?.requestedHours ?? ''),
      manager: String(record.metadata?.manager ?? ''),
      status: record.status
    }))),
    hrDatasetFromRows('Benefits', '/hr/datasets', documents.map((record) => ({
      employeeId: String(record.metadata?.employeeId ?? ''),
      employeeName: String(record.metadata?.employeeName ?? record.title),
      documentType: record.recordType,
      fileName: String(record.metadata?.fileName ?? record.title),
      status: record.status
    }))),
    hrDatasetFromRows('Hiring', '/hr/datasets', records.filter((record) => record.recordType === 'hiring').map((record) => ({
      candidate: record.title,
      status: record.status,
      owner: String(record.ownerEmail ?? ''),
      updatedAt: record.updatedAt
    }))),
    hrDatasetFromRows('Performance', '/hr/datasets', records.filter((record) => record.recordType === 'performance').map((record) => ({
      employeeName: record.title,
      status: record.status,
      notes: String(record.metadata?.notes ?? ''),
      updatedAt: record.updatedAt
    })))
  ];
  function hrDatasetKind(dataset: Dataset) {
    const name = dataset.fileName.toLowerCase();
    if (name.includes('timesheet') || name.includes('attendance')) return 'Timesheet';
    if (name.includes('payroll') || name.includes('paystub')) return 'Payroll';
    if (name.includes('pto')) return 'PTO';
    if (name.includes('sick')) return 'Sick Leave';
    if (name.includes('benefit') || name.includes('document')) return 'Benefits';
    if (name.includes('hiring')) return 'Hiring';
    if (name.includes('performance')) return 'Performance';
    return 'Employee';
  }
  const realHrDatasets = asArray(datasets)
    .filter((dataset) => (!selectedCompanyId || dataset.companyId === selectedCompanyId) && classifyDatasetModule(dataset) === 'HR & Workforce');
  const hrDatasets = virtualHrDatasets.map((virtualDataset) => {
    const realDataset = realHrDatasets.find((dataset) => hrDatasetKind(dataset) === hrDatasetKind(virtualDataset));
    if (!realDataset) return virtualDataset;
    const virtualRows = datasetPreview(virtualDataset);
    return realDataset.rows || !virtualRows.length ? realDataset : { ...virtualDataset, id: realDataset.id, fileName: realDataset.fileName };
  });
  const selectedHrDataset = hrDatasets.find((dataset) => dataset.id === selectedHrDatasetId) ?? hrDatasets[0] ?? null;
  const employeeDataset = hrDatasets.find((dataset) => /employee/i.test(dataset.fileName) || datasetHeaders(dataset).some((header) => /employee/i.test(header))) ?? selectedHrDataset;
  const hrDatasetSummaries: HrDatasetSummary[] = hrDatasets.map((dataset) => ({
    id: dataset.id,
    fileName: dataset.fileName,
    type: hrDatasetKind(dataset),
    rows: dataset.rows,
    status: dataset.cleanupStatus ?? dataset.status ?? 'draft',
    updatedAt: dataset.uploadedAt
  }));
  const activeHrWorkspace = route.type === 'timesheets' ? 'timesheets'
    : route.type === 'employee' ? 'employees'
      : route.type === 'leave' ? 'pto'
        : route.type === 'hiring' ? 'datasets'
          : route.type === 'payroll' ? 'payroll'
            : route.type === 'reports' ? 'reports'
              : route.type === 'approvals' ? 'approvals'
                : route.type === 'ai_insights' ? 'ai'
                  : route.type === 'datasets' ? 'datasets'
                    : 'overview';
  const activeEmployeeDataset = activeHrWorkspace === 'employees' ? employeeDataset : selectedHrDataset;
  const activeDatasetRows = activeEmployeeDataset ? datasetPreview(activeEmployeeDataset).map((row) => normalizeEditableRow(row)) : [];
  const hrWorkspaceTabs = [
    { key: 'overview', label: 'Overview', path: '/hr' },
    { key: 'employees', label: 'Employees', path: '/hr/employees' },
    { key: 'timesheets', label: 'Timesheets', path: '/hr/timesheets' },
    { key: 'payroll', label: 'Payroll', path: '/hr/payroll' },
    { key: 'pto', label: 'PTO', path: '/hr/leave-management' },
    { key: 'reports', label: 'Reports', path: '/hr/reports' },
    { key: 'datasets', label: 'Datasets', path: '/hr/datasets' },
    { key: 'approvals', label: 'Approvals', path: '/hr/approvals' },
    { key: 'ai', label: 'AI Insights', path: '/hr/ai-insights' }
  ];

  useEffect(() => {
    if (route.type === 'timesheets') setAttendanceTab('Live Workforce');
    if (route.type === 'leave') setAttendanceTab('Leave Calendar');
  }, [route.type]);

  useEffect(() => {
    if (!selectedHrDatasetId && hrDatasets[0]) setSelectedHrDatasetId(hrDatasets[0].id);
    if (selectedHrDatasetId && !hrDatasets.some((dataset) => dataset.id === selectedHrDatasetId)) setSelectedHrDatasetId(hrDatasets[0]?.id ?? '');
  }, [hrDatasets, selectedHrDatasetId]);

  async function createHrRecord(recordType: string, title: string, status: string, metadata: Record<string, unknown>, amount?: number | null) {
    const response = await apiFetch('/api/modules/hr/records', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ title, status, recordType, companyId: selectedCompanyId, amount, metadata })
    });
    const payload = await readJson<{ record?: ModuleRecord; dataset?: Dataset; error?: string }>(response);
    if (!response.ok || !payload.record) throw new Error(payload.error || 'HR record could not be saved.');
    setRecords((current) => [payload.record as ModuleRecord, ...current]);
    if (payload.dataset) setDatasets((current) => [normalizeDatasetForClient(payload.dataset as Dataset), ...current.filter((dataset) => dataset.id !== payload.dataset?.id)]);
    return payload.record;
  }

  async function updateHrRecord(record: ModuleRecord, updates: Partial<ModuleRecord>) {
    const response = await apiFetch(`/api/modules/hr/records/${record.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(updates)
    });
    const payload = await readJson<{ record?: ModuleRecord; dataset?: Dataset; error?: string }>(response);
    if (!response.ok || !payload.record) throw new Error(payload.error || 'HR record update failed.');
    setRecords((current) => current.map((entry) => entry.id === record.id ? payload.record as ModuleRecord : entry));
    if (payload.dataset) setDatasets((current) => [normalizeDatasetForClient(payload.dataset as Dataset), ...current.filter((dataset) => dataset.id !== payload.dataset?.id)]);
    return payload.record;
  }

  async function saveHrDatasetRows(dataset: Dataset, rows: Record<string, string>[]) {
    if (!dataset.id.startsWith('hr-virtual-')) {
      await onSaveDatasetRows(dataset, rows);
      return;
    }
    const kind = hrDatasetKind(dataset);
    if (kind !== 'Employee') {
      setError(`${kind} rows are editable after upload/publish. Use the ${kind} workspace actions to add new operational records.`);
      return;
    }
    const identityForRow = (row: Record<string, string>) => String(row.recordId || row.employeeId || row.employee_id || row.email || row.name || row.employeeName || '').toLowerCase().trim();
    const nextIdentities = rows.map(identityForRow).filter(Boolean);
    const duplicateEmployeeIds = findRepeatedValues(rows.map((row) => String(row.employeeId || row.employee_id || '').toLowerCase().trim()).filter(Boolean));
    const duplicateEmails = findRepeatedValues(rows.map((row) => String(row.email || '').toLowerCase().trim()).filter(Boolean));
    const duplicateIdentities = [...duplicateEmployeeIds, ...duplicateEmails];
    if (duplicateIdentities.length) {
      setError(`Employee Dataset blocked duplicate keys: ${duplicateIdentities.slice(0, 3).join(', ')}. Merge or clear duplicate employeeId/email values before saving.`);
      return;
    }
    const employeeIdentity = (employee: ModuleRecord) => String(employee.id || employee.metadata?.employeeId || employee.metadata?.email || employee.title || '').toLowerCase().trim();
    const activeEmployeeIdentities = new Set(nextIdentities);
    const employeesToArchive = employees.filter((employee) => {
      const key = employeeIdentity(employee);
      return key && !activeEmployeeIdentities.has(key);
    });
    for (let index = 0; index < rows.length; index += 1) {
      const row = rows[index];
      const key = identityForRow(row);
      const existing = key ? employees.find((employee) => employeeIdentity(employee) === key) : null;
      const metadata = {
        employeeId: row.employeeId ?? row.employee_id ?? '',
        email: row.email ?? '',
        phone: row.phone ?? '',
        department: row.department ?? '',
        title: row.title ?? '',
        manager: row.manager ?? '',
        hireDate: row.hireDate ?? '',
        employmentType: row.employmentType ?? ''
      };
      if (existing) {
        await updateHrRecord(existing, {
          title: row.name || row.employeeName || existing.title,
          status: row.status || existing.status,
          metadata: { ...(existing.metadata ?? {}), ...metadata }
        });
      } else if (row.name || row.employeeName || row.employeeId) {
        await createHrRecord('employee', row.name || row.employeeName || row.employeeId || 'New employee', row.status || 'active', metadata);
      }
    }
    for (const employee of employeesToArchive) {
      await updateHrRecord(employee, {
        status: 'archived',
        metadata: { ...(employee.metadata ?? {}), archivedAt: new Date().toISOString(), archiveReason: 'Removed from Employee Dataset grid' }
      });
    }
  }

  function runHrDatasetAction(dataset: Dataset, action: string) {
    setSelectedHrDatasetId(dataset.id);
    const kind = hrDatasetKind(dataset);
    const virtual = dataset.id.startsWith('hr-virtual-');
    if (virtual) {
      if (action === 'open' || action === 'preview' || action === 'edit' || action === 'query') {
        navigate(kind === 'Timesheet' ? '/hr/timesheets' : kind === 'Payroll' ? '/hr/payroll' : kind === 'PTO' || kind === 'Sick Leave' ? '/hr/leave-management' : kind === 'Employee' ? '/hr/employees' : '/hr/datasets');
        return;
      }
      if (action === 'clean' || action === 'archive' || action === 'delete') {
        setError(`${kind} Dataset is generated from HR records. Clean or archive the underlying records in the linked workspace.`);
        return;
      }
      if (action === 'approve') {
        setError(`${kind} Dataset has no pending import. Upload or edit rows before approval.`);
        return;
      }
    }
    onDatasetAction(dataset, action);
  }

  async function saveEmployee(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!employeeForm.name.trim()) {
      setError('Employee name is required.');
      return;
    }
    setSavingEmployee(true);
    setError('');
    try {
      await createHrRecord('employee', employeeForm.name.trim(), employeeForm.hireDate ? 'active' : 'onboarding', {
        employeeId: employeeForm.employeeId.trim(),
        email: employeeForm.email.trim(),
        phone: employeeForm.phone.trim(),
        department: employeeForm.department.trim(),
        title: employeeForm.title.trim(),
        manager: employeeForm.manager.trim(),
        hireDate: employeeForm.hireDate,
        salary: Number(employeeForm.salary || 0),
        employmentType: employeeForm.employmentType,
        taxDetails: employeeForm.taxDetails.trim(),
        benefits: employeeForm.benefits.trim(),
        notes: employeeForm.notes.trim()
      }, Number(employeeForm.salary || 0));
      setEmployeeForm({
        employeeId: '',
        name: '',
        email: '',
        phone: '',
        department: '',
        title: '',
        manager: '',
        hireDate: '',
        salary: '',
        employmentType: 'Full-time',
        taxDetails: '',
        benefits: '',
        notes: ''
      });
      setEmployeeFormOpen(false);
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Employee could not be saved.');
    } finally {
      setSavingEmployee(false);
    }
  }

  async function editEmployee(employee: ModuleRecord) {
    setSelectedEmployeeId(employee.id);
    setEmployeeWorkspaceId(employee.id);
    setEmployeeTab('Overview');
    setError('Use the inline employee dataset grid to edit employee values.');
  }

  async function archiveEmployee(employee: ModuleRecord) {
    if (!window.confirm(`Archive ${employee.title}?`)) return;
    try {
      await updateHrRecord(employee, { status: 'archived', metadata: { ...(employee.metadata ?? {}), archivedAt: new Date().toISOString() } });
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Employee archive failed.');
    }
  }

  async function addAttendance(type: 'timesheet' | 'pto_requested' | 'sick_leave') {
    if (!selectedEmployee) return;
    try {
      await createHrRecord('timesheet', `${selectedEmployee.title} ${type.replace('_', ' ')}`, type,
      {
        employeeRecordId: selectedEmployee.id,
        employeeId: selectedEmployee.metadata?.employeeId,
        employeeName: selectedEmployee.title,
        date: new Date().toISOString().slice(0, 10),
        workDate: new Date().toISOString().slice(0, 10),
        hours: type === 'timesheet' ? 8 : 0,
        totalHours: type === 'timesheet' ? 8 : 0,
        overtimeHours: type === 'timesheet' ? 0 : 0,
        approvalStatus: 'draft',
        payrollPeriod: payPeriod,
        note: type === 'pto_requested' ? 'PTO request awaiting manager review' : type === 'sick_leave' ? 'Sick leave logged' : 'Timesheet logged'
      });
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Timesheet record failed.');
    }
  }

  function openLeaveWorkflow(type: 'pto' | 'sick') {
    const employee = selectedEmployee ?? employees[0] ?? null;
    setLeaveWorkflow(type);
    setLeaveForm({
      employeeRecordId: employee?.id ?? '',
      startDate: '',
      endDate: '',
      reason: '',
      attachmentName: '',
      manager: String(employee?.metadata?.manager ?? ''),
      emergency: false
    });
  }

  function calculateLeaveHours(startDate: string, endDate: string) {
    if (!startDate || !endDate) return 0;
    const start = new Date(`${startDate}T00:00:00`);
    const end = new Date(`${endDate}T00:00:00`);
    if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime()) || end < start) return 0;
    return (Math.floor((end.getTime() - start.getTime()) / 86400000) + 1) * 8;
  }

  function employeeLeaveBalance(employee: ModuleRecord | null, type: 'pto' | 'sick') {
    const defaultHours = type === 'pto' ? 120 : 40;
    const key = type === 'pto' ? 'ptoBalanceHours' : 'sickBalanceHours';
    const startingBalance = Number(employee?.metadata?.[key] ?? defaultHours);
    const used = leaveRequests
      .filter((request) => request.status === 'approved' && request.metadata?.employeeRecordId === employee?.id && request.metadata?.leaveType === type)
      .reduce((sum, request) => sum + Number(request.metadata?.requestedHours ?? 0), 0);
    return Math.max(startingBalance - used, 0);
  }

  async function submitLeaveWorkflow(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!leaveWorkflow) return;
    const employee = employees.find((entry) => entry.id === leaveForm.employeeRecordId) ?? null;
    if (!employee) {
      setError('Select an employee before submitting leave.');
      return;
    }
    const requestedHours = calculateLeaveHours(leaveForm.startDate, leaveForm.endDate);
    if (!requestedHours) {
      setError('Select a valid start and end date for the leave request.');
      return;
    }
    setLeaveSaving(true);
    setError('');
    try {
      const manager = leaveForm.manager.trim() || String(employee.metadata?.manager ?? 'Manager review queue');
      const remainingBefore = employeeLeaveBalance(employee, leaveWorkflow);
      await createHrRecord(
        'leave_request',
        `${employee.title} ${leaveWorkflow === 'pto' ? 'PTO' : 'sick leave'} request`,
        'pending_approval',
        {
          leaveType: leaveWorkflow,
          employeeRecordId: employee.id,
          employeeId: employee.metadata?.employeeId,
          employeeName: employee.title,
          startDate: leaveForm.startDate,
          endDate: leaveForm.endDate,
          reason: leaveForm.reason.trim(),
          attachmentName: leaveForm.attachmentName,
          manager,
          approvalChain: ['Employee submitted', `${manager} approval`, 'HR operations archive'],
          emergency: leaveForm.emergency,
          requestedHours,
          remainingBalanceBefore: remainingBefore,
          remainingBalanceAfter: remainingBefore - requestedHours,
          submittedAt: new Date().toISOString(),
          approvalHistory: [
            { action: 'submitted', actor: user?.name ?? user?.email ?? 'Employee', at: new Date().toISOString(), comment: leaveForm.reason.trim() }
          ],
          notification: `${manager} notified for ${leaveWorkflow === 'pto' ? 'PTO' : 'sick leave'} approval.`
        }
      );
      setLeaveWorkflow(null);
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Leave request could not be submitted.');
    } finally {
      setLeaveSaving(false);
    }
  }

  async function decideLeaveRequest(request: ModuleRecord, decision: 'approved' | 'rejected') {
    const comment = decision === 'approved' ? 'Approved from HR Approval Workspace.' : 'Rejected from HR Approval Workspace.';
    try {
      await updateHrRecord(request, {
        status: decision,
        metadata: {
          ...(request.metadata ?? {}),
          decidedAt: new Date().toISOString(),
          decidedBy: user?.name ?? user?.email ?? 'Manager',
          managerComment: comment,
          approvalHistory: [
            ...asArray(request.metadata?.approvalHistory),
            { action: decision, actor: user?.name ?? user?.email ?? 'Manager', at: new Date().toISOString(), comment }
          ]
        }
      });
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Leave approval update failed.');
    }
  }

  function calculateWorkedHours(startTime: string, endTime: string, breakMinutes: string | number) {
    if (!startTime || !endTime) return 0;
    const [startHour, startMinute] = startTime.split(':').map(Number);
    const [endHour, endMinute] = endTime.split(':').map(Number);
    const start = startHour * 60 + startMinute;
    const end = endHour * 60 + endMinute;
    const minutes = Math.max(end - start - Number(breakMinutes || 0), 0);
    return Math.round((minutes / 60) * 100) / 100;
  }

  function attendanceForEmployee(employee: ModuleRecord) {
    const employeeId = employee.metadata?.employeeId;
    return attendanceRecords.filter((record) => record.metadata?.employeeRecordId === employee.id || record.metadata?.employeeId === employeeId);
  }

  function weeklyHoursForEmployee(employee: ModuleRecord) {
    const weekPrefix = attendanceDate.slice(0, 8);
    return attendanceForEmployee(employee)
      .filter((record) => String(record.metadata?.date ?? '').startsWith(weekPrefix))
      .reduce((sum, record) => sum + Number(record.metadata?.hours ?? 0), 0);
  }

  async function clockEmployee(employee: ModuleRecord, action: 'in' | 'out') {
    try {
      const now = new Date();
      if (action === 'in') {
        await createHrRecord('timesheet', `${employee.title} clock in`, 'in progress', {
          employeeRecordId: employee.id,
          employeeId: employee.metadata?.employeeId,
          employeeName: employee.title,
          date: now.toISOString().slice(0, 10),
          workDate: now.toISOString().slice(0, 10),
          clockIn: now.toISOString(),
          shift: 'Live shift',
          location: 'On-site',
          approvalStatus: 'in progress',
          payrollPeriod: payPeriod,
          workType: 'Regular',
          lateArrival: now.getHours() > 9 || (now.getHours() === 9 && now.getMinutes() > 10),
          missingPunch: false,
          payrollReady: false
        });
        return;
      }
      const openShift = attendanceForEmployee(employee).find((record) => record.status === 'in progress' || record.status === 'clocked_in');
      if (!openShift) {
        setError('No open clock-in record was found for this employee.');
        return;
      }
      const start = new Date(String(openShift.metadata?.clockIn ?? now.toISOString()));
      const hours = Math.max(Math.round(((now.getTime() - start.getTime()) / 3600000) * 100) / 100, 0);
      await updateHrRecord(openShift, {
        status: 'draft',
        metadata: {
          ...(openShift.metadata ?? {}),
          clockOut: now.toISOString(),
          hours,
          totalHours: hours,
          overtimeHours: Math.max(hours - 8, 0),
          missingPunch: false,
          payrollReady: true,
          approvalStatus: hours > 10 ? 'pending approval' : 'draft'
        }
      });
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Clock action failed.');
    }
  }

  async function saveTimeEntry(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const employee = employees.find((entry) => entry.id === timeEntryForm.employeeRecordId) ?? selectedEmployee;
    if (!employee) {
      setError('Select an employee for the time entry.');
      return;
    }
    const hours = calculateWorkedHours(timeEntryForm.startTime, timeEntryForm.endTime, timeEntryForm.breakMinutes);
    const duplicateTimesheet = attendanceRecords.some((record) => {
      if (record.status === 'archived') return false;
      return String(record.metadata?.employeeRecordId ?? '') === employee.id
        && String(record.metadata?.workDate ?? record.metadata?.date ?? '') === attendanceDate
        && String(record.metadata?.startTime ?? '') === timeEntryForm.startTime
        && String(record.metadata?.endTime ?? '') === timeEntryForm.endTime;
    });
    if (duplicateTimesheet) {
      setError('Timesheet duplicate blocked: this employee already has the same work date and shift time.');
      return;
    }
    try {
      await createHrRecord('timesheet', `${employee.title} ${attendanceDate} timesheet entry`, timeEntryForm.status, {
        employeeRecordId: employee.id,
        employeeId: employee.metadata?.employeeId,
        employeeName: employee.title,
        date: attendanceDate,
        workDate: attendanceDate,
        startTime: timeEntryForm.startTime,
        endTime: timeEntryForm.endTime,
        breakMinutes: Number(timeEntryForm.breakMinutes || 0),
        hours,
        totalHours: hours,
        overtimeHours: Math.max(hours - 8, 0),
        status: timeEntryForm.status,
        shift: timeEntryForm.shift,
        location: timeEntryForm.location,
        projectCode: timeEntryForm.projectCode,
        taskCode: timeEntryForm.taskCode,
        workType: timeEntryForm.workType,
        notes: timeEntryForm.notes,
        approvalStatus: timeEntryForm.status === 'submitted' ? 'pending approval' : timeEntryForm.status,
        missingPunch: !timeEntryForm.startTime || !timeEntryForm.endTime,
        payrollReady: false,
        payrollPeriod: payPeriod,
        payPeriod
      });
      setTimeEntryForm((current) => ({ ...current, employeeRecordId: employee.id }));
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Time entry could not be saved.');
    }
  }

  async function assignShift(employee: ModuleRecord) {
    const shift = 'Day shift 9:00 AM - 5:00 PM';
    try {
      await createHrRecord('shift', `${employee.title} ${shift}`, 'scheduled', {
        employeeRecordId: employee.id,
        employeeId: employee.metadata?.employeeId,
        employeeName: employee.title,
        date: attendanceDate,
        shift,
        recurring: 'weekly',
        location: 'On-site',
        department: employee.metadata?.department,
        approvalStatus: 'scheduled'
      });
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Shift could not be assigned.');
    }
  }

  async function submitWeeklyTimesheet(employee: ModuleRecord) {
    const weeklyRecords = attendanceForEmployee(employee).filter((record) => String(record.metadata?.payPeriod ?? record.metadata?.payrollPeriod ?? payPeriod) === payPeriod || String(record.metadata?.date ?? '').startsWith(payPeriod));
    const totalHours = weeklyRecords.reduce((sum, record) => sum + Number(record.metadata?.hours ?? record.metadata?.totalHours ?? 0), 0);
    try {
      await createHrRecord('timesheet', `${employee.title} weekly timesheet ${payPeriod}`, 'submitted', {
        employeeRecordId: employee.id,
        employeeId: employee.metadata?.employeeId,
        employeeName: employee.title,
        department: employee.metadata?.department,
        manager: employee.metadata?.manager,
        date: attendanceDate,
        workDate: attendanceDate,
        hours: totalHours,
        totalHours,
        overtimeHours: Math.max(totalHours - 40, 0),
        PTOHours: leaveRequests.filter((record) => record.status === 'approved' && record.metadata?.employeeRecordId === employee.id && record.metadata?.leaveType === 'pto').reduce((sum, record) => sum + Number(record.metadata?.requestedHours ?? 0), 0),
        sickLeaveHours: leaveRequests.filter((record) => record.status === 'approved' && record.metadata?.employeeRecordId === employee.id && record.metadata?.leaveType === 'sick').reduce((sum, record) => sum + Number(record.metadata?.requestedHours ?? 0), 0),
        payrollPeriod: payPeriod,
        approvalStatus: 'pending approval',
        submittedAt: new Date().toISOString(),
        notes: `Submitted ${weeklyRecords.length} time entries for manager review.`,
        sourceEntryIds: weeklyRecords.map((record) => record.id)
      });
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Timesheet submission failed.');
    }
  }

  async function decideTimesheet(record: ModuleRecord, decision: 'approved' | 'rejected') {
    const comment = decision === 'approved' ? 'Approved for payroll.' : 'Correction requested from Timesheet Workspace.';
    try {
      await updateHrRecord(record, {
        status: decision === 'approved' ? 'approved' : 'rejected',
        metadata: {
          ...(record.metadata ?? {}),
          approvalStatus: decision,
          payrollReady: decision === 'approved',
          managerComment: comment,
          decidedAt: new Date().toISOString(),
          decidedBy: user?.name ?? user?.email ?? 'Manager'
        }
      });
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Timesheet approval update failed.');
    }
  }

  async function editTimesheet(record: ModuleRecord) {
    const workDate = String(record.metadata?.workDate ?? record.metadata?.date ?? attendanceDate);
    const projectCode = String(record.metadata?.projectCode ?? 'MetroCare Migration');
    const taskCode = String(record.metadata?.taskCode ?? 'API Integration');
    const startTime = String(record.metadata?.startTime ?? '09:00');
    const endTime = String(record.metadata?.endTime ?? '17:00');
    const totalHours = Number(record.metadata?.totalHours ?? record.metadata?.hours ?? 8);
    const overtimeHours = Number(record.metadata?.overtimeHours ?? Math.max(totalHours - 8, 0));
    const notes = String(record.metadata?.notes ?? record.metadata?.note ?? 'Inline dataset edit requested.');
    try {
      await updateHrRecord(record, {
        status: 'draft',
        metadata: {
          ...(record.metadata ?? {}),
          workDate,
          date: workDate,
          projectCode,
          taskCode,
          startTime,
          endTime,
          totalHours,
          hours: totalHours,
          overtimeHours,
          notes,
          approvalStatus: 'draft',
          payrollReady: false
        }
      });
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Timesheet edit failed.');
    }
  }

  async function deleteTimesheet(record: ModuleRecord) {
    if (!window.confirm(`Delete ${record.title}?`)) return;
    try {
      await updateHrRecord(record, {
        status: 'archived',
        metadata: { ...(record.metadata ?? {}), archivedAt: new Date().toISOString(), approvalStatus: 'archived' }
      });
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Timesheet delete failed.');
    }
  }

  async function resubmitTimesheet(record: ModuleRecord) {
    try {
      await updateHrRecord(record, {
        status: 'submitted',
        metadata: { ...(record.metadata ?? {}), approvalStatus: 'pending approval', submittedAt: new Date().toISOString(), payrollReady: false }
      });
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Timesheet resubmit failed.');
    }
  }

  function openEmployeeTimesheets() {
    const employee = selectedEmployee ?? employees[0] ?? null;
    if (!employee) {
      setEmployeeFormOpen(true);
      return;
    }
    setSelectedEmployeeId(employee.id);
    setEmployeeWorkspaceId(employee.id);
    setEmployeeTab('Timesheets');
  }

  function exportPayrollTimesheet() {
    const rows = [
      ['Employee ID', 'Employee Name', 'Regular Hours', 'Overtime Hours', 'PTO Hours', 'Sick Leave Hours', 'Unpaid Hours', 'Gross Pay Estimate', 'Deductions', 'Tax Placeholder', 'Approval Status', 'Pay Period'],
      ...employees.map((employee) => {
        const employeeAttendance = attendanceForEmployee(employee);
        const regular = employeeAttendance.reduce((sum, record) => sum + Math.min(Number(record.metadata?.hours ?? 0), 8), 0);
        const overtime = employeeAttendance.reduce((sum, record) => sum + Number(record.metadata?.overtimeHours ?? 0), 0);
        const pto = leaveRequests.filter((record) => record.status === 'approved' && record.metadata?.employeeRecordId === employee.id && record.metadata?.leaveType === 'pto').reduce((sum, record) => sum + Number(record.metadata?.requestedHours ?? 0), 0);
        const sick = leaveRequests.filter((record) => record.status === 'approved' && record.metadata?.employeeRecordId === employee.id && record.metadata?.leaveType === 'sick').reduce((sum, record) => sum + Number(record.metadata?.requestedHours ?? 0), 0);
        const rate = Number(employee.metadata?.salary ?? employee.amount ?? 0) / 2080;
        return [
          String(employee.metadata?.employeeId ?? ''),
          employee.title,
          regular.toFixed(2),
          overtime.toFixed(2),
          pto.toFixed(2),
          sick.toFixed(2),
          '0',
          (Math.max(rate, 0) * (regular + overtime * 1.5)).toFixed(2),
          'TODO payroll deductions',
          'TODO tax withholding',
          employeeAttendance.some((record) => record.metadata?.approvalStatus !== 'approved') ? 'needs_review' : 'approved',
          payPeriod
        ];
      })
    ];
    downloadText(rows.map((row) => row.map(csvEscape).join(',')).join('\n'), `payroll-timesheet-${payPeriod}.csv`, 'text/csv');
  }

  async function uploadPayroll(file: File, attachAsPaystub = false) {
    setPayrollFileName(file.name);
    try {
      const inferredEmployee = selectedEmployee ?? employees.find((employee) => {
        const employeeId = String(employee.metadata?.employeeId ?? '').toLowerCase();
        return employeeId && file.name.toLowerCase().includes(employeeId);
      }) ?? null;
      const duplicatePayrollEntries = payrollRecords.filter((record) => record.metadata?.fileName === file.name).length;
      const missingEmployeeDetection = attachAsPaystub && !inferredEmployee ? 1 : 0;
      const datasetTypeToRecord: Record<string, string> = {
        'Employee': 'employee',
        'Employee dataset': 'employee',
        'Payroll': 'payroll',
        'Payroll dataset': 'payroll',
        'Timesheet': 'timesheet',
        'Timesheet dataset': 'timesheet',
        'PTO': 'leave_request',
        'PTO dataset': 'leave_request',
        'Sick leave': 'leave_request',
        'Sick leave dataset': 'leave_request',
        'Hiring': 'hiring',
        'Hiring dataset': 'hiring',
        'Performance': 'performance',
        'Performance dataset': 'performance',
        'Benefits': 'document',
        'Benefits dataset': 'document',
        'Compliance': 'document',
        'Compliance dataset': 'document'
      };
      const recordType = attachAsPaystub ? 'paystub' : datasetTypeToRecord[hrUploadType] ?? 'payroll';
      const title = attachAsPaystub && inferredEmployee ? `${inferredEmployee.title} paystub - ${file.name}` : `${hrUploadType} import - ${file.name}`;
      await createHrRecord(recordType, title, 'pending_validation', {
        employeeRecordId: attachAsPaystub ? inferredEmployee?.id : null,
        employeeId: attachAsPaystub ? inferredEmployee?.metadata?.employeeId : null,
        employeeName: inferredEmployee?.title ?? null,
        attachedEmployeeName: inferredEmployee?.title ?? null,
        fileName: file.name,
        fileType: file.name.split('.').pop()?.toLowerCase(),
        uploadedAt: new Date().toISOString(),
        hrUploadType: attachAsPaystub ? 'paystub' : hrUploadType,
        datasetAction: hrUploadAction,
        targetDataset: hrUploadType,
        mergePreview: {
          matchedRows: inferredEmployee ? 1 : 0,
          missingRequiredFields: attachAsPaystub && !inferredEmployee ? ['employeeId'] : [],
          duplicateEmployees: duplicateEmployeeIds.length,
          invalidColumns: [],
          unmappedColumns: [],
          overwriteWarnings: hrUploadAction.includes('Replace') ? ['Existing rows may be overwritten after approval.'] : []
        },
        validation: {
          duplicatePayrollDetection: duplicatePayrollEntries,
          missingEmployeeDetection,
          missingHoursDetection: attendanceRecords.filter((record) => Number(record.metadata?.hours ?? 0) <= 0).length,
          overtimeWarnings
        }
      });
    } catch (error) {
      setError(error instanceof Error ? error.message : 'Payroll upload failed.');
    }
  }

  function stageHrUpload(file: File | null) {
    if (!file) {
      setStagedHrFile(null);
      setPayrollFileName('');
      setHrUploadStep('upload');
      return;
    }
    setStagedHrFile(file);
    setPayrollFileName(file.name);
    setHrUploadStep('preview');
  }

  function advanceHrUploadStep(nextStep: typeof hrUploadStep) {
    if (!stagedHrFile && nextStep !== 'upload') {
      setError('Choose a file before continuing the HR dataset workflow.');
      return;
    }
    setHrUploadStep(nextStep);
  }

  async function publishHrUpload(attachAsPaystub = false) {
    if (!stagedHrFile) {
      setError('Choose a file before publishing the HR dataset workflow.');
      return;
    }
    await uploadPayroll(stagedHrFile, attachAsPaystub);
    setHrUploadStep('publish');
    setStagedHrFile(null);
  }

  function exportHrReport(kind: string) {
    const lines = [
      `Metenova AI HR ${kind} Report`,
      `Company: ${company?.name ?? 'Selected company'}`,
      `Employees: ${employees.length}`,
      `Active: ${employees.filter((employee) => employee.status === 'active').length}`,
      `Pending onboarding: ${pendingOnboarding}`,
      `Payroll alerts: ${duplicateEmployeeIds.length + missingPaystubs + overtimeWarnings}`,
      `Missing paystubs: ${missingPaystubs}`,
      `PTO requests: ${ptoRequests}`,
      `Sick leave requests: ${sickRequests}`,
      `Pending approvals: ${pendingHrApprovals.length}`,
      '',
      'AI insights',
      ...hrInsightItems().map((item) => `- ${item}`)
    ];
    downloadPdf(lines, `hr-${kind.toLowerCase().replace(/\s+/g, '-')}-report.pdf`);
  }

  function exportHrExcel(kind: string) {
    const rows = [
      ['Employee ID', 'Name', 'Department', 'Title', 'Status', 'Manager', 'Salary'],
      ...employees.map((employee) => [
        String(employee.metadata?.employeeId ?? ''),
        employee.title,
        String(employee.metadata?.department ?? ''),
        String(employee.metadata?.title ?? ''),
        employee.status,
        String(employee.metadata?.manager ?? ''),
        String(employee.metadata?.salary ?? employee.amount ?? '')
      ])
    ];
    downloadText(rows.map((row) => row.map(csvEscape).join(',')).join('\n'), `hr-${kind.toLowerCase().replace(/\s+/g, '-')}.csv`, 'text/csv');
  }

  function hrInsightItems() {
    return [
      employeesMissingIds ? `${employeesMissingIds} employees are missing employee IDs.` : 'Employee ID coverage is healthy.',
      duplicateEmployeeIds.length ? `${duplicateEmployeeIds.length} duplicate employee IDs detected.` : 'No duplicate employee IDs detected.',
      overtimeWarnings ? `${overtimeWarnings} overtime spike warnings need review.` : 'No overtime spikes detected.',
      missingPaystubs ? `${missingPaystubs} employees are missing paystubs.` : 'Paystub coverage looks complete.',
      ptoRequests ? `${ptoRequests} PTO requests need manager review.` : 'No PTO conflicts are currently pending.',
      sickRequests ? `${sickRequests} sick leave requests need coverage review.` : 'No sick leave requests are pending.',
      pendingHrApprovals.length ? `${pendingHrApprovals.length} HR approvals are waiting on a manager.` : 'Approval queues are clear.',
      pendingOnboarding > 3 ? 'Turnover/onboarding risk is elevated due to pending starts.' : 'Turnover risk is stable.'
    ];
  }

  return (
    <article className="panel routed-workspace hr-workforce-workspace">
      <HRWorkspaceFrame
        activeKey={activeHrWorkspace}
        companyName={company?.name ?? 'Selected company'}
        onAi={() => setAiDrawerOpen(true)}
        onNavigate={navigate}
        tabs={hrWorkspaceTabs}
      />

      <DatasetToolbar
        canManage={canManageHr}
        onAddEmployee={() => { setEmployeeFormOpen(true); navigate('/hr/employees'); }}
        onApprove={() => activeEmployeeDataset && runHrDatasetAction(activeEmployeeDataset, 'approve')}
        onBulk={() => navigate('/hr/datasets')}
        onEdit={() => activeEmployeeDataset && runHrDatasetAction(activeEmployeeDataset, 'edit')}
        onExport={exportPayrollTimesheet}
        onReports={() => navigate('/hr/reports')}
        onUpload={() => { setUploadCenterOpen(true); navigate('/hr/datasets'); }}
      />

      {activeHrWorkspace === 'overview' && <div className="hr-dashboard-grid">
        {[
          ['Total employees', employees.length],
          ['Active employees', employees.filter((employee) => employee.status === 'active').length],
          ['Pending onboarding', pendingOnboarding],
          ['Payroll alerts', duplicateEmployeeIds.length + missingPaystubs + overtimeWarnings],
          ['Timesheet rows', attendanceRecords.length],
          ['Missing paystubs', missingPaystubs],
          ['PTO requests', ptoRequests],
          ['Sick leave', sickRequests],
          ['Pending approvals', pendingHrApprovals.length],
          ['HR AI insights', hrInsightItems().filter((item) => !/healthy|No |stable|complete/i.test(item)).length]
        ].map(([label, value]) => (
          <div key={label}>
            <span>{label}</span>
            <strong>{value}</strong>
          </div>
        ))}
      </div>}

      {activeHrWorkspace === 'overview' && (
        <section className="hr-focused-card">
          <div className="panel-header">
            <div>
              <p className="eyebrow">All HR datasets</p>
              <h2>Dataset command cards</h2>
              <span>Every HR workspace has its own dataset. Click a card to open and edit the associated workspace.</span>
            </div>
          </div>
          <div className="hr-dataset-card-grid">
            {hrDatasets.map((dataset) => {
              const path = datasetInsights(dataset)[0]?.match(/linked to ([^.]+)/)?.[1] ?? '/hr/datasets';
              return (
                <button className="hr-dataset-command-card" key={dataset.id} type="button" onClick={() => { setSelectedHrDatasetId(dataset.id); navigate(path); }}>
                  <span className={`status-pill ${String(dataset.cleanupStatus ?? dataset.status ?? 'empty').replace(/\s+/g, '-')}`}>{dataset.cleanupStatus ?? dataset.status ?? 'empty'}</span>
                  <strong>{hrDatasetKind(dataset)} Dataset</strong>
                  <small>{dataset.rows.toLocaleString()} rows | {dataset.columns.toLocaleString()} columns</small>
                  <em>{path.replace('/hr/', '').replace('-', ' ') || 'overview'} workspace</em>
                </button>
              );
            })}
          </div>
        </section>
      )}

      {activeHrWorkspace === 'overview' && (
        <section className="hr-focused-card">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Live activity</p>
              <h2>One-card action center</h2>
              <span>Counts by HR area with direct fix paths for missing or risky records.</span>
            </div>
            <button className="ghost-button compact" type="button" onClick={() => setActivityCleared(true)}>Clear activity</button>
          </div>
          <div className="hr-live-activity-grid">
            {[
              ['Employees missing IDs', employeesMissingIds, '/hr/employees'],
              ['Pending PTO', ptoRequests, '/hr/leave-management'],
              ['Pending sick leave', sickRequests, '/hr/leave-management'],
              ['Overtime warnings', overtimeWarnings, '/hr/timesheets'],
              ['Missing paystubs', missingPaystubs, '/hr/payroll'],
              ['Pending approvals', pendingHrApprovals.length, '/hr/approvals']
            ].map(([label, count, path]) => {
              const visibleCount = activityCleared ? 0 : Number(count);
              return (
              <button key={String(label)} type="button" onClick={() => navigate(String(path))}>
                <strong>{visibleCount}</strong>
                <span>{label}</span>
                <small>{visibleCount ? 'Open workspace to fix' : 'Healthy'}</small>
              </button>
              );
            })}
          </div>
        </section>
      )}

      {activeHrWorkspace === 'employees' && <section className="hr-operations-grid focused-hr-workspace">
        {canManageHr ? <section className={`hr-quick-create ${employeeFormOpen ? 'open' : ''}`}>
          <div className="quick-create-bar">
            <div>
              <p className="eyebrow">Employee Records Dataset</p>
              <h2>Employee management</h2>
              <span>Create employees directly into the company HR dataset.</span>
            </div>
            <button type="button" onClick={() => setEmployeeFormOpen((open) => !open)}>{employeeFormOpen ? 'Collapse' : '+ Add Employee'}</button>
          </div>
          {employeeFormOpen && (
            <form className="hr-employee-form" onSubmit={saveEmployee}>
              <input placeholder="Employee ID" value={employeeForm.employeeId} onChange={(event) => setEmployeeForm((current) => ({ ...current, employeeId: event.target.value }))} />
              <input placeholder="Full name" value={employeeForm.name} onChange={(event) => setEmployeeForm((current) => ({ ...current, name: event.target.value }))} />
              <input placeholder="Email" value={employeeForm.email} onChange={(event) => setEmployeeForm((current) => ({ ...current, email: event.target.value }))} />
              <input placeholder="Phone" value={employeeForm.phone} onChange={(event) => setEmployeeForm((current) => ({ ...current, phone: event.target.value }))} />
              <input placeholder="Department" value={employeeForm.department} onChange={(event) => setEmployeeForm((current) => ({ ...current, department: event.target.value }))} />
              <input placeholder="Role / title" value={employeeForm.title} onChange={(event) => setEmployeeForm((current) => ({ ...current, title: event.target.value }))} />
              <input placeholder="Manager" value={employeeForm.manager} onChange={(event) => setEmployeeForm((current) => ({ ...current, manager: event.target.value }))} />
              <input type="date" value={employeeForm.hireDate} onChange={(event) => setEmployeeForm((current) => ({ ...current, hireDate: event.target.value }))} />
              <input placeholder="Salary / pay rate" value={employeeForm.salary} onChange={(event) => setEmployeeForm((current) => ({ ...current, salary: event.target.value }))} />
              <select value={employeeForm.employmentType} onChange={(event) => setEmployeeForm((current) => ({ ...current, employmentType: event.target.value }))}>
                <option>Full-time</option>
                <option>Part-time</option>
                <option>Contractor</option>
                <option>Seasonal</option>
              </select>
              <input placeholder="Tax details" value={employeeForm.taxDetails} onChange={(event) => setEmployeeForm((current) => ({ ...current, taxDetails: event.target.value }))} />
              <input placeholder="Benefits" value={employeeForm.benefits} onChange={(event) => setEmployeeForm((current) => ({ ...current, benefits: event.target.value }))} />
              <textarea placeholder="Employee notes" value={employeeForm.notes} onChange={(event) => setEmployeeForm((current) => ({ ...current, notes: event.target.value }))} />
              <button type="submit" disabled={savingEmployee}>{savingEmployee ? 'Saving...' : 'Create employee'}</button>
            </form>
          )}
        </section> : (
          <section className="hr-ai-panel">
            <div>
              <p className="eyebrow">Directory access</p>
              <h2>Employee directory</h2>
            </div>
            <p className="muted">Your role can view the company directory and submit assigned HR workflows. Payroll, salary, tax, benefits, and private employee notes are restricted.</p>
          </section>
        )}

        <section className={`hr-ai-panel collapsible-risk-panel ${riskMonitorOpen ? 'open' : ''}`}>
          <button className="quick-create-bar risk-monitor-trigger" type="button" onClick={() => setAiDrawerOpen(true)}>
            <div>
              <p className="eyebrow">HR AI insights</p>
              <h2>Workforce risk monitor</h2>
              <span>{hrInsightItems().filter((item) => !/healthy|No |stable|complete|clear/i.test(item)).length} active signals. Open the AI drawer for recommendations.</span>
            </div>
            <strong>Open AI Insights</strong>
          </button>
        </section>
      </section>}
      {uploadCenterOpen && activeHrWorkspace === 'datasets' && (
        <UploadCenter
          action={hrUploadAction}
          datasetType={hrUploadType}
          disabledPublish={!stagedHrFile || hrUploadStep !== 'process'}
          fileName={stagedHrFile?.name}
          onActionChange={setHrUploadAction}
          onDatasetTypeChange={setHrUploadType}
          onFile={stageHrUpload}
          onPublish={() => void publishHrUpload(false)}
          onStep={advanceHrUploadStep}
          step={hrUploadStep}
        />
      )}
      {payrollFileName && <p className="persistence-note">Latest payroll/paystub file attached: {payrollFileName}</p>}

      {activeHrWorkspace === 'timesheets' && (
        <AttendanceOperationsWorkspace
          assignShift={assignShift}
          attendanceDate={attendanceDate}
          attendanceRecords={attendanceRecords}
          attendanceTab={attendanceTab}
          canManage={canManageHr}
          clockEmployee={clockEmployee}
          employees={employees}
          exportPayrollTimesheet={exportPayrollTimesheet}
          leaveRequests={leaveRequests}
          payPeriod={payPeriod}
          saveTimeEntry={saveTimeEntry}
          setAttendanceDate={setAttendanceDate}
          setAttendanceTab={setAttendanceTab}
          setPayPeriod={setPayPeriod}
          setTimeEntryForm={setTimeEntryForm}
          shiftRecords={shiftRecords}
          submitWeeklyTimesheet={submitWeeklyTimesheet}
          timeEntryForm={timeEntryForm}
          updateTimesheetDecision={decideTimesheet}
          weeklyHoursForEmployee={weeklyHoursForEmployee}
        />
      )}

      {activeHrWorkspace === 'payroll' && (
        <section className="hr-focused-card">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Payroll Workspace</p>
              <h2>Payroll preparation and paystub review</h2>
              <span>Review payroll records, paystubs, missing hours, overtime exceptions, and payroll-ready exports.</span>
            </div>
            <button type="button" onClick={exportPayrollTimesheet}>Export payroll-ready CSV</button>
          </div>
          <div className="hr-dashboard-grid compact-attendance-grid">
            <div><span>Payroll records</span><strong>{payrollRecords.length}</strong></div>
            <div><span>Paystubs/docs</span><strong>{documents.length}</strong></div>
            <div><span>Missing paystubs</span><strong>{missingPaystubs}</strong></div>
            <div><span>Overtime alerts</span><strong>{overtimeWarnings}</strong></div>
          </div>
          <EmployeeRecordList title="Payroll records" records={payrollRecords} empty="No payroll records have been created yet." />
        </section>
      )}

      {activeHrWorkspace === 'pto' && (
        <section className="hr-focused-card">
          <div className="panel-header">
            <div>
              <p className="eyebrow">PTO & Leave Workspace</p>
              <h2>Leave requests and team coverage</h2>
              <span>Submit PTO or sick leave, review approval history, and manage manager comments.</span>
            </div>
            <div className="inline-actions">
              <button type="button" onClick={() => openLeaveWorkflow('pto')} disabled={!selectedEmployee}>Request PTO</button>
              <button className="ghost-button compact" type="button" onClick={() => openLeaveWorkflow('sick')} disabled={!selectedEmployee}>Sick leave</button>
            </div>
          </div>
          <LeaveRequestList canManage={canManageHr} onDecide={decideLeaveRequest} records={leaveRequests} />
        </section>
      )}

      {activeHrWorkspace === 'reports' && (
        <section className="hr-focused-card">
          <div className="panel-header">
            <div>
              <p className="eyebrow">HR Reports Workspace</p>
              <h2>Operational reports</h2>
              <span>Generate employee, payroll, attendance, PTO, workforce analytics, PDF, and Excel-compatible exports.</span>
            </div>
          </div>
          <div className="hr-report-grid">
            {['Employee Report', 'Payroll Summary', 'Timesheet Report', 'PTO Report', 'Workforce Analytics'].map((kind) => (
              <article key={kind}>
                <strong>{kind}</strong>
                <span>{company?.name ?? 'Selected company'} - {employees.length} employees - {pendingHrApprovals.length} approvals</span>
                <div className="inline-actions">
                  <button type="button" onClick={() => exportHrReport(kind)}>Export PDF</button>
                  <button className="ghost-button compact" type="button" onClick={() => exportHrExcel(kind)}>Export Excel</button>
                </div>
              </article>
            ))}
          </div>
        </section>
      )}

      {activeHrWorkspace === 'approvals' && (
        <section className="hr-focused-card">
          <div className="panel-header">
            <div>
              <p className="eyebrow">HR Approval Workspace</p>
              <h2>Centralized approvals</h2>
              <span>Review PTO, sick leave, timesheet, payroll, and staged dataset approvals in one focused queue.</span>
            </div>
          </div>
          <LeaveRequestList canManage={canManageHr} onDecide={decideLeaveRequest} records={pendingHrApprovals} />
          <TimesheetRecordList
            canManage={canManageHr}
            employee={selectedEmployee ?? employees[0] ?? ({ id: 'empty', title: 'No employee selected', metadata: {}, status: 'draft', recordType: 'employee', module: 'hr', createdAt: new Date().toISOString(), updatedAt: new Date().toISOString() } as ModuleRecord)}
            onCreate={openEmployeeTimesheets}
            onDelete={deleteTimesheet}
            onEdit={editTimesheet}
            onResubmit={resubmitTimesheet}
            onTimesheetDecision={decideTimesheet}
            records={attendanceRecords.filter((record) => String(record.metadata?.approvalStatus ?? record.status).includes('pending'))}
          />
        </section>
      )}

      {activeHrWorkspace === 'ai' && (
        <section className="hr-focused-card hr-ai-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">HR AI Insights Workspace</p>
              <h2>Workforce risk and recommendations</h2>
              <span>AI-style operational warnings stay isolated from the employee and dataset workspaces.</span>
            </div>
          </div>
          <div className="risk-monitor-body always-open">
            {hrInsightItems().map((item) => {
              const active = !/healthy|No |stable|complete|clear/i.test(item);
              return (
                <div className={`ai-insight-chip ${active ? 'severity-warning' : 'severity-ok'}`} key={item}>
                  <strong>{active ? 'Warning' : 'Stable'}: {item}</strong>
                  <span>{active ? 'Recommendation: review the affected employee dataset and approval queue.' : 'Company-scoped HR intelligence'}</span>
                </div>
              );
            })}
          </div>
        </section>
      )}

      {activeHrWorkspace === 'datasets' && (
        <section className="hr-focused-card hr-dataset-workspace">
          <div className="panel-header">
            <div>
              <p className="eyebrow">HR Dataset Workspace</p>
              <h2>Selected dataset control</h2>
              <span>Select one dataset. Only its rows, reports, approvals, exports, and actions are shown.</span>
            </div>
            <select value={selectedHrDataset?.id ?? ''} onChange={(event) => setSelectedHrDatasetId(event.target.value)}>
              <option value="">Select dataset</option>
              {hrDatasets.map((dataset) => <option key={dataset.id} value={dataset.id}>{dataset.fileName}</option>)}
            </select>
          </div>
          <DatasetGrid
            activeDatasetId={selectedHrDataset?.id ?? ''}
            datasets={hrDatasetSummaries}
            onAction={(datasetId, action) => {
              const dataset = hrDatasets.find((entry) => entry.id === datasetId);
              if (!dataset) return;
              setSelectedHrDatasetId(dataset.id);
              runHrDatasetAction(dataset, action === 'open' ? 'open' : action);
            }}
            onOpen={setSelectedHrDatasetId}
          />
          {selectedHrDataset ? (
            <div className="selected-dataset-card">
              <div className="dataset-detail-grid">
                <div><span>Dataset</span><strong>{selectedHrDataset.fileName}</strong></div>
                <div><span>Status</span><strong className={`status-pill ${String(selectedHrDataset.cleanupStatus ?? selectedHrDataset.status ?? 'draft').replace(/\s+/g, '-')}`}>{selectedHrDataset.cleanupStatus ?? selectedHrDataset.status ?? 'draft'}</strong></div>
                <div><span>Rows</span><strong>{displayNumber(selectedHrDataset.rows)}</strong></div>
                <div><span>Columns</span><strong>{displayNumber(selectedHrDataset.columns)}</strong></div>
              </div>
              <div className="inline-actions dataset-primary-actions">
                {[
                  ['preview', 'Preview Rows'],
                  ['edit', 'Edit Rows'],
                  ['query', 'Query Dataset'],
                  ['validate', 'Validate'],
                  ['normalize', 'Normalize'],
                  ['clean', 'Clean Dataset'],
                  ['approve', 'Approve'],
                  ['export', 'Export'],
                  ['archive', 'Archive'],
                  ['delete', 'Delete']
                ].map(([action, label]) => (
                  <button className={action === 'delete' ? 'ghost-button compact danger' : 'ghost-button compact'} key={action} type="button" onClick={() => runHrDatasetAction(selectedHrDataset, action)}>
                    {label}
                  </button>
                ))}
              </div>
              <div className="table-wrap preview-table-wrap compact-dataset-table">
                <table className="preview-table">
                  <thead><tr>{getPreviewHeaders(selectedHrDataset).slice(0, 8).map((header) => <th key={header}>{header}</th>)}</tr></thead>
                  <tbody>
                    {getPreviewRows(selectedHrDataset, 'upload').slice(0, 6).map((row, index) => (
                      <tr key={index}>{getPreviewHeaders(selectedHrDataset).slice(0, 8).map((header) => <td key={header}>{String(row[header] ?? '')}</td>)}</tr>
                    ))}
                    {!getPreviewRows(selectedHrDataset, 'upload').length && <tr><td colSpan={Math.max(getPreviewHeaders(selectedHrDataset).slice(0, 8).length, 1)}>No rows are available for this selected dataset.</td></tr>}
                  </tbody>
                </table>
              </div>
            </div>
          ) : <EmptyState title="No HR datasets yet" copy="Upload or create an Employee, Payroll, Timesheet, PTO, Sick Leave, Hiring, Performance, Benefits, or Compliance dataset." />}
        </section>
      )}

      {(activeHrWorkspace === 'employees' || activeHrWorkspace === 'overview') && (
        <section className="hr-focused-card">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Active Employee Dataset</p>
              <h2>{activeEmployeeDataset?.fileName ?? 'No employee dataset selected'}</h2>
              <span>Employee rows render from activeDataset.rows only. Double click cells for inline editing.</span>
            </div>
            <select value={activeEmployeeDataset?.id ?? ''} onChange={(event) => setSelectedHrDatasetId(event.target.value)}>
              <option value="">Select employee dataset</option>
              {hrDatasets.map((dataset) => <option key={dataset.id} value={dataset.id}>{dataset.fileName}</option>)}
            </select>
          </div>
          {activeEmployeeDataset ? (
            <>
              <EmployeeGrid
                canEdit={canManageHr}
                rows={activeDatasetRows as EmployeeGridRow[]}
                onRowsChange={(rows) => {
                  void saveHrDatasetRows(activeEmployeeDataset, rows);
                }}
              />
              {archivedEmployees.length > 0 && <p className="persistence-note">{archivedEmployees.length} archived employees retained in company HR history.</p>}
            </>
          ) : (
            <EmptyState title="No employee dataset selected" copy="Create or upload an Employee dataset to begin spreadsheet-style HR operations." />
          )}
        </section>
      )}
      {aiDrawerOpen && (
        <div className="modal-backdrop" role="presentation">
          <aside className="hr-ai-drawer" aria-label="HR AI insights drawer">
            <div className="panel-header">
              <div>
                <p className="eyebrow">AI Insights Drawer</p>
                <h2>Workforce risk monitor</h2>
                <span>Risks, alerts, recommendations, anomaly warnings, and dataset-driven HR intelligence.</span>
              </div>
              <button className="ghost-button compact" type="button" onClick={() => setAiDrawerOpen(false)}>Close</button>
            </div>
            <div className="risk-monitor-body always-open">
              {hrInsightItems().map((item) => {
                const active = !/healthy|No |stable|complete|clear/i.test(item);
                return (
                  <div className={`ai-insight-chip ${active ? 'severity-warning' : 'severity-ok'}`} key={item}>
                    <strong>{active ? 'Warning' : 'Stable'}: {item}</strong>
                    <span>{active ? 'Recommendation: review the selected HR dataset, approval queue, or employee workspace.' : 'Company-scoped HR intelligence'}</span>
                  </div>
                );
              })}
            </div>
          </aside>
        </div>
      )}
      {leaveWorkflow && (
        <LeaveWorkflowModal
          employees={employees}
          form={leaveForm}
          leaveType={leaveWorkflow}
          onClose={() => setLeaveWorkflow(null)}
          onFile={(file) => setLeaveForm((current) => ({ ...current, attachmentName: file.name }))}
          onSubmit={submitLeaveWorkflow}
          saving={leaveSaving}
          selectedEmployee={employees.find((employee) => employee.id === leaveForm.employeeRecordId) ?? null}
          setForm={setLeaveForm}
          usedHours={(employee, type) => employeeLeaveBalance(employee, type)}
          requestedHours={calculateLeaveHours(leaveForm.startDate, leaveForm.endDate)}
        />
      )}
      {workspaceEmployee && (
        <EmployeeWorkspaceModal
          attendance={workspaceEmployeeAttendance}
          canManage={canManageHr}
          canViewSensitive={canViewSensitiveHr}
          documents={workspaceEmployeeDocs}
          employee={workspaceEmployee}
          leaveRequests={workspaceEmployeeLeave}
          onArchive={() => void archiveEmployee(workspaceEmployee)}
          onBack={() => setEmployeeWorkspaceId('')}
          onDecideLeave={decideLeaveRequest}
          onDeleteTimesheet={deleteTimesheet}
          onEditTimesheet={editTimesheet}
          onResubmitTimesheet={resubmitTimesheet}
          onTimesheetDecision={decideTimesheet}
          onCreateTimesheet={(employee) => {
            setSelectedEmployeeId(employee.id);
            setTimeEntryForm((current) => ({ ...current, employeeRecordId: employee.id }));
            void createHrRecord('timesheet', `${employee.title} draft timesheet`, 'draft', {
              employeeRecordId: employee.id,
              employeeId: employee.metadata?.employeeId,
              employeeName: employee.title,
              workDate: attendanceDate,
              date: attendanceDate,
              projectCode: 'MetroCare Migration',
              taskCode: 'API Integration',
              workType: 'Project',
              location: 'On-site',
              totalHours: 0,
              hours: 0,
              overtimeHours: 0,
              approvalStatus: 'draft',
              payrollPeriod: payPeriod,
              notes: 'Draft timesheet entry'
            });
          }}
          onEdit={() => void editEmployee(workspaceEmployee)}
          onToggleStatus={() => void updateHrRecord(workspaceEmployee, { status: workspaceEmployee.status === 'active' ? 'inactive' : 'active' })}
          payroll={workspaceEmployeePayroll}
          setTab={setEmployeeTab}
          tab={employeeTab}
        />
      )}
    </article>
  );
}

function AttendanceOperationsWorkspace({
  assignShift,
  attendanceDate,
  attendanceRecords,
  attendanceTab,
  canManage,
  clockEmployee,
  employees,
  exportPayrollTimesheet,
  leaveRequests,
  payPeriod,
  saveTimeEntry,
  setAttendanceDate,
  setAttendanceTab,
  setPayPeriod,
  setTimeEntryForm,
  shiftRecords,
  submitWeeklyTimesheet,
  timeEntryForm,
  updateTimesheetDecision,
  weeklyHoursForEmployee
}: {
  assignShift: (employee: ModuleRecord) => void;
  attendanceDate: string;
  attendanceRecords: ModuleRecord[];
  attendanceTab: string;
  canManage: boolean;
  clockEmployee: (employee: ModuleRecord, action: 'in' | 'out') => void;
  employees: ModuleRecord[];
  exportPayrollTimesheet: () => void;
  leaveRequests: ModuleRecord[];
  payPeriod: string;
  saveTimeEntry: (event: FormEvent<HTMLFormElement>) => void;
  setAttendanceDate: (value: string) => void;
  setAttendanceTab: (value: string) => void;
  setPayPeriod: (value: string) => void;
  setTimeEntryForm: Dispatch<SetStateAction<{ employeeRecordId: string; startTime: string; endTime: string; breakMinutes: string; status: string; shift: string; location: string; projectCode: string; taskCode: string; workType: string; notes: string }>>;
  shiftRecords: ModuleRecord[];
  submitWeeklyTimesheet: (employee: ModuleRecord) => void;
  timeEntryForm: { employeeRecordId: string; startTime: string; endTime: string; breakMinutes: string; status: string; shift: string; location: string; projectCode: string; taskCode: string; workType: string; notes: string };
  updateTimesheetDecision: (record: ModuleRecord, decision: 'approved' | 'rejected') => void;
  weeklyHoursForEmployee: (employee: ModuleRecord) => number;
}) {
  const dailyRecords = attendanceRecords.filter((record) => record.metadata?.date === attendanceDate);
  const openShifts = attendanceRecords.filter((record) => record.status === 'in progress' || record.status === 'clocked_in');
  const missingPunches = attendanceRecords.filter((record) => record.metadata?.missingPunch === true || record.status === 'in progress' || record.status === 'clocked_in').length;
  const overtimeEmployees = employees.filter((employee) => weeklyHoursForEmployee(employee) > 40);
  const approvedLeave = leaveRequests.filter((request) => request.status === 'approved');
  const submittedTimesheets = attendanceRecords.filter((record) => ['submitted', 'pending approval'].includes(String(record.status).toLowerCase()) || String(record.metadata?.approvalStatus ?? '').toLowerCase() === 'pending approval');
  const tabs = ['Dashboard', 'Live Workforce', 'Daily Time Entry', 'Weekly Timesheets', 'Payroll Preparation', 'Scheduling', 'Leave Calendar', 'Timesheet Analytics'];
  return (
    <section className="attendance-ops-workspace">
      <div className="employee-tabs sticky-tabs">
        {tabs.map((tab) => <button className={attendanceTab === tab ? 'active' : ''} key={tab} type="button" onClick={() => setAttendanceTab(tab)}>{tab}</button>)}
      </div>
      <div className="attendance-control-bar">
        <label>Date <input type="date" value={attendanceDate} onChange={(event) => setAttendanceDate(event.target.value)} /></label>
        <label>Pay period <input type="month" value={payPeriod} onChange={(event) => setPayPeriod(event.target.value)} /></label>
        <button type="button" onClick={exportPayrollTimesheet}>Export payroll-ready CSV</button>
      </div>
      <TimesheetWorkspaceSummary
        clockedIn={openShifts.length}
        dailyEntries={dailyRecords.length}
        missingPunches={missingPunches}
        overtime={overtimeEmployees.length}
        pendingApproval={submittedTimesheets.length}
      />
      <div className="dataset-preview-strip"><span>Approved leave: {approvedLeave.length}</span></div>
      {attendanceTab === 'Live Workforce' && (
        <div className="attendance-board">
          {employees.map((employee) => {
            const openShift = openShifts.find((record) => record.metadata?.employeeRecordId === employee.id);
            return (
              <article className="attendance-card" key={employee.id}>
                <div>
                  <strong>{employee.title}</strong>
                  <span>{String(employee.metadata?.department ?? 'Unassigned')} | {openShift ? 'Working now' : 'Available'}</span>
                </div>
                <div className="inline-actions">
                  <button type="button" onClick={() => clockEmployee(employee, 'in')} disabled={Boolean(openShift)}>Clock In</button>
                  <button className="ghost-button compact" type="button" onClick={() => clockEmployee(employee, 'out')} disabled={!openShift}>Clock Out</button>
                  {canManage && <button className="ghost-button compact" type="button" onClick={() => assignShift(employee)}>Assign Shift</button>}
                </div>
              </article>
            );
          })}
          {!employees.length && <p className="muted">Create employees before running live workforce operations.</p>}
        </div>
      )}
      {attendanceTab === 'Daily Time Entry' && (
        <form className="attendance-entry-form" onSubmit={saveTimeEntry}>
          <select value={timeEntryForm.employeeRecordId} onChange={(event) => setTimeEntryForm((current) => ({ ...current, employeeRecordId: event.target.value }))}>
            <option value="">Select employee</option>
            {employees.map((employee) => <option key={employee.id} value={employee.id}>{employee.title}</option>)}
          </select>
          <input type="time" value={timeEntryForm.startTime} onChange={(event) => setTimeEntryForm((current) => ({ ...current, startTime: event.target.value }))} />
          <input type="time" value={timeEntryForm.endTime} onChange={(event) => setTimeEntryForm((current) => ({ ...current, endTime: event.target.value }))} />
          <input placeholder="Break minutes" value={timeEntryForm.breakMinutes} onChange={(event) => setTimeEntryForm((current) => ({ ...current, breakMinutes: event.target.value }))} />
          <select value={timeEntryForm.status} onChange={(event) => setTimeEntryForm((current) => ({ ...current, status: event.target.value }))}>
            {['draft', 'in progress', 'submitted', 'pending approval', 'approved', 'rejected', 'payroll processed', 'PTO', 'sick', 'holiday'].map((status) => <option key={status} value={status}>{status}</option>)}
          </select>
          <input placeholder="Shift" value={timeEntryForm.shift} onChange={(event) => setTimeEntryForm((current) => ({ ...current, shift: event.target.value }))} />
          <input placeholder="Project code" value={timeEntryForm.projectCode} onChange={(event) => setTimeEntryForm((current) => ({ ...current, projectCode: event.target.value }))} />
          <input placeholder="Task code" value={timeEntryForm.taskCode} onChange={(event) => setTimeEntryForm((current) => ({ ...current, taskCode: event.target.value }))} />
          <select value={timeEntryForm.workType} onChange={(event) => setTimeEntryForm((current) => ({ ...current, workType: event.target.value }))}>
            {['Regular', 'Project', 'Admin', 'Training', 'Support', 'Overtime'].map((type) => <option key={type}>{type}</option>)}
          </select>
          <select value={timeEntryForm.location} onChange={(event) => setTimeEntryForm((current) => ({ ...current, location: event.target.value }))}>
            <option>On-site</option>
            <option>Remote</option>
            <option>Hybrid</option>
          </select>
          <input placeholder="Notes or supporting file name" value={timeEntryForm.notes} onChange={(event) => setTimeEntryForm((current) => ({ ...current, notes: event.target.value }))} />
          <button type="submit">Save timesheet entry</button>
        </form>
      )}
      {attendanceTab === 'Weekly Timesheets' && (
        <div className="record-list">
          {employees.map((employee) => {
            const hours = weeklyHoursForEmployee(employee);
            const employeeSubmitted = submittedTimesheets.filter((record) => record.metadata?.employeeRecordId === employee.id);
            return (
              <div key={employee.id}>
                <strong>{employee.title}</strong>
                <span>{hours.toFixed(2)} weekly hours | overtime {Math.max(hours - 40, 0).toFixed(2)} | {employeeSubmitted[0]?.status ?? (hours > 40 ? 'manager review' : 'draft')}</span>
                <div className="inline-actions">
                  <button type="button" onClick={() => submitWeeklyTimesheet(employee)}>Submit weekly timesheet</button>
                  {canManage && employeeSubmitted.map((record) => (
                    <span className="inline-actions" key={record.id}>
                      <button className="ghost-button compact" type="button" onClick={() => updateTimesheetDecision(record, 'approved')}>Approve</button>
                      <button className="ghost-button compact" type="button" onClick={() => updateTimesheetDecision(record, 'rejected')}>Request correction</button>
                    </span>
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      )}
      {attendanceTab === 'Payroll Preparation' && (
        <div className="employee-tab-panel">
          <h3>Payroll-ready summary</h3>
          <p>Exports include employee ID, regular hours, overtime hours, PTO, sick leave, unpaid hours, gross pay estimate, deductions placeholders, tax placeholders, approval status, and pay period.</p>
          <button type="button" onClick={exportPayrollTimesheet}>Download payroll summary</button>
        </div>
      )}
      {attendanceTab === 'Scheduling' && <EmployeeRecordList title="Shift schedule" records={shiftRecords} empty="No shifts assigned yet." />}
      {attendanceTab === 'Leave Calendar' && <LeaveRequestList canManage={canManage} onDecide={() => undefined} records={leaveRequests} />}
      {attendanceTab === 'Timesheet Analytics' && (
        <div className="hr-ai-panel">
          {[
            missingPunches ? `${missingPunches} missing punch or open clock-in records detected.` : 'No missing punches detected.',
            overtimeEmployees.length ? `${overtimeEmployees.length} employees are over 40 hours this week.` : 'No excessive overtime detected.',
            leaveRequests.some((request) => request.status === 'pending_approval') ? 'Unapproved leave may affect payroll preparation.' : 'Leave approvals are aligned with timesheets.',
            submittedTimesheets.length ? `${submittedTimesheets.length} submitted timesheets are waiting for manager approval.` : 'No submitted timesheets are blocked.',
            'Natural searches: show missing punches this week, employees over 45 hours, who is absent today, show overtime by department, show PTO next week.'
          ].map((item) => <div className="ai-insight-chip" key={item}><strong>{item}</strong><span>AI timesheet intelligence</span></div>)}
        </div>
      )}
    </section>
  );
}

function LeaveWorkflowModal({
  employees,
  form,
  leaveType,
  onClose,
  onFile,
  onSubmit,
  requestedHours,
  saving,
  selectedEmployee,
  setForm,
  usedHours
}: {
  employees: ModuleRecord[];
  form: { employeeRecordId: string; startDate: string; endDate: string; reason: string; attachmentName: string; manager: string; emergency: boolean };
  leaveType: 'pto' | 'sick';
  onClose: () => void;
  onFile: (file: File) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  requestedHours: number;
  saving: boolean;
  selectedEmployee: ModuleRecord | null;
  setForm: Dispatch<SetStateAction<{ employeeRecordId: string; startDate: string; endDate: string; reason: string; attachmentName: string; manager: string; emergency: boolean }>>;
  usedHours: (employee: ModuleRecord | null, type: 'pto' | 'sick') => number;
}) {
  const balance = usedHours(selectedEmployee, leaveType);
  const remaining = balance - requestedHours;
  return (
    <div className="modal-backdrop" role="presentation">
      <form className="access-modal leave-workflow-modal" aria-modal="true" role="dialog" aria-label={`${leaveType === 'pto' ? 'PTO' : 'Sick leave'} request workflow`} onSubmit={onSubmit}>
        <div className="employee-profile-header">
          <div>
            <p className="eyebrow">HR workflow</p>
            <h2>{leaveType === 'pto' ? 'Request PTO' : 'Sick Leave'}</h2>
            <span>Employee submits to manager notification to pending approval to approve or reject.</span>
          </div>
          <button className="ghost-button compact" type="button" onClick={onClose}>Close</button>
        </div>
        <div className="leave-workflow-grid">
          <label>Employee
            <select value={form.employeeRecordId} onChange={(event) => setForm((current) => ({ ...current, employeeRecordId: event.target.value }))}>
              <option value="">Select employee</option>
              {employees.map((employee) => <option key={employee.id} value={employee.id}>{employee.title}</option>)}
            </select>
          </label>
          <label>Manager
            <input placeholder="Manager or approval queue" value={form.manager} onChange={(event) => setForm((current) => ({ ...current, manager: event.target.value }))} />
          </label>
          <label>Start date
            <input type="date" value={form.startDate} onChange={(event) => setForm((current) => ({ ...current, startDate: event.target.value }))} />
          </label>
          <label>End date
            <input type="date" value={form.endDate} onChange={(event) => setForm((current) => ({ ...current, endDate: event.target.value }))} />
          </label>
          <label className="full-span">Reason
            <textarea placeholder="Coverage notes, reason, and manager context" value={form.reason} onChange={(event) => setForm((current) => ({ ...current, reason: event.target.value }))} />
          </label>
          <label>Attachment
            <input type="file" onChange={(event) => event.target.files?.[0] && onFile(event.target.files[0])} />
          </label>
          <label className="checkbox-line">
            <input checked={form.emergency} type="checkbox" onChange={(event) => setForm((current) => ({ ...current, emergency: event.target.checked }))} />
            Emergency leave flag
          </label>
        </div>
        <div className="leave-balance-strip">
          <span><strong>{balance}</strong> hours available</span>
          <span><strong>{requestedHours}</strong> hours requested</span>
          <span className={remaining < 0 ? 'warning-note' : ''}><strong>{remaining}</strong> hours after request</span>
          <span>{form.attachmentName || 'No attachment selected'}</span>
        </div>
        <div className="workflow-history-strip">
          {['Employee submitted', 'Manager notified', 'Pending approval', 'Approve / Reject', 'Status updates'].map((stage, index) => (
            <span key={stage}>{index + 1}. {stage}</span>
          ))}
        </div>
        <div className="modal-actions">
          <button className="ghost-button" type="button" onClick={onClose}>Cancel</button>
          <button type="submit" disabled={saving}>{saving ? 'Submitting...' : 'Submit workflow'}</button>
        </div>
      </form>
    </div>
  );
}

function EmployeeWorkspaceModal({
  attendance,
  canManage,
  canViewSensitive,
  documents,
  employee,
  leaveRequests,
  onArchive,
  onBack,
  onCreateTimesheet,
  onDeleteTimesheet,
  onDecideLeave,
  onEditTimesheet,
  onEdit,
  onResubmitTimesheet,
  onTimesheetDecision,
  onToggleStatus,
  payroll,
  setTab,
  tab
}: {
  attendance: ModuleRecord[];
  canManage: boolean;
  canViewSensitive: boolean;
  documents: ModuleRecord[];
  employee: ModuleRecord;
  leaveRequests: ModuleRecord[];
  onArchive: () => void;
  onBack: () => void;
  onCreateTimesheet: (employee: ModuleRecord) => void;
  onDeleteTimesheet: (record: ModuleRecord) => void;
  onDecideLeave: (request: ModuleRecord, decision: 'approved' | 'rejected') => void;
  onEditTimesheet: (record: ModuleRecord) => void;
  onEdit: () => void;
  onResubmitTimesheet: (record: ModuleRecord) => void;
  onTimesheetDecision: (record: ModuleRecord, decision: 'approved' | 'rejected') => void;
  onToggleStatus: () => void;
  payroll: ModuleRecord[];
  setTab: (tab: string) => void;
  tab: string;
}) {
  const tabs = ['Overview', 'Payroll', 'Timesheets', 'PTO', 'Documents', 'Performance', 'Activity History', 'Approvals'];
  return (
    <div className="modal-backdrop employee-workspace-backdrop" role="presentation">
      <section className="access-modal employee-workspace-modal" aria-modal="true" role="dialog" aria-label={`${employee.title} employee workspace`}>
        <div className="employee-profile-header">
          <div>
            <p className="eyebrow">HR / Employee Workspace</p>
            <h2>{employee.title}</h2>
            <span>{String(employee.metadata?.title ?? 'Role pending')} | {String(employee.metadata?.department ?? 'Department pending')}</span>
          </div>
          <div className="modal-actions inline-actions">
            {canManage && <button className="ghost-button compact" type="button" onClick={onEdit}>Edit</button>}
            {canManage && <button className="ghost-button compact" type="button" onClick={onToggleStatus}>Toggle status</button>}
            {canManage && <button className="ghost-button compact" type="button" onClick={onArchive}>Archive</button>}
            <button type="button" onClick={onBack}>Back</button>
          </div>
        </div>
        <div className="employee-tabs sticky-tabs">
          {tabs.map((item) => (
            <button className={tab === item ? 'active' : ''} key={item} type="button" onClick={() => setTab(item)}>{item}</button>
          ))}
        </div>
        <EmployeeProfileTab
          attendance={attendance}
          canManage={canManage}
          canViewSensitive={canViewSensitive}
          documents={documents}
          employee={employee}
          leaveRequests={leaveRequests}
          onCreateTimesheet={onCreateTimesheet}
          onDeleteTimesheet={onDeleteTimesheet}
          onDecideLeave={onDecideLeave}
          onEditTimesheet={onEditTimesheet}
          onResubmitTimesheet={onResubmitTimesheet}
          onTimesheetDecision={onTimesheetDecision}
          payroll={payroll}
          tab={tab}
        />
      </section>
    </div>
  );
}

function EmployeeProfileTab({
  attendance,
  canManage,
  canViewSensitive,
  documents,
  employee,
  leaveRequests,
  onCreateTimesheet,
  onDeleteTimesheet,
  onDecideLeave,
  onEditTimesheet,
  onResubmitTimesheet,
  onTimesheetDecision,
  payroll,
  tab
}: {
  attendance: ModuleRecord[];
  canManage: boolean;
  canViewSensitive: boolean;
  documents: ModuleRecord[];
  employee: ModuleRecord;
  leaveRequests: ModuleRecord[];
  onCreateTimesheet: (employee: ModuleRecord) => void;
  onDeleteTimesheet: (record: ModuleRecord) => void;
  onDecideLeave: (request: ModuleRecord, decision: 'approved' | 'rejected') => void;
  onEditTimesheet: (record: ModuleRecord) => void;
  onResubmitTimesheet: (record: ModuleRecord) => void;
  onTimesheetDecision: (record: ModuleRecord, decision: 'approved' | 'rejected') => void;
  payroll: ModuleRecord[];
  tab: string;
}) {
  if (tab === 'Payroll') {
    if (!canViewSensitive) return <RestrictedHrPanel title="Payroll and paystubs" />;
    return <EmployeeRecordList title="Payroll and paystubs" records={payroll} empty="No payroll records or paystubs attached yet." />;
  }
  if (tab === 'Timesheets') {
    return (
      <TimesheetRecordList
        canManage={canManage}
        employee={employee}
        onCreate={onCreateTimesheet}
        onDelete={onDeleteTimesheet}
        onEdit={onEditTimesheet}
        onResubmit={onResubmitTimesheet}
        onTimesheetDecision={onTimesheetDecision}
        records={attendance.filter((record) => record.status !== 'archived')}
      />
    );
  }
  if (tab === 'PTO') {
    return <LeaveRequestList canManage={canManage} onDecide={onDecideLeave} records={leaveRequests} />;
  }
  if (tab === 'Documents') {
    if (!canViewSensitive) return <RestrictedHrPanel title="Employee documents" />;
    return <EmployeeRecordList title="Employee documents" records={documents} empty="No employee documents uploaded yet." />;
  }
  if (tab === 'Performance') {
    return (
      <div className="employee-tab-panel">
        <h3>Performance</h3>
        <p>Performance review cycle, goals, manager notes, and turnover risk scoring will be tracked here.</p>
        <p>{String(employee.metadata?.notes ?? 'No performance notes yet.')}</p>
      </div>
    );
  }
  if (tab === 'Activity History') {
    return (
      <div className="employee-tab-panel">
        <h3>Activity history</h3>
        <p>Created {new Date(employee.createdAt).toLocaleString()}</p>
        <p>Last updated {new Date(employee.updatedAt).toLocaleString()}</p>
      </div>
    );
  }
  if (tab === 'Approvals') {
    return <LeaveRequestList canManage={canManage} onDecide={onDecideLeave} records={leaveRequests.filter((record) => record.status === 'pending_approval')} />;
  }
  return (
    <div className="employee-tab-panel employee-overview-grid">
      {(canViewSensitive ? [
        ['Employee ID', employee.metadata?.employeeId],
        ['Email', employee.metadata?.email],
        ['Phone', employee.metadata?.phone],
        ['Hire date', employee.metadata?.hireDate],
        ['Employment type', employee.metadata?.employmentType],
        ['Salary / pay rate', employee.metadata?.salary],
        ['Tax details', employee.metadata?.taxDetails],
        ['Benefits', employee.metadata?.benefits],
        ['Notes', employee.metadata?.notes]
      ] : [
        ['Employee ID', employee.metadata?.employeeId],
        ['Department', employee.metadata?.department],
        ['Role / title', employee.metadata?.title],
        ['Employment type', employee.metadata?.employmentType],
        ['Manager', employee.metadata?.manager]
      ]).map(([label, value]) => (
        <div key={String(label)}><span>{label}</span><strong>{String(value || 'Not set')}</strong></div>
      ))}
    </div>
  );
}

function RestrictedHrPanel({ title }: { title: string }) {
  return (
    <div className="employee-tab-panel">
      <h3>{title}</h3>
      <p className="muted">This information is restricted to managers, HR administrators, and owners.</p>
    </div>
  );
}

function LeaveRequestList({ canManage, onDecide, records }: { canManage: boolean; onDecide: (record: ModuleRecord, decision: 'approved' | 'rejected') => void; records: ModuleRecord[] }) {
  return (
    <div className="employee-tab-panel">
      <h3>PTO, sick leave, and approvals</h3>
      {records.map((record) => (
        <div className="history-item leave-history-item" key={record.id}>
          <div>
            <strong>{record.title}</strong>
            <span>{record.status} | {String(record.metadata?.startDate ?? 'No start')} to {String(record.metadata?.endDate ?? 'No end')}</span>
            <small>{String(record.metadata?.reason ?? 'No reason provided')} | Manager: {String(record.metadata?.manager ?? 'Unassigned')}</small>
          </div>
          {canManage && record.status === 'pending_approval' && (
            <div className="inline-actions">
              <button type="button" onClick={() => onDecide(record, 'approved')}>Approve</button>
              <button className="ghost-button compact" type="button" onClick={() => onDecide(record, 'rejected')}>Reject</button>
            </div>
          )}
        </div>
      ))}
      {!records.length && <p className="muted">No PTO or sick leave workflow history yet.</p>}
    </div>
  );
}

function EmployeeRecordList({ empty, records, title }: { empty: string; records: ModuleRecord[]; title: string }) {
  return (
    <div className="employee-tab-panel">
      <h3>{title}</h3>
      {records.map((record) => (
        <div className="history-item" key={record.id}>
          <div>
            <strong>{record.title}</strong>
            <span>{record.status} | {new Date(record.createdAt).toLocaleString()}</span>
          </div>
        </div>
      ))}
      {!records.length && <p className="muted">{empty}</p>}
    </div>
  );
}

function TimesheetRecordList({
  canManage,
  employee,
  onCreate,
  onDelete,
  onEdit,
  onResubmit,
  onTimesheetDecision,
  records
}: {
  canManage: boolean;
  employee: ModuleRecord;
  onCreate: (employee: ModuleRecord) => void;
  onDelete: (record: ModuleRecord) => void;
  onEdit: (record: ModuleRecord) => void;
  onResubmit: (record: ModuleRecord) => void;
  onTimesheetDecision: (record: ModuleRecord, decision: 'approved' | 'rejected') => void;
  records: ModuleRecord[];
}) {
  return (
    <div className="employee-tab-panel timesheet-tab-panel">
      <div className="panel-header">
        <div>
          <h3>Timesheets</h3>
          <p className="muted">Edit work date, project/task, hours, overtime, PTO, approval, and payroll readiness in one employee workspace.</p>
        </div>
        <button type="button" onClick={() => onCreate(employee)}>Add draft entry</button>
      </div>
      <div className="timesheet-entry-list">
        {records.map((record) => (
          <article className="timesheet-entry-card" key={record.id}>
            <div>
              <strong>{String(record.metadata?.workDate ?? record.metadata?.date ?? 'No date')}</strong>
              <span>{String(record.metadata?.projectCode ?? 'No project')} / {String(record.metadata?.taskCode ?? 'No task')} | {String(record.metadata?.workType ?? 'Regular')}</span>
            </div>
            <div className="dataset-detail-grid compact">
              <div><span>Total hours</span><strong>{String(record.metadata?.totalHours ?? record.metadata?.hours ?? 0)}</strong></div>
              <div><span>Overtime</span><strong>{String(record.metadata?.overtimeHours ?? 0)}</strong></div>
              <div><span>Approval</span><strong>{String(record.metadata?.approvalStatus ?? record.status)}</strong></div>
              <div><span>Payroll</span><strong>{record.metadata?.payrollReady ? 'Ready' : 'Not ready'}</strong></div>
            </div>
            <p className="muted">{String(record.metadata?.notes ?? record.metadata?.note ?? 'No notes')}</p>
            <div className="inline-actions">
              <button type="button" onClick={() => onEdit(record)}>Edit hours/project</button>
              <button className="ghost-button compact" type="button" onClick={() => onResubmit(record)}>Save draft / resubmit</button>
              {canManage && <button className="ghost-button compact" type="button" onClick={() => onTimesheetDecision(record, 'approved')}>Approve</button>}
              {canManage && <button className="ghost-button compact" type="button" onClick={() => onTimesheetDecision(record, 'rejected')}>Reject</button>}
              <button className="ghost-button compact danger" type="button" onClick={() => onDelete(record)}>Delete</button>
            </div>
          </article>
        ))}
        {!records.length && <p className="muted">No timesheet entries yet. Add a draft entry to start project-based time tracking.</p>}
      </div>
    </div>
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
  onSaveRows,
  onRestore
}: {
  allDatasets: Dataset[];
  company?: Company;
  dataset: Dataset;
  mode: PreviewMode;
  onApprove: (dataset: Dataset) => void | Promise<void>;
  onClose: () => void;
  onDownload: (dataset: Dataset | null) => void;
  onReprocess: (dataset: Dataset) => void;
  onSaveRows: (dataset: Dataset, records: Record<string, string>[]) => Promise<void>;
  onRestore: (dataset: Dataset) => void;
}) {
  const [page, setPage] = useState(1);
  const [filter, setFilter] = useState('');
  const [sortColumn, setSortColumn] = useState(datasetHeaders(dataset)[0] ?? '');
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');
  const [selectedColumn, setSelectedColumn] = useState('all');
  const [savingRows, setSavingRows] = useState(false);
  const [editRows, setEditRows] = useState<Record<string, string>[]>(() => datasetPreview(dataset).map((row) => normalizeEditableRow(row)));
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [hiddenColumns, setHiddenColumns] = useState<string[]>([]);
  const [selectedRowKeys, setSelectedRowKeys] = useState<string[]>([]);
  const [columnActionMessage, setColumnActionMessage] = useState('');
  const [savedQueries, setSavedQueries] = useState<string[]>([]);
  const pageSize = 10;
  const previewRows = getPreviewRows(dataset, mode);
  const activeRows = mode === 'edit' ? editRows : previewRows;
  const allHeaders = getPreviewHeaders(dataset, activeRows);
  const headers = allHeaders.filter((header) => !hiddenColumns.includes(header) && (!selectedColumns.length || mode !== 'query' || selectedColumn !== 'selected_only' || selectedColumns.includes(header)));
  const columnTypes = inferColumnTypes(dataset.preview, dataset.headers);
  const validation = summarizeValidation(dataset);
  const duplicates = findDuplicateRows(datasetPreview(dataset));
  const versionHistory = getDatasetVersions(allDatasets, dataset);
  const filteredRows = [...activeRows]
    .filter((row) => {
      if (!filter.trim()) return true;
      const query = filter.toLowerCase();
      if (selectedColumn !== 'all') return String(row[selectedColumn] ?? '').toLowerCase().includes(query);
      return Object.values(row).some((value) => String(value ?? '').toLowerCase().includes(query));
    })
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
    history: 'Version History',
    edit: 'Edit Rows',
    query: 'Query Dataset'
  }[mode];

  useEffect(() => {
    setPage(1);
    setFilter('');
    setSelectedColumn('all');
    setSortColumn(datasetHeaders(dataset)[0] ?? '');
    setEditRows(datasetPreview(dataset).map((row) => normalizeEditableRow(row)));
    setSelectedColumns([]);
    setHiddenColumns([]);
    setSelectedRowKeys([]);
    setColumnActionMessage('');
  }, [dataset.id, mode]);

  function sortBy(header: string) {
    setSortDirection((current) => sortColumn === header && current === 'asc' ? 'desc' : 'asc');
    setSortColumn(header);
    setPage(1);
  }

  function updateEditCell(rowIndex: number, header: string, value: string) {
    const absoluteIndex = (page - 1) * pageSize + rowIndex;
    const sourceRow = filteredRows[absoluteIndex];
    const sourceIndex = editRows.findIndex((row) => row === sourceRow);
    if (sourceIndex < 0) return;
    setEditRows((current) => current.map((row, index) => index === sourceIndex ? { ...row, [header]: value } : row));
  }

  function rowKey(row: Record<string, string>) {
    return JSON.stringify(row);
  }

  function toggleRow(row: Record<string, string>) {
    const key = rowKey(row);
    setSelectedRowKeys((current) => current.includes(key) ? current.filter((entry) => entry !== key) : [...current, key]);
  }

  function toggleColumn(header: string) {
    setSelectedColumns((current) => current.includes(header) ? current.filter((entry) => entry !== header) : [...current, header]);
  }

  function mutateRows(mutator: (row: Record<string, string>) => Record<string, string>) {
    setEditRows((current) => current.map(mutator));
  }

  function runColumnAction(action: string) {
    const columns = selectedColumns.length ? selectedColumns : selectedColumn !== 'all' && selectedColumn !== 'selected_only' ? [selectedColumn] : [];
    if (!columns.length && !['export_selected_columns', 'create_calculated'].includes(action)) {
      setColumnActionMessage('Select one or more columns before running a column action.');
      return;
    }
    if (action === 'show_selected') {
      setHiddenColumns(allHeaders.filter((header) => !columns.includes(header)));
      setColumnActionMessage(`Showing ${columns.length} selected column${columns.length === 1 ? '' : 's'}.`);
      return;
    }
    if (action === 'hide_selected') {
      setHiddenColumns((current) => [...new Set([...current, ...columns])]);
      setColumnActionMessage(`Hidden ${columns.length} column${columns.length === 1 ? '' : 's'}.`);
      return;
    }
    if (action === 'show_all') {
      setHiddenColumns([]);
      setSelectedColumns([]);
      setSelectedColumn('all');
      setColumnActionMessage('All columns restored.');
      return;
    }
    if (action === 'rename') {
      const source = columns[0];
      const nextName = `${source}_renamed`;
      mutateRows((row) => {
        const next = { ...row, [nextName]: row[source] ?? '' };
        delete next[source];
        return next;
      });
      setSelectedColumns([nextName]);
      setColumnActionMessage(`${source} renamed to ${nextName}. Save edited rows to persist.`);
      return;
    }
    if (action === 'delete') {
      mutateRows((row) => Object.fromEntries(Object.entries(row).filter(([key]) => !columns.includes(key))));
      setHiddenColumns((current) => [...new Set([...current, ...columns])]);
      setColumnActionMessage(`${columns.join(', ')} deleted from the edit buffer. Save edited rows to persist.`);
      return;
    }
    if (action === 'duplicate') {
      mutateRows((row) => columns.reduce((next, column) => ({ ...next, [`${column}_copy`]: row[column] ?? '' }), row));
      setColumnActionMessage(`${columns.join(', ')} duplicated. Save edited rows to persist.`);
      return;
    }
    if (action === 'normalize') {
      mutateRows((row) => columns.reduce((next, column) => ({ ...next, [column]: String(next[column] ?? '').trim().replace(/\s+/g, ' ') }), row));
      setColumnActionMessage(`${columns.join(', ')} normalized. Save edited rows to persist.`);
      return;
    }
    if (action === 'replace') {
      const findValue = 'HR Dept';
      const replaceValue = 'HR';
      mutateRows((row) => columns.reduce((next, column) => ({ ...next, [column]: String(next[column] ?? '').replaceAll(findValue, replaceValue) }), row));
      setColumnActionMessage(`Replacement applied to ${columns.join(', ')}. Save edited rows to persist.`);
      return;
    }
    if (action === 'bulk_edit') {
      const value = 'Updated';
      mutateRows((row) => columns.reduce((next, column) => ({ ...next, [column]: value }), row));
      setColumnActionMessage(`Bulk edit applied to ${columns.join(', ')}. Save edited rows to persist.`);
      return;
    }
    if (action === 'calculated') {
      const name = 'calculated_value';
      mutateRows((row) => ({ ...row, [name]: columns.map((column) => row[column] ?? '').join(' ') }));
      setColumnActionMessage(`${name} calculated from ${columns.join(', ')}. Save edited rows to persist.`);
      return;
    }
    if (action === 'export_columns') {
      const exportRows = filteredRows.map((row) => Object.fromEntries(columns.map((column) => [column, row[column] ?? ''])));
      const csv = [columns.map(csvEscape).join(','), ...exportRows.map((row) => columns.map((column) => csvEscape(row[column] ?? '')).join(','))].join('\n');
      downloadText(csv, `${dataset.fileName.replace(/\.(csv|xlsx|xls|json)$/i, '')}-columns.csv`, 'text/csv');
      setColumnActionMessage(`Exported ${columns.length} selected column${columns.length === 1 ? '' : 's'}.`);
      return;
    }
    setColumnActionMessage(`${action} staged for selected columns.`);
  }

  function deleteSelectedRows() {
    const selected = new Set(selectedRowKeys);
    setEditRows((current) => current.filter((row) => !selected.has(rowKey(row))));
    setSelectedRowKeys([]);
    setColumnActionMessage('Selected rows removed from the edit buffer. Save edited rows to persist.');
  }

  function saveQuery() {
    const label = `${selectedColumn}: ${filter || 'all values'}`;
    setSavedQueries((current) => [...new Set([label, ...current])].slice(0, 5));
    setColumnActionMessage(`Saved query: ${label}`);
  }

  async function saveEditedRows() {
    setSavingRows(true);
    try {
      await onSaveRows(dataset, editRows);
    } finally {
      setSavingRows(false);
    }
  }

  function exportFilteredRows() {
    const exportHeaders = headers.length ? headers : Array.from(new Set(filteredRows.flatMap((row) => Object.keys(row))));
    const csv = [
      exportHeaders.map(csvEscape).join(','),
      ...filteredRows.map((row) => exportHeaders.map((header) => csvEscape(row[header] ?? '')).join(','))
    ].join('\n');
    downloadText(csv, `${dataset.fileName.replace(/\.(csv|xlsx|xls|json)$/i, '')}-filtered.csv`, 'text/csv');
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

        <div className="record-toolbar preview-toolbar preview-query-builder">
          <input placeholder="Search rows, values, employee names, projects, invoices..." value={filter} onChange={(event) => { setFilter(event.target.value); setPage(1); }} />
          <select value={selectedColumn} onChange={(event) => { setSelectedColumn(event.target.value); setPage(1); }}>
            <option value="all">All columns</option>
            <option value="selected_only">Selected columns only</option>
            {allHeaders.map((header) => <option key={header} value={header}>{header}</option>)}
          </select>
          <button className="ghost-button compact" type="button" onClick={saveQuery}>Save query</button>
          <button className="ghost-button compact" type="button" onClick={exportFilteredRows}>Export filtered</button>
          <button className="ghost-button compact" type="button" disabled={page <= 1} onClick={() => setPage((current) => Math.max(current - 1, 1))}>Previous</button>
          <span>Page {page} of {pageCount}</span>
          <button className="ghost-button compact" type="button" disabled={page >= pageCount} onClick={() => setPage((current) => Math.min(current + 1, pageCount))}>Next</button>
        </div>
        <div className="column-management-panel">
          <div>
            <strong>Column Actions</strong>
            <span>{selectedColumns.length ? `${selectedColumns.length} selected` : 'Select columns from the header row to enable actions.'}</span>
          </div>
          <div className="column-action-row">
            <button type="button" onClick={() => runColumnAction('show_selected')}>Show selected</button>
            <button type="button" onClick={() => runColumnAction('hide_selected')}>Hide selected</button>
            <button type="button" onClick={() => runColumnAction('show_all')}>Show all</button>
            <button type="button" onClick={() => runColumnAction('rename')}>Rename</button>
            <button type="button" onClick={() => runColumnAction('bulk_edit')}>Edit values</button>
            <button type="button" onClick={() => runColumnAction('replace')}>Replace values</button>
            <button type="button" onClick={() => runColumnAction('normalize')}>Normalize</button>
            <button type="button" onClick={() => runColumnAction('duplicate')}>Duplicate</button>
            <button type="button" onClick={() => runColumnAction('delete')}>Delete column</button>
            <button type="button" onClick={() => runColumnAction('calculated')}>Calculated column</button>
            <button type="button" onClick={() => runColumnAction('export_columns')}>Export selected columns</button>
          </div>
          <div className="column-chip-row">
            {allHeaders.map((header) => (
              <button className={selectedColumns.includes(header) ? 'active' : hiddenColumns.includes(header) ? 'muted-chip' : ''} key={header} type="button" onClick={() => toggleColumn(header)}>
                {header}
              </button>
            ))}
          </div>
          <div className="inline-actions">
            <button className="ghost-button compact" type="button" disabled={!selectedRowKeys.length} onClick={deleteSelectedRows}>Delete selected rows</button>
            <button className="ghost-button compact" type="button" disabled={!selectedRowKeys.length} onClick={() => setColumnActionMessage(`${selectedRowKeys.length} selected rows approved for review.`)}>Approve selected rows</button>
            <button className="ghost-button compact" type="button" disabled={!selectedRowKeys.length} onClick={exportFilteredRows}>Export selected/filter rows</button>
          </div>
          {Boolean(savedQueries.length) && <span>Saved queries: {savedQueries.join(' | ')}</span>}
          {columnActionMessage && <span>{columnActionMessage}</span>}
        </div>

        <div className="table-wrap preview-table-wrap">
          <table className="preview-table">
            <thead>
              <tr>
                <th>Select</th>
                {headers.map((header) => (
                  <th className={selectedColumns.includes(header) ? 'selected-column' : ''} key={header}>
                    <label className="column-select-label">
                      <input checked={selectedColumns.includes(header)} type="checkbox" onChange={() => toggleColumn(header)} />
                      <button type="button" onClick={() => sortBy(header)}>{header}{sortColumn === header ? ` ${sortDirection === 'asc' ? 'up' : 'down'}` : ''}</button>
                    </label>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {pagedRows.map((row, rowIndex) => (
                <tr className={selectedRowKeys.includes(rowKey(row)) ? 'selected-row' : ''} key={`${rowIndex}-${page}`}>
                  <td><input checked={selectedRowKeys.includes(rowKey(row))} type="checkbox" onChange={() => toggleRow(row)} /></td>
                  {headers.map((header) => (
                  <td className={cellClass(row[header], header)} key={header}>
                    {mode === 'edit' ? (
                      <input
                        className="editable-cell-input"
                        value={String(row[header] ?? '')}
                        onChange={(event) => updateEditCell(rowIndex, header, event.target.value)}
                      />
                    ) : String(row[header] ?? '')}
                  </td>
                ))}</tr>
              ))}
              {!pagedRows.length && <tr><td colSpan={(headers.length || 1) + 1}>No preview rows match this filter.</td></tr>}
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
          {mode === 'edit' && <button className="ghost-button" disabled={savingRows} type="button" onClick={() => void saveEditedRows()}>{savingRows ? 'Saving rows...' : 'Save edited rows'}</button>}
          <button className="ghost-button" type="button" onClick={() => void onApprove(dataset)}>Approve</button>
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
      {view === 'hr' ? (
        <article className="panel module-workbench compact-module-launcher">
          <div>
            <p className="eyebrow">HR & Workforce</p>
            <h2>Employee-centered HR workspace</h2>
            <p className="persistence-note">HR now runs through active datasets, employee grids, timesheets, approvals, reports, and AI insights. Generic module item creation is hidden for HR to keep the workspace compact.</p>
          </div>
          <div className="module-quick-links">
            {[
              ['Employee Workspace', '/hr/employees'],
              ['Timesheets', '/hr/timesheets'],
              ['HR Datasets', '/hr/datasets'],
              ['Approvals', '/hr/approvals'],
              ['Reports', '/hr/reports'],
              ['AI Insights', '/hr/ai-insights']
            ].map(([label, path]) => (
              <button key={path} type="button" onClick={() => navigate(path)}>{label}</button>
            ))}
          </div>
        </article>
      ) : (
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
      )}
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
    chart: datasetChart(dataset),
    insights: datasetInsights(dataset),
    numericSummary: datasetNumericSummary(dataset)
  };
}

function buildReportLines(dataset: Dataset) {
  const intelligence = buildLocalBusinessIntelligence(dataset);
  return [
    'Metenova AI Executive Operations Report',
    `Dataset: ${dataset.fileName}`,
    `File type: ${(dataset.fileType ?? 'csv').toUpperCase()}`,
    ...(dataset.worksheetName ? [`Worksheet: ${dataset.worksheetName}`] : []),
    `Uploaded: ${new Date(dataset.uploadedAt).toLocaleString()}`,
    `Rows: ${dataset.rows}`,
    `Columns: ${dataset.columns}`,
    `Quality score: ${intelligence.metrics.qualityScore}%`,
    `Duplicate rows: ${intelligence.metrics.duplicates}`,
    `Failed rows: ${intelligence.metrics.failedRows}`,
    '',
    'Executive Summary',
    ...(intelligence.executiveSummary.length ? intelligence.executiveSummary.map((item) => `- ${item}`) : ['- No generated summary is available yet.']),
    '',
    'AI Operational Insights',
    ...intelligence.aiInsights.map((item) => `- ${item.title}: ${item.summary}`),
    '',
    'Operational Recommendations',
    ...intelligence.recommendations.map((item) => `- ${item}`),
    '',
    'Numeric Summary',
    ...(datasetNumericSummary(dataset).length
      ? datasetNumericSummary(dataset).map((item) => `${item.column}: total ${item.total.toFixed(2)}, average ${item.average.toFixed(2)}, min ${item.min.toFixed(2)}, max ${item.max.toFixed(2)}`)
      : ['No numeric columns detected.'])
  ];
}

function buildLocalBusinessIntelligence(dataset: Dataset) {
  const duplicates = findDuplicateRows(datasetPreview(dataset)).length;
  const missingValues = datasetPreview(dataset).reduce((total, row) => total + Object.values(row).filter((value) => value == null || String(value).trim() === '').length, 0);
  const failedRows = dataset.cleanupMetrics?.failedRows ?? 0;
  const invalidValues = dataset.cleanupMetrics?.invalidValuesDetected ?? missingValues;
  const qualityScore = Math.max(0, Math.min(100, Math.round(dataset.qualityScore ?? Math.max(45, 100 - duplicates - failedRows - Math.ceil(invalidValues / 2)))));
  const anomalyScore = Math.min(100, (dataset.cleanupMetrics?.anomaliesDetected ?? 0) + duplicates * 6 + failedRows * 12 + Math.ceil(invalidValues / 2));
  const trends = datasetNumericSummary(dataset).slice(0, 3).map((summary) => ({
    column: summary.column,
    total: summary.total,
    average: summary.average,
    change: summary.change,
    changePercent: summary.changePercent,
    direction: summary.change >= 0 ? 'up' : 'down'
  }));
  const executiveSummary = [
    `${dataset.fileName} contains ${displayNumber(dataset.rows)} rows and ${displayNumber(dataset.columns)} columns for business review.`,
    qualityScore >= 85 ? `Quality score is strong at ${qualityScore}%.` : `Quality score is ${qualityScore}%, so approval review is recommended.`,
    trends[0] ? `${trends[0].column} is trending ${trends[0].direction} by ${Math.abs(Number(trends[0].change)).toLocaleString()}.` : 'No numeric trend was detected yet.'
  ];
  const aiInsights = [
    missingValues > 0 ? { title: 'Missing employee IDs detected', summary: `${missingValues} blank values may represent missing identifiers or required fields.`, severity: 'medium', confidence: 0.91 } : null,
    duplicates > 0 ? { title: 'Duplicate payroll entries found', summary: `${duplicates} duplicate row${duplicates === 1 ? '' : 's'} detected for review.`, severity: 'medium', confidence: 0.88 } : null,
    trends.some((trend) => trend.direction === 'down') ? { title: 'Regional sales decline detected', summary: 'A tracked numeric metric declined across the uploaded sequence.', severity: 'medium', confidence: 0.82 } : null,
    anomalyScore > 0 ? { title: 'Data quality risks identified', summary: `Composite anomaly score is ${anomalyScore}.`, severity: anomalyScore > 45 ? 'high' : 'medium', confidence: 0.89 } : null
  ].filter(Boolean) as Array<{ title: string; summary: string; severity: string; confidence: number }>;
  if (!aiInsights.length) {
    aiInsights.push({ title: 'Dataset is reporting-ready', summary: 'No major duplicate, missing-value, or failed-row signals were detected.', severity: 'low', confidence: 0.86 });
  }
  return {
    metrics: {
      rowCount: dataset.rows,
      columnCount: dataset.columns,
      duplicates,
      missingValues,
      invalidValues,
      failedRows,
      rowsFixed: dataset.cleanupMetrics?.rowsFixed ?? 0,
      standardizedColumns: dataset.cleanupMetrics?.columnsStandardized ?? 0,
      qualityScore,
      anomalyScore
    },
    trends,
    executiveSummary,
    aiInsights,
    recommendations: [
      duplicates ? 'Review duplicate records before payroll, invoice, or export processing.' : 'Keep duplicate monitoring enabled for scheduled uploads.',
      missingValues ? 'Assign a data owner to resolve required-field gaps.' : 'Maintain current required-field validation rules.',
      failedRows ? 'Route failed rows into approval workflow before executive reporting.' : 'Dataset is ready for approval review.',
      'TODO: Add AI recommendations, anomaly detection, predictive analytics, and automated report summaries.'
    ],
    approvalStatus: failedRows || anomalyScore > 45 ? 'needs_review' : qualityScore >= 85 ? 'approved_ready' : 'waiting_approval'
  };
}

function csvEscape(value: string | number | boolean | null | undefined) {
  const text = String(value ?? '');
  return /[",\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

function downloadText(content: string, fileName: string, type: string) {
  const link = document.createElement('a');
  link.href = URL.createObjectURL(new Blob([content], { type }));
  link.download = fileName;
  link.click();
  URL.revokeObjectURL(link.href);
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
