import 'dotenv/config';
import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { createHmac, randomBytes, randomUUID, scrypt, timingSafeEqual } from 'node:crypto';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import cors from 'cors';
import express from 'express';
import multer from 'multer';
import * as XLSX from 'xlsx';
import {
  companyExists,
  createAccountToken,
  createCompany,
  createInvitation,
  createModuleRecord,
  createSession,
  createUser,
  deleteCleanupJob,
  deleteCompany,
  deleteDataset,
  deleteReport,
  deleteUser,
  deleteModuleRecord,
  deletePipeline,
  ensureOwnerAccount,
  findUserByEmail,
  findUserById,
  findAccountToken,
  findInvitationByToken,
  findEmailLog,
  getDatabaseRuntimeStatus,
  getAccessibleCompanyIds,
  hasRole,
  canAccessCompany,
  findSessionById,
  getDevUser,
  getDataset,
  getModuleMetrics,
  getPipeline,
  initDatabase,
  listAuditLogs,
  listCompanies,
  listCleanupJobs,
  listDashboards,
  listDatasets,
  listEmailLogs,
  listInvitations,
  listModuleRecords,
  listNotifications,
  listPipelines,
  listReports,
  listSessions,
  listUserCompanyAssignments,
  listUsers,
  markAccountTokenUsed,
  markInvitationAccepted,
  ownerEmail,
  promoteAdminEmails,
  recordLoginFailure,
  recordLoginSuccess,
  removeDemoAccounts,
  saveDashboard,
  saveCleanupJob,
  saveDataset,
  saveEmailLog,
  saveNotification,
  savePipeline,
  saveReport,
  saveAuditLog,
  revokeSession,
  revokeUserSessions,
  roles,
  updateUserPassword,
  updateEmailLog,
  updateCompany,
  updateNotification,
  updatePipeline,
  updateModuleRecord,
  updateUser,
  replaceUserCompanyAssignments,
  usingPostgres
} from './db.js';
import { cleanDataset, recordsToCsv } from './dataCleanup.js';

const scryptAsync = promisify(scrypt);
const app = express();
const maxUploadBytes = Number(process.env.MAX_UPLOAD_BYTES || 15 * 1024 * 1024);
const maxSpreadsheetRows = Number(process.env.MAX_SPREADSHEET_ROWS || 10000);
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: maxUploadBytes }
});
const port = Number(process.env.PORT || 4000);
const clientOrigin = process.env.CLIENT_ORIGIN || 'http://localhost:5174';
const authDisabled = process.env.AUTH_DISABLED === 'true';
const jwtSecret = process.env.JWT_SECRET || (!usingPostgres ? 'local-mvp-secret' : '');
const csrfToken = process.env.CSRF_TOKEN || randomBytes(32).toString('base64url');
const requireStoredSessions = usingPostgres || process.env.REQUIRE_STORED_SESSIONS === 'true';
const sessionTtlMinutes = Number(process.env.SESSION_TTL_MINUTES || 30);
const sessionTtlMs = Math.max(sessionTtlMinutes, 5) * 60 * 1000;
const warningSeconds = Number(process.env.SESSION_WARNING_SECONDS || 60);
const isEphemeralProductionStorage = !usingPostgres && process.env.VERCEL === '1';
const adminEmails = (process.env.ADMIN_EMAILS || ownerEmail)
  .split(',')
  .map((email) => email.trim().toLowerCase())
  .filter(Boolean);
const rootDir = fileURLToPath(new URL('../..', import.meta.url));
const frontendDist = join(rootDir, 'frontend/dist');
const legacyDatasetsFile = join(rootDir, 'backend/data/uploads/datasets.json');
const authAttempts = new Map();
const supportRecipientEmail = 'melakue@metenovaai.com';
const supportSenderEmail = 'support@metenovaai.com';
const emailFrom = process.env.EMAIL_FROM || supportSenderEmail;
const resendConfigured = Boolean(process.env.RESEND_API_KEY);
const emailConfigured = resendConfigured;
const appBaseUrl = process.env.APP_BASE_URL || clientOrigin;
const apiUrl = process.env.API_URL || (process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : `http://localhost:${port}`);
const corsOrigins = new Set([
  clientOrigin,
  appBaseUrl,
  'https://www.metenovaai.com',
  'https://metenovaai.com',
  ...(process.env.CORS_ORIGINS || '').split(',').map((origin) => origin.trim()).filter(Boolean)
]);

const insights = {
  metrics: [
    { label: 'Automations live', value: 18, trend: '+12%' },
    { label: 'Hours saved', value: 246, trend: '+31%' },
    { label: 'Active workflows', value: 7, trend: '+4%' }
  ],
  recommendations: [
    'Prioritize invoice intake automation for the finance team.',
    'Add human approval to customer escalation workflows.',
    'Review stale sales handoff tasks older than 48 hours.'
  ]
};

if (!authDisabled && !jwtSecret) {
  throw new Error('JWT_SECRET is required. Set it in backend/.env.');
}

function parseCsv(text) {
  const rows = [];
  let current = '';
  let row = [];
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];

    if (char === '"' && inQuotes && next === '"') {
      current += '"';
      index += 1;
    } else if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      row.push(current.trim());
      current = '';
    } else if ((char === '\n' || char === '\r') && !inQuotes) {
      if (char === '\r' && next === '\n') {
        index += 1;
      }
      row.push(current.trim());
      if (row.some((value) => value !== '')) {
        rows.push(row);
      }
      row = [];
      current = '';
    } else {
      current += char;
    }
  }

  row.push(current.trim());
  if (row.some((value) => value !== '')) {
    rows.push(row);
  }

  return rowsToRecords(rows);
}

function parseJsonDataset(text) {
  let payload;
  try {
    payload = JSON.parse(text);
  } catch {
    const error = new Error('Upload a valid JSON file.');
    error.statusCode = 400;
    throw error;
  }

  const records = Array.isArray(payload)
    ? payload
    : Array.isArray(payload?.records)
      ? payload.records
      : Array.isArray(payload?.data)
        ? payload.data
        : [];

  if (!records.length || records.some((record) => record == null || typeof record !== 'object' || Array.isArray(record))) {
    const error = new Error('JSON uploads must contain an array of objects.');
    error.statusCode = 400;
    throw error;
  }

  const columns = [...new Set(records.flatMap((record) => Object.keys(record)))];
  return {
    columns,
    records: records.map((record) =>
      columns.reduce((normalized, column) => {
        normalized[column] = record[column] == null ? '' : String(record[column]);
        return normalized;
      }, {})
    )
  };
}

function rowsToRecords(rows) {
  const normalizedRows = rows
    .map((row) => row.map((value) => String(value ?? '').trim()))
    .filter((row) => row.some((value) => value !== ''));

  if (normalizedRows.length === 0) {
    return { columns: [], records: [] };
  }

  const seenColumns = new Map();
  const columns = normalizedRows[0].map((column, index) => {
    const baseName = column || `Column ${index + 1}`;
    const count = seenColumns.get(baseName) ?? 0;
    seenColumns.set(baseName, count + 1);
    return count ? `${baseName} ${count + 1}` : baseName;
  });
  const records = normalizedRows.slice(1).map((values) =>
    columns.reduce((record, column, index) => {
      record[column] = values[index] ?? '';
      return record;
    }, {})
  );

  return { columns, records };
}

function getFileExtension(fileName) {
  return String(fileName || '').split('.').pop()?.toLowerCase() || '';
}

function validateUploadMetadata(file) {
  const extension = getFileExtension(file.originalname);
  const allowedExtensions = ['csv', 'xlsx', 'xls', 'json'];
  const allowedMimeTypes = new Set([
    'text/csv',
    'application/csv',
    'application/json',
    'application/vnd.ms-excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'application/octet-stream'
  ]);
  if (!allowedExtensions.includes(extension) || (file.mimetype && !allowedMimeTypes.has(file.mimetype))) {
    const error = new Error('Upload a supported CSV, XLSX, XLS, or JSON file.');
    error.statusCode = 400;
    throw error;
  }
}

function parseUploadedFile(file, requestedWorksheetName) {
  const extension = getFileExtension(file.originalname);

  if (!['csv', 'xlsx', 'xls', 'json'].includes(extension)) {
    const error = new Error('Upload a .csv, .xlsx, .xls, or .json file.');
    error.statusCode = 400;
    throw error;
  }

  if (extension === 'csv') {
    const parsed = parseCsv(file.buffer.toString('utf8'));
    return {
      ...parsed,
      fileType: 'csv',
      worksheetName: 'CSV',
      worksheets: ['CSV']
    };
  }

  if (extension === 'json') {
    const parsed = parseJsonDataset(file.buffer.toString('utf8'));
    return {
      ...parsed,
      fileType: 'json',
      worksheetName: 'JSON',
      worksheets: ['JSON']
    };
  }

  const workbook = XLSX.read(file.buffer, {
    type: 'buffer',
    cellDates: true,
    sheetRows: maxSpreadsheetRows + 1
  });
  const worksheets = workbook.SheetNames ?? [];

  if (!worksheets.length) {
    const error = new Error('This workbook does not contain any worksheets.');
    error.statusCode = 400;
    throw error;
  }

  const worksheetName = worksheets.includes(requestedWorksheetName) ? requestedWorksheetName : worksheets[0];
  const worksheet = workbook.Sheets[worksheetName];
  const rows = XLSX.utils.sheet_to_json(worksheet, {
    header: 1,
    defval: '',
    raw: false,
    blankrows: false
  });
  const parsed = rowsToRecords(rows);

  return {
    ...parsed,
    fileType: extension,
    worksheetName,
    worksheets,
    truncated: parsed.records.length >= maxSpreadsheetRows
  };
}

function summarizeCsv(columns, records) {
  const numericColumns = columns
    .map((column) => ({
      column,
      rows: records
        .map((record, index) => ({
          index,
          label: '',
          value: Number(String(record[column]).replace(/[$,%]/g, ''))
        }))
        .filter((entry) => Number.isFinite(entry.value)),
      values: records
        .map((record) => Number(String(record[column]).replace(/[$,%]/g, '')))
        .filter(Number.isFinite)
    }))
    .filter((entry) => entry.values.length > 0);

  const chartColumn = numericColumns[0]?.column ?? columns[0] ?? 'Rows';
  const labelColumn = columns.find((column) => column !== chartColumn) ?? columns[0] ?? 'Rows';
  const chart = records.slice(0, 8).map((record, index) => ({
    label: String(record[labelColumn] || `Row ${index + 1}`),
    value: numericColumns[0]?.values[index] ?? index + 1
  }));
  const numericSummary = numericColumns.slice(0, 4).map(({ column, rows, values }) => {
    const total = values.reduce((sum, value) => sum + value, 0);
    const labeledRows = rows.map((entry) => ({
      ...entry,
      label: String(records[entry.index]?.[labelColumn] || `Row ${entry.index + 1}`)
    }));
    const first = labeledRows[0];
    const last = labeledRows[labeledRows.length - 1];
    const highest = labeledRows.reduce((best, entry) => (entry.value > best.value ? entry : best), labeledRows[0]);
    const lowest = labeledRows.reduce((best, entry) => (entry.value < best.value ? entry : best), labeledRows[0]);
    const change = first && last ? last.value - first.value : 0;
    const changePercent = first?.value ? (change / Math.abs(first.value)) * 100 : 0;

    return {
      column,
      total,
      average: values.length ? total / values.length : 0,
      max: highest?.value ?? 0,
      min: lowest?.value ?? 0,
      highest,
      lowest,
      first,
      last,
      change,
      changePercent
    };
  });

  const primarySummary = numericSummary[0];
  const insightsList = [
    `The file contains ${records.length} rows across ${columns.length} columns.`,
    numericColumns.length > 0
      ? `${numericColumns.length} column${numericColumns.length === 1 ? '' : 's'} look numeric and chart-ready.`
      : 'No strongly numeric columns were detected, so the chart uses row order.',
    primarySummary
      ? `${primarySummary.column} ${primarySummary.change >= 0 ? 'increased' : 'decreased'} by ${formatNumber(Math.abs(primarySummary.change))} from ${primarySummary.first.label} to ${primarySummary.last.label}.`
      : records.length > 100
        ? 'This dataset is large enough to segment before making decisions.'
        : 'This dataset is compact enough for quick review and cleanup.'
  ];

  return { chartColumn, labelColumn, chart, numericSummary, insights: insightsList };
}

function publicDataset(dataset) {
  const { records: _records, userId: _userId, ...rest } = dataset;
  return rest;
}

function dashboardSnapshot(dataset, chartType) {
  return {
    dataset: publicDataset(dataset),
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

function reportLines(dataset) {
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
      ? dataset.numericSummary.map((item) => `${item.column}: total ${Number(item.total).toFixed(2)}, average ${Number(item.average).toFixed(2)}, min ${Number(item.min).toFixed(2)}, max ${Number(item.max).toFixed(2)}`)
      : ['No numeric columns detected.'])
  ];
}

function publicUser(user) {
  return {
    id: user.id,
    companyId: user.companyId,
    name: user.name,
    email: user.email,
    role: user.role,
    active: user.active !== false,
    emailVerified: user.emailVerified === true,
    profilePhotoUrl: user.profilePhotoUrl ?? '',
    notificationSettings: user.notificationSettings ?? {},
    preferences: user.preferences ?? {},
    twoFactorEnabled: user.twoFactorEnabled === true,
    lastLoginAt: user.lastLoginAt,
    createdAt: user.createdAt,
    assignedCompanies: user.assignedCompanies ?? []
  };
}

function roleForEmail(email) {
  const normalizedEmail = email.toLowerCase();
  if (normalizedEmail === ownerEmail) {
    return 'owner';
  }
  return adminEmails.includes(normalizedEmail) ? 'admin' : 'employee';
}

function base64Url(input) {
  return Buffer.from(input).toString('base64url');
}

function signJwt(payload, expiresAt = new Date(Date.now() + sessionTtlMs)) {
  const header = { alg: 'HS256', typ: 'JWT' };
  const body = {
    ...payload,
    exp: Math.floor(new Date(expiresAt).getTime() / 1000)
  };
  const unsigned = `${base64Url(JSON.stringify(header))}.${base64Url(JSON.stringify(body))}`;
  const signature = createHmac('sha256', jwtSecret).update(unsigned).digest('base64url');
  return `${unsigned}.${signature}`;
}

async function createLoginSession(user) {
  const session = await createSession({
    id: randomUUID(),
    userId: user.id,
    expiresAt: new Date(Date.now() + sessionTtlMs).toISOString()
  });
  const token = signJwt(
    {
      sub: user.id,
      sid: session.id,
      companyId: user.companyId,
      email: user.email,
      name: user.name,
      role: user.role
    },
    session.expiresAt
  );
  return { token, session };
}

function verifyJwt(token) {
  const [encodedHeader, encodedPayload, signature] = token.split('.');
  if (!encodedHeader || !encodedPayload || !signature) {
    throw new Error('Invalid token');
  }

  const unsigned = `${encodedHeader}.${encodedPayload}`;
  const expected = createHmac('sha256', jwtSecret).update(unsigned).digest();
  const actual = Buffer.from(signature, 'base64url');
  if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) {
    throw new Error('Invalid token');
  }

  const payload = JSON.parse(Buffer.from(encodedPayload, 'base64url').toString('utf8'));
  if (!payload.exp || payload.exp < Math.floor(Date.now() / 1000)) {
    throw new Error('Token expired');
  }

  return payload;
}

async function hashPassword(password) {
  const salt = randomBytes(16).toString('base64url');
  const hash = await scryptAsync(password, salt, 64);
  return `scrypt:${salt}:${Buffer.from(hash).toString('base64url')}`;
}

async function verifyPassword(password, passwordHash) {
  const [algorithm, salt, storedHash] = passwordHash.split(':');
  if (algorithm !== 'scrypt' || !salt || !storedHash) {
    return false;
  }

  const candidate = await scryptAsync(password, salt, 64);
  const stored = Buffer.from(storedHash, 'base64url');
  return stored.length === candidate.length && timingSafeEqual(stored, candidate);
}

async function requireAuth(req, res, next) {
  if (authDisabled) {
    req.user = getDevUser();
    req.user.accessibleCompanyIds = await getAccessibleCompanyIds(req.user);
    next();
    return;
  }

  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';
    if (!token) {
      res.status(401).json({ error: 'Authentication required.', code: 'AUTH_REQUIRED' });
      return;
    }

    let payload;
    try {
      payload = verifyJwt(token);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Invalid token';
      res.status(401).json({
        error: message === 'Token expired' ? 'Session expired. Please sign in again.' : 'Authentication required.',
        code: message === 'Token expired' ? 'SESSION_EXPIRED' : 'INVALID_TOKEN'
      });
      return;
    }
    const storedUser = await findUserById(payload.sub);
    const session = payload.sid ? await findSessionById(payload.sid, payload.sub) : undefined;
    const user = storedUser ?? (!usingPostgres ? userFromToken(payload) : undefined);

    if (!user || user.active === false || (requireStoredSessions && !session)) {
      res.status(401).json({ error: 'Session expired. Please sign in again.', code: 'SESSION_EXPIRED' });
      return;
    }

    req.user = user;
    req.user.accessibleCompanyIds = await getAccessibleCompanyIds(user);
    req.session = session;
    next();
  } catch {
    res.status(401).json({ error: 'Authentication required.', code: 'AUTH_REQUIRED' });
  }
}

function userFromToken(payload) {
  if (!payload?.sub || !payload?.email) {
    return undefined;
  }

  return {
    id: payload.sub,
    companyId: payload.companyId,
    name: payload.name || payload.email,
    email: payload.email,
    role: roles.includes(payload.role) ? payload.role : 'employee',
    active: true
  };
}

function requireRole(role) {
  return (req, res, next) => {
    if (!hasRole(req.user, role)) {
      res.status(403).json({ error: 'Insufficient workspace permissions.' });
      return;
    }

    next();
  };
}

function canManageCompany(user, companyId) {
  return hasRole(user, 'admin') || (hasRole(user, 'manager') && Array.isArray(user.accessibleCompanyIds) && user.accessibleCompanyIds.includes(companyId));
}

function canManageDataset(user, dataset) {
  return hasRole(user, 'admin') || (hasRole(user, 'manager') && Array.isArray(user.accessibleCompanyIds) && user.accessibleCompanyIds.includes(dataset.companyId));
}

async function requireCompanyAccess(req, res, companyId) {
  if (!companyId) return true;
  if (!(await canAccessCompany(req.user, companyId))) {
    res.status(403).json({ error: 'Company workspace is not assigned to this account.', code: 'COMPANY_FORBIDDEN', requestId: req.requestId });
    return false;
  }
  return true;
}

function hashToken(token) {
  return createHmac('sha256', jwtSecret).update(token).digest('base64url');
}

function issueAccountToken() {
  return randomBytes(32).toString('base64url');
}

async function audit(req, action, targetType, targetId, metadata = {}) {
  await saveAuditLog({
    id: randomUUID(),
    actorUserId: req.user?.id,
    actorEmail: req.user?.email ?? req.body?.email,
    action,
    targetType,
    targetId,
    metadata
  });
}

async function notifyWorkspace({ companyId, userId, type, title, message, metadata = {} }) {
  await saveNotification({
    id: randomUUID(),
    companyId,
    userId,
    type,
    title,
    message,
    metadata
  });
}

function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email || '').trim());
}

function validateCompanyInput(body) {
  const company = {
    name: String(body?.name || '').trim(),
    industry: String(body?.industry || '').trim(),
    ownerName: String(body?.ownerName || body?.owner_name || '').trim(),
    email: String(body?.email || '').trim().toLowerCase(),
    phone: String(body?.phone || '').trim()
  };

  if (!company.name || !company.industry || !company.ownerName || !company.email || !company.phone) {
    const error = new Error('Company name, industry, owner name, email, and phone are required.');
    error.statusCode = 400;
    throw error;
  }

  if (!isValidEmail(company.email)) {
    const error = new Error('Enter a valid company email address.');
    error.statusCode = 400;
    throw error;
  }

  if (company.name.length > 160 || company.industry.length > 120 || company.ownerName.length > 120 || company.phone.length > 40) {
    const error = new Error('Company fields are too long.');
    error.statusCode = 400;
    throw error;
  }

  return company;
}

function requestedCompanyId(req) {
  return String(req.query?.companyId || req.body?.companyId || '').trim();
}

async function sendEmail({ to, subject, text, replyTo, attachments, from }) {
  if (!process.env.RESEND_API_KEY) {
    const error = 'RESEND_API_KEY is missing.';
    console.error(`Email send failed: ${error}`);
    return { delivered: false, provider: 'not-configured', error };
  }

  const response = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${process.env.RESEND_API_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      from: from || emailFrom,
      to,
      subject,
      text,
      ...(replyTo ? { reply_to: replyTo } : {}),
      ...(attachments?.length ? { attachments } : {})
    })
  });

  if (!response.ok) {
    const details = await response.text().catch(() => '');
    console.error('Resend email send failed:', {
      status: response.status,
      to,
      from: from || emailFrom,
      subject,
      details
    });
    throw new Error(details || 'Email provider rejected the message.');
  }

  const payload = await response.json().catch(() => ({}));
  console.log('Resend email sent:', {
    id: payload.id ?? 'accepted',
    to,
    from: from || emailFrom,
    subject
  });
  return { delivered: true, provider: 'resend', id: payload.id };
}

async function deliverEmail({ type, to, subject, text, userId, companyId, replyTo, attachments, from }) {
  let result;
  let status = 'sent';
  let error = null;

  try {
    result = await sendEmail({ to, subject, text, replyTo, attachments, from });
    if (!result.delivered) {
      status = 'failed';
      error = result.error || 'Email delivery is not configured.';
    }
  } catch (deliveryError) {
    result = { delivered: false, provider: resendConfigured ? 'resend' : 'not-configured' };
    status = 'failed';
    error = deliveryError instanceof Error ? deliveryError.message : 'Email delivery failed.';
  }

  const log = await saveEmailLog({
    id: randomUUID(),
    companyId,
    userId,
    emailType: type,
    recipient: to,
    subject,
    body: text,
    provider: result.provider,
    status,
    error,
    attempts: 1
  });

  return { ...result, status, error, log };
}

function requireDurableStorage(req, res, next) {
  const dbStatus = getDatabaseRuntimeStatus();
  if (isEphemeralProductionStorage) {
    res.status(503).json({
      error: 'Protected workspace storage must be connected before saving changes in production.',
      code: 'DURABLE_STORAGE_REQUIRED'
    });
    return;
  }
  if (dbStatus.usingPostgres && !dbStatus.connected) {
    res.status(503).json({
      error: 'PostgreSQL storage is not connected. Please check the production database configuration.',
      code: 'POSTGRESQL_UNAVAILABLE'
    });
    return;
  }

  next();
}

async function canManageTargetUser(actor, target) {
  if (!target) return false;
  if (actor.email === ownerEmail || actor.role === 'owner' || actor.role === 'admin') return true;
  if (!hasRole(actor, 'manager') || !Array.isArray(actor.accessibleCompanyIds)) return false;
  const assignments = await listUserCompanyAssignments(target.id);
  const targetCompanyIds = new Set([target.companyId, ...assignments.map((assignment) => assignment.companyId)].filter(Boolean));
  return [...targetCompanyIds].some((companyId) => actor.accessibleCompanyIds.includes(companyId));
}

function canAssignRole(actor, role) {
  if (role === 'owner') {
    return actor.email === ownerEmail;
  }
  if (role === 'admin') {
    return hasRole(actor, 'admin');
  }
  if (role === 'manager') {
    return hasRole(actor, 'admin');
  }
  return hasRole(actor, 'manager');
}

function roleLabel(role) {
  return {
    owner: 'Owner / Super Admin',
    admin: 'Company Admin',
    manager: 'Manager',
    employee: 'Employee',
    viewer: 'Viewer'
  }[role] || 'Employee';
}

function formatNumber(value) {
  return Number(value.toFixed(2)).toLocaleString();
}

function formatPercent(value) {
  return `${Math.abs(value).toFixed(1)}%`;
}

function pickSummary(dataset, question) {
  const normalized = question.toLowerCase();
  const summary =
    dataset.numericSummary?.find((summary) => normalized.includes(summary.column.toLowerCase())) ??
    dataset.numericSummary?.[0];

  return enrichSummary(dataset, summary);
}

function enrichSummary(dataset, summary) {
  if (!summary) {
    return undefined;
  }

  if (summary.highest && summary.lowest && summary.first && summary.last) {
    return summary;
  }

  const labelColumn = dataset.labelColumn ?? dataset.headers.find((column) => column !== summary.column) ?? dataset.headers[0];
  const rows = (dataset.records ?? dataset.preview ?? [])
    .map((record, index) => ({
      index,
      label: String(record[labelColumn] || `Row ${index + 1}`),
      value: Number(String(record[summary.column]).replace(/[$,%]/g, ''))
    }))
    .filter((entry) => Number.isFinite(entry.value));

  if (rows.length === 0) {
    return summary;
  }

  const first = rows[0];
  const last = rows[rows.length - 1];
  const highest = rows.reduce((best, entry) => (entry.value > best.value ? entry : best), rows[0]);
  const lowest = rows.reduce((best, entry) => (entry.value < best.value ? entry : best), rows[0]);
  const change = last.value - first.value;

  return {
    ...summary,
    highest,
    lowest,
    first,
    last,
    change,
    changePercent: first.value ? (change / Math.abs(first.value)) * 100 : 0
  };
}

function describeTrend(summary) {
  if (!summary?.first || !summary?.last) {
    return 'There is not enough numeric data to describe a trend yet.';
  }

  if (summary.change === 0) {
    return `${summary.column} was flat at ${formatNumber(summary.last.value)} from ${summary.first.label} to ${summary.last.label}.`;
  }

  const direction = summary.change > 0 ? 'increased' : 'decreased';
  const tone = summary.change > 0 ? 'positive momentum' : 'a softening trend';
  return `${summary.column} ${direction} by ${formatNumber(Math.abs(summary.change))} (${formatPercent(summary.changePercent)}) from ${summary.first.label} to ${summary.last.label}, signaling ${tone}.`;
}

function executiveSummary(dataset) {
  const primary = enrichSummary(dataset, dataset.numericSummary?.[0]);
  if (!primary) {
    return `${dataset.fileName} has ${dataset.rows} records across ${dataset.columns} fields. The data is mostly categorical, so the next best step is to group recurring values and inspect segment concentration.`;
  }

  const direction = primary.change >= 0 ? 'upward' : 'downward';
  const peakLine = primary.highest && primary.lowest
    ? `Peak performance appears at ${primary.highest.label} (${formatNumber(primary.highest.value)}), while the low point is ${primary.lowest.label} (${formatNumber(primary.lowest.value)}).`
    : `The average ${primary.column} value is ${formatNumber(primary.average)}.`;
  return [
    `${dataset.fileName} shows ${direction} movement in ${primary.column}: ${describeTrend(primary)}`,
    peakLine,
    dataset.numericSummary.length > 1
      ? `Secondary metrics to watch: ${dataset.numericSummary.slice(1, 3).map((item) => item.column).join(', ')}.`
      : 'The main management focus should be validating what drove this movement.'
  ].join(' ');
}

function answerDatasetQuestion(dataset, question) {
  const normalized = question.toLowerCase();
  const summary = pickSummary(dataset, question);

  if (normalized.includes('row') || normalized.includes('many')) {
    return `${dataset.fileName} has ${dataset.rows} rows and ${dataset.columns} columns.`;
  }

  if (normalized.includes('column') || normalized.includes('field')) {
    return `The columns are ${dataset.headers.join(', ')}.`;
  }

  if (normalized.includes('summary') || normalized.includes('summarize') || normalized.includes('overview') || normalized.includes('executive')) {
    return executiveSummary(dataset);
  }

  if (normalized.includes('trend') || normalized.includes('performance') || normalized.includes('movement')) {
    return describeTrend(summary);
  }

  if (normalized.includes('increase') || normalized.includes('up') || normalized.includes('growth')) {
    if (!summary) {
      return 'I do not see a numeric metric to evaluate increases yet.';
    }
    return summary.change > 0
      ? `${summary.column} increased from ${formatNumber(summary.first.value)} at ${summary.first.label} to ${formatNumber(summary.last.value)} at ${summary.last.label}, a gain of ${formatNumber(summary.change)} (${formatPercent(summary.changePercent)}).`
      : `${summary.column} did not increase overall; it decreased by ${formatNumber(Math.abs(summary.change))} from ${summary.first.label} to ${summary.last.label}.`;
  }

  if (normalized.includes('decrease') || normalized.includes('down') || normalized.includes('decline') || normalized.includes('drop')) {
    if (!summary) {
      return 'I do not see a numeric metric to evaluate decreases yet.';
    }
    return summary.change < 0
      ? `${summary.column} decreased from ${formatNumber(summary.first.value)} at ${summary.first.label} to ${formatNumber(summary.last.value)} at ${summary.last.label}, a decline of ${formatNumber(Math.abs(summary.change))} (${formatPercent(summary.changePercent)}).`
      : `${summary.column} did not decrease overall; it increased by ${formatNumber(summary.change)} from ${summary.first.label} to ${summary.last.label}.`;
  }

  if ((normalized.includes('total') || normalized.includes('sum')) && summary) {
    return `Total ${summary.column} is ${formatNumber(summary.total)} across ${dataset.rows} rows.`;
  }

  if ((normalized.includes('average') || normalized.includes('mean')) && summary) {
    return `Average ${summary.column} is ${formatNumber(summary.average)}. Use this as the baseline when comparing individual periods or segments.`;
  }

  if ((normalized.includes('highest') || normalized.includes('max') || normalized.includes('largest') || normalized.includes('best')) && summary) {
    return `Highest ${summary.column} is ${formatNumber(summary.highest.value)} at ${summary.highest.label}. That is the strongest point in this dataset.`;
  }

  if ((normalized.includes('lowest') || normalized.includes('min') || normalized.includes('smallest') || normalized.includes('worst')) && summary) {
    return `Lowest ${summary.column} is ${formatNumber(summary.lowest.value)} at ${summary.lowest.label}. That is the key period or segment to investigate.`;
  }

  return executiveSummary(dataset);
}

async function importLegacyDatasets() {
  if (!existsSync(legacyDatasetsFile)) {
    return;
  }

  const legacy = JSON.parse(await readFile(legacyDatasetsFile, 'utf8'));
  const datasets = Array.isArray(legacy) ? legacy : [legacy];
  await Promise.all(datasets.map((dataset) => saveDataset({
    ...dataset,
    userId: dataset.userId ?? getDevUser().id,
    companyId: dataset.companyId ?? getDevUser().companyId
  })));
}

app.use(cors({
  origin(origin, callback) {
    if (!origin || corsOrigins.has(origin)) {
      callback(null, true);
      return;
    }
    callback(new Error(`CORS origin not allowed: ${origin}`));
  },
  credentials: true
}));
app.use((req, res, next) => {
  const requestId = req.headers['x-request-id'] || randomUUID();
  const startedAt = Date.now();
  req.requestId = requestId;
  res.setHeader('X-Request-Id', requestId);
  res.on('finish', () => {
    const durationMs = Date.now() - startedAt;
    if (req.path.startsWith('/api')) {
      console.log(JSON.stringify({
        level: res.statusCode >= 500 ? 'error' : 'info',
        requestId,
        method: req.method,
        path: req.path,
        statusCode: res.statusCode,
        durationMs
      }));
    }
  });
  next();
});
app.use(express.json());

app.get('/api/health', (_req, res) => {
  res.json({
    status: 'ok',
    service: 'metenova-business-platform',
    timestamp: new Date().toISOString(),
    uptimeSeconds: Math.round(process.uptime())
  });
});

app.get('/api/readiness', (_req, res) => {
  const dbStatus = getDatabaseRuntimeStatus();
  const ready = !dbStatus.usingPostgres || (dbStatus.connected && dbStatus.tablesInitialized);
  res.status(ready ? 200 : 503).json({
    ready,
    database: dbStatus,
    emailConfigured,
    durableStorage: !isEphemeralProductionStorage,
    timestamp: new Date().toISOString()
  });
});

app.get('/api/diagnostics', requireAuth, requireRole('admin'), (_req, res) => {
  const dbStatus = getDatabaseRuntimeStatus();
  res.json({
    environment: process.env.VERCEL === '1' ? 'vercel-production' : process.env.NODE_ENV || 'local',
    database: dbStatus,
    emailConfigured,
    authDisabled,
    uploadLimitMb: Math.round(maxUploadBytes / 1024 / 1024),
    maxSpreadsheetRows,
    timestamp: new Date().toISOString()
  });
});

function authRateLimit(req, res, next) {
  const key = `${req.ip}:${String(req.body?.email || '').toLowerCase()}`;
  const now = Date.now();
  const windowMs = 15 * 60 * 1000;
  const entry = authAttempts.get(key) ?? { count: 0, resetAt: now + windowMs };

  if (entry.resetAt <= now) {
    entry.count = 0;
    entry.resetAt = now + windowMs;
  }

  entry.count += 1;
  authAttempts.set(key, entry);

  if (entry.count > 20) {
    res.status(429).json({ error: 'Too many authentication attempts. Try again later.' });
    return;
  }

  next();
}

app.get('/health', (_req, res) => {
  const dbStatus = getDatabaseRuntimeStatus();
  res.json({
    status: 'ok',
    service: 'business-ai-platform-backend',
    mode: authDisabled ? 'auth-disabled' : usingPostgres ? 'secure-postgresql' : 'local-auth',
    durableStorage: dbStatus.usingPostgres && dbStatus.connected,
    postgresqlConnected: dbStatus.usingPostgres && dbStatus.connected,
    tablesInitialized: dbStatus.tablesInitialized,
    databaseError: dbStatus.connectionError,
    emailConfigured,
    resendConfigured,
    environment: process.env.VERCEL === '1' ? 'vercel-production' : process.env.NODE_ENV || 'local',
    apiUrl
  });
});

app.get('/api/config', (_req, res) => {
  const dbStatus = getDatabaseRuntimeStatus();
  res.cookie?.('metenova_csrf', csrfToken, {
    httpOnly: false,
    sameSite: 'strict',
    secure: process.env.NODE_ENV === 'production'
  });
  res.json({
    authDisabled,
    storage: usingPostgres ? 'postgresql' : 'local-development',
    durableStorage: dbStatus.usingPostgres && dbStatus.connected,
    postgresqlConnected: dbStatus.usingPostgres && dbStatus.connected,
    emailConfigured,
    sessionTimeoutMinutes: Math.max(sessionTtlMinutes, 5),
    sessionWarningSeconds: warningSeconds,
    csrfToken
  });
});

app.post('/api/auth/signup', authRateLimit, async (req, res, next) => {
  try {
    const name = String(req.body?.name || '').trim();
    const email = String(req.body?.email || '').trim().toLowerCase();
    const password = String(req.body?.password || '');

    if (!name || !email || password.length < 8) {
      res.status(400).json({ error: 'Name, email, and an 8+ character password are required.' });
      return;
    }

    const existing = await findUserByEmail(email);
    if (existing) {
      res.status(409).json({ error: 'An account with this email already exists.' });
      return;
    }

    const user = await createUser({
      id: randomUUID(),
      name,
      email,
      role: roleForEmail(email),
      emailVerified: email === ownerEmail,
      passwordHash: await hashPassword(password)
    });
    await saveAuditLog({
      id: randomUUID(),
      actorUserId: user.id,
      actorEmail: user.email,
      action: 'auth.signup',
      targetType: 'user',
      targetId: user.id,
      metadata: { role: user.role }
    });
    const { token, session } = await createLoginSession(user);
    res.status(201).json({ token, user: publicUser(user), session });
  } catch (error) {
    next(error);
  }
});

app.post('/api/auth/accept-invite', authRateLimit, async (req, res, next) => {
  try {
    const token = String(req.body?.token || '').trim();
    const name = String(req.body?.name || '').trim();
    const password = String(req.body?.password || '');
    const invitation = token ? await findInvitationByToken(hashToken(token)) : undefined;

    if (!invitation || !name || password.length < 8) {
      res.status(400).json({ error: 'A valid invite, name, and 8+ character password are required.' });
      return;
    }

    let user = await findUserByEmail(invitation.email);
    if (user) {
      user = await updateUser(user.id, {
        name,
        role: invitation.role,
        active: true,
        emailVerified: true
      });
      await updateUserPassword(user.id, await hashPassword(password));
    } else {
      user = await createUser({
        id: randomUUID(),
        companyId: invitation.companyId,
        name,
        email: invitation.email,
        role: invitation.role,
        active: true,
        emailVerified: true,
        passwordHash: await hashPassword(password)
      });
    }

    await markInvitationAccepted(invitation.id);
    await audit({ user, body: { email: user.email } }, 'auth.invitation_accepted', 'invitation', invitation.id, { role: user.role });
    const sessionPayload = await createLoginSession(user);
    res.status(201).json({ token: sessionPayload.token, user: publicUser(user), session: sessionPayload.session });
  } catch (error) {
    next(error);
  }
});

app.post('/api/auth/forgot-password', authRateLimit, async (req, res, next) => {
  try {
    const email = String(req.body?.email || '').trim().toLowerCase();
    const user = email ? await findUserByEmail(email) : undefined;
    let resetUrl;

    if (user) {
      const token = issueAccountToken();
      await createAccountToken({
        id: randomUUID(),
        userId: user.id,
        tokenHash: hashToken(token),
        purpose: 'password_reset',
        expiresAt: new Date(Date.now() + 60 * 60 * 1000).toISOString()
      });
      resetUrl = `${appBaseUrl}/reset-password?token=${encodeURIComponent(token)}`;
      await deliverEmail({
        type: 'password_reset',
        to: user.email,
        subject: 'Reset your Metenova AI password',
        text: `Use this secure link to reset your password: ${resetUrl}`,
        userId: user.id,
        companyId: user.companyId
      });
      await saveAuditLog({
        id: randomUUID(),
        actorEmail: email,
        action: 'auth.password_reset_requested',
        targetType: 'user',
        targetId: user.id,
        metadata: { delivery: 'email-ready' }
      });
    }

    res.json({
      message: 'If the account exists, password reset instructions have been prepared.',
      ...(resetUrl && !emailConfigured ? { resetUrl } : {})
    });
  } catch (error) {
    next(error);
  }
});

app.post('/api/auth/reset-password', async (req, res, next) => {
  try {
    const token = String(req.body?.token || '').trim();
    const password = String(req.body?.password || '');

    if (!token || password.length < 8) {
      res.status(400).json({ error: 'A valid reset token and 8+ character password are required.' });
      return;
    }

    const accountToken = await findAccountToken(hashToken(token), 'password_reset');
    if (!accountToken) {
      res.status(400).json({ error: 'Reset token is invalid or expired.' });
      return;
    }

    await updateUserPassword(accountToken.userId, await hashPassword(password));
    await revokeUserSessions(accountToken.userId);
    await markAccountTokenUsed(accountToken.id);
    res.json({ message: 'Password reset complete. Please log in again.' });
  } catch (error) {
    next(error);
  }
});

app.post('/api/auth/recover-username', async (req, res, next) => {
  try {
    const email = String(req.body?.email || '').trim().toLowerCase();
    const user = email ? await findUserByEmail(email) : undefined;
    if (user) {
      await deliverEmail({
        type: 'username_recovery',
        to: user.email,
        subject: 'Your Metenova AI username',
        text: `Your Metenova AI username is ${user.email}.`,
        userId: user.id,
        companyId: user.companyId
      });
    }
    res.json({
      message: 'If the account exists, username recovery instructions have been prepared.',
      ...(user && !emailConfigured ? { username: user.email, name: user.name } : {})
    });
  } catch (error) {
    next(error);
  }
});

app.post('/api/auth/request-verification', requireAuth, async (req, res, next) => {
  try {
    const token = issueAccountToken();
    await createAccountToken({
      id: randomUUID(),
      userId: req.user.id,
      tokenHash: hashToken(token),
      purpose: 'email_verification',
      expiresAt: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString()
    });
    const verificationUrl = `${appBaseUrl}/verify-email?token=${encodeURIComponent(token)}`;
    const delivery = await deliverEmail({
      type: 'email_verification',
      to: req.user.email,
      subject: 'Verify your Metenova AI email',
      text: `Verify your email address here: ${verificationUrl}`,
      userId: req.user.id,
      companyId: req.user.companyId
    });
    res.json({
      message: delivery.delivered ? 'Verification email sent.' : 'Verification request was saved, but delivery could not be completed.',
      ...(!emailConfigured ? { verificationUrl } : {})
    });
  } catch (error) {
    next(error);
  }
});

app.post('/api/auth/verify-email', async (req, res, next) => {
  try {
    const token = String(req.body?.token || '').trim();
    const accountToken = await findAccountToken(hashToken(token), 'email_verification');
    if (!accountToken) {
      res.status(400).json({ error: 'Verification token is invalid or expired.' });
      return;
    }

    await updateUser(accountToken.userId, { emailVerified: true });
    await markAccountTokenUsed(accountToken.id);
    res.json({ message: 'Email verified.' });
  } catch (error) {
    next(error);
  }
});

app.post('/api/auth/login', authRateLimit, async (req, res, next) => {
  try {
    const email = String(req.body?.email || '').trim().toLowerCase();
    const password = String(req.body?.password || '');
    const user = await findUserByEmail(email);

    if (user?.lockedUntil && new Date(user.lockedUntil).getTime() > Date.now()) {
      res.status(423).json({ error: 'Account temporarily locked after repeated failed logins. Try again later.' });
      return;
    }

    if (!user || !(await verifyPassword(password, user.passwordHash))) {
      await recordLoginFailure(email);
      await saveAuditLog({
        id: randomUUID(),
        actorEmail: email,
        action: 'auth.login_failed',
        targetType: 'user',
        targetId: user?.id,
        metadata: { reason: 'invalid_credentials' }
      });
      res.status(401).json({ error: 'Invalid email or password.' });
      return;
    }

    if (user.active === false) {
      await saveAuditLog({
        id: randomUUID(),
        actorUserId: user.id,
        actorEmail: user.email,
        action: 'auth.login_blocked',
        targetType: 'user',
        targetId: user.id,
        metadata: { reason: 'disabled_account' }
      });
      res.status(403).json({ error: 'This account is disabled.' });
      return;
    }

    await recordLoginSuccess(user.id);
    const { token, session } = await createLoginSession(user);
    await saveAuditLog({
      id: randomUUID(),
      actorUserId: user.id,
      actorEmail: user.email,
      action: 'auth.login',
      targetType: 'user',
      targetId: user.id,
      metadata: { role: user.role }
    });
    res.json({ token, user: publicUser(user), session });
  } catch (error) {
    next(error);
  }
});

app.get('/api/auth/me', requireAuth, (req, res) => {
  res.json({ user: publicUser(req.user) });
});

app.post('/api/auth/refresh', requireAuth, async (req, res, next) => {
  try {
    if (req.session?.id) {
      await revokeSession(req.session.id, req.user.id);
    }
    const { token, session } = await createLoginSession(req.user);
    res.json({ token, user: publicUser(req.user), session });
  } catch (error) {
    next(error);
  }
});

app.get('/api/auth/sessions', requireAuth, async (req, res, next) => {
  try {
    res.json({ sessions: await listSessions(req.user.id) });
  } catch (error) {
    next(error);
  }
});

app.get('/api/admin/workspace', requireAuth, requireRole('manager'), async (req, res, next) => {
  try {
    const [dashboards, reports] = await Promise.all([
      listDashboards(req.user),
      listReports(req.user)
    ]);
    res.json({
      role: req.user.role,
      totals: {
        dashboards: dashboards.length,
        reports: reports.length
      }
    });
  } catch (error) {
    next(error);
  }
});

app.get('/api/admin/users', requireAuth, requireRole('manager'), async (req, res, next) => {
  try {
    res.json({ users: (await listUsers(req.user)).map(publicUser) });
  } catch (error) {
    next(error);
  }
});

app.patch('/api/admin/users/:id', requireAuth, requireRole('manager'), requireDurableStorage, async (req, res, next) => {
  try {
    const updates = {};

    if (req.body?.role != null) {
      const requestedRole = String(req.body.role);
      if (!canAssignRole(req.user, requestedRole)) {
        res.status(403).json({ error: 'Only the permanent owner can assign owner permissions.' });
        return;
      }
      updates.role = roles.includes(requestedRole) ? requestedRole : 'employee';
    }

    if (req.body?.active != null) {
      updates.active = Boolean(req.body.active);
    }

    if (updates.active === false && req.params.id === req.user.id) {
      res.status(400).json({ error: 'You cannot disable your own active account.' });
      return;
    }

    const existing = await findUserById(req.params.id);
    if (!(await canManageTargetUser(req.user, existing))) {
      res.status(403).json({ error: 'You can only manage users in your company workspace.' });
      return;
    }
    if (existing?.email === ownerEmail && req.user.email !== ownerEmail) {
      res.status(403).json({ error: 'Owner permissions cannot be changed by another account.' });
      return;
    }

    const user = await updateUser(req.params.id, updates);
    if (!user) {
      res.status(404).json({ error: 'User not found.' });
      return;
    }

    if (user.id !== req.user.id && (updates.role != null || updates.active === false)) {
      await revokeUserSessions(user.id);
    }
    res.json({ user: publicUser(user) });
    await audit(req, 'admin.user_updated', 'user', user.id, updates);
  } catch (error) {
    next(error);
  }
});

app.get('/api/admin/users/:id/company-assignments', requireAuth, requireRole('manager'), async (req, res, next) => {
  try {
    const existing = await findUserById(req.params.id);
    if (!(await canManageTargetUser(req.user, existing))) {
      res.status(403).json({ error: 'You can only manage users assigned to your company workspace.', code: 'COMPANY_FORBIDDEN', requestId: req.requestId });
      return;
    }
    res.json({ assignments: await listUserCompanyAssignments(req.params.id) });
  } catch (error) {
    next(error);
  }
});

app.put('/api/admin/users/:id/company-assignments', requireAuth, requireRole('manager'), requireDurableStorage, async (req, res, next) => {
  try {
    const existing = await findUserById(req.params.id);
    if (!existing) {
      res.status(404).json({ error: 'User not found.' });
      return;
    }
    if (existing.email === ownerEmail && req.user.email !== ownerEmail) {
      res.status(403).json({ error: 'Owner company access cannot be changed by another account.' });
      return;
    }
    if (!(await canManageTargetUser(req.user, existing))) {
      res.status(403).json({ error: 'You can only manage users assigned to your company workspace.', code: 'COMPANY_FORBIDDEN', requestId: req.requestId });
      return;
    }

    const requestedAssignments = Array.isArray(req.body?.assignments) ? req.body.assignments : [];
    const allowedCompanyIds = await getAccessibleCompanyIds(req.user);
    const normalizedAssignments = requestedAssignments
      .map((assignment) => ({
        companyId: String(assignment.companyId || '').trim(),
        role: roles.includes(assignment.role) ? assignment.role : existing.role
      }))
      .filter((assignment) => assignment.companyId);

    if (allowedCompanyIds !== null && normalizedAssignments.some((assignment) => !allowedCompanyIds.includes(assignment.companyId))) {
      res.status(403).json({ error: 'Company workspace is not assigned to this account.', code: 'COMPANY_FORBIDDEN', requestId: req.requestId });
      return;
    }
    if (normalizedAssignments.some((assignment) => assignment.role === 'owner' && req.user.email !== ownerEmail)) {
      res.status(403).json({ error: 'Only the permanent owner can assign owner company access.' });
      return;
    }

    const assignments = await replaceUserCompanyAssignments(req.params.id, normalizedAssignments);
    const updatedUser = await findUserById(req.params.id);
    const responseUser = { ...updatedUser, assignedCompanies: assignments };
    await audit(req, 'admin.company_assignments_updated', 'user', req.params.id, {
      companyIds: assignments.map((assignment) => assignment.companyId)
    });
    res.json({ user: publicUser(responseUser), assignments });
  } catch (error) {
    next(error);
  }
});

app.delete('/api/admin/users/:id', requireAuth, requireRole('admin'), async (req, res, next) => {
  try {
    if (req.params.id === req.user.id) {
      res.status(400).json({ error: 'You cannot delete your own active account.' });
      return;
    }

    const existing = await findUserById(req.params.id);
    if (!(await canManageTargetUser(req.user, existing))) {
      res.status(403).json({ error: 'You can only manage users in your company workspace.' });
      return;
    }
    if (existing?.email === ownerEmail) {
      res.status(403).json({ error: 'The permanent owner account cannot be deleted.' });
      return;
    }

    await deleteUser(req.params.id);
    await audit(req, 'admin.user_deleted', 'user', req.params.id);
    res.status(204).end();
  } catch (error) {
    next(error);
  }
});

app.post('/api/admin/users/:id/revoke', requireAuth, requireRole('manager'), async (req, res, next) => {
  try {
    if (req.params.id === req.user.id) {
      res.status(400).json({ error: 'Use logout to end your current session.' });
      return;
    }

    const existing = await findUserById(req.params.id);
    if (!(await canManageTargetUser(req.user, existing))) {
      res.status(403).json({ error: 'You can only manage users in your company workspace.' });
      return;
    }

    await revokeUserSessions(req.params.id);
    await audit(req, 'admin.sessions_revoked', 'user', req.params.id);
    res.json({ revoked: true });
  } catch (error) {
    next(error);
  }
});

app.get('/api/admin/invitations', requireAuth, requireRole('admin'), async (req, res, next) => {
  try {
    res.json({ invitations: await listInvitations(req.user) });
  } catch (error) {
    next(error);
  }
});

app.post('/api/admin/invitations', requireAuth, requireRole('admin'), requireDurableStorage, async (req, res, next) => {
  try {
    const email = String(req.body?.email || '').trim().toLowerCase();
    const requestedRole = String(req.body?.role || 'employee');
    const role = roles.includes(requestedRole) ? requestedRole : 'employee';

    if (!email.includes('@')) {
      res.status(400).json({ error: 'A valid email is required.' });
      return;
    }

    if (!canAssignRole(req.user, role)) {
      res.status(403).json({ error: 'You cannot assign that role.' });
      return;
    }

    const token = issueAccountToken();
    const invitation = await createInvitation({
      id: randomUUID(),
      companyId: req.user.companyId,
      email,
      role,
      tokenHash: hashToken(token),
      invitedBy: req.user.id,
      expiresAt: new Date(Date.now() + 7 * 24 * 60 * 60 * 1000).toISOString()
    });
    const acceptUrl = `${appBaseUrl}/accept-invite?token=${encodeURIComponent(token)}`;
    const delivery = await deliverEmail({
      type: 'workspace_invitation',
      to: email,
      subject: 'You are invited to Metenova AI',
      text: `${req.user.name} invited you to join their Metenova AI workspace as ${roleLabel(role)}. Accept here: ${acceptUrl}`,
      userId: req.user.id,
      companyId: req.user.companyId,
      replyTo: req.user.email
    });
    await audit(req, 'admin.invitation_created', 'invitation', invitation.id, { email, role, delivery: delivery.status });
    res.status(201).json({
      invitation,
      message: delivery.delivered ? 'Invitation sent.' : 'Invitation created, but email delivery could not be completed.',
      ...(!emailConfigured ? { acceptUrl } : {})
    });
  } catch (error) {
    next(error);
  }
});

app.get('/api/admin/email-logs', requireAuth, requireRole('admin'), async (req, res, next) => {
  try {
    res.json({ emailLogs: await listEmailLogs(req.user) });
  } catch (error) {
    next(error);
  }
});

app.post('/api/admin/email-logs/:id/retry', requireAuth, requireRole('admin'), async (req, res, next) => {
  try {
    const log = await findEmailLog(req.user, req.params.id);
    if (!log) {
      res.status(404).json({ error: 'Email delivery record not found.' });
      return;
    }

    let delivery;
    try {
      delivery = await sendEmail({ to: log.recipient, subject: log.subject, text: log.body });
      if (!delivery.delivered) {
        throw new Error(delivery.error || 'Email delivery could not be completed.');
      }
      const emailLog = await updateEmailLog(log.id, { status: 'sent', provider: delivery.provider, error: null });
      await audit(req, 'admin.email_retry_sent', 'email', log.id, { recipient: log.recipient });
      res.json({ emailLog, message: 'Email resent.' });
    } catch (error) {
      const emailLog = await updateEmailLog(log.id, {
        status: 'failed',
        provider: delivery?.provider ?? (emailConfigured ? 'resend' : 'not-configured'),
        error: error instanceof Error ? error.message : 'Email retry failed.'
      });
      res.status(502).json({ emailLog, error: 'Email retry failed.' });
    }
  } catch (error) {
    next(error);
  }
});

app.post('/api/auth/logout', requireAuth, async (req, res, next) => {
  try {
    if (req.session) {
      await revokeSession(req.session.id, req.user.id);
    }
    res.status(204).end();
  } catch (error) {
    next(error);
  }
});

app.use('/api', requireAuth);
app.use('/api', (req, res, next) => {
  if (authDisabled || ['GET', 'HEAD', 'OPTIONS'].includes(req.method)) {
    next();
    return;
  }
  const provided = req.headers['x-csrf-token'];
  if (provided !== csrfToken) {
    res.status(403).json({ error: 'Invalid CSRF token.', code: 'CSRF_INVALID', requestId: req.requestId });
    return;
  }
  next();
});

app.get('/api/roles', (_req, res) => {
  res.json({
    roles,
    permissions: {
      owner: ['all'],
      admin: ['users.manage', 'reports.view_all', 'modules.manage', 'security.manage'],
      manager: ['team.manage', 'reports.view', 'modules.use'],
      employee: ['modules.use', 'datasets.own'],
      viewer: ['reports.read', 'dashboards.read']
    }
  });
});

app.get('/api/profile', (req, res) => {
  res.json({ user: publicUser(req.user) });
});

app.get('/api/companies', async (req, res, next) => {
  try {
    res.json({ companies: await listCompanies(req.user) });
  } catch (error) {
    next(error);
  }
});

app.post('/api/companies', requireRole('owner'), requireDurableStorage, async (req, res, next) => {
  try {
    const companyInput = validateCompanyInput(req.body);
    const company = await createCompany({
      id: randomUUID(),
      ...companyInput,
      status: 'Active'
    });

    await audit(req, 'company.created', 'company', company.id, {
      name: company.name,
      industry: company.industry,
      email: company.email
    });

    res.status(201).json({ company });
  } catch (error) {
    next(error);
  }
});

app.patch('/api/companies/:id', requireRole('manager'), requireDurableStorage, async (req, res, next) => {
  try {
    if (!canManageCompany(req.user, req.params.id)) {
      res.status(403).json({ error: 'Insufficient company permissions.' });
      return;
    }
    const company = await updateCompany(req.params.id, {
      name: req.body?.name,
      industry: req.body?.industry,
      ownerName: req.body?.ownerName,
      email: req.body?.email,
      phone: req.body?.phone,
      status: req.body?.status
    });
    if (!company) {
      res.status(404).json({ error: 'Company not found.' });
      return;
    }
    await audit(req, 'company.updated', 'company', company.id, { name: company.name });
    res.json({ company });
  } catch (error) {
    next(error);
  }
});

app.delete('/api/companies/:id', requireRole('owner'), requireDurableStorage, async (req, res, next) => {
  try {
    const deleted = await deleteCompany(req.params.id);
    if (!deleted) {
      res.status(404).json({ error: 'Company not found or cannot be deleted.' });
      return;
    }
    await audit(req, 'company.deleted', 'company', req.params.id);
    res.status(204).end();
  } catch (error) {
    next(error);
  }
});

app.get('/api/pipelines', async (req, res, next) => {
  try {
    const companyId = requestedCompanyId(req);
    if (!(await requireCompanyAccess(req, res, companyId))) return;
    res.json({ pipelines: await listPipelines(req.user, companyId) });
  } catch (error) {
    next(error);
  }
});

app.post('/api/pipelines', requireRole('manager'), requireDurableStorage, async (req, res, next) => {
  try {
    const companyId = String(req.body?.companyId || requestedCompanyId(req) || '').trim();
    if (!companyId) {
      res.status(400).json({ error: 'Company workspace is required.' });
      return;
    }
    if (!(await requireCompanyAccess(req, res, companyId))) return;
    if (!canManageCompany(req.user, companyId)) {
      res.status(403).json({ error: 'Insufficient pipeline permissions.', code: 'COMPANY_FORBIDDEN', requestId: req.requestId });
      return;
    }

    const name = String(req.body?.name || '').trim();
    const department = String(req.body?.department || '').trim();
    if (!name || !department) {
      res.status(400).json({ error: 'Pipeline name and department are required.' });
      return;
    }

    const pipeline = await savePipeline({
      id: randomUUID(),
      companyId,
      userId: req.user.id,
      name,
      department,
      status: String(req.body?.status || 'active'),
      metadata: req.body?.metadata ?? {}
    });
    await audit(req, 'pipeline.created', 'pipeline', pipeline.id, { companyId, department });
    res.status(201).json({ pipeline });
  } catch (error) {
    next(error);
  }
});

app.patch('/api/pipelines/:id', requireRole('manager'), requireDurableStorage, async (req, res, next) => {
  try {
    const pipeline = await getPipeline(req.params.id, req.user);
    if (!pipeline) {
      res.status(404).json({ error: 'Pipeline not found.' });
      return;
    }
    if (!canManageCompany(req.user, pipeline.companyId)) {
      res.status(403).json({ error: 'Insufficient pipeline permissions.', code: 'COMPANY_FORBIDDEN', requestId: req.requestId });
      return;
    }
    const updated = await updatePipeline(req.params.id, {
      name: req.body?.name,
      department: req.body?.department,
      status: req.body?.status,
      metadata: req.body?.metadata
    }, req.user);
    await audit(req, 'pipeline.updated', 'pipeline', req.params.id, { companyId: pipeline.companyId });
    res.json({ pipeline: updated });
  } catch (error) {
    next(error);
  }
});

app.delete('/api/pipelines/:id', requireRole('manager'), requireDurableStorage, async (req, res, next) => {
  try {
    const pipeline = await getPipeline(req.params.id, req.user);
    if (!pipeline) {
      res.status(404).json({ error: 'Pipeline not found.' });
      return;
    }
    if (!canManageCompany(req.user, pipeline.companyId)) {
      res.status(403).json({ error: 'Insufficient pipeline permissions.', code: 'COMPANY_FORBIDDEN', requestId: req.requestId });
      return;
    }
    await deletePipeline(req.params.id, req.user);
    await audit(req, 'pipeline.deleted', 'pipeline', req.params.id, { companyId: pipeline.companyId });
    res.status(204).end();
  } catch (error) {
    next(error);
  }
});

app.patch('/api/profile', requireDurableStorage, async (req, res, next) => {
  try {
    const user = await updateUser(req.user.id, {
      name: req.body?.name,
      profilePhotoUrl: req.body?.profilePhotoUrl,
      notificationSettings: req.body?.notificationSettings,
      preferences: req.body?.preferences,
      twoFactorEnabled: req.body?.twoFactorEnabled
    });
    await audit(req, 'profile.updated', 'user', req.user.id);
    res.json({ user: publicUser(user) });
  } catch (error) {
    next(error);
  }
});

app.post('/api/profile/change-password', requireDurableStorage, async (req, res, next) => {
  try {
    const currentPassword = String(req.body?.currentPassword || '');
    const nextPassword = String(req.body?.newPassword || '');
    const user = await findUserByEmail(req.user.email);

    if (!user || !(await verifyPassword(currentPassword, user.passwordHash))) {
      res.status(401).json({ error: 'Current password is incorrect.' });
      return;
    }

    if (nextPassword.length < 8) {
      res.status(400).json({ error: 'New password must be at least 8 characters.' });
      return;
    }

    await updateUserPassword(req.user.id, await hashPassword(nextPassword));
    await revokeUserSessions(req.user.id);
    await audit(req, 'profile.password_changed', 'user', req.user.id);
    res.json({ message: 'Password changed. Please log in again.' });
  } catch (error) {
    next(error);
  }
});

app.get('/api/admin/audit-logs', requireRole('admin'), async (_req, res, next) => {
  try {
    res.json({ auditLogs: await listAuditLogs() });
  } catch (error) {
    next(error);
  }
});

app.get('/api/admin/system', requireRole('admin'), async (_req, res) => {
  const dbStatus = getDatabaseRuntimeStatus();
  res.json({
    status: 'operational',
    storage: usingPostgres ? 'protected-workspace-storage' : 'workspace-storage-pending',
    auth: requireStoredSessions ? 'protected-workspace-sessions' : 'local-session-mode',
    durableStorage: dbStatus.usingPostgres && dbStatus.connected,
    postgresqlConnected: dbStatus.usingPostgres && dbStatus.connected,
    tablesInitialized: dbStatus.tablesInitialized,
    emailConfigured,
    uptimeSeconds: Math.round(process.uptime()),
    uploadLimitMb: Math.round(maxUploadBytes / 1024 / 1024),
    maxSpreadsheetRows
  });
});

app.get('/api/modules/metrics', async (req, res, next) => {
  try {
    res.json({ metrics: await getModuleMetrics(req.user) });
  } catch (error) {
    next(error);
  }
});

app.get('/api/modules/:module/records', async (req, res, next) => {
  try {
    res.json({ records: await listModuleRecords(req.user, req.params.module) });
  } catch (error) {
    next(error);
  }
});

app.post('/api/modules/:module/records', requireDurableStorage, async (req, res, next) => {
  try {
    const title = String(req.body?.title || '').trim();
    const recordType = String(req.body?.recordType || 'item').trim();
    const companyId = String(req.body?.companyId || req.user.companyId || '').trim();

    if (!title) {
      res.status(400).json({ error: 'Title is required.' });
      return;
    }
    if (!(await requireCompanyAccess(req, res, companyId))) return;
    if (!hasRole(req.user, 'employee')) {
      res.status(403).json({ error: 'Insufficient workflow permissions.' });
      return;
    }

    const record = await createModuleRecord({
      id: randomUUID(),
      companyId,
      userId: req.user.id,
      module: req.params.module,
      recordType,
      title,
      status: String(req.body?.status || 'open'),
      amount: req.body?.amount === '' || req.body?.amount == null ? null : Number(req.body.amount),
      metadata: req.body?.metadata ?? {}
    });
    await audit(req, 'module.record_created', req.params.module, record.id, { recordType });
    res.status(201).json({ record });
  } catch (error) {
    next(error);
  }
});

async function updateModuleRecordHandler(req, res, next) {
  try {
    const record = await updateModuleRecord(req.user, req.params.id, {
      title: req.body?.title,
      status: req.body?.status,
      amount: req.body?.amount,
      metadata: req.body?.metadata
    });
    if (!record || record.module !== req.params.module) {
      res.status(404).json({ error: 'Workspace record not found.' });
      return;
    }
    await audit(req, 'module.record_updated', req.params.module, record.id, { status: record.status });
    res.json({ record });
  } catch (error) {
    next(error);
  }
}

app.patch('/api/modules/:module/records/:id', requireDurableStorage, updateModuleRecordHandler);
app.put('/api/modules/:module/records/:id', requireDurableStorage, updateModuleRecordHandler);

app.delete('/api/modules/:module/records/:id', requireDurableStorage, async (req, res, next) => {
  try {
    const deleted = await deleteModuleRecord(req.user, req.params.id);
    if (!deleted) {
      res.status(404).json({ error: 'Workspace record not found.' });
      return;
    }
    await audit(req, 'module.record_deleted', req.params.module, req.params.id);
    res.status(204).end();
  } catch (error) {
    next(error);
  }
});

app.post('/api/contact', upload.array('attachments', 3), async (req, res, next) => {
  try {
    const name = String(req.body?.name || '').trim();
    const email = String(req.body?.email || '').trim();
    const message = String(req.body?.message || '').trim();
    const pageContext = String(req.body?.pageContext || '').trim();
    const attachments = (req.files ?? []).map((file) => ({
      filename: file.originalname,
      content: file.buffer.toString('base64')
    }));
    const attachmentSummary = (req.files ?? [])
      .map((file) => `${file.originalname} (${Math.round(file.size / 1024)} KB)`)
      .join(', ');

    if (!name || !email || !message) {
      res.status(400).json({ success: false, error: 'Name, email, and message are required.' });
      return;
    }

    if (!isValidEmail(email)) {
      res.status(400).json({ success: false, error: 'Enter a valid email address.' });
      return;
    }

    await audit(req, 'support.contact_submitted', 'support', req.user.id, { name, email, pageContext, attachments: attachmentSummary });
    const delivery = await deliverEmail({
      type: 'support_request',
      to: supportRecipientEmail,
      subject: `Metenova  Business Platform support request from ${name}`,
      text: [
        `From: ${name} <${email}>`,
        `User: ${req.user.name} <${req.user.email}>`,
        `Role: ${req.user.role}`,
        `Company: ${req.user.companyId ?? 'default'}`,
        pageContext ? `Page/module: ${pageContext}` : '',
        attachmentSummary ? `Attachments: ${attachmentSummary}` : '',
        '',
        message
      ].filter(Boolean).join('\n'),
      userId: req.user.id,
      companyId: req.user.companyId,
      replyTo: email,
      attachments,
      from: supportSenderEmail
    });
    if (!delivery.delivered) {
      const errorMessage = delivery.error || 'Email delivery failed.';
      res.status(resendConfigured ? 502 : 503).json({
        success: false,
        error: errorMessage,
        message: 'Support request was received, but email delivery failed.',
        delivery: {
          status: delivery.status,
          provider: delivery.provider,
          error: errorMessage
        }
      });
      return;
    }

    res.status(201).json({
      success: true,
      message: 'Message sent successfully.',
      delivery: {
        status: delivery.status,
        provider: delivery.provider,
        error: delivery.error
      },
      contact: {
        owner: 'Melaku',
        email: supportRecipientEmail,
        phone: '202-607-1255'
      }
    });
  } catch (error) {
    next(error);
  }
});

app.get('/api/insights', (_req, res) => {
  res.json(insights);
});

app.get('/api/workflows', async (_req, res, next) => {
  try {
    const workflows = JSON.parse(await readFile(join(rootDir, 'backend/data/workflows.json'), 'utf8'));
    res.json({ workflows });
  } catch (error) {
    next(error);
  }
});

async function handleDatasetUpload(req, res) {
  if (!req.file) {
    res.status(400).json({ error: 'A .csv, .xlsx, .xls, or .json file is required.' });
    return;
  }
  validateUploadMetadata(req.file);

  const requestedWorksheetName = String(req.body?.worksheetName || '').trim();
  const companyId = requestedCompanyId(req);
  if (!companyId) {
    res.status(400).json({ error: 'Select a company before uploading a dataset.' });
    return;
  }
  if (!(await requireCompanyAccess(req, res, companyId))) {
    return;
  }
  if (!(await companyExists(companyId))) {
    res.status(400).json({ error: 'Selected company workspace was not found.' });
    return;
  }

  const parsed = parseUploadedFile(req.file, requestedWorksheetName);

  if (!parsed.columns.length || !parsed.records.length) {
    res.status(400).json({ error: 'The uploaded file does not contain tabular data with headers and rows.' });
    return;
  }

  const summary = summarizeCsv(parsed.columns, parsed.records);
  const dataset = {
    id: randomUUID(),
    fileName: req.file.originalname,
    fileType: parsed.fileType,
    worksheetName: parsed.worksheetName,
    worksheets: parsed.worksheets,
    uploadedAt: new Date().toISOString(),
    rows: parsed.records.length,
    columns: parsed.columns.length,
    headers: parsed.columns,
    preview: parsed.records.slice(0, 10),
    records: parsed.records,
    userId: req.user.id,
    companyId,
    originalDatasetId: null,
    cleanedDatasetId: null,
    cleanupStatus: 'pending',
    cleanupLogs: [],
    cleanupMetrics: {},
    cleanupPreview: null,
    cleanupOperations: [],
    futureAiReady: false,
    warnings: parsed.truncated ? [`Only the first ${maxSpreadsheetRows.toLocaleString()} rows were imported for safe processing.`] : [],
    ...summary
  };
  await saveDataset(dataset);
  await notifyWorkspace({
    companyId,
    userId: req.user.id,
    type: 'upload_completed',
    title: 'Upload completed',
    message: `${dataset.fileName} was uploaded to the company workspace.`,
    metadata: { datasetId: dataset.id }
  });
  await audit(req, 'dataset.uploaded', 'dataset', dataset.id, { companyId, fileName: dataset.fileName });

  res.json(publicDataset(dataset));
}

app.post('/api/files/upload', requireDurableStorage, upload.single('file'), (req, res) => {
  Promise.resolve().then(async () => {
    await handleDatasetUpload(req, res);
  }).catch((error) => {
    res.status(error.statusCode || 500).json({ error: error.message || 'Upload failed.' });
  });
});

app.post('/api/csv/upload', requireDurableStorage, upload.single('file'), (req, res) => {
  Promise.resolve().then(async () => {
    await handleDatasetUpload(req, res);
  }).catch((error) => {
    res.status(error.statusCode || 500).json({ error: error.message || 'Upload failed.' });
  });
});

app.get('/api/datasets', async (req, res, next) => {
  try {
    const companyId = requestedCompanyId(req);
    if (!(await requireCompanyAccess(req, res, companyId))) return;
    const datasets = await listDatasets(req.user, companyId);
    res.json({ datasets: datasets.map(publicDataset) });
  } catch (error) {
    next(error);
  }
});

app.get('/api/datasets/:id', async (req, res, next) => {
  try {
    const dataset = await getDataset(req.params.id, req.user);
    if (!dataset) {
      res.status(404).json({ error: 'Dataset not found.' });
      return;
    }

    res.json(publicDataset(dataset));
  } catch (error) {
    next(error);
  }
});

app.get('/api/datasets/:id/cleanup-jobs', async (req, res, next) => {
  try {
    const dataset = await getDataset(req.params.id, req.user);
    if (!dataset) {
      res.status(404).json({ error: 'Dataset not found.' });
      return;
    }

    res.json({ cleanupJobs: await listCleanupJobs(req.user, dataset.originalDatasetId ?? dataset.id) });
  } catch (error) {
    next(error);
  }
});

app.get('/api/cleanup-jobs', async (req, res, next) => {
  try {
    const companyId = requestedCompanyId(req);
    if (!(await requireCompanyAccess(req, res, companyId))) return;
    res.json({ cleanupJobs: await listCleanupJobs(req.user, req.query?.datasetId, companyId) });
  } catch (error) {
    next(error);
  }
});

app.post('/api/datasets/:id/archive', requireRole('manager'), requireDurableStorage, async (req, res, next) => {
  try {
    const dataset = await getDataset(req.params.id, req.user);
    if (!dataset) {
      res.status(404).json({ error: 'Dataset not found.' });
      return;
    }
    if (!canManageDataset(req.user, dataset)) {
      res.status(403).json({ error: 'Insufficient dataset permissions.' });
      return;
    }
    const archived = await saveDataset({
      ...dataset,
      cleanupStatus: 'archived',
      cleanupLogs: [...new Set([...(dataset.cleanupLogs ?? []), 'Dataset archived.'])]
    });
    await audit(req, 'dataset.archived', 'dataset', dataset.id, { companyId: dataset.companyId });
    await notifyWorkspace({
      companyId: dataset.companyId,
      userId: req.user.id,
      type: 'dataset_archived',
      title: 'Dataset archived',
      message: `${dataset.fileName} was archived.`,
      metadata: { datasetId: dataset.id }
    });
    res.json({ dataset: publicDataset(archived) });
  } catch (error) {
    next(error);
  }
});

app.post('/api/datasets/:id/cleanup', requireDurableStorage, async (req, res, next) => {
  const jobId = randomUUID();
  let job;

  try {
    const dataset = await getDataset(req.params.id, req.user);
    if (!dataset) {
      res.status(404).json({ error: 'Dataset not found.' });
      return;
    }
    if (dataset.originalDatasetId) {
      res.status(400).json({ error: 'This is already a cleaned dataset. Select the original dataset to run cleanup again.' });
      return;
    }

    job = await saveCleanupJob({
      id: jobId,
      companyId: dataset.companyId,
      userId: req.user.id,
      originalDatasetId: dataset.id,
      status: 'pending',
      logs: ['Cleanup job queued.']
    });

    await saveDataset({
      ...dataset,
      cleanupStatus: 'processing',
      cleanupLogs: ['Cleanup job queued.', 'Cleanup processing started.']
    });
    job = await saveCleanupJob({
      ...job,
      status: 'processing',
      logs: [...job.logs, 'Cleanup processing started.']
    });

    const cleaned = cleanDataset(dataset);
    const summary = summarizeCsv(cleaned.headers, cleaned.records);
    const cleanedDataset = {
      id: randomUUID(),
      fileName: `${dataset.fileName.replace(/\.(csv|xlsx|xls)$/i, '')}-cleaned.csv`,
      fileType: 'csv',
      worksheetName: dataset.worksheetName,
      worksheets: dataset.worksheets,
      uploadedAt: new Date().toISOString(),
      rows: cleaned.records.length,
      columns: cleaned.headers.length,
      headers: cleaned.headers,
      preview: cleaned.records.slice(0, 10),
      records: cleaned.records,
      userId: req.user.id,
      companyId: dataset.companyId,
      originalDatasetId: dataset.id,
      cleanedDatasetId: null,
      cleanupStatus: 'completed',
      cleanupLogs: cleaned.logs,
      cleanupMetrics: cleaned.metrics,
      cleanupPreview: cleaned.preview,
      cleanupOperations: cleaned.operations,
      futureAiReady: cleaned.futureAiReady,
      warnings: [],
      ...summary
    };

    await saveDataset(cleanedDataset);
    const originalLogs = [
      ...new Set([
        ...(dataset.cleanupLogs ?? []),
        'Cleanup completed.',
        `Cleaned dataset created: ${cleanedDataset.fileName}`
      ])
    ];
    const originalDataset = await saveDataset({
      ...dataset,
      cleanedDatasetId: cleanedDataset.id,
      cleanupStatus: 'completed',
      cleanupLogs: originalLogs,
      cleanupMetrics: cleaned.metrics,
      cleanupPreview: cleaned.preview,
      cleanupOperations: cleaned.operations,
      futureAiReady: true
    });
    const completedJob = await saveCleanupJob({
      ...job,
      cleanedDatasetId: cleanedDataset.id,
      status: 'completed',
      metrics: cleaned.metrics,
      logs: [...cleaned.logs, 'Cleanup job completed.']
    });
    await audit(req, 'dataset.cleanup_completed', 'dataset', dataset.id, {
      companyId: dataset.companyId,
      cleanedDatasetId: cleanedDataset.id,
      metrics: cleaned.metrics
    });
    await notifyWorkspace({
      companyId: dataset.companyId,
      userId: req.user.id,
      type: 'cleanup_completed',
      title: 'Cleanup completed',
      message: `${cleanedDataset.fileName} is ready for export and analytics.`,
      metadata: { datasetId: dataset.id, cleanedDatasetId: cleanedDataset.id, metrics: cleaned.metrics }
    });

    res.status(201).json({
      job: completedJob,
      originalDataset: publicDataset(originalDataset),
      cleanedDataset: publicDataset(cleanedDataset)
    });
  } catch (error) {
    if (job) {
      await saveCleanupJob({
        ...job,
        status: 'failed',
        error: error instanceof Error ? error.message : 'Cleanup failed.',
        logs: [...(job.logs ?? []), 'Cleanup failed.']
      }).catch(() => {});
    }
    if (job?.companyId) {
      await notifyWorkspace({
        companyId: job.companyId,
        userId: req.user.id,
        type: 'pipeline_failed',
        title: 'Cleanup failed',
        message: error instanceof Error ? error.message : 'Cleanup failed.',
        metadata: { jobId: job.id }
      }).catch(() => {});
    }
    next(error);
  }
});

app.delete('/api/datasets/:id', requireRole('manager'), requireDurableStorage, async (req, res, next) => {
  try {
    const dataset = await getDataset(req.params.id, req.user);
    if (!dataset) {
      res.status(404).json({ error: 'Dataset not found.' });
      return;
    }
    if (!canManageDataset(req.user, dataset)) {
      res.status(403).json({ error: 'Insufficient dataset permissions.' });
      return;
    }
    const deleted = await deleteDataset(req.user, req.params.id);
    await audit(req, 'dataset.deleted', 'dataset', req.params.id, {
      companyId: dataset.companyId,
      fileName: dataset.fileName,
      cleanedDatasetId: dataset.cleanedDatasetId,
      originalDatasetId: dataset.originalDatasetId
    });
    await notifyWorkspace({
      companyId: dataset.companyId,
      userId: req.user.id,
      type: 'dataset_deleted',
      title: 'Dataset deleted',
      message: `${dataset.fileName} and linked cleanup assets were deleted.`,
      metadata: { datasetId: dataset.id }
    });
    res.json({ deleted: publicDataset(deleted) });
  } catch (error) {
    next(error);
  }
});

app.delete('/api/cleanup-jobs/:id', requireRole('manager'), requireDurableStorage, async (req, res, next) => {
  try {
    const job = await deleteCleanupJob(req.user, req.params.id);
    if (!job) {
      res.status(404).json({ error: 'Cleanup job not found.' });
      return;
    }
    await audit(req, 'dataset.cleanup_job_deleted', 'cleanup_job', req.params.id, { companyId: job.companyId });
    res.json({ cleanupJob: job });
  } catch (error) {
    next(error);
  }
});

app.get('/api/datasets/:id/export', async (req, res, next) => {
  try {
    const dataset = await getDataset(req.params.id, req.user);
    if (!dataset) {
      res.status(404).json({ error: 'Dataset not found.' });
      return;
    }

    const csv = recordsToCsv(dataset.headers, dataset.records);
    const fileName = dataset.fileName.replace(/\.(csv|xlsx|xls)$/i, '') || 'dataset';
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${fileName}.csv"`);
    res.send(csv);
  } catch (error) {
    next(error);
  }
});

app.post('/api/datasets/:id/chat', async (req, res, next) => {
  try {
    const dataset = await getDataset(req.params.id, req.user);
    if (!dataset) {
      res.status(404).json({ error: 'Dataset not found.' });
      return;
    }

    const question = String(req.body?.question || '').trim();
    if (!question) {
      res.status(400).json({ error: 'Question is required.' });
      return;
    }

    res.json({ answer: answerDatasetQuestion(dataset, question) });
  } catch (error) {
    next(error);
  }
});

app.get('/api/dashboards', async (req, res, next) => {
  try {
    const companyId = requestedCompanyId(req);
    if (!(await requireCompanyAccess(req, res, companyId))) return;
    res.json({ dashboards: await listDashboards(req.user, companyId) });
  } catch (error) {
    next(error);
  }
});

app.post('/api/dashboards', requireDurableStorage, async (req, res, next) => {
  try {
    const dataset = await getDataset(req.body?.datasetId, req.user);
    if (!dataset) {
      res.status(404).json({ error: 'Dataset not found.' });
      return;
    }

    const dashboard = {
      id: req.body?.id || randomUUID(),
      userId: req.user.id,
      companyId: dataset.companyId,
      name: String(req.body?.name || `${dataset.fileName} dashboard`),
      datasetId: dataset.id,
      chartType: String(req.body?.chartType || 'bar'),
      config: req.body?.config ?? {},
      snapshot: req.body?.snapshot ?? dashboardSnapshot(dataset, String(req.body?.chartType || 'bar'))
    };
    const savedDashboard = await saveDashboard(dashboard);
    res.status(201).json({
      dashboard: savedDashboard
    });
  } catch (error) {
    next(error);
  }
});

app.get('/api/reports', async (req, res, next) => {
  try {
    const companyId = requestedCompanyId(req);
    if (!(await requireCompanyAccess(req, res, companyId))) return;
    res.json({ reports: await listReports(req.user, companyId) });
  } catch (error) {
    next(error);
  }
});

app.delete('/api/reports/:id', requireRole('manager'), requireDurableStorage, async (req, res, next) => {
  try {
    const report = await deleteReport(req.user, req.params.id);
    if (!report) {
      res.status(404).json({ error: 'Report not found.' });
      return;
    }
    await audit(req, 'report.deleted', 'report', req.params.id, { companyId: report.companyId, datasetId: report.datasetId });
    res.json({ report });
  } catch (error) {
    next(error);
  }
});

app.post('/api/reports', requireDurableStorage, async (req, res, next) => {
  try {
    const dataset = await getDataset(req.body?.datasetId, req.user);
    if (!dataset) {
      res.status(404).json({ error: 'Dataset not found.' });
      return;
    }

    const report = {
      id: randomUUID(),
      userId: req.user.id,
      companyId: dataset.companyId,
      datasetId: dataset.id,
      datasetName: dataset.fileName,
      title: String(req.body?.title || `${dataset.fileName} report`),
      reportType: String(req.body?.reportType || 'pdf'),
      content: {
        lines: reportLines(dataset),
        dataset: publicDataset(dataset),
        chartType: req.body?.content?.chartType,
        ...(req.body?.content ?? {})
      }
    };
    const savedReport = await saveReport(report);
    await notifyWorkspace({
      companyId: dataset.companyId,
      userId: req.user.id,
      type: 'report_generated',
      title: 'Report generated',
      message: `${savedReport.title} is available in report history.`,
      metadata: { reportId: savedReport.id, datasetId: dataset.id }
    });
    res.status(201).json({
      report: savedReport
    });
  } catch (error) {
    next(error);
  }
});

app.get('/api/notifications', async (req, res, next) => {
  try {
    const companyId = requestedCompanyId(req);
    if (!(await requireCompanyAccess(req, res, companyId))) return;
    res.json({ notifications: await listNotifications(req.user, companyId) });
  } catch (error) {
    next(error);
  }
});

app.patch('/api/notifications/:id', requireDurableStorage, async (req, res, next) => {
  try {
    const notification = await updateNotification(req.user, req.params.id, {
      status: req.body?.status,
      archive: req.body?.archive
    });
    if (!notification) {
      res.status(404).json({ error: 'Notification not found.' });
      return;
    }
    await audit(req, req.body?.archive ? 'notification.archived' : 'notification.updated', 'notification', req.params.id, {
      status: notification.status
    });
    res.json({ notification });
  } catch (error) {
    next(error);
  }
});

if (existsSync(frontendDist)) {
  app.use(express.static(frontendDist));
  app.get('*', (_req, res) => {
    res.sendFile(join(frontendDist, 'index.html'));
  });
}

app.use((error, _req, res, _next) => {
  const statusCode = error.statusCode || 500;
  console.error(JSON.stringify({
    level: 'error',
    requestId: _req.requestId,
    method: _req.method,
    path: _req.path,
    statusCode,
    error: error.message || 'Unexpected server error.'
  }));
  res.status(statusCode).json({
    success: false,
    error: error.message || 'Unexpected server error.',
    requestId: _req.requestId
  });
});

initDatabase()
  .then(removeDemoAccounts)
  .then(() => promoteAdminEmails(adminEmails))
  .then(ensureOwnerAccount)
  .then(importLegacyDatasets)
  .catch((error) => {
    console.error(`PostgreSQL startup failed: ${error instanceof Error ? error.message : 'Unknown database error'}`);
  })
  .finally(() => {
    const dbStatus = getDatabaseRuntimeStatus();
    console.log(`Environment loaded: ${process.env.VERCEL === '1' ? 'vercel-production' : process.env.NODE_ENV || 'local'}`);
    console.log(`PostgreSQL configured: ${dbStatus.usingPostgres ? 'true' : 'false'}`);
    console.log(`PostgreSQL connected: ${dbStatus.usingPostgres && dbStatus.connected ? 'true' : 'false'}${dbStatus.database ? ` (${dbStatus.database})` : ''}`);
    console.log(`PostgreSQL tables initialized: ${dbStatus.tablesInitialized ? 'true' : dbStatus.usingPostgres ? 'false' : 'local-mode'}`);
    if (dbStatus.connectionError) {
      console.error(`PostgreSQL startup error: ${dbStatus.connectionError}`);
    }
    console.log(`Email configured: ${emailConfigured ? 'true' : 'false'}`);
    console.log('RESEND CONFIGURED:', Boolean(process.env.RESEND_API_KEY));
    app.listen(port, () => {
      console.log(`Backend listening on http://localhost:${port}`);
    });
  });
