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
  createAccountToken,
  createSession,
  createUser,
  deleteUser,
  ensureOwnerAccount,
  findUserByEmail,
  findUserById,
  findAccountToken,
  hasRole,
  findSessionById,
  getDevUser,
  getDataset,
  initDatabase,
  listAuditLogs,
  listDashboards,
  listDatasets,
  listReports,
  listSessions,
  listUsers,
  markAccountTokenUsed,
  ownerEmail,
  promoteAdminEmails,
  recordLoginFailure,
  recordLoginSuccess,
  removeDemoAccounts,
  saveDashboard,
  saveDataset,
  saveReport,
  saveAuditLog,
  revokeSession,
  revokeUserSessions,
  roles,
  updateUserPassword,
  updateUser,
  usingSqlServer
} from './db.js';

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
const jwtSecret = process.env.JWT_SECRET || (!usingSqlServer ? 'local-mvp-secret' : '');
const requireStoredSessions = usingSqlServer || process.env.REQUIRE_STORED_SESSIONS === 'true';
const adminEmails = (process.env.ADMIN_EMAILS || ownerEmail)
  .split(',')
  .map((email) => email.trim().toLowerCase())
  .filter(Boolean);
const rootDir = fileURLToPath(new URL('../..', import.meta.url));
const frontendDist = join(rootDir, 'frontend/dist');
const legacyDatasetsFile = join(rootDir, 'backend/data/uploads/datasets.json');

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

function parseUploadedFile(file, requestedWorksheetName) {
  const extension = getFileExtension(file.originalname);

  if (!['csv', 'xlsx', 'xls'].includes(extension)) {
    const error = new Error('Upload a .csv, .xlsx, or .xls file.');
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
      ? dataset.numericSummary.map((item) => `${item.column}: total ${Number(item.total).toFixed(2)}, average ${Number(item.average).toFixed(2)}, min ${Number(item.min).toFixed(2)}, max ${Number(item.max).toFixed(2)}`)
      : ['No numeric columns detected.'])
  ];
}

function publicUser(user) {
  return {
    id: user.id,
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
    createdAt: user.createdAt
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

function signJwt(payload, expiresAt = new Date(Date.now() + 60 * 60 * 24 * 1000)) {
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
    expiresAt: new Date(Date.now() + 60 * 60 * 24 * 1000).toISOString()
  });
  const token = signJwt(
    {
      sub: user.id,
      sid: session.id,
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
    next();
    return;
  }

  try {
    const authHeader = req.headers.authorization || '';
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : '';
    if (!token) {
      res.status(401).json({ error: 'Authentication required.' });
      return;
    }

    const payload = verifyJwt(token);
    const storedUser = await findUserById(payload.sub);
    const session = payload.sid ? await findSessionById(payload.sid, payload.sub) : undefined;
    const user = storedUser ?? (!usingSqlServer ? userFromToken(payload) : undefined);

    if (!user || user.active === false || (requireStoredSessions && !session)) {
      res.status(401).json({ error: 'Authentication required.' });
      return;
    }

    req.user = user;
    req.session = session;
    next();
  } catch {
    res.status(401).json({ error: 'Authentication required.' });
  }
}

function userFromToken(payload) {
  if (!payload?.sub || !payload?.email) {
    return undefined;
  }

  return {
    id: payload.sub,
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
  await Promise.all(datasets.map((dataset) => saveDataset({ ...dataset, userId: dataset.userId ?? getDevUser().id })));
}

app.use(cors({
  origin: clientOrigin,
  credentials: true
}));
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({
    status: 'ok',
    service: 'business-ai-platform-backend',
    mode: authDisabled ? 'auth-disabled' : usingSqlServer ? 'secure-sql-server' : 'local-auth'
  });
});

app.get('/api/config', (_req, res) => {
  res.json({
    authDisabled,
    storage: usingSqlServer ? 'sql-server' : 'local-json'
  });
});

app.post('/api/auth/signup', async (req, res, next) => {
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

app.post('/api/auth/forgot-password', async (req, res, next) => {
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
      resetUrl = `/reset-password?token=${encodeURIComponent(token)}`;
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
      ...(resetUrl && !process.env.EMAIL_PROVIDER ? { resetUrl } : {})
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
    res.json({
      message: 'If the account exists, username recovery instructions have been prepared.',
      ...(user && !process.env.EMAIL_PROVIDER ? { username: user.email, name: user.name } : {})
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
    res.json({
      message: 'Verification instructions have been prepared.',
      ...(!process.env.EMAIL_PROVIDER ? { verificationUrl: `/verify-email?token=${encodeURIComponent(token)}` } : {})
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

app.post('/api/auth/login', async (req, res, next) => {
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
      res.status(401).json({ error: 'Invalid email or password.' });
      return;
    }

    if (user.active === false) {
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

app.get('/api/auth/sessions', requireAuth, async (req, res, next) => {
  try {
    res.json({ sessions: await listSessions(req.user.id) });
  } catch (error) {
    next(error);
  }
});

app.get('/api/admin/workspace', requireAuth, requireRole('admin'), async (req, res, next) => {
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

app.get('/api/admin/users', requireAuth, requireRole('admin'), async (_req, res, next) => {
  try {
    res.json({ users: (await listUsers()).map(publicUser) });
  } catch (error) {
    next(error);
  }
});

app.patch('/api/admin/users/:id', requireAuth, requireRole('admin'), async (req, res, next) => {
  try {
    const updates = {};

    if (req.body?.role != null) {
      const requestedRole = String(req.body.role);
      if (requestedRole === 'owner' && req.user.email !== ownerEmail) {
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
    if (existing?.email === ownerEmail && req.user.email !== ownerEmail) {
      res.status(403).json({ error: 'Owner permissions cannot be changed by another account.' });
      return;
    }

    const user = await updateUser(req.params.id, updates);
    if (!user) {
      res.status(404).json({ error: 'User not found.' });
      return;
    }

    res.json({ user: publicUser(user) });
    await audit(req, 'admin.user_updated', 'user', user.id, updates);
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

app.post('/api/admin/users/:id/revoke', requireAuth, requireRole('admin'), async (req, res, next) => {
  try {
    if (req.params.id === req.user.id) {
      res.status(400).json({ error: 'Use logout to end your current session.' });
      return;
    }

    await revokeUserSessions(req.params.id);
    await audit(req, 'admin.sessions_revoked', 'user', req.params.id);
    res.json({ revoked: true });
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

app.patch('/api/profile', async (req, res, next) => {
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

app.post('/api/profile/change-password', async (req, res, next) => {
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
  res.json({
    status: 'operational',
    storage: usingSqlServer ? 'sql-server' : 'local-json',
    auth: requireStoredSessions ? 'jwt-with-stored-sessions' : 'jwt-stateless-local',
    uptimeSeconds: Math.round(process.uptime()),
    uploadLimitMb: Math.round(maxUploadBytes / 1024 / 1024),
    maxSpreadsheetRows
  });
});

app.post('/api/contact', async (req, res, next) => {
  try {
    const name = String(req.body?.name || '').trim();
    const email = String(req.body?.email || '').trim();
    const message = String(req.body?.message || '').trim();

    if (!name || !email || !message) {
      res.status(400).json({ error: 'Name, email, and message are required.' });
      return;
    }

    await audit(req, 'support.contact_submitted', 'support', req.user.id, { name, email });
    res.status(201).json({
      message: 'Support request received. Metenova AI will follow up by email.',
      contact: {
        owner: 'Melaku',
        email: ownerEmail,
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
    res.status(400).json({ error: 'A .csv, .xlsx, or .xls file is required.' });
    return;
  }

  const requestedWorksheetName = String(req.body?.worksheetName || '').trim();
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
    warnings: parsed.truncated ? [`Only the first ${maxSpreadsheetRows.toLocaleString()} rows were imported for safe processing.`] : [],
    ...summary
  };
  await saveDataset(dataset);

  res.json(publicDataset(dataset));
}

app.post('/api/files/upload', upload.single('file'), (req, res) => {
  Promise.resolve().then(async () => {
    await handleDatasetUpload(req, res);
  }).catch((error) => {
    res.status(error.statusCode || 500).json({ error: error.message || 'Upload failed.' });
  });
});

app.post('/api/csv/upload', upload.single('file'), (req, res) => {
  Promise.resolve().then(async () => {
    await handleDatasetUpload(req, res);
  }).catch((error) => {
    res.status(error.statusCode || 500).json({ error: error.message || 'Upload failed.' });
  });
});

app.get('/api/datasets', async (req, res, next) => {
  try {
    const datasets = await listDatasets(req.user.id);
    res.json({ datasets: datasets.map(publicDataset) });
  } catch (error) {
    next(error);
  }
});

app.get('/api/datasets/:id', async (req, res, next) => {
  try {
    const dataset = await getDataset(req.params.id, req.user.id);
    if (!dataset) {
      res.status(404).json({ error: 'Dataset not found.' });
      return;
    }

    res.json(publicDataset(dataset));
  } catch (error) {
    next(error);
  }
});

app.post('/api/datasets/:id/chat', async (req, res, next) => {
  try {
    const dataset = await getDataset(req.params.id, req.user.id);
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
    res.json({ dashboards: await listDashboards(req.user) });
  } catch (error) {
    next(error);
  }
});

app.post('/api/dashboards', async (req, res, next) => {
  try {
    const dataset = await getDataset(req.body?.datasetId, req.user.id);
    if (!dataset) {
      res.status(404).json({ error: 'Dataset not found.' });
      return;
    }

    const dashboard = {
      id: req.body?.id || randomUUID(),
      userId: req.user.id,
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
    res.json({ reports: await listReports(req.user) });
  } catch (error) {
    next(error);
  }
});

app.post('/api/reports', async (req, res, next) => {
  try {
    const dataset = await getDataset(req.body?.datasetId, req.user.id);
    if (!dataset) {
      res.status(404).json({ error: 'Dataset not found.' });
      return;
    }

    const report = {
      id: randomUUID(),
      userId: req.user.id,
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
    res.status(201).json({
      report: savedReport
    });
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
  res.status(500).json({ error: error.message || 'Unexpected server error.' });
});

initDatabase()
  .then(removeDemoAccounts)
  .then(() => promoteAdminEmails(adminEmails))
  .then(ensureOwnerAccount)
  .then(importLegacyDatasets)
  .then(() => {
    app.listen(port, () => {
      console.log(`Backend listening on http://localhost:${port}`);
    });
  })
  .catch((error) => {
    console.error(`Database startup failed: ${error.message}`);
    process.exit(1);
  });
