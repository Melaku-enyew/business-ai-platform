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
import {
  createSession,
  createUser,
  findUserByEmail,
  findUserById,
  findSessionById,
  getDevUser,
  getDataset,
  initDatabase,
  listDashboards,
  listDatasets,
  listReports,
  listSessions,
  promoteAdminEmails,
  saveDashboard,
  saveDataset,
  saveReport,
  revokeSession,
  usingSqlServer
} from './db.js';

const scryptAsync = promisify(scrypt);
const app = express();
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 }
});
const port = Number(process.env.PORT || 4000);
const clientOrigin = process.env.CLIENT_ORIGIN || 'http://localhost:5173';
const authDisabled = process.env.AUTH_DISABLED === 'true';
const jwtSecret = process.env.JWT_SECRET || (!usingSqlServer ? 'local-mvp-secret' : '');
const demoEmail = 'admin@businessai.com';
const demoPassword = 'admin123';
const adminEmails = (process.env.ADMIN_EMAILS || demoEmail)
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

  if (rows.length === 0) {
    return { columns: [], records: [] };
  }

  const columns = rows[0].map((column, index) => column || `Column ${index + 1}`);
  const records = rows.slice(1).map((values) =>
    columns.reduce((record, column, index) => {
      record[column] = values[index] ?? '';
      return record;
    }, {})
  );

  return { columns, records };
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
      ? dataset.numericSummary.map((item) => `${item.column}: total ${Number(item.total).toFixed(2)}, average ${Number(item.average).toFixed(2)}, min ${Number(item.min).toFixed(2)}, max ${Number(item.max).toFixed(2)}`)
      : ['No numeric columns detected.'])
  ];
}

function publicUser(user) {
  return {
    id: user.id,
    name: user.name,
    email: user.email,
    role: user.role === 'admin' ? 'admin' : 'user'
  };
}

function roleForEmail(email) {
  return adminEmails.includes(email.toLowerCase()) ? 'admin' : 'user';
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
      email: user.email
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
    const user = await findUserById(payload.sub);
    const session = payload.sid ? await findSessionById(payload.sid, payload.sub) : undefined;
    if (!user || !session) {
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

function requireRole(role) {
  return (req, res, next) => {
    if (req.user?.role !== role) {
      res.status(403).json({ error: 'Insufficient workspace permissions.' });
      return;
    }

    next();
  };
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
      passwordHash: await hashPassword(password)
    });
    const { token, session } = await createLoginSession(user);
    res.status(201).json({ token, user: publicUser(user), session });
  } catch (error) {
    next(error);
  }
});

app.post('/api/auth/login', async (req, res, next) => {
  try {
    const email = String(req.body?.email || '').trim().toLowerCase();
    const password = String(req.body?.password || '');

    if (!usingSqlServer && email === demoEmail && password === demoPassword) {
      let demoUser = await findUserByEmail(demoEmail);
      if (!demoUser || demoUser.role !== 'admin') {
        demoUser = await createUser({
          id: 'demo-admin-user',
          name: 'Demo Admin',
          email: demoEmail,
          role: 'admin',
          passwordHash: await hashPassword(demoPassword)
        });
      }

      const { token, session } = await createLoginSession(demoUser);
      res.json({ token, user: publicUser(demoUser), session, demo: true });
      return;
    }

    const user = await findUserByEmail(email);

    if (!user || !(await verifyPassword(password, user.passwordHash))) {
      res.status(401).json({ error: 'Invalid email or password.' });
      return;
    }

    const { token, session } = await createLoginSession(user);
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

app.post('/api/csv/upload', upload.single('file'), (req, res) => {
  Promise.resolve().then(async () => {
  if (!req.file) {
    res.status(400).json({ error: 'CSV file is required.' });
    return;
  }

  if (!req.file.originalname.toLowerCase().endsWith('.csv')) {
    res.status(400).json({ error: 'Only .csv files are supported.' });
    return;
  }

  const text = req.file.buffer.toString('utf8');
  const { columns, records } = parseCsv(text);
  const summary = summarizeCsv(columns, records);
  const dataset = {
    id: randomUUID(),
    fileName: req.file.originalname,
    uploadedAt: new Date().toISOString(),
    rows: records.length,
    columns: columns.length,
    headers: columns,
    preview: records.slice(0, 10),
    records,
    userId: req.user.id,
    ...summary
  };
  await saveDataset(dataset);

  res.json(publicDataset(dataset));
  }).catch((error) => {
    res.status(500).json({ error: error.message || 'Upload failed.' });
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
  .then(() => promoteAdminEmails(adminEmails))
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
