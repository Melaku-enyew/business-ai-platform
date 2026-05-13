import { config } from 'dotenv';
import { existsSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';

config({ path: fileURLToPath(new URL('../.env', import.meta.url)) });

const { Pool } = pg;
const devStoreFile = fileURLToPath(new URL('../data/dev-store.json', import.meta.url));
const defaultCompanyId = 'metenova-default-company';
const defaultCompanyName = process.env.DEFAULT_COMPANY_NAME || 'Metenova AI Workspace';
const postgresConnectRetries = Number(process.env.POSTGRES_CONNECT_RETRIES || 3);
const postgresRetryDelayMs = Number(process.env.POSTGRES_RETRY_DELAY_MS || 1500);

export const ownerEmail = (process.env.OWNER_EMAIL || 'melakue@metenovaai.com').toLowerCase();
export const roles = ['viewer', 'employee', 'manager', 'admin', 'owner'];
export const usingPostgres = Boolean(process.env.DATABASE_URL);
export let pool = null;

let connected = false;
let tablesInitialized = false;
let lastConnectionError = null;

const devUser = {
  id: 'local-dev-user',
  companyId: defaultCompanyId,
  name: 'Metenova Workspace Owner',
  email: 'local@example.com',
  role: 'owner',
  active: true,
  emailVerified: true,
  profilePhotoUrl: '',
  notificationSettings: {},
  preferences: {},
  twoFactorEnabled: false,
  createdAt: new Date().toISOString()
};

function getPostgresConfig() {
  assertPublicDatabaseUrl();
  return {
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
  };
}

function assertPublicDatabaseUrl() {
  if (!process.env.DATABASE_URL) return;

  try {
    const host = new URL(process.env.DATABASE_URL).hostname;
    if (process.env.VERCEL === '1' && host.endsWith('.internal')) {
      throw new Error('DATABASE_URL points to a Railway private hostname. Configure the Railway public proxy URL for Vercel.');
    }
    if (process.env.VERCEL === '1' && !host.endsWith('.proxy.rlwy.net')) {
      console.warn('DATABASE_URL does not use the Railway public proxy hostname expected for Vercel deployments.');
    }
  } catch (error) {
    lastConnectionError = error instanceof Error ? error.message : 'Invalid DATABASE_URL.';
    throw error;
  }
}

async function getPool() {
  if (!pool) {
    pool = new Pool({
      ...getPostgresConfig(),
      max: Number(process.env.PGPOOL_MAX || 10),
      idleTimeoutMillis: 30000
    });
    pool.on('error', (error) => {
      connected = false;
      lastConnectionError = error instanceof Error ? error.message : 'PostgreSQL pool error.';
      console.error(`PostgreSQL pool error: ${lastConnectionError}`);
    });
  }

  if (!connected) {
    await connectWithRetry(async () => {
      const client = await pool.connect();
      try {
        await client.query('SELECT 1;');
      } finally {
        client.release();
      }
    }, 'PostgreSQL');
    connected = true;
    lastConnectionError = null;
  }

  return pool;
}

async function pgQuery(text, params = []) {
  const db = await getPool();
  return db.query(text, params);
}

export async function initDatabase() {
  if (!usingPostgres) {
    await saveDevStore(await loadDevStore());
    tablesInitialized = false;
    return;
  }

  await pgQuery(`
    CREATE TABLE IF NOT EXISTS companies (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      owner_user_id TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}' REFERENCES companies(id),
      name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      role TEXT NOT NULL DEFAULT 'employee',
      active BOOLEAN NOT NULL DEFAULT TRUE,
      email_verified BOOLEAN NOT NULL DEFAULT FALSE,
      profile_photo_url TEXT,
      notification_settings JSONB NOT NULL DEFAULT '{}'::jsonb,
      preferences JSONB NOT NULL DEFAULT '{}'::jsonb,
      two_factor_enabled BOOLEAN NOT NULL DEFAULT FALSE,
      last_login_at TIMESTAMPTZ,
      failed_login_count INTEGER NOT NULL DEFAULT 0,
      locked_until TIMESTAMPTZ,
      password_hash TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS sessions (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      expires_at TIMESTAMPTZ NOT NULL,
      revoked_at TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS datasets (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      file_name TEXT NOT NULL,
      file_type TEXT NOT NULL DEFAULT 'csv',
      worksheet_name TEXT,
      uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      row_count INTEGER NOT NULL,
      column_count INTEGER NOT NULL,
      headers JSONB NOT NULL,
      preview JSONB NOT NULL,
      records JSONB NOT NULL,
      analysis JSONB NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dashboards (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      name TEXT NOT NULL,
      dataset_id TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
      chart_type TEXT NOT NULL DEFAULT 'bar',
      config JSONB NOT NULL DEFAULT '{}'::jsonb,
      snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS reports (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      dataset_id TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
      title TEXT NOT NULL,
      report_type TEXT NOT NULL DEFAULT 'pdf',
      content JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS module_records (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL REFERENCES companies(id),
      user_id TEXT NOT NULL REFERENCES users(id),
      module TEXT NOT NULL,
      record_type TEXT NOT NULL,
      title TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'open',
      amount NUMERIC(18, 2),
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS account_tokens (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      token_hash TEXT NOT NULL,
      purpose TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      expires_at TIMESTAMPTZ NOT NULL,
      used_at TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS audit_logs (
      id TEXT PRIMARY KEY,
      actor_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      actor_email TEXT,
      action TEXT NOT NULL,
      target_type TEXT,
      target_id TEXT,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS email_logs (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      user_id TEXT,
      email_type TEXT NOT NULL,
      recipient TEXT NOT NULL,
      subject TEXT NOT NULL,
      body TEXT NOT NULL DEFAULT '',
      provider TEXT,
      status TEXT NOT NULL,
      error TEXT,
      attempts INTEGER NOT NULL DEFAULT 1,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS invitations (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL REFERENCES companies(id),
      email TEXT NOT NULL,
      role TEXT NOT NULL,
      token_hash TEXT NOT NULL,
      invited_by TEXT NOT NULL REFERENCES users(id),
      status TEXT NOT NULL DEFAULT 'pending',
      expires_at TIMESTAMPTZ NOT NULL,
      accepted_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_sessions_user_expires_at ON sessions (user_id, expires_at DESC);
    CREATE INDEX IF NOT EXISTS idx_datasets_user_uploaded_at ON datasets (user_id, uploaded_at DESC);
    CREATE INDEX IF NOT EXISTS idx_dashboards_user_updated_at ON dashboards (user_id, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_reports_user_created_at ON reports (user_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_module_records_company_module ON module_records (company_id, module, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_account_tokens_user_purpose ON account_tokens (user_id, purpose, expires_at DESC);
    CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs (created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_email_logs_company_created_at ON email_logs (company_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_invitations_company_created_at ON invitations (company_id, created_at DESC);
  `);

  await pgQuery(
    `INSERT INTO companies (id, name)
     VALUES ($1, $2)
     ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;`,
    [defaultCompanyId, defaultCompanyName]
  );
  tablesInitialized = true;
}

export function getDatabaseRuntimeStatus() {
  return {
    usingPostgres,
    hostConfigured: Boolean(process.env.DATABASE_URL),
    database: usingPostgres ? databaseFromUrl(process.env.DATABASE_URL) : null,
    connected,
    tablesInitialized,
    connectionError: lastConnectionError,
    retries: postgresConnectRetries
  };
}

export async function createUser(user) {
  const savedUser = {
    id: user.id,
    companyId: user.companyId ?? defaultCompanyId,
    name: user.name,
    email: user.email.toLowerCase(),
    role: roleForUser(user.email, user.role),
    active: user.active ?? true,
    emailVerified: user.emailVerified ?? user.email?.toLowerCase() === ownerEmail,
    profilePhotoUrl: user.profilePhotoUrl ?? '',
    notificationSettings: user.notificationSettings ?? {},
    preferences: user.preferences ?? {},
    twoFactorEnabled: user.twoFactorEnabled ?? false,
    failedLoginCount: 0,
    lockedUntil: null,
    passwordHash: user.passwordHash,
    createdAt: new Date().toISOString()
  };

  if (!usingPostgres) {
    const store = await loadDevStore();
    store.users = [savedUser, ...store.users.filter((entry) => entry.email !== savedUser.email)];
    await saveDevStore(store);
    return rowToUser(savedUser);
  }

  const result = await pgQuery(
    `INSERT INTO users (
       id, company_id, name, email, role, active, email_verified, profile_photo_url,
       notification_settings, preferences, two_factor_enabled, password_hash
     )
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
     ON CONFLICT (email) DO UPDATE SET
       name = EXCLUDED.name,
       role = EXCLUDED.role,
       active = EXCLUDED.active,
       email_verified = EXCLUDED.email_verified,
       profile_photo_url = EXCLUDED.profile_photo_url,
       notification_settings = EXCLUDED.notification_settings,
       preferences = EXCLUDED.preferences,
       two_factor_enabled = EXCLUDED.two_factor_enabled,
       password_hash = EXCLUDED.password_hash
     RETURNING *;`,
    [
      savedUser.id,
      savedUser.companyId,
      savedUser.name,
      savedUser.email,
      savedUser.role,
      savedUser.active,
      savedUser.emailVerified,
      savedUser.profilePhotoUrl,
      JSON.stringify(savedUser.notificationSettings),
      JSON.stringify(savedUser.preferences),
      savedUser.twoFactorEnabled,
      savedUser.passwordHash
    ]
  );
  return rowToUser(result.rows[0]);
}

export async function findUserByEmail(email) {
  const normalizedEmail = String(email || '').toLowerCase();
  if (!usingPostgres) {
    const store = await loadDevStore();
    const user = store.users.find((entry) => entry.email === normalizedEmail);
    return user ? rowToUser(user, true) : undefined;
  }

  const result = await pgQuery('SELECT * FROM users WHERE email = $1 LIMIT 1;', [normalizedEmail]);
  return result.rows[0] ? rowToUser(result.rows[0], true) : undefined;
}

export async function findUserById(id) {
  if (!usingPostgres && id === devUser.id) {
    return rowToUser(devUser);
  }
  if (!usingPostgres) {
    const store = await loadDevStore();
    const user = store.users.find((entry) => entry.id === id);
    return user ? rowToUser(user, true) : undefined;
  }

  const result = await pgQuery('SELECT * FROM users WHERE id = $1 LIMIT 1;', [id]);
  return result.rows[0] ? rowToUser(result.rows[0], true) : undefined;
}

export async function listUsers(requestingUser) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    return store.users
      .filter((user) => isOwner(requestingUser) || user.companyId === getCompanyId(requestingUser))
      .map((user) => rowToUser(user));
  }

  const params = [];
  const where = isOwner(requestingUser) ? '' : 'WHERE company_id = $1';
  if (!isOwner(requestingUser)) params.push(getCompanyId(requestingUser));
  const result = await pgQuery(`SELECT * FROM users ${where} ORDER BY created_at DESC;`, params);
  return result.rows.map((row) => rowToUser(row));
}

export async function updateUser(id, updates) {
  const existing = await findUserById(id);
  if (!existing) return undefined;

  const normalizedUpdates = protectOwnerUpdates(existing, {
    ...(updates.name != null ? { name: String(updates.name).trim() } : {}),
    ...(updates.role != null ? { role: normalizeRole(updates.role) } : {}),
    ...(updates.active != null ? { active: Boolean(updates.active) } : {}),
    ...(updates.emailVerified != null ? { emailVerified: Boolean(updates.emailVerified) } : {}),
    ...(updates.profilePhotoUrl != null ? { profilePhotoUrl: String(updates.profilePhotoUrl).trim() } : {}),
    ...(updates.notificationSettings != null ? { notificationSettings: updates.notificationSettings } : {}),
    ...(updates.preferences != null ? { preferences: updates.preferences } : {}),
    ...(updates.twoFactorEnabled != null ? { twoFactorEnabled: Boolean(updates.twoFactorEnabled) } : {})
  });

  const next = { ...existing, ...normalizedUpdates };
  if (!usingPostgres) {
    const store = await loadDevStore();
    let updated;
    store.users = store.users.map((user) => {
      if (user.id !== id) return user;
      updated = { ...user, ...next };
      return updated;
    });
    await saveDevStore(store);
    return updated ? rowToUser(updated) : undefined;
  }

  const result = await pgQuery(
    `UPDATE users SET
       name = $2,
       company_id = $3,
       role = $4,
       active = $5,
       email_verified = $6,
       profile_photo_url = $7,
       notification_settings = $8,
       preferences = $9,
       two_factor_enabled = $10
     WHERE id = $1
     RETURNING *;`,
    [
      id,
      next.name,
      next.companyId ?? defaultCompanyId,
      next.role,
      next.active,
      next.emailVerified,
      next.profilePhotoUrl || null,
      JSON.stringify(next.notificationSettings ?? {}),
      JSON.stringify(next.preferences ?? {}),
      next.twoFactorEnabled
    ]
  );
  return result.rows[0] ? rowToUser(result.rows[0]) : undefined;
}

export async function deleteUser(id) {
  const user = await findUserById(id);
  if (isOwner(user)) throw new Error('The permanent owner account cannot be deleted.');

  if (!usingPostgres) {
    const store = await loadDevStore();
    store.users = store.users.filter((entry) => entry.id !== id);
    store.sessions = store.sessions.filter((session) => session.userId !== id);
    store.datasets = store.datasets.filter((dataset) => dataset.userId !== id);
    store.dashboards = store.dashboards.filter((dashboard) => dashboard.userId !== id);
    store.reports = store.reports.filter((report) => report.userId !== id);
    store.moduleRecords = (store.moduleRecords ?? []).filter((record) => record.userId !== id);
    await saveDevStore(store);
    return;
  }

  await pgQuery('DELETE FROM users WHERE id = $1;', [id]);
}

export async function setUserRoleByEmail(email, role) {
  const normalizedEmail = String(email || '').toLowerCase();
  const normalizedRole = roleForUser(normalizedEmail, role);
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.users = store.users.map((user) => user.email === normalizedEmail ? { ...user, role: normalizedRole } : user);
    await saveDevStore(store);
    return;
  }
  await pgQuery('UPDATE users SET role = $1 WHERE email = $2;', [normalizedRole, normalizedEmail]);
}

export async function promoteAdminEmails(emails) {
  await Promise.all(emails.map((email) => setUserRoleByEmail(email, email === ownerEmail ? 'owner' : 'admin')));
}

export async function ensureOwnerAccount() {
  const existing = await findUserByEmail(ownerEmail);
  if (existing) {
    await updateUser(existing.id, { role: 'owner', active: true, emailVerified: true });
  }
}

export async function removeDemoAccounts() {
  const demoEmails = ['admin@businessai.com', 'demo@businessai.com', 'local@example.com'];
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.users = store.users.filter((user) => !demoEmails.includes(user.email) && !user.email.startsWith('analyst+'));
    await saveDevStore(store);
    return;
  }
  await pgQuery("DELETE FROM users WHERE email = ANY($1::text[]) OR email LIKE 'analyst+%@businessai.com';", [demoEmails]);
}

export async function createSession(session) {
  const saved = {
    id: session.id,
    userId: session.userId,
    expiresAt: session.expiresAt,
    createdAt: new Date().toISOString(),
    revokedAt: null
  };
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.sessions = [saved, ...store.sessions.filter((entry) => entry.id !== saved.id)];
    await saveDevStore(store);
    return saved;
  }
  const result = await pgQuery(
    `INSERT INTO sessions (id, user_id, expires_at)
     VALUES ($1, $2, $3)
     RETURNING *;`,
    [saved.id, saved.userId, saved.expiresAt]
  );
  return rowToSession(result.rows[0]);
}

export async function findSessionById(id, userId) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    const session = store.sessions.find((entry) =>
      entry.id === id &&
      entry.userId === userId &&
      !entry.revokedAt &&
      new Date(entry.expiresAt).getTime() > Date.now()
    );
    return session ? rowToSession(session) : undefined;
  }
  const result = await pgQuery(
    `SELECT * FROM sessions
     WHERE id = $1 AND user_id = $2 AND revoked_at IS NULL AND expires_at > NOW()
     LIMIT 1;`,
    [id, userId]
  );
  return result.rows[0] ? rowToSession(result.rows[0]) : undefined;
}

export async function listSessions(userId) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    return store.sessions.filter((session) => session.userId === userId).slice(0, 20).map(rowToSession);
  }
  const result = await pgQuery(
    'SELECT * FROM sessions WHERE user_id = $1 ORDER BY expires_at DESC LIMIT 20;',
    [userId]
  );
  return result.rows.map(rowToSession);
}

export async function revokeSession(id, userId) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.sessions = store.sessions.map((session) => session.id === id && session.userId === userId ? { ...session, revokedAt: new Date().toISOString() } : session);
    await saveDevStore(store);
    return;
  }
  await pgQuery('UPDATE sessions SET revoked_at = NOW() WHERE id = $1 AND user_id = $2;', [id, userId]);
}

export async function revokeUserSessions(userId) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.sessions = store.sessions.map((session) => session.userId === userId ? { ...session, revokedAt: new Date().toISOString() } : session);
    await saveDevStore(store);
    return;
  }
  await pgQuery('UPDATE sessions SET revoked_at = NOW() WHERE user_id = $1 AND revoked_at IS NULL;', [userId]);
}

export async function updateUserPassword(userId, passwordHash) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.users = store.users.map((user) => user.id === userId ? { ...user, passwordHash } : user);
    await saveDevStore(store);
    return;
  }
  await pgQuery('UPDATE users SET password_hash = $2 WHERE id = $1;', [userId, passwordHash]);
}

export async function recordLoginSuccess(userId) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.users = store.users.map((user) => user.id === userId ? { ...user, failedLoginCount: 0, lockedUntil: null, lastLoginAt: new Date().toISOString() } : user);
    await saveDevStore(store);
    return;
  }
  await pgQuery(
    'UPDATE users SET failed_login_count = 0, locked_until = NULL, last_login_at = NOW() WHERE id = $1;',
    [userId]
  );
}

export async function recordLoginFailure(email) {
  const normalizedEmail = String(email || '').toLowerCase();
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.users = store.users.map((user) => {
      if (user.email !== normalizedEmail) return user;
      const count = (user.failedLoginCount ?? 0) + 1;
      return {
        ...user,
        failedLoginCount: count,
        lockedUntil: count >= 5 ? new Date(Date.now() + 15 * 60 * 1000).toISOString() : user.lockedUntil
      };
    });
    await saveDevStore(store);
    return;
  }
  await pgQuery(
    `UPDATE users
     SET failed_login_count = failed_login_count + 1,
         locked_until = CASE WHEN failed_login_count + 1 >= 5 THEN NOW() + INTERVAL '15 minutes' ELSE locked_until END
     WHERE email = $1;`,
    [normalizedEmail]
  );
}

export async function createAccountToken(token) {
  const saved = { ...token, createdAt: new Date().toISOString(), usedAt: null };
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.accountTokens = [saved, ...(store.accountTokens ?? [])].slice(0, 500);
    await saveDevStore(store);
    return saved;
  }
  const result = await pgQuery(
    `INSERT INTO account_tokens (id, user_id, token_hash, purpose, expires_at)
     VALUES ($1,$2,$3,$4,$5)
     RETURNING *;`,
    [token.id, token.userId, token.tokenHash, token.purpose, token.expiresAt]
  );
  return rowToAccountToken(result.rows[0]);
}

export async function findAccountToken(tokenHash, purpose) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    const token = (store.accountTokens ?? []).find((entry) =>
      entry.tokenHash === tokenHash &&
      entry.purpose === purpose &&
      !entry.usedAt &&
      new Date(entry.expiresAt).getTime() > Date.now()
    );
    return token ? rowToAccountToken(token) : undefined;
  }
  const result = await pgQuery(
    `SELECT * FROM account_tokens
     WHERE token_hash = $1 AND purpose = $2 AND used_at IS NULL AND expires_at > NOW()
     LIMIT 1;`,
    [tokenHash, purpose]
  );
  return result.rows[0] ? rowToAccountToken(result.rows[0]) : undefined;
}

export async function markAccountTokenUsed(id) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.accountTokens = (store.accountTokens ?? []).map((token) => token.id === id ? { ...token, usedAt: new Date().toISOString() } : token);
    await saveDevStore(store);
    return;
  }
  await pgQuery('UPDATE account_tokens SET used_at = NOW() WHERE id = $1;', [id]);
}

export async function saveAuditLog(entry) {
  const saved = {
    id: entry.id,
    actorUserId: entry.actorUserId ?? null,
    actorEmail: entry.actorEmail ?? null,
    action: entry.action,
    targetType: entry.targetType ?? null,
    targetId: entry.targetId ?? null,
    metadata: entry.metadata ?? {},
    createdAt: new Date().toISOString()
  };
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.auditLogs = [saved, ...(store.auditLogs ?? [])].slice(0, 500);
    await saveDevStore(store);
    return saved;
  }
  await pgQuery(
    `INSERT INTO audit_logs (id, actor_user_id, actor_email, action, target_type, target_id, metadata)
     VALUES ($1,$2,$3,$4,$5,$6,$7);`,
    [saved.id, saved.actorUserId, saved.actorEmail, saved.action, saved.targetType, saved.targetId, JSON.stringify(saved.metadata)]
  );
  return saved;
}

export async function listAuditLogs() {
  if (!usingPostgres) {
    const store = await loadDevStore();
    return (store.auditLogs ?? []).slice(0, 100).map(rowToAuditLog);
  }
  const result = await pgQuery('SELECT * FROM audit_logs ORDER BY created_at DESC LIMIT 100;');
  return result.rows.map(rowToAuditLog);
}

export async function saveDataset(dataset) {
  const analysis = {
    chartColumn: dataset.chartColumn,
    labelColumn: dataset.labelColumn,
    chart: dataset.chart,
    numericSummary: dataset.numericSummary,
    insights: dataset.insights,
    fileType: dataset.fileType,
    worksheetName: dataset.worksheetName,
    worksheets: dataset.worksheets
  };

  if (!usingPostgres) {
    const store = await loadDevStore();
    store.datasets = [dataset, ...store.datasets.filter((entry) => entry.id !== dataset.id)].slice(0, 50);
    await saveDevStore(store);
    return dataset;
  }

  await pgQuery(
    `INSERT INTO datasets (
       id, user_id, company_id, file_name, file_type, worksheet_name, uploaded_at,
       row_count, column_count, headers, preview, records, analysis
     )
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
     ON CONFLICT (id) DO UPDATE SET
       file_name = EXCLUDED.file_name,
       file_type = EXCLUDED.file_type,
       worksheet_name = EXCLUDED.worksheet_name,
       uploaded_at = EXCLUDED.uploaded_at,
       row_count = EXCLUDED.row_count,
       column_count = EXCLUDED.column_count,
       headers = EXCLUDED.headers,
       preview = EXCLUDED.preview,
       records = EXCLUDED.records,
       analysis = EXCLUDED.analysis;`,
    [
      dataset.id,
      dataset.userId,
      dataset.companyId ?? defaultCompanyId,
      dataset.fileName,
      dataset.fileType ?? 'csv',
      dataset.worksheetName ?? null,
      dataset.uploadedAt,
      dataset.rows,
      dataset.columns,
      JSON.stringify(dataset.headers),
      JSON.stringify(dataset.preview),
      JSON.stringify(dataset.records),
      JSON.stringify(analysis)
    ]
  );
  return dataset;
}

export async function listDatasets(user) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    return store.datasets
      .filter((dataset) => isOwner(user) || dataset.companyId === getCompanyId(user))
      .slice(0, 50)
      .map(rowToDataset);
  }
  const params = [];
  const where = isOwner(user) ? '' : 'WHERE company_id = $1';
  if (!isOwner(user)) params.push(getCompanyId(user));
  const result = await pgQuery(`SELECT * FROM datasets ${where} ORDER BY uploaded_at DESC LIMIT 50;`, params);
  return result.rows.map(rowToDataset);
}

export async function getDataset(id, user) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    const dataset = store.datasets.find((entry) => entry.id === id && (isOwner(user) || entry.companyId === getCompanyId(user)));
    return dataset ? rowToDataset(dataset) : undefined;
  }
  const params = [id];
  const companyFilter = isOwner(user) ? '' : 'AND company_id = $2';
  if (!isOwner(user)) params.push(getCompanyId(user));
  const result = await pgQuery(`SELECT * FROM datasets WHERE id = $1 ${companyFilter} LIMIT 1;`, params);
  return result.rows[0] ? rowToDataset(result.rows[0]) : undefined;
}

export async function saveDashboard(dashboard) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    const saved = { ...dashboard, createdAt: dashboard.createdAt ?? new Date().toISOString(), updatedAt: new Date().toISOString() };
    store.dashboards = [saved, ...store.dashboards.filter((entry) => entry.id !== saved.id)].slice(0, 100);
    await saveDevStore(store);
    return rowToDashboard({ ...saved, dataset_name: saved.datasetName });
  }
  const result = await pgQuery(
    `INSERT INTO dashboards (id, user_id, company_id, name, dataset_id, chart_type, config, snapshot)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (id) DO UPDATE SET
       name = EXCLUDED.name,
       chart_type = EXCLUDED.chart_type,
       config = EXCLUDED.config,
       snapshot = EXCLUDED.snapshot,
       updated_at = NOW()
     RETURNING *;`,
    [
      dashboard.id,
      dashboard.userId,
      dashboard.companyId ?? defaultCompanyId,
      dashboard.name,
      dashboard.datasetId,
      dashboard.chartType,
      JSON.stringify(dashboard.config ?? {}),
      JSON.stringify(dashboard.snapshot ?? {})
    ]
  );
  const dashboardId = result.rows[0].id;
  const joined = await pgQuery(
    `SELECT dashboards.*, datasets.file_name AS dataset_name, users.name AS owner_name, users.email AS owner_email
     FROM dashboards
     INNER JOIN datasets ON datasets.id = dashboards.dataset_id
     INNER JOIN users ON users.id = dashboards.user_id
     WHERE dashboards.id = $1
     LIMIT 1;`,
    [dashboardId]
  );
  return rowToDashboard(joined.rows[0] ?? result.rows[0]);
}

export async function listDashboards(user) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    return store.dashboards
      .filter((dashboard) => isOwner(user) || dashboard.companyId === getCompanyId(user))
      .slice(0, 50)
      .map((dashboard) => {
        const dataset = store.datasets.find((entry) => entry.id === dashboard.datasetId);
        return rowToDashboard({ ...dashboard, dataset_name: dataset?.fileName ?? dashboard.datasetName });
      });
  }
  const params = [];
  const companyFilter = isOwner(user) ? '' : 'WHERE dashboards.company_id = $1';
  if (!isOwner(user)) params.push(getCompanyId(user));
  const result = await pgQuery(
    `SELECT dashboards.*, datasets.file_name AS dataset_name, users.name AS owner_name, users.email AS owner_email
     FROM dashboards
     INNER JOIN datasets ON datasets.id = dashboards.dataset_id
     INNER JOIN users ON users.id = dashboards.user_id
     ${companyFilter}
     ORDER BY dashboards.updated_at DESC
     LIMIT 50;`,
    params
  );
  return result.rows.map(rowToDashboard);
}

export async function saveReport(report) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    const saved = { ...report, createdAt: new Date().toISOString() };
    store.reports = [saved, ...store.reports].slice(0, 100);
    await saveDevStore(store);
    return saved;
  }
  const result = await pgQuery(
    `INSERT INTO reports (id, user_id, company_id, dataset_id, title, report_type, content)
     VALUES ($1,$2,$3,$4,$5,$6,$7)
     RETURNING *;`,
    [
      report.id,
      report.userId,
      report.companyId ?? defaultCompanyId,
      report.datasetId,
      report.title,
      report.reportType ?? 'pdf',
      JSON.stringify(report.content ?? {})
    ]
  );
  const reportId = result.rows[0].id;
  const joined = await pgQuery(
    `SELECT reports.*, datasets.file_name AS dataset_name, users.name AS owner_name, users.email AS owner_email
     FROM reports
     INNER JOIN datasets ON datasets.id = reports.dataset_id
     INNER JOIN users ON users.id = reports.user_id
     WHERE reports.id = $1
     LIMIT 1;`,
    [reportId]
  );
  return rowToReport(joined.rows[0] ?? result.rows[0]);
}

export async function listReports(user) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    return store.reports
      .filter((report) => isOwner(user) || report.companyId === getCompanyId(user))
      .slice(0, 50)
      .map((report) => {
        const dataset = store.datasets.find((entry) => entry.id === report.datasetId);
        return rowToReport({ ...report, dataset_name: dataset?.fileName ?? report.datasetName });
      });
  }
  const params = [];
  const companyFilter = isOwner(user) ? '' : 'WHERE reports.company_id = $1';
  if (!isOwner(user)) params.push(getCompanyId(user));
  const result = await pgQuery(
    `SELECT reports.*, datasets.file_name AS dataset_name, users.name AS owner_name, users.email AS owner_email
     FROM reports
     INNER JOIN datasets ON datasets.id = reports.dataset_id
     INNER JOIN users ON users.id = reports.user_id
     ${companyFilter}
     ORDER BY reports.created_at DESC
     LIMIT 50;`,
    params
  );
  return result.rows.map(rowToReport);
}

export async function createModuleRecord(record) {
  const saved = {
    ...record,
    companyId: record.companyId ?? defaultCompanyId,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.moduleRecords = [saved, ...(store.moduleRecords ?? [])].slice(0, 500);
    await saveDevStore(store);
    return saved;
  }
  const result = await pgQuery(
    `INSERT INTO module_records (id, company_id, user_id, module, record_type, title, status, amount, metadata)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     RETURNING *;`,
    [
      saved.id,
      saved.companyId,
      saved.userId,
      saved.module,
      saved.recordType,
      saved.title,
      saved.status ?? 'open',
      saved.amount ?? null,
      JSON.stringify(saved.metadata ?? {})
    ]
  );
  return rowToModuleRecord(result.rows[0]);
}

export async function listModuleRecords(user, module) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    return (store.moduleRecords ?? [])
      .filter((record) => record.module === module && canAccessCompanyRecord(user, record))
      .sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime())
      .slice(0, 100)
      .map(rowToModuleRecord);
  }
  const params = [module];
  const companyFilter = isOwner(user) ? '' : 'AND company_id = $2';
  if (!isOwner(user)) params.push(getCompanyId(user));
  const result = await pgQuery(
    `SELECT * FROM module_records
     WHERE module = $1 ${companyFilter}
     ORDER BY updated_at DESC
     LIMIT 100;`,
    params
  );
  return result.rows.map(rowToModuleRecord);
}

export async function updateModuleRecord(user, id, updates) {
  const amountProvided = updates.amount !== undefined;
  const normalized = {
    title: updates.title != null ? String(updates.title).trim() : null,
    status: updates.status != null ? String(updates.status).trim() || 'open' : null,
    amount: amountProvided ? (updates.amount === '' || updates.amount == null ? null : Number(updates.amount)) : null,
    metadata: updates.metadata != null ? updates.metadata : null
  };

  if (!usingPostgres) {
    const store = await loadDevStore();
    let updated;
    store.moduleRecords = (store.moduleRecords ?? []).map((record) => {
      if (record.id !== id || !canAccessCompanyRecord(user, record)) return record;
      updated = {
        ...record,
        ...(normalized.title != null ? { title: normalized.title } : {}),
        ...(normalized.status != null ? { status: normalized.status } : {}),
        ...(amountProvided ? { amount: normalized.amount } : {}),
        ...(normalized.metadata != null ? { metadata: normalized.metadata } : {}),
        updatedAt: new Date().toISOString()
      };
      return updated;
    });
    await saveDevStore(store);
    return updated ? rowToModuleRecord(updated) : undefined;
  }

  const params = [
    id,
    normalized.title,
    normalized.status,
    normalized.amount,
    normalized.metadata == null ? null : JSON.stringify(normalized.metadata),
    amountProvided
  ];
  let companyFilter = '';
  if (!isOwner(user)) {
    params.push(getCompanyId(user));
    companyFilter = `AND company_id = $${params.length}`;
  }
  const result = await pgQuery(
    `UPDATE module_records SET
       title = COALESCE($2, title),
       status = COALESCE($3, status),
       amount = CASE WHEN $6::boolean THEN $4 ELSE amount END,
       metadata = COALESCE($5::jsonb, metadata),
       updated_at = NOW()
     WHERE id = $1 ${companyFilter}
     RETURNING *;`,
    params
  );
  return result.rows[0] ? rowToModuleRecord(result.rows[0]) : undefined;
}

export async function deleteModuleRecord(user, id) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    const before = store.moduleRecords?.length ?? 0;
    store.moduleRecords = (store.moduleRecords ?? []).filter((record) => record.id !== id || !canAccessCompanyRecord(user, record));
    await saveDevStore(store);
    return (store.moduleRecords?.length ?? 0) < before;
  }
  const params = [id];
  let companyFilter = '';
  if (!isOwner(user)) {
    params.push(getCompanyId(user));
    companyFilter = `AND company_id = $${params.length}`;
  }
  const result = await pgQuery(`DELETE FROM module_records WHERE id = $1 ${companyFilter};`, params);
  return result.rowCount > 0;
}

export async function getModuleMetrics(user) {
  const modules = ['accounting', 'engineering', 'hr', 'crm', 'dataProcessing'];
  if (!usingPostgres) {
    const store = await loadDevStore();
    return modules.reduce((metrics, module) => {
      const records = (store.moduleRecords ?? []).filter((record) => record.module === module && canAccessCompanyRecord(user, record));
      metrics[module] = { total: records.length, open: records.filter((record) => record.status !== 'closed').length };
      return metrics;
    }, {});
  }
  const params = [];
  const where = isOwner(user) ? '' : 'WHERE company_id = $1';
  if (!isOwner(user)) params.push(getCompanyId(user));
  const result = await pgQuery(
    `SELECT module, COUNT(*)::int AS total_count,
      SUM(CASE WHEN status <> 'closed' THEN 1 ELSE 0 END)::int AS open_count
     FROM module_records
     ${where}
     GROUP BY module;`,
    params
  );
  const metrics = Object.fromEntries(modules.map((module) => [module, { total: 0, open: 0 }]));
  result.rows.forEach((row) => {
    metrics[row.module] = { total: Number(row.total_count ?? 0), open: Number(row.open_count ?? 0) };
  });
  return metrics;
}

export async function saveEmailLog(log) {
  const saved = {
    id: log.id,
    companyId: log.companyId ?? defaultCompanyId,
    userId: log.userId ?? null,
    emailType: log.emailType,
    recipient: log.recipient,
    subject: log.subject,
    body: log.body ?? '',
    provider: log.provider ?? null,
    status: log.status,
    error: log.error ?? null,
    attempts: log.attempts ?? 1,
    createdAt: log.createdAt ?? new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.emailLogs = [saved, ...(store.emailLogs ?? [])].slice(0, 500);
    await saveDevStore(store);
    return saved;
  }
  const result = await pgQuery(
    `INSERT INTO email_logs (id, company_id, user_id, email_type, recipient, subject, body, provider, status, error, attempts)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
     RETURNING *;`,
    [saved.id, saved.companyId, saved.userId, saved.emailType, saved.recipient, saved.subject, saved.body, saved.provider, saved.status, saved.error, saved.attempts]
  );
  return rowToEmailLog(result.rows[0]);
}

export async function listEmailLogs(user) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    return (store.emailLogs ?? []).filter((log) => canAccessCompanyRecord(user, log)).slice(0, 100).map(rowToEmailLog);
  }
  const params = [];
  const where = isOwner(user) ? '' : 'WHERE company_id = $1';
  if (!isOwner(user)) params.push(getCompanyId(user));
  const result = await pgQuery(`SELECT * FROM email_logs ${where} ORDER BY created_at DESC LIMIT 100;`, params);
  return result.rows.map(rowToEmailLog);
}

export async function findEmailLog(user, id) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    const log = (store.emailLogs ?? []).find((entry) => entry.id === id);
    return log && canAccessCompanyRecord(user, log) ? rowToEmailLog(log) : undefined;
  }
  const params = [id];
  const companyFilter = isOwner(user) ? '' : 'AND company_id = $2';
  if (!isOwner(user)) params.push(getCompanyId(user));
  const result = await pgQuery(`SELECT * FROM email_logs WHERE id = $1 ${companyFilter} LIMIT 1;`, params);
  return result.rows[0] ? rowToEmailLog(result.rows[0]) : undefined;
}

export async function updateEmailLog(id, updates) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    let updated;
    store.emailLogs = (store.emailLogs ?? []).map((log) => {
      if (log.id !== id) return log;
      updated = { ...log, ...updates, attempts: (log.attempts ?? 1) + 1, updatedAt: new Date().toISOString() };
      return updated;
    });
    await saveDevStore(store);
    return updated ? rowToEmailLog(updated) : undefined;
  }
  const result = await pgQuery(
    `UPDATE email_logs SET
       status = $2,
       provider = $3,
       error = $4,
       attempts = attempts + 1,
       updated_at = NOW()
     WHERE id = $1
     RETURNING *;`,
    [id, updates.status, updates.provider ?? null, updates.error ?? null]
  );
  return result.rows[0] ? rowToEmailLog(result.rows[0]) : undefined;
}

export async function createInvitation(invitation) {
  const saved = {
    id: invitation.id,
    companyId: invitation.companyId ?? defaultCompanyId,
    email: invitation.email.toLowerCase(),
    role: normalizeRole(invitation.role),
    tokenHash: invitation.tokenHash,
    invitedBy: invitation.invitedBy,
    status: 'pending',
    expiresAt: invitation.expiresAt,
    acceptedAt: null,
    createdAt: new Date().toISOString()
  };
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.invitations = [saved, ...(store.invitations ?? [])].slice(0, 300);
    await saveDevStore(store);
    return saved;
  }
  const result = await pgQuery(
    `INSERT INTO invitations (id, company_id, email, role, token_hash, invited_by, expires_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7)
     RETURNING *;`,
    [saved.id, saved.companyId, saved.email, saved.role, saved.tokenHash, saved.invitedBy, saved.expiresAt]
  );
  return rowToInvitation(result.rows[0]);
}

export async function listInvitations(user) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    return (store.invitations ?? []).filter((invite) => canAccessCompanyRecord(user, invite)).slice(0, 100).map(rowToInvitation);
  }
  const params = [];
  const where = isOwner(user) ? '' : 'WHERE company_id = $1';
  if (!isOwner(user)) params.push(getCompanyId(user));
  const result = await pgQuery(`SELECT * FROM invitations ${where} ORDER BY created_at DESC LIMIT 100;`, params);
  return result.rows.map(rowToInvitation);
}

export async function findInvitationByToken(tokenHash) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    const invitation = (store.invitations ?? []).find((entry) =>
      entry.tokenHash === tokenHash &&
      entry.status === 'pending' &&
      new Date(entry.expiresAt).getTime() > Date.now()
    );
    return invitation ? rowToInvitation(invitation) : undefined;
  }
  const result = await pgQuery(
    `SELECT * FROM invitations
     WHERE token_hash = $1 AND status = 'pending' AND expires_at > NOW()
     LIMIT 1;`,
    [tokenHash]
  );
  return result.rows[0] ? rowToInvitation(result.rows[0]) : undefined;
}

export async function markInvitationAccepted(id) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.invitations = (store.invitations ?? []).map((invite) =>
      invite.id === id ? { ...invite, status: 'accepted', acceptedAt: new Date().toISOString() } : invite
    );
    await saveDevStore(store);
    return;
  }
  await pgQuery("UPDATE invitations SET status = 'accepted', accepted_at = NOW() WHERE id = $1;", [id]);
}

function rowToDashboard(row) {
  return {
    id: row.id,
    name: row.name,
    datasetId: row.dataset_id ?? row.datasetId,
    companyId: row.company_id ?? row.companyId,
    datasetName: row.dataset_name ?? row.datasetName,
    ownerName: row.owner_name,
    ownerEmail: row.owner_email,
    chartType: row.chart_type ?? row.chartType,
    config: parseJson(row.config, {}),
    snapshot: parseJson(row.snapshot, {}),
    createdAt: toIso(row.created_at ?? row.createdAt),
    updatedAt: toIso(row.updated_at ?? row.updatedAt)
  };
}

function rowToReport(row) {
  return {
    id: row.id,
    datasetId: row.dataset_id ?? row.datasetId,
    companyId: row.company_id ?? row.companyId,
    datasetName: row.dataset_name ?? row.datasetName,
    ownerName: row.owner_name,
    ownerEmail: row.owner_email,
    title: row.title,
    reportType: row.report_type ?? row.reportType,
    content: parseJson(row.content, {}),
    createdAt: toIso(row.created_at ?? row.createdAt)
  };
}

function rowToDataset(row) {
  const analysis = parseJson(row.analysis, {});
  return {
    id: row.id,
    userId: row.user_id ?? row.userId,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    fileName: row.file_name ?? row.fileName,
    fileType: row.file_type ?? row.fileType ?? analysis.fileType ?? 'csv',
    worksheetName: row.worksheet_name ?? row.worksheetName ?? analysis.worksheetName ?? null,
    worksheets: analysis.worksheets ?? row.worksheets ?? [],
    uploadedAt: toIso(row.uploaded_at ?? row.uploadedAt),
    rows: row.row_count ?? row.rows,
    columns: row.column_count ?? row.columns,
    headers: parseJson(row.headers, []),
    preview: parseJson(row.preview, []),
    records: parseJson(row.records, []),
    chartColumn: analysis.chartColumn,
    labelColumn: analysis.labelColumn,
    chart: analysis.chart ?? [],
    numericSummary: analysis.numericSummary ?? [],
    insights: analysis.insights ?? []
  };
}

function rowToUser(row, includePassword = false) {
  const email = row.email?.toLowerCase();
  return {
    id: row.id,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    name: row.name,
    email,
    role: roleForUser(email, row.role),
    active: row.active ?? row.activeStatus ?? true,
    emailVerified: row.email_verified ?? row.emailVerified ?? email === ownerEmail,
    profilePhotoUrl: row.profile_photo_url ?? row.profilePhotoUrl ?? '',
    notificationSettings: parseJson(row.notification_settings ?? row.notificationSettings, {}),
    preferences: parseJson(row.preferences, {}),
    twoFactorEnabled: row.two_factor_enabled ?? row.twoFactorEnabled ?? false,
    lastLoginAt: toIso(row.last_login_at ?? row.lastLoginAt),
    failedLoginCount: row.failed_login_count ?? row.failedLoginCount ?? 0,
    lockedUntil: toIso(row.locked_until ?? row.lockedUntil),
    createdAt: toIso(row.created_at ?? row.createdAt),
    ...(includePassword ? { passwordHash: row.password_hash ?? row.passwordHash } : {})
  };
}

function rowToModuleRecord(row) {
  return {
    id: row.id,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    userId: row.user_id ?? row.userId,
    module: row.module,
    recordType: row.record_type ?? row.recordType,
    title: row.title,
    status: row.status,
    amount: row.amount == null ? null : Number(row.amount),
    metadata: parseJson(row.metadata, {}),
    createdAt: toIso(row.created_at ?? row.createdAt),
    updatedAt: toIso(row.updated_at ?? row.updatedAt)
  };
}

function rowToAccountToken(row) {
  return {
    id: row.id,
    userId: row.user_id ?? row.userId,
    tokenHash: row.token_hash ?? row.tokenHash,
    purpose: row.purpose,
    createdAt: toIso(row.created_at ?? row.createdAt),
    expiresAt: toIso(row.expires_at ?? row.expiresAt),
    usedAt: toIso(row.used_at ?? row.usedAt)
  };
}

function rowToAuditLog(row) {
  return {
    id: row.id,
    actorUserId: row.actor_user_id ?? row.actorUserId,
    actorEmail: row.actor_email ?? row.actorEmail,
    action: row.action,
    targetType: row.target_type ?? row.targetType,
    targetId: row.target_id ?? row.targetId,
    metadata: parseJson(row.metadata, {}),
    createdAt: toIso(row.created_at ?? row.createdAt)
  };
}

function rowToEmailLog(row) {
  return {
    id: row.id,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    userId: row.user_id ?? row.userId,
    emailType: row.email_type ?? row.emailType,
    recipient: row.recipient,
    subject: row.subject,
    body: row.body,
    provider: row.provider,
    status: row.status,
    error: row.error,
    attempts: row.attempts ?? 1,
    createdAt: toIso(row.created_at ?? row.createdAt),
    updatedAt: toIso(row.updated_at ?? row.updatedAt)
  };
}

function rowToInvitation(row) {
  return {
    id: row.id,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    email: row.email,
    role: normalizeRole(row.role),
    invitedBy: row.invited_by ?? row.invitedBy,
    status: row.status,
    expiresAt: toIso(row.expires_at ?? row.expiresAt),
    acceptedAt: toIso(row.accepted_at ?? row.acceptedAt),
    createdAt: toIso(row.created_at ?? row.createdAt)
  };
}

function rowToSession(row) {
  return {
    id: row.id,
    userId: row.user_id ?? row.userId,
    createdAt: toIso(row.created_at ?? row.createdAt),
    expiresAt: toIso(row.expires_at ?? row.expiresAt),
    revokedAt: toIso(row.revoked_at ?? row.revokedAt)
  };
}

function getCompanyId(user) {
  return typeof user === 'object' ? user.companyId ?? defaultCompanyId : defaultCompanyId;
}

function canAccessCompanyRecord(user, record) {
  return isOwner(user) || (record.companyId ?? defaultCompanyId) === getCompanyId(user);
}

export function hasRole(user, requiredRole) {
  if (!user || typeof user !== 'object') return false;
  return roleRank(roleForUser(user.email, user.role)) >= roleRank(requiredRole);
}

export function isOwner(user) {
  return Boolean(user && typeof user === 'object' && roleForUser(user.email, user.role) === 'owner');
}

function protectOwnerUpdates(existingUser, updates) {
  if (isOwner(existingUser)) {
    return { ...updates, role: 'owner', active: true, emailVerified: true };
  }
  if (normalizeRole(updates.role) === 'owner' && existingUser.email !== ownerEmail) {
    return { ...updates, role: 'admin' };
  }
  return updates;
}

function roleForUser(email, role) {
  return String(email || '').toLowerCase() === ownerEmail ? 'owner' : normalizeRole(role);
}

function roleRank(role) {
  return roles.indexOf(normalizeRole(role));
}

function normalizeRole(role) {
  if (role === 'super_admin') return 'owner';
  if (role === 'user') return 'employee';
  return roles.includes(role) ? role : 'employee';
}

function parseJson(value, fallback) {
  if (value == null) return fallback;
  if (typeof value !== 'string') return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function toIso(value) {
  if (!value) return value ?? null;
  if (value instanceof Date) return value.toISOString();
  return value;
}

function databaseFromUrl(value) {
  if (!value) return null;
  try {
    return new URL(value).pathname.replace(/^\//, '') || null;
  } catch {
    return null;
  }
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function connectWithRetry(connect, label) {
  let lastError;
  const attempts = Math.max(postgresConnectRetries, 1);
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await connect();
    } catch (error) {
      lastError = error;
      lastConnectionError = error instanceof Error ? error.message : 'Unknown PostgreSQL connection error.';
      console.error(`${label} connection attempt ${attempt}/${attempts} failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
      if (attempt < attempts) await sleep(postgresRetryDelayMs);
    }
  }
  throw lastError;
}

export function getDevUser() {
  return devUser;
}

async function loadDevStore() {
  if (!existsSync(devStoreFile)) {
    return {
      users: [],
      sessions: [],
      datasets: [],
      dashboards: [],
      reports: [],
      moduleRecords: [],
      accountTokens: [],
      auditLogs: [],
      emailLogs: [],
      invitations: []
    };
  }

  const store = JSON.parse(await readFile(devStoreFile, 'utf8'));
  return {
    users: store.users ?? [],
    sessions: store.sessions ?? [],
    datasets: store.datasets ?? [],
    dashboards: store.dashboards ?? [],
    reports: store.reports ?? [],
    moduleRecords: store.moduleRecords ?? [],
    accountTokens: store.accountTokens ?? [],
    auditLogs: store.auditLogs ?? [],
    emailLogs: store.emailLogs ?? [],
    invitations: store.invitations ?? []
  };
}

async function saveDevStore(store) {
  await mkdir(dirname(devStoreFile), { recursive: true });
  try {
    await writeFile(devStoreFile, JSON.stringify(store, null, 2));
  } catch {
    // Some deployment environments expose a read-only filesystem; keep optional local persistence best-effort.
  }
}
