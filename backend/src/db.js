import { config } from 'dotenv';
import { existsSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import sql from 'mssql';

config({ path: fileURLToPath(new URL('../.env', import.meta.url)) });

const devUser = {
  id: 'local-dev-user',
  name: 'Local MVP',
  email: 'local@example.com',
  role: 'admin',
  active: true,
  createdAt: new Date().toISOString()
};
const devStoreFile = fileURLToPath(new URL('../data/dev-store.json', import.meta.url));
const sqlServerDatabase = process.env.SQLSERVER_DATABASE || 'business_ai_platform';

export const usingSqlServer = Boolean(process.env.SQLSERVER_HOST || process.env.SQLSERVER_CONNECTION_STRING);
export let pool = null;

let connectedPool;

async function getPool() {
  if (!connectedPool) {
    pool = new sql.ConnectionPool(await buildSqlServerConfig());
    connectedPool = await pool.connect();
  }
  return connectedPool;
}

async function buildSqlServerConfig() {
  if (process.env.SQLSERVER_CONNECTION_STRING) {
    return process.env.SQLSERVER_CONNECTION_STRING;
  }

  const baseConfig = {
    server: process.env.SQLSERVER_HOST,
    port: Number(process.env.SQLSERVER_PORT || 1433),
    user: process.env.SQLSERVER_USER,
    password: process.env.SQLSERVER_PASSWORD,
    pool: {
      max: Number(process.env.SQLSERVER_POOL_MAX || 10),
      min: 0,
      idleTimeoutMillis: 30000
    },
    options: {
      encrypt: process.env.SQLSERVER_ENCRYPT === 'true',
      trustServerCertificate: process.env.SQLSERVER_TRUST_SERVER_CERTIFICATE !== 'false'
    }
  };

  const masterPool = await new sql.ConnectionPool({ ...baseConfig, database: 'master' }).connect();
  try {
    await masterPool.request().query(`
      IF DB_ID(N'${escapeSqlString(sqlServerDatabase)}') IS NULL
      BEGIN
        CREATE DATABASE ${quoteSqlIdentifier(sqlServerDatabase)};
      END;
    `);
  } finally {
    await masterPool.close();
  }

  return {
    ...baseConfig,
    database: sqlServerDatabase
  };
}

export async function initDatabase() {
  if (!usingSqlServer) {
    await saveDevStore(await loadDevStore());
    return;
  }

  const db = await getPool();
  await db.request().batch(`
    IF OBJECT_ID('dbo.users', 'U') IS NULL
    BEGIN
      CREATE TABLE dbo.users (
        id NVARCHAR(64) NOT NULL PRIMARY KEY,
        name NVARCHAR(200) NOT NULL,
        email NVARCHAR(320) NOT NULL UNIQUE,
        role NVARCHAR(40) NOT NULL CONSTRAINT DF_users_role DEFAULT 'user',
        active BIT NOT NULL CONSTRAINT DF_users_active DEFAULT 1,
        password_hash NVARCHAR(500) NOT NULL,
        created_at DATETIME2 NOT NULL CONSTRAINT DF_users_created_at DEFAULT SYSUTCDATETIME()
      );
    END;

    IF COL_LENGTH('dbo.users', 'role') IS NULL
      ALTER TABLE dbo.users ADD role NVARCHAR(40) NOT NULL CONSTRAINT DF_users_role DEFAULT 'user';

    IF COL_LENGTH('dbo.users', 'active') IS NULL
      ALTER TABLE dbo.users ADD active BIT NOT NULL CONSTRAINT DF_users_active DEFAULT 1;

    IF OBJECT_ID('dbo.sessions', 'U') IS NULL
    BEGIN
      CREATE TABLE dbo.sessions (
        id NVARCHAR(64) NOT NULL PRIMARY KEY,
        user_id NVARCHAR(64) NOT NULL,
        created_at DATETIME2 NOT NULL CONSTRAINT DF_sessions_created_at DEFAULT SYSUTCDATETIME(),
        expires_at DATETIME2 NOT NULL,
        revoked_at DATETIME2 NULL,
        CONSTRAINT FK_sessions_users FOREIGN KEY (user_id) REFERENCES dbo.users(id) ON DELETE CASCADE
      );
    END;

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_sessions_user_expires_at' AND object_id = OBJECT_ID('dbo.sessions'))
      CREATE INDEX IX_sessions_user_expires_at ON dbo.sessions (user_id, expires_at DESC);

    IF OBJECT_ID('dbo.datasets', 'U') IS NULL
    BEGIN
      CREATE TABLE dbo.datasets (
        id NVARCHAR(64) NOT NULL PRIMARY KEY,
        user_id NVARCHAR(64) NOT NULL,
        file_name NVARCHAR(260) NOT NULL,
        file_type NVARCHAR(20) NOT NULL CONSTRAINT DF_datasets_file_type DEFAULT 'csv',
        worksheet_name NVARCHAR(260) NULL,
        uploaded_at DATETIME2 NOT NULL CONSTRAINT DF_datasets_uploaded_at DEFAULT SYSUTCDATETIME(),
        row_count INT NOT NULL,
        column_count INT NOT NULL,
        headers NVARCHAR(MAX) NOT NULL,
        preview NVARCHAR(MAX) NOT NULL,
        records NVARCHAR(MAX) NOT NULL,
        analysis NVARCHAR(MAX) NOT NULL,
        CONSTRAINT FK_datasets_users FOREIGN KEY (user_id) REFERENCES dbo.users(id) ON DELETE CASCADE
      );
    END;

    IF COL_LENGTH('dbo.datasets', 'file_type') IS NULL
      ALTER TABLE dbo.datasets ADD file_type NVARCHAR(20) NOT NULL CONSTRAINT DF_datasets_file_type DEFAULT 'csv';

    IF COL_LENGTH('dbo.datasets', 'worksheet_name') IS NULL
      ALTER TABLE dbo.datasets ADD worksheet_name NVARCHAR(260) NULL;

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_datasets_user_uploaded_at' AND object_id = OBJECT_ID('dbo.datasets'))
      CREATE INDEX IX_datasets_user_uploaded_at ON dbo.datasets (user_id, uploaded_at DESC);

    IF OBJECT_ID('dbo.dashboards', 'U') IS NULL
    BEGIN
      CREATE TABLE dbo.dashboards (
        id NVARCHAR(64) NOT NULL PRIMARY KEY,
        user_id NVARCHAR(64) NOT NULL,
        name NVARCHAR(260) NOT NULL,
        dataset_id NVARCHAR(64) NOT NULL,
        chart_type NVARCHAR(40) NOT NULL CONSTRAINT DF_dashboards_chart_type DEFAULT 'bar',
        config NVARCHAR(MAX) NOT NULL CONSTRAINT DF_dashboards_config DEFAULT '{}',
        snapshot NVARCHAR(MAX) NOT NULL CONSTRAINT DF_dashboards_snapshot DEFAULT '{}',
        created_at DATETIME2 NOT NULL CONSTRAINT DF_dashboards_created_at DEFAULT SYSUTCDATETIME(),
        updated_at DATETIME2 NOT NULL CONSTRAINT DF_dashboards_updated_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_dashboards_users FOREIGN KEY (user_id) REFERENCES dbo.users(id) ON DELETE CASCADE,
        CONSTRAINT FK_dashboards_datasets FOREIGN KEY (dataset_id) REFERENCES dbo.datasets(id) ON DELETE CASCADE
      );
    END;

    IF COL_LENGTH('dbo.dashboards', 'snapshot') IS NULL
      ALTER TABLE dbo.dashboards ADD snapshot NVARCHAR(MAX) NOT NULL CONSTRAINT DF_dashboards_snapshot DEFAULT '{}';

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_dashboards_user_updated_at' AND object_id = OBJECT_ID('dbo.dashboards'))
      CREATE INDEX IX_dashboards_user_updated_at ON dbo.dashboards (user_id, updated_at DESC);

    IF OBJECT_ID('dbo.reports', 'U') IS NULL
    BEGIN
      CREATE TABLE dbo.reports (
        id NVARCHAR(64) NOT NULL PRIMARY KEY,
        user_id NVARCHAR(64) NOT NULL,
        dataset_id NVARCHAR(64) NOT NULL,
        title NVARCHAR(260) NOT NULL,
        report_type NVARCHAR(40) NOT NULL CONSTRAINT DF_reports_report_type DEFAULT 'pdf',
        content NVARCHAR(MAX) NOT NULL CONSTRAINT DF_reports_content DEFAULT '{}',
        created_at DATETIME2 NOT NULL CONSTRAINT DF_reports_created_at DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_reports_users FOREIGN KEY (user_id) REFERENCES dbo.users(id) ON DELETE CASCADE,
        CONSTRAINT FK_reports_datasets FOREIGN KEY (dataset_id) REFERENCES dbo.datasets(id) ON DELETE CASCADE
      );
    END;

    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_reports_user_created_at' AND object_id = OBJECT_ID('dbo.reports'))
      CREATE INDEX IX_reports_user_created_at ON dbo.reports (user_id, created_at DESC);
  `);
}

export async function createUser(user) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    const savedUser = {
      id: user.id,
      name: user.name,
      email: user.email.toLowerCase(),
      role: normalizeRole(user.role),
      active: user.active ?? true,
      passwordHash: user.passwordHash,
      createdAt: new Date().toISOString()
    };
    store.users = [savedUser, ...store.users.filter((entry) => entry.email !== savedUser.email)];
    await saveDevStore(store);
    return rowToUser(savedUser);
  }

  const db = await getPool();
  const result = await db.request()
    .input('id', sql.NVarChar(64), user.id)
    .input('name', sql.NVarChar(200), user.name)
    .input('email', sql.NVarChar(320), user.email.toLowerCase())
    .input('role', sql.NVarChar(40), normalizeRole(user.role))
    .input('active', sql.Bit, user.active ?? true)
    .input('passwordHash', sql.NVarChar(500), user.passwordHash)
    .query(`
      INSERT INTO dbo.users (id, name, email, role, active, password_hash)
      OUTPUT inserted.id, inserted.name, inserted.email, inserted.role, inserted.active, inserted.created_at
      VALUES (@id, @name, @email, @role, @active, @passwordHash);
    `);
  return rowToUser(result.recordset[0]);
}

export async function findUserByEmail(email) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    const user = store.users.find((entry) => entry.email === email.toLowerCase());
    return user ? rowToUser(user, true) : undefined;
  }

  const db = await getPool();
  const result = await db.request()
    .input('email', sql.NVarChar(320), email.toLowerCase())
    .query('SELECT TOP (1) * FROM dbo.users WHERE email = @email;');
  return result.recordset[0] ? rowToUser(result.recordset[0], true) : undefined;
}

export async function findUserById(id) {
  if (!usingSqlServer && id === devUser.id) {
    return devUser;
  }

  if (!usingSqlServer) {
    const store = await loadDevStore();
    const user = store.users.find((entry) => entry.id === id);
    return user ? rowToUser(user) : undefined;
  }

  const db = await getPool();
  const result = await db.request()
    .input('id', sql.NVarChar(64), id)
    .query('SELECT TOP (1) id, name, email, role, active, created_at FROM dbo.users WHERE id = @id;');
  return result.recordset[0] ? rowToUser(result.recordset[0]) : undefined;
}

export async function listUsers() {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    return store.users
      .map((user) => rowToUser(user))
      .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());
  }

  const db = await getPool();
  const result = await db.request().query(`
    SELECT id, name, email, role, active, created_at
    FROM dbo.users
    ORDER BY created_at DESC;
  `);
  return result.recordset.map(rowToUser);
}

export async function updateUser(id, updates) {
  const normalizedUpdates = {
    ...(updates.name != null ? { name: String(updates.name).trim() } : {}),
    ...(updates.role != null ? { role: normalizeRole(updates.role) } : {}),
    ...(updates.active != null ? { active: Boolean(updates.active) } : {})
  };

  if (!usingSqlServer) {
    const store = await loadDevStore();
    let updated;
    store.users = store.users.map((user) => {
      if (user.id !== id) {
        return user;
      }
      updated = { ...user, ...normalizedUpdates };
      return updated;
    });
    await saveDevStore(store);
    return updated ? rowToUser(updated) : undefined;
  }

  const existing = await findUserById(id);
  if (!existing) {
    return undefined;
  }

  const next = {
    name: normalizedUpdates.name ?? existing.name,
    role: normalizedUpdates.role ?? existing.role,
    active: normalizedUpdates.active ?? existing.active
  };
  const db = await getPool();
  const result = await db.request()
    .input('id', sql.NVarChar(64), id)
    .input('name', sql.NVarChar(200), next.name)
    .input('role', sql.NVarChar(40), next.role)
    .input('active', sql.Bit, next.active)
    .query(`
      UPDATE dbo.users
      SET name = @name,
          role = @role,
          active = @active
      OUTPUT inserted.id, inserted.name, inserted.email, inserted.role, inserted.active, inserted.created_at
      WHERE id = @id;
    `);
  return result.recordset[0] ? rowToUser(result.recordset[0]) : undefined;
}

export async function deleteUser(id) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    store.users = store.users.filter((user) => user.id !== id);
    store.sessions = store.sessions.filter((session) => session.userId !== id);
    store.datasets = store.datasets.filter((dataset) => dataset.userId !== id);
    const datasetIds = new Set(store.datasets.map((dataset) => dataset.id));
    store.dashboards = store.dashboards.filter((dashboard) => dashboard.userId !== id && datasetIds.has(dashboard.datasetId));
    store.reports = store.reports.filter((report) => report.userId !== id && datasetIds.has(report.datasetId));
    await saveDevStore(store);
    return;
  }

  const db = await getPool();
  await db.request()
    .input('id', sql.NVarChar(64), id)
    .query('DELETE FROM dbo.users WHERE id = @id;');
}

export async function setUserRoleByEmail(email, role) {
  const normalizedEmail = email.toLowerCase();
  const normalizedRole = normalizeRole(role);

  if (!usingSqlServer) {
    const store = await loadDevStore();
    store.users = store.users.map((user) =>
      user.email === normalizedEmail ? { ...user, role: normalizedRole } : user
    );
    await saveDevStore(store);
    return;
  }

  const db = await getPool();
  await db.request()
    .input('email', sql.NVarChar(320), normalizedEmail)
    .input('role', sql.NVarChar(40), normalizedRole)
    .query('UPDATE dbo.users SET role = @role WHERE email = @email;');
}

export async function promoteAdminEmails(emails) {
  await Promise.all(emails.map((email) => setUserRoleByEmail(email, 'admin')));
}

export async function createSession(session) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    store.sessions = [
      {
        id: session.id,
        userId: session.userId,
        createdAt: new Date().toISOString(),
        expiresAt: session.expiresAt,
        revokedAt: null
      },
      ...store.sessions.filter((entry) => entry.id !== session.id)
    ].slice(0, 100);
    await saveDevStore(store);
    return store.sessions[0];
  }

  const db = await getPool();
  const result = await db.request()
    .input('id', sql.NVarChar(64), session.id)
    .input('userId', sql.NVarChar(64), session.userId)
    .input('expiresAt', sql.DateTime2, new Date(session.expiresAt))
    .query(`
      INSERT INTO dbo.sessions (id, user_id, expires_at)
      OUTPUT inserted.id, inserted.user_id, inserted.created_at, inserted.expires_at, inserted.revoked_at
      VALUES (@id, @userId, @expiresAt);
    `);
  return rowToSession(result.recordset[0]);
}

export async function findSessionById(id, userId) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    const session = store.sessions.find((entry) => entry.id === id && entry.userId === userId);
    if (!session || session.revokedAt || new Date(session.expiresAt).getTime() <= Date.now()) {
      return undefined;
    }
    return session;
  }

  const db = await getPool();
  const result = await db.request()
    .input('id', sql.NVarChar(64), id)
    .input('userId', sql.NVarChar(64), userId)
    .query(`
      SELECT TOP (1) id, user_id, created_at, expires_at, revoked_at
      FROM dbo.sessions
      WHERE id = @id
        AND user_id = @userId
        AND revoked_at IS NULL
        AND expires_at > SYSUTCDATETIME();
    `);
  return result.recordset[0] ? rowToSession(result.recordset[0]) : undefined;
}

export async function listSessions(userId) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    return store.sessions
      .filter((entry) => entry.userId === userId)
      .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())
      .slice(0, 20);
  }

  const db = await getPool();
  const result = await db.request()
    .input('userId', sql.NVarChar(64), userId)
    .query(`
      SELECT TOP (20) id, user_id, created_at, expires_at, revoked_at
      FROM dbo.sessions
      WHERE user_id = @userId
      ORDER BY created_at DESC;
    `);
  return result.recordset.map(rowToSession);
}

export async function revokeSession(id, userId) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    store.sessions = store.sessions.map((entry) =>
      entry.id === id && entry.userId === userId
        ? { ...entry, revokedAt: new Date().toISOString() }
        : entry
    );
    await saveDevStore(store);
    return;
  }

  const db = await getPool();
  await db.request()
    .input('id', sql.NVarChar(64), id)
    .input('userId', sql.NVarChar(64), userId)
    .query('UPDATE dbo.sessions SET revoked_at = SYSUTCDATETIME() WHERE id = @id AND user_id = @userId;');
}

export async function revokeUserSessions(userId) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    const revokedAt = new Date().toISOString();
    store.sessions = store.sessions.map((entry) =>
      entry.userId === userId && !entry.revokedAt ? { ...entry, revokedAt } : entry
    );
    await saveDevStore(store);
    return;
  }

  const db = await getPool();
  await db.request()
    .input('userId', sql.NVarChar(64), userId)
    .query('UPDATE dbo.sessions SET revoked_at = SYSUTCDATETIME() WHERE user_id = @userId AND revoked_at IS NULL;');
}

export async function saveDataset(dataset) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    store.datasets = [dataset, ...store.datasets.filter((entry) => entry.id !== dataset.id)].slice(0, 50);
    await saveDevStore(store);
    return;
  }

  const analysis = {
    chartColumn: dataset.chartColumn,
    labelColumn: dataset.labelColumn,
    chart: dataset.chart,
    numericSummary: dataset.numericSummary,
    insights: dataset.insights,
    fileType: dataset.fileType ?? 'csv',
    worksheetName: dataset.worksheetName ?? null,
    worksheets: dataset.worksheets ?? []
  };
  const db = await getPool();
  await db.request()
    .input('id', sql.NVarChar(64), dataset.id)
    .input('userId', sql.NVarChar(64), dataset.userId)
    .input('fileName', sql.NVarChar(260), dataset.fileName)
    .input('fileType', sql.NVarChar(20), dataset.fileType ?? 'csv')
    .input('worksheetName', sql.NVarChar(260), dataset.worksheetName ?? null)
    .input('uploadedAt', sql.DateTime2, new Date(dataset.uploadedAt))
    .input('rows', sql.Int, dataset.rows)
    .input('columns', sql.Int, dataset.columns)
    .input('headers', sql.NVarChar(sql.MAX), JSON.stringify(dataset.headers))
    .input('preview', sql.NVarChar(sql.MAX), JSON.stringify(dataset.preview))
    .input('records', sql.NVarChar(sql.MAX), JSON.stringify(dataset.records))
    .input('analysis', sql.NVarChar(sql.MAX), JSON.stringify(analysis))
    .query(`
      IF EXISTS (SELECT 1 FROM dbo.datasets WHERE id = @id)
      BEGIN
        UPDATE dbo.datasets
        SET user_id = @userId,
            file_name = @fileName,
            file_type = @fileType,
            worksheet_name = @worksheetName,
            uploaded_at = @uploadedAt,
            row_count = @rows,
            column_count = @columns,
            headers = @headers,
            preview = @preview,
            records = @records,
            analysis = @analysis
        WHERE id = @id;
      END
      ELSE
      BEGIN
        INSERT INTO dbo.datasets (
          id, user_id, file_name, file_type, worksheet_name, uploaded_at, row_count, column_count,
          headers, preview, records, analysis
        )
        VALUES (
          @id, @userId, @fileName, @fileType, @worksheetName, @uploadedAt, @rows, @columns,
          @headers, @preview, @records, @analysis
        );
      END;
    `);
}

export async function listDatasets(userId) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    return store.datasets
      .filter((dataset) => dataset.userId === userId)
      .sort((a, b) => new Date(b.uploadedAt).getTime() - new Date(a.uploadedAt).getTime())
      .slice(0, 50);
  }

  const db = await getPool();
  const result = await db.request()
    .input('userId', sql.NVarChar(64), userId)
    .query('SELECT TOP (50) * FROM dbo.datasets WHERE user_id = @userId ORDER BY uploaded_at DESC;');
  return result.recordset.map(rowToDataset);
}

export async function getDataset(id, userId) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    return store.datasets.find((dataset) => dataset.id === id && dataset.userId === userId);
  }

  const db = await getPool();
  const result = await db.request()
    .input('id', sql.NVarChar(64), id)
    .input('userId', sql.NVarChar(64), userId)
    .query('SELECT TOP (1) * FROM dbo.datasets WHERE id = @id AND user_id = @userId;');
  return result.recordset[0] ? rowToDataset(result.recordset[0]) : undefined;
}

export async function saveDashboard(dashboard) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    const now = new Date().toISOString();
    const savedDashboard = {
        createdAt: dashboard.createdAt ?? now,
        updatedAt: now,
        ...dashboard
    };
    store.dashboards = [
      savedDashboard,
      ...store.dashboards.filter((entry) => entry.id !== dashboard.id)
    ].slice(0, 50);
    await saveDevStore(store);
    const dataset = store.datasets.find((entry) => entry.id === savedDashboard.datasetId);
    return {
      ...savedDashboard,
      datasetName: dataset?.fileName ?? 'Unknown dataset'
    };
  }

  const db = await getPool();
  await db.request()
    .input('id', sql.NVarChar(64), dashboard.id)
    .input('userId', sql.NVarChar(64), dashboard.userId)
    .input('name', sql.NVarChar(260), dashboard.name)
    .input('datasetId', sql.NVarChar(64), dashboard.datasetId)
    .input('chartType', sql.NVarChar(40), dashboard.chartType)
    .input('config', sql.NVarChar(sql.MAX), JSON.stringify(dashboard.config ?? {}))
    .input('snapshot', sql.NVarChar(sql.MAX), JSON.stringify(dashboard.snapshot ?? {}))
    .query(`
      IF EXISTS (SELECT 1 FROM dbo.dashboards WHERE id = @id)
      BEGIN
        UPDATE dbo.dashboards
        SET user_id = @userId,
            name = @name,
            dataset_id = @datasetId,
            chart_type = @chartType,
            config = @config,
            snapshot = @snapshot,
            updated_at = SYSUTCDATETIME()
        WHERE id = @id;
      END
      ELSE
      BEGIN
        INSERT INTO dbo.dashboards (id, user_id, name, dataset_id, chart_type, config, snapshot)
        VALUES (@id, @userId, @name, @datasetId, @chartType, @config, @snapshot);
      END;
    `);

  const result = await db.request()
    .input('id', sql.NVarChar(64), dashboard.id)
    .input('userId', sql.NVarChar(64), dashboard.userId)
    .query(`
      SELECT TOP (1)
        dashboards.*,
        datasets.file_name AS dataset_name
      FROM dbo.dashboards AS dashboards
      INNER JOIN dbo.datasets AS datasets ON datasets.id = dashboards.dataset_id
      WHERE dashboards.id = @id
        AND dashboards.user_id = @userId;
    `);
  return rowToDashboard(result.recordset[0]);
}

export async function listDashboards(user) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    return store.dashboards
      .filter((dashboard) => isAdmin(user) || dashboard.userId === getUserId(user))
      .map((dashboard) => ({
        ...dashboard,
        datasetName: store.datasets.find((dataset) => dataset.id === dashboard.datasetId)?.fileName ?? 'Unknown dataset',
        ownerName: store.users.find((entry) => entry.id === dashboard.userId)?.name,
        ownerEmail: store.users.find((entry) => entry.id === dashboard.userId)?.email
      }))
      .sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime())
      .slice(0, 50);
  }

  const db = await getPool();
  const request = db.request();
  const ownershipFilter = isAdmin(user) ? '' : 'WHERE dashboards.user_id = @userId';
  if (!isAdmin(user)) {
    request.input('userId', sql.NVarChar(64), getUserId(user));
  }
  const result = await request.query(`
      SELECT TOP (50)
        dashboards.*,
        datasets.file_name AS dataset_name,
        users.name AS owner_name,
        users.email AS owner_email
      FROM dbo.dashboards AS dashboards
      INNER JOIN dbo.datasets AS datasets ON datasets.id = dashboards.dataset_id
      INNER JOIN dbo.users AS users ON users.id = dashboards.user_id
      ${ownershipFilter}
      ORDER BY dashboards.updated_at DESC;
    `);

  return result.recordset.map(rowToDashboard);
}

export async function saveReport(report) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    const savedReport = {
        createdAt: new Date().toISOString(),
        ...report
    };
    store.reports = [
      savedReport,
      ...store.reports
    ].slice(0, 50);
    await saveDevStore(store);
    const dataset = store.datasets.find((entry) => entry.id === savedReport.datasetId);
    return {
      ...savedReport,
      datasetName: dataset?.fileName ?? 'Unknown dataset'
    };
  }

  const db = await getPool();
  const result = await db.request()
    .input('id', sql.NVarChar(64), report.id)
    .input('userId', sql.NVarChar(64), report.userId)
    .input('datasetId', sql.NVarChar(64), report.datasetId)
    .input('title', sql.NVarChar(260), report.title)
    .input('reportType', sql.NVarChar(40), report.reportType ?? 'pdf')
    .input('content', sql.NVarChar(sql.MAX), JSON.stringify(report.content ?? {}))
    .query(`
      INSERT INTO dbo.reports (id, user_id, dataset_id, title, report_type, content)
      OUTPUT inserted.id, inserted.user_id, inserted.dataset_id, inserted.title, inserted.report_type, inserted.content, inserted.created_at
      VALUES (@id, @userId, @datasetId, @title, @reportType, @content);
    `);

  return {
    ...rowToReport(result.recordset[0]),
    datasetName: report.datasetName ?? 'Unknown dataset'
  };
}

export async function listReports(user) {
  if (!usingSqlServer) {
    const store = await loadDevStore();
    return store.reports
      .filter((report) => isAdmin(user) || report.userId === getUserId(user))
      .map((report) => ({
        ...report,
        datasetName: store.datasets.find((dataset) => dataset.id === report.datasetId)?.fileName ?? 'Unknown dataset',
        ownerName: store.users.find((entry) => entry.id === report.userId)?.name,
        ownerEmail: store.users.find((entry) => entry.id === report.userId)?.email
      }))
      .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())
      .slice(0, 50);
  }

  const db = await getPool();
  const request = db.request();
  const ownershipFilter = isAdmin(user) ? '' : 'WHERE reports.user_id = @userId';
  if (!isAdmin(user)) {
    request.input('userId', sql.NVarChar(64), getUserId(user));
  }
  const result = await request.query(`
      SELECT TOP (50)
        reports.*,
        datasets.file_name AS dataset_name,
        users.name AS owner_name,
        users.email AS owner_email
      FROM dbo.reports AS reports
      INNER JOIN dbo.datasets AS datasets ON datasets.id = reports.dataset_id
      INNER JOIN dbo.users AS users ON users.id = reports.user_id
      ${ownershipFilter}
      ORDER BY reports.created_at DESC;
    `);

  return result.recordset.map(rowToReport);
}

function rowToDashboard(row) {
  return {
    id: row.id,
    name: row.name,
    datasetId: row.dataset_id,
    datasetName: row.dataset_name,
    ownerName: row.owner_name,
    ownerEmail: row.owner_email,
    chartType: row.chart_type,
    config: parseJson(row.config, {}),
    snapshot: parseJson(row.snapshot, {}),
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

function rowToReport(row) {
  return {
    id: row.id,
    datasetId: row.dataset_id,
    datasetName: row.dataset_name,
    ownerName: row.owner_name,
    ownerEmail: row.owner_email,
    title: row.title,
    reportType: row.report_type,
    content: parseJson(row.content, {}),
    createdAt: row.created_at
  };
}

function rowToDataset(row) {
  const analysis = parseJson(row.analysis, {});
  return {
    id: row.id,
    userId: row.user_id ?? row.userId,
    fileName: row.file_name ?? row.fileName,
    fileType: row.file_type ?? row.fileType ?? analysis.fileType ?? 'csv',
    worksheetName: row.worksheet_name ?? row.worksheetName ?? analysis.worksheetName ?? null,
    worksheets: analysis.worksheets ?? row.worksheets ?? [],
    uploadedAt: row.uploaded_at ?? row.uploadedAt,
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
  return {
    id: row.id,
    name: row.name,
    email: row.email,
    role: normalizeRole(row.role),
    active: row.active ?? row.activeStatus ?? true,
    createdAt: row.created_at ?? row.createdAt,
    ...(includePassword ? { passwordHash: row.password_hash ?? row.passwordHash } : {})
  };
}

function getUserId(user) {
  return typeof user === 'string' ? user : user.id;
}

function isAdmin(user) {
  return typeof user === 'object' && normalizeRole(user.role) === 'admin';
}

function normalizeRole(role) {
  return role === 'admin' ? 'admin' : 'user';
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
      reports: []
    };
  }

  const store = JSON.parse(await readFile(devStoreFile, 'utf8'));
  return {
    users: store.users ?? [],
    sessions: store.sessions ?? [],
    datasets: store.datasets ?? [],
    dashboards: store.dashboards ?? [],
    reports: store.reports ?? []
  };
}

async function saveDevStore(store) {
  await mkdir(dirname(devStoreFile), { recursive: true });
  try {
    await writeFile(devStoreFile, JSON.stringify(store, null, 2));
  } catch {
    // Serverless environments may expose a read-only filesystem; local MVP mode keeps running without persistence.
  }
}

function rowToSession(row) {
  return {
    id: row.id,
    userId: row.user_id ?? row.userId,
    createdAt: row.created_at ?? row.createdAt,
    expiresAt: row.expires_at ?? row.expiresAt,
    revokedAt: row.revoked_at ?? row.revokedAt
  };
}

function parseJson(value, fallback) {
  if (value == null) {
    return fallback;
  }

  if (typeof value !== 'string') {
    return value;
  }

  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function escapeSqlString(value) {
  return String(value).replace(/'/g, "''");
}

function quoteSqlIdentifier(value) {
  return `[${String(value).replace(/]/g, ']]')}]`;
}
