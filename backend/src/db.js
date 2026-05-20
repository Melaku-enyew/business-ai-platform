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
const isVercelRuntime = process.env.VERCEL === '1';
const sampleCompanies = [
  {
    id: 'company-brightpath-logistics',
    name: 'BrightPath Logistics LLC',
    industry: 'Logistics',
    ownerName: 'Operations Team',
    email: 'ops@brightpath.example',
    phone: '202-555-0141',
    status: 'Active'
  },
  {
    id: 'company-metrocare-health',
    name: 'MetroCare Health',
    industry: 'Healthcare',
    ownerName: 'Care Administration',
    email: 'admin@metrocare.example',
    phone: '202-555-0186',
    status: 'Active'
  },
  {
    id: 'company-apex-accounting',
    name: 'Apex Accounting Group',
    industry: 'Finance',
    ownerName: 'Client Services',
    email: 'hello@apexaccounting.example',
    phone: '202-555-0198',
    status: 'Active'
  }
];

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
  const databaseUrl = process.env.DATABASE_URL || '';
  const sslRequired = /sslmode=require/i.test(databaseUrl) || isVercelRuntime || /neon\.tech/i.test(databaseUrl);
  return {
    connectionString: databaseUrl,
    ssl: sslRequired ? { rejectUnauthorized: false } : false,
    application_name: process.env.PGAPPNAME || 'metenova-ai'
  };
}

function assertPublicDatabaseUrl() {
  if (!process.env.DATABASE_URL) return;

  try {
    const host = new URL(process.env.DATABASE_URL).hostname;
    if (process.env.VERCEL === '1' && host.endsWith('.internal')) {
      throw new Error('DATABASE_URL points to a private database hostname. Configure a public Neon PostgreSQL connection string for Vercel.');
    }
    if (process.env.VERCEL === '1' && !host.includes('neon.tech')) {
      console.warn('DATABASE_URL does not use the expected Neon PostgreSQL hostname for Vercel deployments.');
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
      max: Number(process.env.PGPOOL_MAX || (isVercelRuntime ? 1 : 10)),
      idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || (isVercelRuntime ? 5000 : 30000)),
      connectionTimeoutMillis: Number(process.env.PG_CONNECT_TIMEOUT_MS || (isVercelRuntime ? 15000 : 10000)),
      keepAlive: true,
      keepAliveInitialDelayMillis: 10000,
      allowExitOnIdle: isVercelRuntime
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

async function withTransaction(operation) {
  const db = await getPool();
  const client = await db.connect();
  try {
    await client.query('BEGIN');
    const result = await operation(client);
    await client.query('COMMIT');
    return result;
  } catch (error) {
    await client.query('ROLLBACK');
    throw error;
  } finally {
    client.release();
  }
}

export async function initDatabase() {
  if (!usingPostgres) {
    const store = await loadDevStore();
    if (!(store.companies ?? []).length) {
      store.companies = sampleCompanies.map((company) => ({
        ...company,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString()
      }));
    }
    await saveDevStore(store);
    tablesInitialized = false;
    return;
  }

  await pgQuery(`
    CREATE TABLE IF NOT EXISTS companies (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      industry TEXT NOT NULL DEFAULT '',
      owner_name TEXT NOT NULL DEFAULT '',
      email TEXT NOT NULL DEFAULT '',
      phone TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'Active',
      owner_user_id TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    ALTER TABLE companies ADD COLUMN IF NOT EXISTS industry TEXT NOT NULL DEFAULT '';
    ALTER TABLE companies ADD COLUMN IF NOT EXISTS owner_name TEXT NOT NULL DEFAULT '';
    ALTER TABLE companies ADD COLUMN IF NOT EXISTS email TEXT NOT NULL DEFAULT '';
    ALTER TABLE companies ADD COLUMN IF NOT EXISTS phone TEXT NOT NULL DEFAULT '';
    ALTER TABLE companies ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'Active';
    ALTER TABLE companies ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

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

    CREATE TABLE IF NOT EXISTS user_company_assignments (
      id TEXT,
      user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      company_id TEXT NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
      role TEXT NOT NULL DEFAULT 'member',
      assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (user_id, company_id)
    );
    ALTER TABLE user_company_assignments ADD COLUMN IF NOT EXISTS id TEXT;
    ALTER TABLE user_company_assignments ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
    UPDATE user_company_assignments
       SET id = COALESCE(id, md5(user_id || ':' || company_id)),
           created_at = COALESCE(created_at, assigned_at, NOW())
     WHERE id IS NULL;

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
      analysis JSONB NOT NULL,
      original_dataset_id TEXT,
      cleaned_dataset_id TEXT,
      cleanup_status TEXT NOT NULL DEFAULT 'original',
      cleanup_logs JSONB NOT NULL DEFAULT '[]'::jsonb
    );

    ALTER TABLE datasets ADD COLUMN IF NOT EXISTS original_dataset_id TEXT;
    ALTER TABLE datasets ADD COLUMN IF NOT EXISTS cleaned_dataset_id TEXT;
    ALTER TABLE datasets ADD COLUMN IF NOT EXISTS cleanup_status TEXT NOT NULL DEFAULT 'original';
    ALTER TABLE datasets ADD COLUMN IF NOT EXISTS cleanup_logs JSONB NOT NULL DEFAULT '[]'::jsonb;
    ALTER TABLE datasets ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
    ALTER TABLE datasets ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;
    ALTER TABLE datasets ADD COLUMN IF NOT EXISTS retention_until TIMESTAMPTZ;

    CREATE TABLE IF NOT EXISTS cleanup_jobs (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      original_dataset_id TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
      cleaned_dataset_id TEXT REFERENCES datasets(id) ON DELETE SET NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
      logs JSONB NOT NULL DEFAULT '[]'::jsonb,
      error TEXT,
      deleted_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    ALTER TABLE cleanup_jobs ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

    CREATE TABLE IF NOT EXISTS pipelines (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      department TEXT NOT NULL,
      name TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'active',
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS pipeline_runs (
      id TEXT PRIMARY KEY,
      pipeline_id TEXT REFERENCES pipelines(id) ON DELETE CASCADE,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      status TEXT NOT NULL DEFAULT 'queued',
      progress INTEGER NOT NULL DEFAULT 0,
      logs JSONB NOT NULL DEFAULT '[]'::jsonb,
      dependencies JSONB NOT NULL DEFAULT '[]'::jsonb,
      scheduled_for TIMESTAMPTZ,
      started_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS pipeline_rules (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      module TEXT NOT NULL,
      rule_key TEXT NOT NULL,
      label TEXT NOT NULL,
      config JSONB NOT NULL DEFAULT '{}'::jsonb,
      enabled BOOLEAN NOT NULL DEFAULT TRUE,
      created_by TEXT REFERENCES users(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS pipeline_stage_runs (
      id TEXT PRIMARY KEY,
      pipeline_id TEXT REFERENCES pipelines(id) ON DELETE CASCADE,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      module TEXT NOT NULL,
      dataset_id TEXT REFERENCES datasets(id) ON DELETE SET NULL,
      stage_name TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'queued',
      operator_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      logs JSONB NOT NULL DEFAULT '[]'::jsonb,
      validation_output JSONB NOT NULL DEFAULT '{}'::jsonb,
      metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
      started_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS enterprise_connectors (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      name TEXT NOT NULL,
      connector_type TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'not_configured',
      health_status TEXT NOT NULL DEFAULT 'unknown',
      permissions JSONB NOT NULL DEFAULT '{}'::jsonb,
      schedule JSONB NOT NULL DEFAULT '{}'::jsonb,
      encrypted_credentials TEXT,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      last_sync_at TIMESTAMPTZ,
      next_sync_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS connector_sync_logs (
      id TEXT PRIMARY KEY,
      connector_id TEXT REFERENCES enterprise_connectors(id) ON DELETE CASCADE,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      status TEXT NOT NULL DEFAULT 'queued',
      records_processed INTEGER NOT NULL DEFAULT 0,
      failed_rows INTEGER NOT NULL DEFAULT 0,
      retries INTEGER NOT NULL DEFAULT 0,
      duration_ms INTEGER NOT NULL DEFAULT 0,
      error TEXT,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      started_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS pipeline_schedules (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      pipeline_id TEXT REFERENCES pipelines(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      schedule_type TEXT NOT NULL DEFAULT 'cron',
      cron_expression TEXT,
      event_trigger TEXT,
      priority INTEGER NOT NULL DEFAULT 5,
      sla_minutes INTEGER NOT NULL DEFAULT 60,
      retry_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
      dependencies JSONB NOT NULL DEFAULT '[]'::jsonb,
      status TEXT NOT NULL DEFAULT 'queued',
      next_run_at TIMESTAMPTZ,
      last_run_at TIMESTAMPTZ,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_by TEXT REFERENCES users(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS workflow_intelligence (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      dataset_id TEXT REFERENCES datasets(id) ON DELETE SET NULL,
      module TEXT NOT NULL,
      insight_type TEXT NOT NULL,
      severity TEXT NOT NULL DEFAULT 'info',
      title TEXT NOT NULL,
      summary TEXT NOT NULL,
      confidence NUMERIC(5,2) NOT NULL DEFAULT 0,
      recommendations JSONB NOT NULL DEFAULT '[]'::jsonb,
      explainability JSONB NOT NULL DEFAULT '{}'::jsonb,
      status TEXT NOT NULL DEFAULT 'active',
      created_by TEXT REFERENCES users(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS workflows (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      pipeline_id TEXT REFERENCES pipelines(id) ON DELETE SET NULL,
      dataset_id TEXT REFERENCES datasets(id) ON DELETE SET NULL,
      module TEXT NOT NULL DEFAULT 'data-processing',
      name TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'queued',
      priority INTEGER NOT NULL DEFAULT 5,
      current_stage TEXT,
      metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
      logs JSONB NOT NULL DEFAULT '[]'::jsonb,
      created_by TEXT REFERENCES users(id) ON DELETE SET NULL,
      started_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS workflow_stage_runs (
      id TEXT PRIMARY KEY,
      workflow_id TEXT REFERENCES workflows(id) ON DELETE CASCADE,
      pipeline_stage_run_id TEXT REFERENCES pipeline_stage_runs(id) ON DELETE SET NULL,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      stage_name TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'queued',
      operator_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      validation_output JSONB NOT NULL DEFAULT '{}'::jsonb,
      metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
      logs JSONB NOT NULL DEFAULT '[]'::jsonb,
      retry_count INTEGER NOT NULL DEFAULT 0,
      started_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS workflow_rules (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      pipeline_rule_id TEXT REFERENCES pipeline_rules(id) ON DELETE SET NULL,
      module TEXT NOT NULL,
      rule_key TEXT NOT NULL,
      label TEXT NOT NULL,
      config JSONB NOT NULL DEFAULT '{}'::jsonb,
      enabled BOOLEAN NOT NULL DEFAULT TRUE,
      created_by TEXT REFERENCES users(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS approvals (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      workflow_id TEXT REFERENCES workflows(id) ON DELETE SET NULL,
      dataset_id TEXT REFERENCES datasets(id) ON DELETE SET NULL,
      requested_by TEXT REFERENCES users(id) ON DELETE SET NULL,
      approver_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      decision_notes TEXT NOT NULL DEFAULT '',
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      decided_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS connector_configs (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      connector_id TEXT REFERENCES enterprise_connectors(id) ON DELETE SET NULL,
      connector_type TEXT NOT NULL,
      name TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'not_configured',
      encrypted_credentials TEXT,
      permissions JSONB NOT NULL DEFAULT '{}'::jsonb,
      schedule JSONB NOT NULL DEFAULT '{}'::jsonb,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_by TEXT REFERENCES users(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS sync_logs (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      connector_id TEXT REFERENCES enterprise_connectors(id) ON DELETE SET NULL,
      connector_config_id TEXT REFERENCES connector_configs(id) ON DELETE SET NULL,
      status TEXT NOT NULL DEFAULT 'queued',
      records_processed INTEGER NOT NULL DEFAULT 0,
      failed_rows INTEGER NOT NULL DEFAULT 0,
      retries INTEGER NOT NULL DEFAULT 0,
      duration_ms INTEGER NOT NULL DEFAULT 0,
      error TEXT,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      started_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS schedules (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      pipeline_id TEXT REFERENCES pipelines(id) ON DELETE SET NULL,
      workflow_id TEXT REFERENCES workflows(id) ON DELETE SET NULL,
      name TEXT NOT NULL,
      schedule_type TEXT NOT NULL DEFAULT 'cron',
      cron_expression TEXT,
      event_trigger TEXT,
      status TEXT NOT NULL DEFAULT 'queued',
      priority INTEGER NOT NULL DEFAULT 5,
      sla_minutes INTEGER NOT NULL DEFAULT 60,
      retry_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_by TEXT REFERENCES users(id) ON DELETE SET NULL,
      next_run_at TIMESTAMPTZ,
      last_run_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS ai_insights (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      workflow_intelligence_id TEXT REFERENCES workflow_intelligence(id) ON DELETE SET NULL,
      dataset_id TEXT REFERENCES datasets(id) ON DELETE SET NULL,
      module TEXT NOT NULL,
      insight_type TEXT NOT NULL DEFAULT 'recommendation',
      severity TEXT NOT NULL DEFAULT 'info',
      title TEXT NOT NULL,
      summary TEXT NOT NULL,
      confidence NUMERIC(5,2) NOT NULL DEFAULT 0,
      recommendations JSONB NOT NULL DEFAULT '[]'::jsonb,
      explainability JSONB NOT NULL DEFAULT '{}'::jsonb,
      status TEXT NOT NULL DEFAULT 'active',
      created_by TEXT REFERENCES users(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS dataset_versions (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      dataset_id TEXT REFERENCES datasets(id) ON DELETE CASCADE,
      version_number INTEGER NOT NULL DEFAULT 1,
      version_type TEXT NOT NULL DEFAULT 'cleaned',
      records JSONB NOT NULL DEFAULT '[]'::jsonb,
      metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_by TEXT REFERENCES users(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS export_history (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      dataset_id TEXT REFERENCES datasets(id) ON DELETE SET NULL,
      user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      export_type TEXT NOT NULL DEFAULT 'csv',
      filename TEXT NOT NULL DEFAULT '',
      row_count INTEGER NOT NULL DEFAULT 0,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS login_history (
      id TEXT PRIMARY KEY,
      user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      email TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL,
      ip_address TEXT,
      user_agent TEXT,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS access_requests (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      requester_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      target_user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      department TEXT,
      requested_role TEXT NOT NULL DEFAULT 'viewer',
      reason TEXT NOT NULL DEFAULT '',
      status TEXT NOT NULL DEFAULT 'pending',
      expires_at TIMESTAMPTZ,
      approved_by TEXT REFERENCES users(id) ON DELETE SET NULL,
      approved_at TIMESTAMPTZ,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS analytics (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      user_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      dataset_id TEXT REFERENCES datasets(id) ON DELETE CASCADE,
      analytics_type TEXT NOT NULL,
      summary JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS notifications (
      id TEXT PRIMARY KEY,
      company_id TEXT NOT NULL DEFAULT '${defaultCompanyId}',
      user_id TEXT REFERENCES users(id) ON DELETE CASCADE,
      type TEXT NOT NULL,
      title TEXT NOT NULL,
      message TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'unread',
      archived_at TIMESTAMPTZ,
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    ALTER TABLE notifications ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ;

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
      deleted_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    ALTER TABLE reports ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

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
    CREATE UNIQUE INDEX IF NOT EXISTS idx_user_company_assignments_id ON user_company_assignments (id);
    CREATE INDEX IF NOT EXISTS idx_user_company_assignments_company ON user_company_assignments (company_id, user_id);
    CREATE INDEX IF NOT EXISTS idx_companies_updated_at ON companies (updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_datasets_user_uploaded_at ON datasets (user_id, uploaded_at DESC);
    CREATE INDEX IF NOT EXISTS idx_datasets_company_uploaded_at ON datasets (company_id, uploaded_at DESC);
    CREATE INDEX IF NOT EXISTS idx_datasets_company_active ON datasets (company_id, uploaded_at DESC) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS idx_cleanup_jobs_company_created_at ON cleanup_jobs (company_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_cleanup_jobs_original_dataset ON cleanup_jobs (original_dataset_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_pipelines_company_department ON pipelines (company_id, department, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_pipeline_runs_company_status ON pipeline_runs (company_id, status, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_pipeline_rules_company_module ON pipeline_rules (company_id, module, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_pipeline_stage_runs_company_status ON pipeline_stage_runs (company_id, status, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_pipeline_stage_runs_dataset ON pipeline_stage_runs (dataset_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_enterprise_connectors_company_status ON enterprise_connectors (company_id, status, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_connector_sync_logs_company_created ON connector_sync_logs (company_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_pipeline_schedules_company_status ON pipeline_schedules (company_id, status, next_run_at DESC);
    CREATE INDEX IF NOT EXISTS idx_workflow_intelligence_company_module ON workflow_intelligence (company_id, module, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_workflows_company_status ON workflows (company_id, status, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_workflow_stage_runs_company_status ON workflow_stage_runs (company_id, status, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_workflow_rules_company_module ON workflow_rules (company_id, module, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_approvals_company_status ON approvals (company_id, status, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_connector_configs_company_status ON connector_configs (company_id, status, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_sync_logs_company_created ON sync_logs (company_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_schedules_company_status ON schedules (company_id, status, next_run_at DESC);
    CREATE INDEX IF NOT EXISTS idx_ai_insights_company_module ON ai_insights (company_id, module, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_dataset_versions_dataset ON dataset_versions (dataset_id, version_number DESC);
    CREATE INDEX IF NOT EXISTS idx_export_history_company_created ON export_history (company_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_login_history_email_created ON login_history (email, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_access_requests_company_status ON access_requests (company_id, status, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_analytics_company_created_at ON analytics (company_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_notifications_company_created_at ON notifications (company_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_dashboards_user_updated_at ON dashboards (user_id, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_dashboards_company_updated_at ON dashboards (company_id, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_reports_user_created_at ON reports (user_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_reports_company_created_at ON reports (company_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_reports_company_active ON reports (company_id, created_at DESC) WHERE deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS idx_module_records_company_module ON module_records (company_id, module, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_account_tokens_user_purpose ON account_tokens (user_id, purpose, expires_at DESC);
    CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs (created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_email_logs_company_created_at ON email_logs (company_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_invitations_company_created_at ON invitations (company_id, created_at DESC);
  `);

  await pgQuery(
    `INSERT INTO companies (id, name, industry, owner_name, email, phone, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;`,
    [defaultCompanyId, defaultCompanyName, 'Technology', 'Metenova AI', ownerEmail, '', 'Active']
  );

  const companyCount = await pgQuery('SELECT COUNT(*)::int AS count FROM companies WHERE id <> $1;', [defaultCompanyId]);
  if (Number(companyCount.rows[0]?.count ?? 0) === 0) {
    await Promise.all(sampleCompanies.map((company) => createCompany(company)));
  }

  await pgQuery(`
    INSERT INTO user_company_assignments (id, user_id, company_id, role)
    SELECT md5(id || ':' || company_id), id, company_id, role
    FROM users
    WHERE company_id IS NOT NULL
    ON CONFLICT (user_id, company_id) DO UPDATE SET role = EXCLUDED.role;
  `);

  await pgQuery(`
    DO $$
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_datasets_company') THEN
        ALTER TABLE datasets
          ADD CONSTRAINT fk_datasets_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_datasets_original_dataset') THEN
        ALTER TABLE datasets
          ADD CONSTRAINT fk_datasets_original_dataset FOREIGN KEY (original_dataset_id) REFERENCES datasets(id) ON DELETE SET NULL;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_datasets_cleaned_dataset') THEN
        ALTER TABLE datasets
          ADD CONSTRAINT fk_datasets_cleaned_dataset FOREIGN KEY (cleaned_dataset_id) REFERENCES datasets(id) ON DELETE SET NULL;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_cleanup_jobs_company') THEN
        ALTER TABLE cleanup_jobs
          ADD CONSTRAINT fk_cleanup_jobs_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_pipelines_company') THEN
        ALTER TABLE pipelines
          ADD CONSTRAINT fk_pipelines_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_pipeline_runs_company') THEN
        ALTER TABLE pipeline_runs
          ADD CONSTRAINT fk_pipeline_runs_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_pipeline_rules_company') THEN
        ALTER TABLE pipeline_rules
          ADD CONSTRAINT fk_pipeline_rules_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_pipeline_stage_runs_company') THEN
        ALTER TABLE pipeline_stage_runs
          ADD CONSTRAINT fk_pipeline_stage_runs_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_enterprise_connectors_company') THEN
        ALTER TABLE enterprise_connectors
          ADD CONSTRAINT fk_enterprise_connectors_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_connector_sync_logs_company') THEN
        ALTER TABLE connector_sync_logs
          ADD CONSTRAINT fk_connector_sync_logs_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_pipeline_schedules_company') THEN
        ALTER TABLE pipeline_schedules
          ADD CONSTRAINT fk_pipeline_schedules_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_workflow_intelligence_company') THEN
        ALTER TABLE workflow_intelligence
          ADD CONSTRAINT fk_workflow_intelligence_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_workflows_company') THEN
        ALTER TABLE workflows
          ADD CONSTRAINT fk_workflows_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_workflow_stage_runs_company') THEN
        ALTER TABLE workflow_stage_runs
          ADD CONSTRAINT fk_workflow_stage_runs_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_workflow_rules_company') THEN
        ALTER TABLE workflow_rules
          ADD CONSTRAINT fk_workflow_rules_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_approvals_company') THEN
        ALTER TABLE approvals
          ADD CONSTRAINT fk_approvals_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_connector_configs_company') THEN
        ALTER TABLE connector_configs
          ADD CONSTRAINT fk_connector_configs_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_sync_logs_company') THEN
        ALTER TABLE sync_logs
          ADD CONSTRAINT fk_sync_logs_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_schedules_company') THEN
        ALTER TABLE schedules
          ADD CONSTRAINT fk_schedules_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_ai_insights_company') THEN
        ALTER TABLE ai_insights
          ADD CONSTRAINT fk_ai_insights_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_dataset_versions_company') THEN
        ALTER TABLE dataset_versions
          ADD CONSTRAINT fk_dataset_versions_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_export_history_company') THEN
        ALTER TABLE export_history
          ADD CONSTRAINT fk_export_history_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_access_requests_company') THEN
        ALTER TABLE access_requests
          ADD CONSTRAINT fk_access_requests_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_analytics_company') THEN
        ALTER TABLE analytics
          ADD CONSTRAINT fk_analytics_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_notifications_company') THEN
        ALTER TABLE notifications
          ADD CONSTRAINT fk_notifications_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_dashboards_company') THEN
        ALTER TABLE dashboards
          ADD CONSTRAINT fk_dashboards_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
      IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_reports_company') THEN
        ALTER TABLE reports
          ADD CONSTRAINT fk_reports_company FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE;
      END IF;
    END $$;
  `);
  tablesInitialized = true;
}

export function getDatabaseRuntimeStatus() {
  return {
    usingPostgres,
    hostConfigured: Boolean(process.env.DATABASE_URL),
    database: usingPostgres ? databaseFromUrl(process.env.DATABASE_URL) : null,
    host: usingPostgres ? publicHostLabel(process.env.DATABASE_URL) : null,
    port: usingPostgres ? databasePort(process.env.DATABASE_URL) : null,
    connected,
    tablesInitialized,
    connectionError: lastConnectionError,
    retries: postgresConnectRetries
  };
}

export async function getAccessibleCompanyIds(user) {
  if (hasGlobalCompanyAccess(user)) return null;
  const assigned = new Set();

  if (!usingPostgres) {
    const store = await loadDevStore();
    (store.userCompanyAssignments ?? [])
      .filter((assignment) => assignment.userId === user?.id)
      .forEach((assignment) => assigned.add(assignment.companyId));
    if (user?.companyId && user.companyId !== defaultCompanyId) assigned.add(user.companyId);
    return [...assigned];
  }

  const result = await pgQuery(
    `SELECT company_id FROM user_company_assignments WHERE user_id = $1
     UNION
     SELECT company_id FROM users WHERE id = $1 AND company_id <> $2;`,
    [user?.id, defaultCompanyId]
  );
  return result.rows.map((row) => row.company_id);
}

export async function canAccessCompany(user, companyId) {
  const normalizedCompanyId = String(companyId || '').trim();
  if (!normalizedCompanyId) return false;
  if (hasGlobalCompanyAccess(user)) return normalizedCompanyId !== defaultCompanyId || isOwner(user);
  const assigned = await getAccessibleCompanyIds(user);
  return assigned.includes(normalizedCompanyId);
}

export async function listCompanies(user) {
  if (!usingPostgres) {
    const store = await loadDevStore();
    const assignedCompanyIds = await getAccessibleCompanyIds(user);
    return (store.companies ?? [])
      .filter((company) => company.id !== defaultCompanyId)
      .filter((company) => assignedCompanyIds === null || assignedCompanyIds.includes(company.id))
      .sort((a, b) => new Date(b.updatedAt ?? b.createdAt).getTime() - new Date(a.updatedAt ?? a.createdAt).getTime())
      .map(rowToCompany);
  }

  if (!hasGlobalCompanyAccess(user)) {
    const assignedCompanyIds = await getAccessibleCompanyIds(user);
    if (!assignedCompanyIds.length) return [];
    const result = await pgQuery(
      `SELECT * FROM companies
       WHERE id = ANY($1::text[]) AND id <> $2
       ORDER BY updated_at DESC, created_at DESC;`,
      [assignedCompanyIds, defaultCompanyId]
    );
    return result.rows.map(rowToCompany);
  }

  const result = await pgQuery(
    `SELECT * FROM companies
     WHERE id <> $1
     ORDER BY updated_at DESC, created_at DESC;`,
    [defaultCompanyId]
  );
  return result.rows.map(rowToCompany);
}

export async function listUserCompanyAssignments(userId) {
  const normalizedUserId = String(userId || '').trim();
  if (!normalizedUserId) return [];

  if (!usingPostgres) {
    const store = await loadDevStore();
    const companiesById = new Map((store.companies ?? []).map((company) => [company.id, rowToCompany(company)]));
    return (store.userCompanyAssignments ?? [])
      .filter((assignment) => assignment.userId === normalizedUserId)
      .map((assignment) => rowToCompanyAssignment({
        ...assignment,
        id: assignment.id ?? `${assignment.userId}:${assignment.companyId}`,
        companyName: companiesById.get(assignment.companyId)?.name ?? assignment.companyName ?? 'Unknown company'
      }))
      .sort((a, b) => a.companyName.localeCompare(b.companyName));
  }

  const result = await pgQuery(
    `SELECT assignments.*, companies.name AS company_name
       FROM user_company_assignments assignments
       INNER JOIN companies ON companies.id = assignments.company_id
      WHERE assignments.user_id = $1
      ORDER BY companies.name ASC;`,
    [normalizedUserId]
  );
  return result.rows.map(rowToCompanyAssignment);
}

export async function replaceUserCompanyAssignments(userId, assignments) {
  const normalizedUserId = String(userId || '').trim();
  const normalizedAssignments = (Array.isArray(assignments) ? assignments : [])
    .map((assignment) => ({
      companyId: String(assignment.companyId || assignment.company_id || '').trim(),
      role: normalizeRole(assignment.role)
    }))
    .filter((assignment) => assignment.companyId && assignment.companyId !== defaultCompanyId);

  const uniqueAssignments = [...new Map(normalizedAssignments.map((assignment) => [assignment.companyId, assignment])).values()];

  if (!usingPostgres) {
    const store = await loadDevStore();
    const companies = new Set((store.companies ?? []).map((company) => company.id));
    const validAssignments = uniqueAssignments.filter((assignment) => companies.has(assignment.companyId));
    store.userCompanyAssignments = [
      ...(store.userCompanyAssignments ?? []).filter((assignment) => assignment.userId !== normalizedUserId),
      ...validAssignments.map((assignment) => ({
        id: `${normalizedUserId}:${assignment.companyId}`,
        userId: normalizedUserId,
        companyId: assignment.companyId,
        role: assignment.role,
        assignedAt: new Date().toISOString(),
        createdAt: new Date().toISOString()
      }))
    ];
    const primaryCompanyId = validAssignments[0]?.companyId ?? defaultCompanyId;
    store.users = (store.users ?? []).map((user) => user.id === normalizedUserId ? { ...user, companyId: primaryCompanyId } : user);
    await saveDevStore(store);
    return listUserCompanyAssignments(normalizedUserId);
  }

  await withTransaction(async (client) => {
    await client.query('DELETE FROM user_company_assignments WHERE user_id = $1;', [normalizedUserId]);
    for (const assignment of uniqueAssignments) {
      await client.query(
        `INSERT INTO user_company_assignments (id, user_id, company_id, role)
         SELECT $1, $2, $3, $4
         WHERE EXISTS (SELECT 1 FROM companies WHERE id = $3 AND id <> $5)
         ON CONFLICT (user_id, company_id) DO UPDATE SET role = EXCLUDED.role;`,
        [`${normalizedUserId}:${assignment.companyId}`, normalizedUserId, assignment.companyId, assignment.role, defaultCompanyId]
      );
    }
    const primaryCompanyId = uniqueAssignments[0]?.companyId ?? defaultCompanyId;
    await client.query('UPDATE users SET company_id = $2 WHERE id = $1;', [normalizedUserId, primaryCompanyId]);
  });

  return listUserCompanyAssignments(normalizedUserId);
}

export async function createCompany(company) {
  const saved = {
    id: company.id,
    name: company.name,
    industry: company.industry,
    ownerName: company.ownerName,
    email: company.email.toLowerCase(),
    phone: company.phone,
    status: company.status ?? 'Active',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };

  if (!usingPostgres) {
    const store = await loadDevStore();
    store.companies = [saved, ...(store.companies ?? []).filter((entry) => entry.id !== saved.id)];
    await saveDevStore(store);
    return rowToCompany(saved);
  }

  const result = await pgQuery(
    `INSERT INTO companies (id, name, industry, owner_name, email, phone, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     RETURNING *;`,
    [saved.id, saved.name, saved.industry, saved.ownerName, saved.email, saved.phone, saved.status]
  );
  return rowToCompany(result.rows[0]);
}

export async function updateCompany(id, updates) {
  const companyId = String(id || '').trim();
  const normalized = {
    name: updates.name != null ? String(updates.name).trim() : null,
    industry: updates.industry != null ? String(updates.industry).trim() : null,
    ownerName: updates.ownerName != null ? String(updates.ownerName).trim() : null,
    email: updates.email != null ? String(updates.email).trim().toLowerCase() : null,
    phone: updates.phone != null ? String(updates.phone).trim() : null,
    status: updates.status != null ? String(updates.status).trim() || 'Active' : null
  };

  if (!usingPostgres) {
    const store = await loadDevStore();
    let updated;
    store.companies = (store.companies ?? []).map((company) => {
      if (company.id !== companyId || company.id === defaultCompanyId) return company;
      updated = {
        ...company,
        ...(normalized.name != null ? { name: normalized.name } : {}),
        ...(normalized.industry != null ? { industry: normalized.industry } : {}),
        ...(normalized.ownerName != null ? { ownerName: normalized.ownerName } : {}),
        ...(normalized.email != null ? { email: normalized.email } : {}),
        ...(normalized.phone != null ? { phone: normalized.phone } : {}),
        ...(normalized.status != null ? { status: normalized.status } : {}),
        updatedAt: new Date().toISOString()
      };
      return updated;
    });
    await saveDevStore(store);
    return updated ? rowToCompany(updated) : undefined;
  }

  const result = await pgQuery(
    `UPDATE companies SET
       name = COALESCE($2, name),
       industry = COALESCE($3, industry),
       owner_name = COALESCE($4, owner_name),
       email = COALESCE($5, email),
       phone = COALESCE($6, phone),
       status = COALESCE($7, status),
       updated_at = NOW()
     WHERE id = $1 AND id <> $8
     RETURNING *;`,
    [companyId, normalized.name, normalized.industry, normalized.ownerName, normalized.email, normalized.phone, normalized.status, defaultCompanyId]
  );
  return result.rows[0] ? rowToCompany(result.rows[0]) : undefined;
}

export async function deleteCompany(id) {
  const companyId = String(id || '').trim();
  if (!companyId || companyId === defaultCompanyId) return false;

  if (!usingPostgres) {
    const store = await loadDevStore();
    const before = store.companies?.length ?? 0;
    store.companies = (store.companies ?? []).filter((company) => company.id !== companyId);
    store.users = (store.users ?? []).filter((user) => user.companyId !== companyId);
    store.datasets = (store.datasets ?? []).filter((dataset) => dataset.companyId !== companyId);
    store.cleanupJobs = (store.cleanupJobs ?? []).filter((job) => job.companyId !== companyId);
    store.dashboards = (store.dashboards ?? []).filter((dashboard) => dashboard.companyId !== companyId);
    store.reports = (store.reports ?? []).filter((report) => report.companyId !== companyId);
    store.moduleRecords = (store.moduleRecords ?? []).filter((record) => record.companyId !== companyId);
    store.notifications = (store.notifications ?? []).filter((notification) => notification.companyId !== companyId);
    store.analytics = (store.analytics ?? []).filter((entry) => entry.companyId !== companyId);
    store.pipelines = (store.pipelines ?? []).filter((entry) => entry.companyId !== companyId);
    store.enterpriseConnectors = (store.enterpriseConnectors ?? []).filter((entry) => entry.companyId !== companyId);
    store.connectorSyncLogs = (store.connectorSyncLogs ?? []).filter((entry) => entry.companyId !== companyId);
    store.pipelineSchedules = (store.pipelineSchedules ?? []).filter((entry) => entry.companyId !== companyId);
    store.workflowIntelligence = (store.workflowIntelligence ?? []).filter((entry) => entry.companyId !== companyId);
    store.accessRequests = (store.accessRequests ?? []).filter((entry) => entry.companyId !== companyId);
    store.userCompanyAssignments = (store.userCompanyAssignments ?? []).filter((assignment) => assignment.companyId !== companyId);
    await saveDevStore(store);
    return (store.companies?.length ?? 0) < before;
  }

  await pgQuery('DELETE FROM cleanup_jobs WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM reports WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM dashboards WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM analytics WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM notifications WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM connector_sync_logs WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM enterprise_connectors WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM pipeline_schedules WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM workflow_intelligence WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM access_requests WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM pipelines WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM module_records WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM invitations WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM email_logs WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM datasets WHERE company_id = $1;', [companyId]);
  await pgQuery('DELETE FROM users WHERE company_id = $1;', [companyId]);
  const result = await pgQuery('DELETE FROM companies WHERE id = $1 AND id <> $2;', [companyId, defaultCompanyId]);
  return result.rowCount > 0;
}

export async function companyExists(id) {
  const companyId = String(id || '').trim();
  if (!companyId) return false;

  if (!usingPostgres) {
    const store = await loadDevStore();
    return companyId === defaultCompanyId || (store.companies ?? []).some((company) => company.id === companyId);
  }

  const result = await pgQuery('SELECT id FROM companies WHERE id = $1 LIMIT 1;', [companyId]);
  return result.rows.length > 0;
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
    store.userCompanyAssignments = [
      { id: `${savedUser.id}:${savedUser.companyId}`, userId: savedUser.id, companyId: savedUser.companyId, role: savedUser.role, assignedAt: new Date().toISOString(), createdAt: new Date().toISOString() },
      ...(store.userCompanyAssignments ?? []).filter((assignment) => assignment.userId !== savedUser.id || assignment.companyId !== savedUser.companyId)
    ];
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
  await pgQuery(
    `INSERT INTO user_company_assignments (id, user_id, company_id, role)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (user_id, company_id) DO UPDATE SET role = EXCLUDED.role;`,
    [`${savedUser.id}:${savedUser.companyId}`, savedUser.id, savedUser.companyId, savedUser.role]
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
    const accessibleIds = await getAccessibleCompanyIds(requestingUser);
    const visibleUsers = store.users
      .filter((user) => accessibleIds === null || accessibleIds.includes(user.companyId))
      .map((user) => rowToUser(user));
    return Promise.all(visibleUsers.map(async (user) => ({
      ...user,
      assignedCompanies: await listUserCompanyAssignments(user.id)
    })));
  }

  const params = [];
  let where = '';
  const accessibleIds = await getAccessibleCompanyIds(requestingUser);
  if (accessibleIds !== null) {
    if (!accessibleIds.length) return [];
    params.push(accessibleIds);
    where = 'WHERE company_id = ANY($1::text[]) OR id IN (SELECT user_id FROM user_company_assignments WHERE company_id = ANY($1::text[]))';
  }
  const result = await pgQuery(`SELECT * FROM users ${where} ORDER BY created_at DESC;`, params);
  return Promise.all(result.rows.map(async (row) => {
    const user = rowToUser(row);
    return {
      ...user,
      assignedCompanies: await listUserCompanyAssignments(user.id)
    };
  }));
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
    store.userCompanyAssignments = [
      { id: `${id}:${next.companyId ?? defaultCompanyId}`, userId: id, companyId: next.companyId ?? defaultCompanyId, role: next.role, assignedAt: new Date().toISOString(), createdAt: new Date().toISOString() },
      ...(store.userCompanyAssignments ?? []).filter((assignment) => assignment.userId !== id || assignment.companyId !== (next.companyId ?? defaultCompanyId))
    ];
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
  await pgQuery(
    `INSERT INTO user_company_assignments (id, user_id, company_id, role)
     VALUES ($1, $2, $3, $4)
     ON CONFLICT (user_id, company_id) DO UPDATE SET role = EXCLUDED.role;`,
    [`${id}:${next.companyId ?? defaultCompanyId}`, id, next.companyId ?? defaultCompanyId, next.role]
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
    store.userCompanyAssignments = (store.userCompanyAssignments ?? []).filter((assignment) => assignment.userId !== id);
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

export async function countUsers() {
  if (!usingPostgres) {
    const store = await loadDevStore();
    return (store.users ?? []).length;
  }

  const result = await pgQuery('SELECT COUNT(*)::int AS count FROM users;');
  return Number(result.rows[0]?.count ?? 0);
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
    worksheets: dataset.worksheets,
    cleanupStatus: dataset.cleanupStatus ?? 'original',
    cleanupLogs: dataset.cleanupLogs ?? [],
    cleanupMetrics: dataset.cleanupMetrics ?? {},
    cleanupPreview: dataset.cleanupPreview ?? null,
    cleanupOperations: dataset.cleanupOperations ?? [],
    futureAiReady: dataset.futureAiReady === true
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
       row_count, column_count, headers, preview, records, analysis,
       original_dataset_id, cleaned_dataset_id, cleanup_status, cleanup_logs
     )
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
     ON CONFLICT (id) DO UPDATE SET
       company_id = EXCLUDED.company_id,
       file_name = EXCLUDED.file_name,
       file_type = EXCLUDED.file_type,
       worksheet_name = EXCLUDED.worksheet_name,
       uploaded_at = EXCLUDED.uploaded_at,
       row_count = EXCLUDED.row_count,
       column_count = EXCLUDED.column_count,
       headers = EXCLUDED.headers,
       preview = EXCLUDED.preview,
       records = EXCLUDED.records,
       analysis = EXCLUDED.analysis,
       original_dataset_id = EXCLUDED.original_dataset_id,
       cleaned_dataset_id = EXCLUDED.cleaned_dataset_id,
       cleanup_status = EXCLUDED.cleanup_status,
       cleanup_logs = EXCLUDED.cleanup_logs;`,
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
      JSON.stringify(analysis),
      dataset.originalDatasetId ?? null,
      dataset.cleanedDatasetId ?? null,
      dataset.cleanupStatus ?? 'original',
      JSON.stringify(dataset.cleanupLogs ?? [])
    ]
  );
  return dataset;
}

export async function listDatasets(user, companyId) {
  const requestedCompanyId = String(companyId || '').trim();
  const assignedCompanyIds = await getAccessibleCompanyIds(user);
  if (!usingPostgres) {
    const store = await loadDevStore();
    return store.datasets
      .filter((dataset) => {
        if (dataset.deletedAt) return false;
        if (!requestedCompanyId) return assignedCompanyIds === null || assignedCompanyIds.includes(dataset.companyId);
        return dataset.companyId === requestedCompanyId && (assignedCompanyIds === null || assignedCompanyIds.includes(dataset.companyId));
      })
      .slice(0, 50)
      .map(rowToDataset);
  }
  const params = [];
  let where = '';
  if (requestedCompanyId) {
    params.push(requestedCompanyId);
    if (assignedCompanyIds === null) {
      where = 'WHERE company_id = $1';
    } else {
      params.push(assignedCompanyIds);
      where = 'WHERE company_id = $1 AND company_id = ANY($2::text[])';
    }
  } else if (assignedCompanyIds !== null) {
    if (!assignedCompanyIds.length) return [];
    params.push(assignedCompanyIds);
    where = 'WHERE company_id = ANY($1::text[])';
  }
  const activeWhere = where ? `${where} AND deleted_at IS NULL` : 'WHERE deleted_at IS NULL';
  const result = await pgQuery(`SELECT * FROM datasets ${activeWhere} ORDER BY uploaded_at DESC LIMIT 50;`, params);
  return result.rows.map(rowToDataset);
}

export async function getDataset(id, user) {
  const assignedCompanyIds = await getAccessibleCompanyIds(user);
  if (!usingPostgres) {
    const store = await loadDevStore();
    const dataset = store.datasets.find((entry) => !entry.deletedAt && entry.id === id && (assignedCompanyIds === null || assignedCompanyIds.includes(entry.companyId)));
    return dataset ? rowToDataset(dataset) : undefined;
  }
  const params = [id];
  const accessFilter = assignedCompanyIds === null ? '' : 'AND company_id = ANY($2::text[])';
  if (assignedCompanyIds !== null) params.push(assignedCompanyIds);
  const result = await pgQuery(`SELECT * FROM datasets WHERE id = $1 AND deleted_at IS NULL ${accessFilter} LIMIT 1;`, params);
  return result.rows[0] ? rowToDataset(result.rows[0]) : undefined;
}

export async function deleteDataset(user, id) {
  const dataset = await getDataset(id, user);
  if (!dataset) return undefined;

  if (!usingPostgres) {
    const store = await loadDevStore();
    const relatedVersionIds = new Set([dataset.id]);
    if (!dataset.originalDatasetId) {
      (store.datasets ?? [])
        .filter((entry) => entry.originalDatasetId === dataset.id)
        .forEach((entry) => relatedVersionIds.add(entry.id));
    }
    const ids = [...relatedVersionIds];
    const deletedAt = new Date().toISOString();
    const retentionUntil = new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString();
    store.cleanupJobs = (store.cleanupJobs ?? []).map((job) => (
      ids.includes(job.originalDatasetId) || ids.includes(job.cleanedDatasetId)
        ? { ...job, deletedAt, status: 'archived' }
        : job
    ));
    store.reports = (store.reports ?? []).map((report) => ids.includes(report.datasetId) ? { ...report, deletedAt } : report);
    store.analytics = (store.analytics ?? []).map((entry) => ids.includes(entry.datasetId) ? { ...entry, deletedAt } : entry);
    store.dashboards = (store.dashboards ?? []).filter((dashboard) => !ids.includes(dashboard.datasetId));
    store.datasets = (store.datasets ?? []).map((entry) => {
      if (ids.includes(entry.id)) return { ...entry, deletedAt, retentionUntil, cleanupStatus: 'archived' };
      if (ids.includes(entry.cleanedDatasetId)) return { ...entry, cleanedDatasetId: null };
      return entry;
    });
    await saveDevStore(store);
    return { dataset, deletedIds: ids };
  }

  const ids = [dataset.id];
  if (!dataset.originalDatasetId) {
    const related = await pgQuery(
      'SELECT id FROM datasets WHERE original_dataset_id = $1 AND deleted_at IS NULL;',
      [dataset.id]
    );
    related.rows.forEach((row) => ids.push(row.id));
  }

  await withTransaction(async (client) => {
    await client.query('UPDATE cleanup_jobs SET deleted_at = NOW(), status = $2, updated_at = NOW() WHERE original_dataset_id = ANY($1::text[]) OR cleaned_dataset_id = ANY($1::text[]);', [ids, 'archived']);
    await client.query('UPDATE reports SET deleted_at = NOW() WHERE dataset_id = ANY($1::text[]);', [ids]);
    await client.query('DELETE FROM dashboards WHERE dataset_id = ANY($1::text[]);', [ids]);
    await client.query('UPDATE analytics SET summary = jsonb_set(summary, $2, to_jsonb(NOW()::text), true) WHERE dataset_id = ANY($1::text[]);', [ids, '{deletedAt}']);
    await client.query('UPDATE datasets SET cleaned_dataset_id = NULL WHERE cleaned_dataset_id = ANY($1::text[]);', [ids]);
    await client.query("UPDATE datasets SET deleted_at = NOW(), retention_until = NOW() + INTERVAL '30 days', cleanup_status = 'archived' WHERE id = ANY($1::text[]);", [ids]);
  });
  return { dataset, deletedIds: ids };
}

export async function saveCleanupJob(job) {
  const saved = {
    id: job.id,
    companyId: job.companyId ?? defaultCompanyId,
    userId: job.userId,
    originalDatasetId: job.originalDatasetId,
    cleanedDatasetId: job.cleanedDatasetId ?? null,
    status: job.status ?? 'pending',
    metrics: job.metrics ?? {},
    logs: job.logs ?? [],
    error: job.error ?? null,
    createdAt: job.createdAt ?? new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };

  if (!usingPostgres) {
    const store = await loadDevStore();
    store.cleanupJobs = [saved, ...(store.cleanupJobs ?? []).filter((entry) => entry.id !== saved.id)].slice(0, 200);
    await saveDevStore(store);
    return saved;
  }

  const result = await pgQuery(
    `INSERT INTO cleanup_jobs (
       id, company_id, user_id, original_dataset_id, cleaned_dataset_id, status, metrics, logs, error, created_at, updated_at
     )
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
     ON CONFLICT (id) DO UPDATE SET
       cleaned_dataset_id = EXCLUDED.cleaned_dataset_id,
       status = EXCLUDED.status,
       metrics = EXCLUDED.metrics,
       logs = EXCLUDED.logs,
       error = EXCLUDED.error,
       updated_at = NOW()
     RETURNING *;`,
    [
      saved.id,
      saved.companyId,
      saved.userId,
      saved.originalDatasetId,
      saved.cleanedDatasetId,
      saved.status,
      JSON.stringify(saved.metrics),
      JSON.stringify(saved.logs),
      saved.error,
      saved.createdAt,
      saved.updatedAt
    ]
  );
  return rowToCleanupJob(result.rows[0]);
}

export async function listCleanupJobs(user, datasetId, companyId) {
  const requestedDatasetId = String(datasetId || '').trim();
  const requestedCompanyId = String(companyId || '').trim();
  const assignedCompanyIds = await getAccessibleCompanyIds(user);

  if (!usingPostgres) {
    const store = await loadDevStore();
    return (store.cleanupJobs ?? [])
      .filter((job) => !job.deletedAt && (!requestedCompanyId || job.companyId === requestedCompanyId) && (!requestedDatasetId || job.originalDatasetId === requestedDatasetId || job.cleanedDatasetId === requestedDatasetId) && (assignedCompanyIds === null || assignedCompanyIds.includes(job.companyId)))
      .slice(0, 50)
      .map(rowToCleanupJob);
  }

  const params = [];
  const filters = [];
  if (requestedDatasetId) {
    params.push(requestedDatasetId);
    filters.push(`(original_dataset_id = $${params.length} OR cleaned_dataset_id = $${params.length})`);
  }
  if (requestedCompanyId) {
    params.push(requestedCompanyId);
    filters.push(`company_id = $${params.length}`);
  }
  if (assignedCompanyIds !== null) {
    if (!assignedCompanyIds.length) return [];
    params.push(assignedCompanyIds);
    filters.push(`company_id = ANY($${params.length}::text[])`);
  }
  filters.push('deleted_at IS NULL');
  const where = `WHERE ${filters.join(' AND ')}`;
  const result = await pgQuery(`SELECT * FROM cleanup_jobs ${where} ORDER BY created_at DESC LIMIT 50;`, params);
  return result.rows.map(rowToCleanupJob);
}

export async function deleteCleanupJob(user, id) {
  const assignedCompanyIds = await getAccessibleCompanyIds(user);
  if (!usingPostgres) {
    const store = await loadDevStore();
    const deletedAt = new Date().toISOString();
    const job = (store.cleanupJobs ?? []).find((entry) => entry.id === id && !entry.deletedAt && (assignedCompanyIds === null || assignedCompanyIds.includes(entry.companyId)));
    store.cleanupJobs = (store.cleanupJobs ?? []).map((entry) => entry.id === id && (assignedCompanyIds === null || assignedCompanyIds.includes(entry.companyId)) ? { ...entry, deletedAt, status: 'archived' } : entry);
    await saveDevStore(store);
    return job ? rowToCleanupJob(job) : undefined;
  }
  const params = [id];
  const filter = assignedCompanyIds === null ? '' : 'AND company_id = ANY($2::text[])';
  if (assignedCompanyIds !== null) params.push(assignedCompanyIds);
  const result = await pgQuery(`UPDATE cleanup_jobs SET deleted_at = NOW(), status = 'archived', updated_at = NOW() WHERE id = $1 ${filter} RETURNING *;`, params);
  return result.rows[0] ? rowToCleanupJob(result.rows[0]) : undefined;
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

export async function listDashboards(user, companyId) {
  const requestedCompanyId = String(companyId || '').trim();
  const assignedCompanyIds = await getAccessibleCompanyIds(user);
  if (!usingPostgres) {
    const store = await loadDevStore();
    return store.dashboards
      .filter((dashboard) => {
        if (!requestedCompanyId) return assignedCompanyIds === null || assignedCompanyIds.includes(dashboard.companyId);
        return dashboard.companyId === requestedCompanyId && (assignedCompanyIds === null || assignedCompanyIds.includes(dashboard.companyId));
      })
      .slice(0, 50)
      .map((dashboard) => {
        const dataset = store.datasets.find((entry) => entry.id === dashboard.datasetId);
        return rowToDashboard({ ...dashboard, dataset_name: dataset?.fileName ?? dashboard.datasetName });
      });
  }
  const params = [];
  let companyFilter = '';
  if (requestedCompanyId) {
    params.push(requestedCompanyId);
    if (assignedCompanyIds === null) {
      companyFilter = 'WHERE dashboards.company_id = $1';
    } else {
      params.push(assignedCompanyIds);
      companyFilter = 'WHERE dashboards.company_id = $1 AND dashboards.company_id = ANY($2::text[])';
    }
  } else if (assignedCompanyIds !== null) {
    if (!assignedCompanyIds.length) return [];
    params.push(assignedCompanyIds);
    companyFilter = 'WHERE dashboards.company_id = ANY($1::text[])';
  }
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

export async function listReports(user, companyId) {
  const requestedCompanyId = String(companyId || '').trim();
  const assignedCompanyIds = await getAccessibleCompanyIds(user);
  if (!usingPostgres) {
    const store = await loadDevStore();
    return store.reports
      .filter((report) => {
        if (report.deletedAt) return false;
        if (!requestedCompanyId) return assignedCompanyIds === null || assignedCompanyIds.includes(report.companyId);
        return report.companyId === requestedCompanyId && (assignedCompanyIds === null || assignedCompanyIds.includes(report.companyId));
      })
      .slice(0, 50)
      .map((report) => {
        const dataset = store.datasets.find((entry) => entry.id === report.datasetId);
        return rowToReport({ ...report, dataset_name: dataset?.fileName ?? report.datasetName });
      });
  }
  const params = [];
  let companyFilter = '';
  if (requestedCompanyId) {
    params.push(requestedCompanyId);
    if (assignedCompanyIds === null) {
      companyFilter = 'WHERE reports.company_id = $1 AND reports.deleted_at IS NULL';
    } else {
      params.push(assignedCompanyIds);
      companyFilter = 'WHERE reports.company_id = $1 AND reports.company_id = ANY($2::text[]) AND reports.deleted_at IS NULL';
    }
  } else if (assignedCompanyIds !== null) {
    if (!assignedCompanyIds.length) return [];
    params.push(assignedCompanyIds);
    companyFilter = 'WHERE reports.company_id = ANY($1::text[]) AND reports.deleted_at IS NULL';
  } else {
    companyFilter = 'WHERE reports.deleted_at IS NULL';
  }
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

export async function deleteReport(user, id) {
  const assignedCompanyIds = await getAccessibleCompanyIds(user);
  if (!usingPostgres) {
    const store = await loadDevStore();
    const deletedAt = new Date().toISOString();
    const report = (store.reports ?? []).find((entry) => entry.id === id && !entry.deletedAt && (assignedCompanyIds === null || assignedCompanyIds.includes(entry.companyId)));
    store.reports = (store.reports ?? []).map((entry) => entry.id === id && (assignedCompanyIds === null || assignedCompanyIds.includes(entry.companyId)) ? { ...entry, deletedAt } : entry);
    await saveDevStore(store);
    return report ? rowToReport(report) : undefined;
  }
  const params = [id];
  const filter = assignedCompanyIds === null ? '' : 'AND company_id = ANY($2::text[])';
  if (assignedCompanyIds !== null) params.push(assignedCompanyIds);
  const result = await pgQuery(`UPDATE reports SET deleted_at = NOW() WHERE id = $1 ${filter} RETURNING *;`, params);
  return result.rows[0] ? rowToReport(result.rows[0]) : undefined;
}

export async function saveNotification(notification) {
  const saved = {
    id: notification.id,
    companyId: notification.companyId ?? defaultCompanyId,
    userId: notification.userId ?? null,
    type: notification.type,
    title: notification.title,
    message: notification.message,
    status: notification.status ?? 'unread',
    metadata: notification.metadata ?? {},
    createdAt: notification.createdAt ?? new Date().toISOString()
  };
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.notifications = [saved, ...(store.notifications ?? [])].slice(0, 300);
    await saveDevStore(store);
    return saved;
  }
  const result = await pgQuery(
    `INSERT INTO notifications (id, company_id, user_id, type, title, message, status, metadata)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     RETURNING *;`,
    [saved.id, saved.companyId, saved.userId, saved.type, saved.title, saved.message, saved.status, JSON.stringify(saved.metadata)]
  );
  return rowToNotification(result.rows[0]);
}

export async function listNotifications(user, companyId) {
  const requestedCompanyId = String(companyId || '').trim();
  const assignedCompanyIds = await getAccessibleCompanyIds(user);
  if (!usingPostgres) {
    const store = await loadDevStore();
    return (store.notifications ?? [])
      .filter((notification) => !notification.archivedAt && (!requestedCompanyId || notification.companyId === requestedCompanyId) && (assignedCompanyIds === null || assignedCompanyIds.includes(notification.companyId)))
      .slice(0, 50)
      .map(rowToNotification);
  }
  const params = [];
  const filters = [];
  if (requestedCompanyId) {
    params.push(requestedCompanyId);
    filters.push(`company_id = $${params.length}`);
  }
  if (assignedCompanyIds !== null) {
    if (!assignedCompanyIds.length) return [];
    params.push(assignedCompanyIds);
    filters.push(`company_id = ANY($${params.length}::text[])`);
  }
  filters.push('archived_at IS NULL');
  const where = `WHERE ${filters.join(' AND ')}`;
  const result = await pgQuery(`SELECT * FROM notifications ${where} ORDER BY created_at DESC LIMIT 50;`, params);
  return result.rows.map(rowToNotification);
}

export async function updateNotification(user, id, updates) {
  const assignedCompanyIds = await getAccessibleCompanyIds(user);
  const status = updates.status != null ? String(updates.status) : null;
  const archive = updates.archive === true;

  if (!usingPostgres) {
    const store = await loadDevStore();
    let updated;
    store.notifications = (store.notifications ?? []).map((notification) => {
      if (notification.id !== id || !(assignedCompanyIds === null || assignedCompanyIds.includes(notification.companyId))) return notification;
      updated = {
        ...notification,
        ...(status ? { status } : {}),
        ...(archive ? { archivedAt: new Date().toISOString() } : {})
      };
      return updated;
    });
    await saveDevStore(store);
    return updated ? rowToNotification(updated) : undefined;
  }

  const params = [id, status, archive];
  const filter = assignedCompanyIds === null ? '' : 'AND company_id = ANY($4::text[])';
  if (assignedCompanyIds !== null) params.push(assignedCompanyIds);
  const result = await pgQuery(
    `UPDATE notifications SET
       status = COALESCE($2, status),
       archived_at = CASE WHEN $3::boolean THEN NOW() ELSE archived_at END
     WHERE id = $1 ${filter}
     RETURNING *;`,
    params
  );
  return result.rows[0] ? rowToNotification(result.rows[0]) : undefined;
}

export async function listPipelines(user, companyId) {
  const requestedCompanyId = String(companyId || '').trim();
  const assignedCompanyIds = await getAccessibleCompanyIds(user);

  if (!usingPostgres) {
    const store = await loadDevStore();
    return (store.pipelines ?? [])
      .filter((pipeline) => !requestedCompanyId || pipeline.companyId === requestedCompanyId)
      .filter((pipeline) => assignedCompanyIds === null || assignedCompanyIds.includes(pipeline.companyId))
      .sort((a, b) => new Date(b.updatedAt ?? b.createdAt).getTime() - new Date(a.updatedAt ?? a.createdAt).getTime())
      .map(rowToPipeline);
  }

  const filters = [];
  const params = [];
  if (requestedCompanyId) {
    params.push(requestedCompanyId);
    filters.push(`company_id = $${params.length}`);
  }
  if (assignedCompanyIds !== null) {
    if (!assignedCompanyIds.length) return [];
    params.push(assignedCompanyIds);
    filters.push(`company_id = ANY($${params.length}::text[])`);
  }
  const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
  const result = await pgQuery(`SELECT * FROM pipelines ${where} ORDER BY updated_at DESC, created_at DESC;`, params);
  return result.rows.map(rowToPipeline);
}

export async function getPipeline(id, user) {
  const assignedCompanyIds = await getAccessibleCompanyIds(user);
  if (!usingPostgres) {
    const store = await loadDevStore();
    const pipeline = (store.pipelines ?? []).find((entry) => entry.id === id && (assignedCompanyIds === null || assignedCompanyIds.includes(entry.companyId)));
    return pipeline ? rowToPipeline(pipeline) : undefined;
  }

  const params = [id];
  const accessFilter = assignedCompanyIds === null ? '' : 'AND company_id = ANY($2::text[])';
  if (assignedCompanyIds !== null) params.push(assignedCompanyIds);
  const result = await pgQuery(`SELECT * FROM pipelines WHERE id = $1 ${accessFilter} LIMIT 1;`, params);
  return result.rows[0] ? rowToPipeline(result.rows[0]) : undefined;
}

export async function savePipeline(pipeline) {
  const saved = {
    id: pipeline.id,
    companyId: pipeline.companyId,
    userId: pipeline.userId,
    department: String(pipeline.department || '').trim(),
    name: String(pipeline.name || '').trim(),
    status: pipeline.status ?? 'active',
    metadata: pipeline.metadata ?? {},
    createdAt: pipeline.createdAt ?? new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };

  if (!usingPostgres) {
    const store = await loadDevStore();
    store.pipelines = [saved, ...(store.pipelines ?? []).filter((entry) => entry.id !== saved.id)];
    await saveDevStore(store);
    return rowToPipeline(saved);
  }

  const result = await pgQuery(
    `INSERT INTO pipelines (id, company_id, user_id, department, name, status, metadata)
     VALUES ($1,$2,$3,$4,$5,$6,$7)
     ON CONFLICT (id) DO UPDATE SET
       company_id = EXCLUDED.company_id,
       user_id = EXCLUDED.user_id,
       department = EXCLUDED.department,
       name = EXCLUDED.name,
       status = EXCLUDED.status,
       metadata = EXCLUDED.metadata,
       updated_at = NOW()
     RETURNING *;`,
    [saved.id, saved.companyId, saved.userId, saved.department, saved.name, saved.status, JSON.stringify(saved.metadata)]
  );
  return rowToPipeline(result.rows[0]);
}

export async function updatePipeline(id, updates, user) {
  const pipeline = await getPipeline(id, user);
  if (!pipeline) return undefined;
  return savePipeline({
    ...pipeline,
    ...(updates.department != null ? { department: String(updates.department).trim() } : {}),
    ...(updates.name != null ? { name: String(updates.name).trim() } : {}),
    ...(updates.status != null ? { status: String(updates.status).trim() || pipeline.status } : {}),
    ...(updates.metadata != null ? { metadata: updates.metadata } : {})
  });
}

export async function deletePipeline(id, user) {
  const pipeline = await getPipeline(id, user);
  if (!pipeline) return false;

  if (!usingPostgres) {
    const store = await loadDevStore();
    store.pipelines = (store.pipelines ?? []).filter((entry) => entry.id !== id);
    await saveDevStore(store);
    return true;
  }

  const result = await pgQuery('DELETE FROM pipelines WHERE id = $1 AND company_id = $2;', [id, pipeline.companyId]);
  return result.rowCount > 0;
}

const defaultConnectorTemplates = [
  ['sql_server', 'SQL Server'],
  ['postgresql', 'PostgreSQL'],
  ['mysql', 'MySQL'],
  ['sharepoint', 'SharePoint'],
  ['onedrive', 'OneDrive'],
  ['google_drive', 'Google Drive'],
  ['excel_online', 'Excel Online'],
  ['csv_watch_folder', 'CSV watch folders'],
  ['sftp', 'SFTP'],
  ['rest_api', 'REST APIs'],
  ['email_attachment', 'Email attachment ingestion'],
  ['webhook', 'Webhook ingestion']
];

function enterpriseSeed(companyId, userId) {
  const now = new Date();
  const connectors = defaultConnectorTemplates.map(([type, name], index) => ({
    id: `${companyId}-${type}`,
    companyId,
    userId,
    name,
    connectorType: type,
    status: index < 3 ? 'ready' : 'not_configured',
    healthStatus: index < 3 ? 'healthy' : 'unknown',
    permissions: { roles: ['owner', 'admin', 'manager'], departmentScoped: true },
    schedule: index === 0 ? { mode: 'nightly', cron: '0 2 * * *' } : index === 1 ? { mode: 'hourly', cron: '0 * * * *' } : {},
    metadata: { incrementalSync: true, credentialEncrypted: false, retryEnabled: true },
    lastSyncAt: index < 3 ? new Date(now.getTime() - (index + 1) * 3600000).toISOString() : null,
    nextSyncAt: index < 3 ? new Date(now.getTime() + (index + 1) * 3600000).toISOString() : null,
    createdAt: now.toISOString(),
    updatedAt: now.toISOString()
  }));
  const schedules = [
    {
      id: `${companyId}-nightly-invoice-cleanup`,
      companyId,
      pipelineId: null,
      name: 'Nightly invoice cleanup',
      scheduleType: 'cron',
      cronExpression: '0 2 * * *',
      eventTrigger: '',
      priority: 2,
      slaMinutes: 45,
      retryPolicy: { attempts: 3, backoffMinutes: 15 },
      dependencies: ['connector:sql_server', 'stage:validate_invoices'],
      status: 'queued',
      nextRunAt: new Date(now.getTime() + 8 * 3600000).toISOString(),
      lastRunAt: null,
      metadata: { module: 'accounting', approvalRequired: true },
      createdBy: userId,
      createdAt: now.toISOString(),
      updatedAt: now.toISOString()
    },
    {
      id: `${companyId}-friday-export`,
      companyId,
      pipelineId: null,
      name: 'Friday approved dataset export',
      scheduleType: 'cron',
      cronExpression: '0 17 * * 5',
      eventTrigger: '',
      priority: 5,
      slaMinutes: 90,
      retryPolicy: { attempts: 2, backoffMinutes: 30 },
      dependencies: ['stage:approval'],
      status: 'waiting',
      nextRunAt: new Date(now.getTime() + 48 * 3600000).toISOString(),
      lastRunAt: null,
      metadata: { module: 'dataProcessing', exportFormat: 'csv' },
      createdBy: userId,
      createdAt: now.toISOString(),
      updatedAt: now.toISOString()
    }
  ];
  const intelligence = [
    {
      id: `${companyId}-invoice-risk`,
      companyId,
      datasetId: null,
      module: 'accounting',
      insightType: 'invoice_risk_analysis',
      severity: 'warning',
      title: 'Invoice risk analysis ready',
      summary: 'AI scoring will flag duplicate payment risk, vendor anomalies, negative amounts, and missing PO patterns.',
      confidence: 0.86,
      recommendations: ['Review invoices with repeated vendor and amount pairs', 'Require PO validation before ERP export'],
      explainability: { signals: ['duplicate invoice number', 'vendor variance', 'tax mismatch'] },
      status: 'active',
      createdBy: userId,
      createdAt: now.toISOString()
    },
    {
      id: `${companyId}-project-risk`,
      companyId,
      datasetId: null,
      module: 'engineering',
      insightType: 'project_risk_forecast',
      severity: 'info',
      title: 'Project dependency risk forecast',
      summary: 'Workflow intelligence is prepared to forecast schedule delay, resource conflict, and dependency risk.',
      confidence: 0.78,
      recommendations: ['Validate predecessor/successor chains', 'Review milestones with missing owners'],
      explainability: { signals: ['schedule overlap', 'resource load', 'blocked dependency'] },
      status: 'active',
      createdBy: userId,
      createdAt: now.toISOString()
    },
    {
      id: `${companyId}-schema-drift`,
      companyId,
      datasetId: null,
      module: 'dataProcessing',
      insightType: 'schema_drift_prediction',
      severity: 'info',
      title: 'Schema drift monitoring enabled',
      summary: 'Quality scoring can compare incoming uploads against approved schema versions and isolate failed rows.',
      confidence: 0.82,
      recommendations: ['Define required columns per workflow', 'Route schema drift to approval before cleanup'],
      explainability: { signals: ['missing columns', 'type mismatch', 'new unexpected fields'] },
      status: 'active',
      createdBy: userId,
      createdAt: now.toISOString()
    }
  ];
  return { connectors, syncLogs: [], schedules, intelligence, accessRequests: [] };
}

export async function listEnterpriseOperations(user, companyId) {
  const requestedCompanyId = String(companyId || '').trim() || getCompanyId(user);
  if (!(await canAccessCompany(user, requestedCompanyId))) {
    return { connectors: [], syncLogs: [], schedules: [], intelligence: [], accessRequests: [] };
  }

  if (!usingPostgres) {
    const store = await loadDevStore();
    if (!(store.enterpriseConnectors ?? []).some((entry) => entry.companyId === requestedCompanyId)) {
      const seeded = enterpriseSeed(requestedCompanyId, user?.id);
      store.enterpriseConnectors = [...(store.enterpriseConnectors ?? []), ...seeded.connectors];
      store.pipelineSchedules = [...(store.pipelineSchedules ?? []), ...seeded.schedules];
      store.workflowIntelligence = [...(store.workflowIntelligence ?? []), ...seeded.intelligence];
      store.connectorSyncLogs = store.connectorSyncLogs ?? [];
      store.accessRequests = store.accessRequests ?? [];
      await saveDevStore(store);
    }
    return {
      connectors: (store.enterpriseConnectors ?? []).filter((entry) => entry.companyId === requestedCompanyId).map(rowToEnterpriseConnector),
      syncLogs: (store.connectorSyncLogs ?? []).filter((entry) => entry.companyId === requestedCompanyId).slice(0, 50).map(rowToConnectorSyncLog),
      schedules: (store.pipelineSchedules ?? []).filter((entry) => entry.companyId === requestedCompanyId).map(rowToPipelineSchedule),
      intelligence: (store.workflowIntelligence ?? []).filter((entry) => entry.companyId === requestedCompanyId).map(rowToWorkflowIntelligence),
      accessRequests: (store.accessRequests ?? []).filter((entry) => entry.companyId === requestedCompanyId).map(rowToAccessRequest)
    };
  }

  const connectorCount = await pgQuery('SELECT COUNT(*)::int AS count FROM enterprise_connectors WHERE company_id = $1;', [requestedCompanyId]);
  if (Number(connectorCount.rows[0]?.count ?? 0) === 0) {
    const seeded = enterpriseSeed(requestedCompanyId, user?.id);
    for (const connector of seeded.connectors) await saveEnterpriseConnector(connector);
    for (const schedule of seeded.schedules) await savePipelineSchedule(schedule);
    for (const insight of seeded.intelligence) await saveWorkflowIntelligence(insight);
  }

  const [connectors, syncLogs, schedules, intelligence, accessRequests] = await Promise.all([
    pgQuery('SELECT * FROM enterprise_connectors WHERE company_id = $1 ORDER BY updated_at DESC;', [requestedCompanyId]),
    pgQuery('SELECT * FROM connector_sync_logs WHERE company_id = $1 ORDER BY created_at DESC LIMIT 50;', [requestedCompanyId]),
    pgQuery('SELECT * FROM pipeline_schedules WHERE company_id = $1 ORDER BY next_run_at NULLS LAST, updated_at DESC;', [requestedCompanyId]),
    pgQuery('SELECT * FROM workflow_intelligence WHERE company_id = $1 ORDER BY created_at DESC LIMIT 50;', [requestedCompanyId]),
    pgQuery('SELECT * FROM access_requests WHERE company_id = $1 ORDER BY created_at DESC LIMIT 50;', [requestedCompanyId])
  ]);
  return {
    connectors: connectors.rows.map(rowToEnterpriseConnector),
    syncLogs: syncLogs.rows.map(rowToConnectorSyncLog),
    schedules: schedules.rows.map(rowToPipelineSchedule),
    intelligence: intelligence.rows.map(rowToWorkflowIntelligence),
    accessRequests: accessRequests.rows.map(rowToAccessRequest)
  };
}

export async function saveEnterpriseConnector(connector) {
  const saved = {
    id: connector.id,
    companyId: connector.companyId ?? defaultCompanyId,
    userId: connector.userId,
    name: String(connector.name || '').trim(),
    connectorType: String(connector.connectorType || connector.connector_type || '').trim(),
    status: connector.status ?? 'ready',
    healthStatus: connector.healthStatus ?? connector.health_status ?? 'healthy',
    permissions: connector.permissions ?? {},
    schedule: connector.schedule ?? {},
    encryptedCredentials: connector.encryptedCredentials ?? connector.encrypted_credentials ?? null,
    metadata: connector.metadata ?? {},
    lastSyncAt: connector.lastSyncAt ?? null,
    nextSyncAt: connector.nextSyncAt ?? null,
    createdAt: connector.createdAt ?? new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.enterpriseConnectors = [saved, ...(store.enterpriseConnectors ?? []).filter((entry) => entry.id !== saved.id)];
    await saveDevStore(store);
    return rowToEnterpriseConnector(saved);
  }
  const result = await pgQuery(
    `INSERT INTO enterprise_connectors (id, company_id, user_id, name, connector_type, status, health_status, permissions, schedule, encrypted_credentials, metadata, last_sync_at, next_sync_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
     ON CONFLICT (id) DO UPDATE SET
       name = EXCLUDED.name,
       status = EXCLUDED.status,
       health_status = EXCLUDED.health_status,
       permissions = EXCLUDED.permissions,
       schedule = EXCLUDED.schedule,
       encrypted_credentials = COALESCE(EXCLUDED.encrypted_credentials, enterprise_connectors.encrypted_credentials),
       metadata = EXCLUDED.metadata,
       last_sync_at = EXCLUDED.last_sync_at,
       next_sync_at = EXCLUDED.next_sync_at,
       updated_at = NOW()
     RETURNING *;`,
    [saved.id, saved.companyId, saved.userId, saved.name, saved.connectorType, saved.status, saved.healthStatus, JSON.stringify(saved.permissions), JSON.stringify(saved.schedule), saved.encryptedCredentials, JSON.stringify(saved.metadata), saved.lastSyncAt, saved.nextSyncAt]
  );
  return rowToEnterpriseConnector(result.rows[0]);
}

export async function saveConnectorSyncLog(log) {
  const saved = {
    id: log.id,
    connectorId: log.connectorId,
    companyId: log.companyId ?? defaultCompanyId,
    status: log.status ?? 'completed',
    recordsProcessed: Number(log.recordsProcessed ?? 0),
    failedRows: Number(log.failedRows ?? 0),
    retries: Number(log.retries ?? 0),
    durationMs: Number(log.durationMs ?? 0),
    error: log.error ?? null,
    metadata: log.metadata ?? {},
    startedAt: log.startedAt ?? new Date().toISOString(),
    completedAt: log.completedAt ?? new Date().toISOString(),
    createdAt: new Date().toISOString()
  };
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.connectorSyncLogs = [saved, ...(store.connectorSyncLogs ?? [])].slice(0, 500);
    store.enterpriseConnectors = (store.enterpriseConnectors ?? []).map((connector) => connector.id === saved.connectorId ? {
      ...connector,
      status: saved.status === 'failed' ? 'failed' : 'ready',
      healthStatus: saved.status === 'failed' ? 'degraded' : 'healthy',
      lastSyncAt: saved.completedAt,
      updatedAt: new Date().toISOString()
    } : connector);
    await saveDevStore(store);
    return rowToConnectorSyncLog(saved);
  }
  const result = await pgQuery(
    `INSERT INTO connector_sync_logs (id, connector_id, company_id, status, records_processed, failed_rows, retries, duration_ms, error, metadata, started_at, completed_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
     RETURNING *;`,
    [saved.id, saved.connectorId, saved.companyId, saved.status, saved.recordsProcessed, saved.failedRows, saved.retries, saved.durationMs, saved.error, JSON.stringify(saved.metadata), saved.startedAt, saved.completedAt]
  );
  await pgQuery(
    `UPDATE enterprise_connectors
        SET status = $2, health_status = $3, last_sync_at = $4, updated_at = NOW()
      WHERE id = $1 AND company_id = $5;`,
    [saved.connectorId, saved.status === 'failed' ? 'failed' : 'ready', saved.status === 'failed' ? 'degraded' : 'healthy', saved.completedAt, saved.companyId]
  );
  return rowToConnectorSyncLog(result.rows[0]);
}

export async function savePipelineSchedule(schedule) {
  const saved = {
    id: schedule.id,
    companyId: schedule.companyId ?? defaultCompanyId,
    pipelineId: schedule.pipelineId ?? null,
    name: String(schedule.name || '').trim(),
    scheduleType: schedule.scheduleType ?? 'cron',
    cronExpression: schedule.cronExpression ?? '',
    eventTrigger: schedule.eventTrigger ?? '',
    priority: Number(schedule.priority ?? 5),
    slaMinutes: Number(schedule.slaMinutes ?? 60),
    retryPolicy: schedule.retryPolicy ?? {},
    dependencies: schedule.dependencies ?? [],
    status: schedule.status ?? 'queued',
    nextRunAt: schedule.nextRunAt ?? null,
    lastRunAt: schedule.lastRunAt ?? null,
    metadata: schedule.metadata ?? {},
    createdBy: schedule.createdBy ?? null,
    createdAt: schedule.createdAt ?? new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.pipelineSchedules = [saved, ...(store.pipelineSchedules ?? []).filter((entry) => entry.id !== saved.id)];
    await saveDevStore(store);
    return rowToPipelineSchedule(saved);
  }
  const result = await pgQuery(
    `INSERT INTO pipeline_schedules (id, company_id, pipeline_id, name, schedule_type, cron_expression, event_trigger, priority, sla_minutes, retry_policy, dependencies, status, next_run_at, last_run_at, metadata, created_by)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
     ON CONFLICT (id) DO UPDATE SET
       name = EXCLUDED.name,
       schedule_type = EXCLUDED.schedule_type,
       cron_expression = EXCLUDED.cron_expression,
       event_trigger = EXCLUDED.event_trigger,
       priority = EXCLUDED.priority,
       sla_minutes = EXCLUDED.sla_minutes,
       retry_policy = EXCLUDED.retry_policy,
       dependencies = EXCLUDED.dependencies,
       status = EXCLUDED.status,
       next_run_at = EXCLUDED.next_run_at,
       last_run_at = EXCLUDED.last_run_at,
       metadata = EXCLUDED.metadata,
       updated_at = NOW()
     RETURNING *;`,
    [saved.id, saved.companyId, saved.pipelineId, saved.name, saved.scheduleType, saved.cronExpression, saved.eventTrigger, saved.priority, saved.slaMinutes, JSON.stringify(saved.retryPolicy), JSON.stringify(saved.dependencies), saved.status, saved.nextRunAt, saved.lastRunAt, JSON.stringify(saved.metadata), saved.createdBy]
  );
  return rowToPipelineSchedule(result.rows[0]);
}

export async function saveWorkflowIntelligence(insight) {
  const saved = {
    id: insight.id,
    companyId: insight.companyId ?? defaultCompanyId,
    datasetId: insight.datasetId ?? null,
    module: insight.module ?? 'operations',
    insightType: insight.insightType ?? 'workflow_recommendation',
    severity: insight.severity ?? 'info',
    title: String(insight.title || '').trim(),
    summary: String(insight.summary || '').trim(),
    confidence: Number(insight.confidence ?? 0),
    recommendations: insight.recommendations ?? [],
    explainability: insight.explainability ?? {},
    status: insight.status ?? 'active',
    createdBy: insight.createdBy ?? null,
    createdAt: insight.createdAt ?? new Date().toISOString()
  };
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.workflowIntelligence = [saved, ...(store.workflowIntelligence ?? []).filter((entry) => entry.id !== saved.id)];
    await saveDevStore(store);
    return rowToWorkflowIntelligence(saved);
  }
  const result = await pgQuery(
    `INSERT INTO workflow_intelligence (id, company_id, dataset_id, module, insight_type, severity, title, summary, confidence, recommendations, explainability, status, created_by)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
     ON CONFLICT (id) DO UPDATE SET
       severity = EXCLUDED.severity,
       title = EXCLUDED.title,
       summary = EXCLUDED.summary,
       confidence = EXCLUDED.confidence,
       recommendations = EXCLUDED.recommendations,
       explainability = EXCLUDED.explainability,
       status = EXCLUDED.status
     RETURNING *;`,
    [saved.id, saved.companyId, saved.datasetId, saved.module, saved.insightType, saved.severity, saved.title, saved.summary, saved.confidence, JSON.stringify(saved.recommendations), JSON.stringify(saved.explainability), saved.status, saved.createdBy]
  );
  return rowToWorkflowIntelligence(result.rows[0]);
}

export async function savePipelineStageRun(stageRun) {
  const saved = {
    id: stageRun.id,
    pipelineId: stageRun.pipelineId ?? null,
    companyId: stageRun.companyId ?? defaultCompanyId,
    module: stageRun.module ?? 'dataProcessing',
    datasetId: stageRun.datasetId ?? null,
    stageName: stageRun.stageName,
    status: stageRun.status ?? 'queued',
    operatorUserId: stageRun.operatorUserId ?? null,
    logs: stageRun.logs ?? [],
    validationOutput: stageRun.validationOutput ?? {},
    metrics: stageRun.metrics ?? {},
    startedAt: stageRun.startedAt ?? null,
    completedAt: stageRun.completedAt ?? null,
    createdAt: stageRun.createdAt ?? new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.pipelineStageRuns = [saved, ...(store.pipelineStageRuns ?? []).filter((entry) => entry.id !== saved.id)].slice(0, 1000);
    await saveDevStore(store);
    return rowToPipelineStageRun(saved);
  }
  const result = await pgQuery(
    `INSERT INTO pipeline_stage_runs (id, pipeline_id, company_id, module, dataset_id, stage_name, status, operator_user_id, logs, validation_output, metrics, started_at, completed_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
     ON CONFLICT (id) DO UPDATE SET
       status = EXCLUDED.status,
       logs = EXCLUDED.logs,
       validation_output = EXCLUDED.validation_output,
       metrics = EXCLUDED.metrics,
       started_at = COALESCE(EXCLUDED.started_at, pipeline_stage_runs.started_at),
       completed_at = EXCLUDED.completed_at,
       updated_at = NOW()
     RETURNING *;`,
    [saved.id, saved.pipelineId, saved.companyId, saved.module, saved.datasetId, saved.stageName, saved.status, saved.operatorUserId, JSON.stringify(saved.logs), JSON.stringify(saved.validationOutput), JSON.stringify(saved.metrics), saved.startedAt, saved.completedAt]
  );
  return rowToPipelineStageRun(result.rows[0]);
}

export async function listPipelineStageRuns(user, companyId, datasetId) {
  const requestedCompanyId = String(companyId || '').trim();
  if (!(await canAccessCompany(user, requestedCompanyId))) return [];
  if (!usingPostgres) {
    const store = await loadDevStore();
    return (store.pipelineStageRuns ?? [])
      .filter((entry) => entry.companyId === requestedCompanyId)
      .filter((entry) => !datasetId || entry.datasetId === datasetId)
      .map(rowToPipelineStageRun);
  }
  const params = [requestedCompanyId];
  const datasetFilter = datasetId ? 'AND dataset_id = $2' : '';
  if (datasetId) params.push(datasetId);
  const result = await pgQuery(`SELECT * FROM pipeline_stage_runs WHERE company_id = $1 ${datasetFilter} ORDER BY created_at DESC LIMIT 100;`, params);
  return result.rows.map(rowToPipelineStageRun);
}

export async function savePipelineRule(rule) {
  const saved = {
    id: rule.id,
    companyId: rule.companyId ?? defaultCompanyId,
    module: rule.module ?? 'dataProcessing',
    ruleKey: rule.ruleKey,
    label: rule.label,
    config: rule.config ?? {},
    enabled: rule.enabled !== false,
    createdBy: rule.createdBy ?? null,
    createdAt: rule.createdAt ?? new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.pipelineRules = [saved, ...(store.pipelineRules ?? []).filter((entry) => entry.id !== saved.id)];
    await saveDevStore(store);
    return rowToPipelineRule(saved);
  }
  const result = await pgQuery(
    `INSERT INTO pipeline_rules (id, company_id, module, rule_key, label, config, enabled, created_by)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
     ON CONFLICT (id) DO UPDATE SET
       label = EXCLUDED.label,
       config = EXCLUDED.config,
       enabled = EXCLUDED.enabled,
       updated_at = NOW()
     RETURNING *;`,
    [saved.id, saved.companyId, saved.module, saved.ruleKey, saved.label, JSON.stringify(saved.config), saved.enabled, saved.createdBy]
  );
  return rowToPipelineRule(result.rows[0]);
}

export async function listPipelineRules(user, companyId, module) {
  const requestedCompanyId = String(companyId || '').trim();
  if (!(await canAccessCompany(user, requestedCompanyId))) return [];
  if (!usingPostgres) {
    const store = await loadDevStore();
    return (store.pipelineRules ?? [])
      .filter((entry) => entry.companyId === requestedCompanyId)
      .filter((entry) => !module || entry.module === module)
      .map(rowToPipelineRule);
  }
  const params = [requestedCompanyId];
  const moduleFilter = module ? 'AND module = $2' : '';
  if (module) params.push(module);
  const result = await pgQuery(`SELECT * FROM pipeline_rules WHERE company_id = $1 ${moduleFilter} ORDER BY updated_at DESC;`, params);
  return result.rows.map(rowToPipelineRule);
}

export async function saveAccessRequest(request) {
  const saved = {
    id: request.id,
    companyId: request.companyId ?? defaultCompanyId,
    requesterUserId: request.requesterUserId,
    targetUserId: request.targetUserId ?? null,
    department: request.department ?? '',
    requestedRole: normalizeRole(request.requestedRole),
    reason: String(request.reason || '').trim(),
    status: request.status ?? 'pending',
    expiresAt: request.expiresAt ?? null,
    approvedBy: request.approvedBy ?? null,
    approvedAt: request.approvedAt ?? null,
    metadata: request.metadata ?? {},
    createdAt: request.createdAt ?? new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
  if (!usingPostgres) {
    const store = await loadDevStore();
    store.accessRequests = [saved, ...(store.accessRequests ?? []).filter((entry) => entry.id !== saved.id)];
    await saveDevStore(store);
    return rowToAccessRequest(saved);
  }
  const result = await pgQuery(
    `INSERT INTO access_requests (id, company_id, requester_user_id, target_user_id, department, requested_role, reason, status, expires_at, approved_by, approved_at, metadata)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
     ON CONFLICT (id) DO UPDATE SET
       status = EXCLUDED.status,
       expires_at = EXCLUDED.expires_at,
       approved_by = EXCLUDED.approved_by,
       approved_at = EXCLUDED.approved_at,
       metadata = EXCLUDED.metadata,
       updated_at = NOW()
     RETURNING *;`,
    [saved.id, saved.companyId, saved.requesterUserId, saved.targetUserId, saved.department, saved.requestedRole, saved.reason, saved.status, saved.expiresAt, saved.approvedBy, saved.approvedAt, JSON.stringify(saved.metadata)]
  );
  return rowToAccessRequest(result.rows[0]);
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
  const assignedCompanyIds = await getAccessibleCompanyIds(user);
  if (!usingPostgres) {
    const store = await loadDevStore();
    return (store.moduleRecords ?? [])
      .filter((record) => record.module === module && canAccessCompanyRecord(user, record))
      .sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime())
      .slice(0, 100)
      .map(rowToModuleRecord);
  }
  const params = [module];
  let companyFilter = '';
  if (assignedCompanyIds !== null) {
    if (!assignedCompanyIds.length) return [];
    params.push(assignedCompanyIds);
    companyFilter = 'AND company_id = ANY($2::text[])';
  }
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
  const assignedCompanyIds = await getAccessibleCompanyIds(user);
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
  if (assignedCompanyIds !== null) {
    if (!assignedCompanyIds.length) return undefined;
    params.push(assignedCompanyIds);
    companyFilter = `AND company_id = ANY($${params.length}::text[])`;
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
  const assignedCompanyIds = await getAccessibleCompanyIds(user);
  if (!usingPostgres) {
    const store = await loadDevStore();
    const before = store.moduleRecords?.length ?? 0;
    store.moduleRecords = (store.moduleRecords ?? []).filter((record) => record.id !== id || !canAccessCompanyRecord(user, record));
    await saveDevStore(store);
    return (store.moduleRecords?.length ?? 0) < before;
  }
  const params = [id];
  let companyFilter = '';
  if (assignedCompanyIds !== null) {
    if (!assignedCompanyIds.length) return false;
    params.push(assignedCompanyIds);
    companyFilter = `AND company_id = ANY($${params.length}::text[])`;
  }
  const result = await pgQuery(`DELETE FROM module_records WHERE id = $1 ${companyFilter};`, params);
  return result.rowCount > 0;
}

export async function getModuleMetrics(user) {
  const modules = ['accounting', 'engineering', 'hr', 'crm', 'dataProcessing'];
  const assignedCompanyIds = await getAccessibleCompanyIds(user);
  if (!usingPostgres) {
    const store = await loadDevStore();
    return modules.reduce((metrics, module) => {
      const records = (store.moduleRecords ?? []).filter((record) => record.module === module && canAccessCompanyRecord(user, record));
      metrics[module] = { total: records.length, open: records.filter((record) => record.status !== 'closed').length };
      return metrics;
    }, {});
  }
  const params = [];
  let where = '';
  if (assignedCompanyIds !== null) {
    if (!assignedCompanyIds.length) return modules.reduce((metrics, module) => ({ ...metrics, [module]: { total: 0, open: 0 } }), {});
    params.push(assignedCompanyIds);
    where = 'WHERE company_id = ANY($1::text[])';
  }
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

function rowToCompany(row) {
  return {
    id: row.id,
    name: row.name,
    industry: row.industry ?? '',
    ownerName: row.owner_name ?? row.ownerName ?? '',
    email: row.email ?? '',
    phone: row.phone ?? '',
    status: row.status ?? 'Active',
    createdAt: toIso(row.created_at ?? row.createdAt),
    updatedAt: toIso(row.updated_at ?? row.updatedAt ?? row.created_at ?? row.createdAt)
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
    deletedAt: toIso(row.deleted_at ?? row.deletedAt),
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
    insights: analysis.insights ?? [],
    originalDatasetId: row.original_dataset_id ?? row.originalDatasetId ?? null,
    cleanedDatasetId: row.cleaned_dataset_id ?? row.cleanedDatasetId ?? null,
    cleanupStatus: row.cleanup_status ?? row.cleanupStatus ?? analysis.cleanupStatus ?? 'original',
    cleanupLogs: parseJson(row.cleanup_logs ?? row.cleanupLogs ?? analysis.cleanupLogs, []),
    cleanupMetrics: analysis.cleanupMetrics ?? {},
    cleanupPreview: analysis.cleanupPreview ?? null,
    cleanupOperations: analysis.cleanupOperations ?? [],
    futureAiReady: analysis.futureAiReady === true,
    deletedAt: toIso(row.deleted_at ?? row.deletedAt),
    archivedAt: toIso(row.archived_at ?? row.archivedAt),
    retentionUntil: toIso(row.retention_until ?? row.retentionUntil)
  };
}

function rowToCleanupJob(row) {
  return {
    id: row.id,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    userId: row.user_id ?? row.userId,
    originalDatasetId: row.original_dataset_id ?? row.originalDatasetId,
    cleanedDatasetId: row.cleaned_dataset_id ?? row.cleanedDatasetId ?? null,
    status: row.status,
    metrics: parseJson(row.metrics, {}),
    logs: parseJson(row.logs, []),
    error: row.error ?? null,
    deletedAt: toIso(row.deleted_at ?? row.deletedAt),
    createdAt: toIso(row.created_at ?? row.createdAt),
    updatedAt: toIso(row.updated_at ?? row.updatedAt)
  };
}

function rowToNotification(row) {
  return {
    id: row.id,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    userId: row.user_id ?? row.userId,
    type: row.type,
    title: row.title,
    message: row.message,
    status: row.status ?? 'unread',
    metadata: parseJson(row.metadata, {}),
    archivedAt: toIso(row.archived_at ?? row.archivedAt),
    createdAt: toIso(row.created_at ?? row.createdAt)
  };
}

function rowToCompanyAssignment(row) {
  return {
    id: row.id ?? `${row.user_id ?? row.userId}:${row.company_id ?? row.companyId}`,
    userId: row.user_id ?? row.userId,
    companyId: row.company_id ?? row.companyId,
    companyName: row.company_name ?? row.companyName ?? '',
    role: normalizeRole(row.role),
    assignedAt: toIso(row.assigned_at ?? row.assignedAt ?? row.created_at ?? row.createdAt),
    createdAt: toIso(row.created_at ?? row.createdAt ?? row.assigned_at ?? row.assignedAt)
  };
}

function rowToPipeline(row) {
  return {
    id: row.id,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    userId: row.user_id ?? row.userId,
    department: row.department,
    name: row.name,
    status: row.status ?? 'active',
    metadata: parseJson(row.metadata, {}),
    createdAt: toIso(row.created_at ?? row.createdAt),
    updatedAt: toIso(row.updated_at ?? row.updatedAt)
  };
}

function rowToEnterpriseConnector(row) {
  return {
    id: row.id,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    userId: row.user_id ?? row.userId,
    name: row.name,
    connectorType: row.connector_type ?? row.connectorType,
    status: row.status ?? 'not_configured',
    healthStatus: row.health_status ?? row.healthStatus ?? 'unknown',
    permissions: parseJson(row.permissions, {}),
    schedule: parseJson(row.schedule, {}),
    metadata: parseJson(row.metadata, {}),
    credentialEncrypted: Boolean(row.encrypted_credentials ?? row.encryptedCredentials),
    lastSyncAt: toIso(row.last_sync_at ?? row.lastSyncAt),
    nextSyncAt: toIso(row.next_sync_at ?? row.nextSyncAt),
    createdAt: toIso(row.created_at ?? row.createdAt),
    updatedAt: toIso(row.updated_at ?? row.updatedAt)
  };
}

function rowToConnectorSyncLog(row) {
  return {
    id: row.id,
    connectorId: row.connector_id ?? row.connectorId,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    status: row.status,
    recordsProcessed: row.records_processed ?? row.recordsProcessed ?? 0,
    failedRows: row.failed_rows ?? row.failedRows ?? 0,
    retries: row.retries ?? 0,
    durationMs: row.duration_ms ?? row.durationMs ?? 0,
    error: row.error,
    metadata: parseJson(row.metadata, {}),
    startedAt: toIso(row.started_at ?? row.startedAt),
    completedAt: toIso(row.completed_at ?? row.completedAt),
    createdAt: toIso(row.created_at ?? row.createdAt)
  };
}

function rowToPipelineSchedule(row) {
  return {
    id: row.id,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    pipelineId: row.pipeline_id ?? row.pipelineId,
    name: row.name,
    scheduleType: row.schedule_type ?? row.scheduleType,
    cronExpression: row.cron_expression ?? row.cronExpression,
    eventTrigger: row.event_trigger ?? row.eventTrigger,
    priority: row.priority ?? 5,
    slaMinutes: row.sla_minutes ?? row.slaMinutes ?? 60,
    retryPolicy: parseJson(row.retry_policy ?? row.retryPolicy, {}),
    dependencies: parseJson(row.dependencies, []),
    status: row.status,
    nextRunAt: toIso(row.next_run_at ?? row.nextRunAt),
    lastRunAt: toIso(row.last_run_at ?? row.lastRunAt),
    metadata: parseJson(row.metadata, {}),
    createdBy: row.created_by ?? row.createdBy,
    createdAt: toIso(row.created_at ?? row.createdAt),
    updatedAt: toIso(row.updated_at ?? row.updatedAt)
  };
}

function rowToWorkflowIntelligence(row) {
  return {
    id: row.id,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    datasetId: row.dataset_id ?? row.datasetId,
    module: row.module,
    insightType: row.insight_type ?? row.insightType,
    severity: row.severity,
    title: row.title,
    summary: row.summary,
    confidence: Number(row.confidence ?? 0),
    recommendations: parseJson(row.recommendations, []),
    explainability: parseJson(row.explainability, {}),
    status: row.status,
    createdBy: row.created_by ?? row.createdBy,
    createdAt: toIso(row.created_at ?? row.createdAt)
  };
}

function rowToPipelineStageRun(row) {
  return {
    id: row.id,
    pipelineId: row.pipeline_id ?? row.pipelineId,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    module: row.module,
    datasetId: row.dataset_id ?? row.datasetId,
    stageName: row.stage_name ?? row.stageName,
    status: row.status,
    operatorUserId: row.operator_user_id ?? row.operatorUserId,
    logs: parseJson(row.logs, []),
    validationOutput: parseJson(row.validation_output ?? row.validationOutput, {}),
    metrics: parseJson(row.metrics, {}),
    startedAt: toIso(row.started_at ?? row.startedAt),
    completedAt: toIso(row.completed_at ?? row.completedAt),
    createdAt: toIso(row.created_at ?? row.createdAt),
    updatedAt: toIso(row.updated_at ?? row.updatedAt)
  };
}

function rowToPipelineRule(row) {
  return {
    id: row.id,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    module: row.module,
    ruleKey: row.rule_key ?? row.ruleKey,
    label: row.label,
    config: parseJson(row.config, {}),
    enabled: row.enabled !== false,
    createdBy: row.created_by ?? row.createdBy,
    createdAt: toIso(row.created_at ?? row.createdAt),
    updatedAt: toIso(row.updated_at ?? row.updatedAt)
  };
}

function rowToAccessRequest(row) {
  return {
    id: row.id,
    companyId: row.company_id ?? row.companyId ?? defaultCompanyId,
    requesterUserId: row.requester_user_id ?? row.requesterUserId,
    targetUserId: row.target_user_id ?? row.targetUserId,
    department: row.department,
    requestedRole: normalizeRole(row.requested_role ?? row.requestedRole),
    reason: row.reason,
    status: row.status,
    expiresAt: toIso(row.expires_at ?? row.expiresAt),
    approvedBy: row.approved_by ?? row.approvedBy,
    approvedAt: toIso(row.approved_at ?? row.approvedAt),
    metadata: parseJson(row.metadata, {}),
    createdAt: toIso(row.created_at ?? row.createdAt),
    updatedAt: toIso(row.updated_at ?? row.updatedAt)
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
  if (hasGlobalCompanyAccess(user)) return true;
  if (Array.isArray(user?.accessibleCompanyIds)) return user.accessibleCompanyIds.includes(record.companyId ?? defaultCompanyId);
  return (record.companyId ?? defaultCompanyId) === getCompanyId(user) && getCompanyId(user) !== defaultCompanyId;
}

export function hasRole(user, requiredRole) {
  if (!user || typeof user !== 'object') return false;
  return roleRank(roleForUser(user.email, user.role)) >= roleRank(requiredRole);
}

export function isOwner(user) {
  return Boolean(user && typeof user === 'object' && roleForUser(user.email, user.role) === 'owner');
}

export function hasGlobalCompanyAccess(user) {
  const role = user && typeof user === 'object' ? roleForUser(user.email, user.role) : '';
  return role === 'owner' || role === 'admin';
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

function publicHostLabel(value) {
  if (!value) return null;
  try {
    const host = new URL(value).hostname;
    if (host.includes('neon.tech')) return 'neon-postgres';
    return 'external-postgres';
  } catch {
    return 'invalid-url';
  }
}

function databasePort(value) {
  if (!value) return null;
  try {
    const url = new URL(value);
    return url.port || 'default';
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
      companies: [],
      users: [],
      sessions: [],
      datasets: [],
      dashboards: [],
      reports: [],
      moduleRecords: [],
      accountTokens: [],
      auditLogs: [],
      emailLogs: [],
      invitations: [],
      cleanupJobs: [],
      notifications: [],
      analytics: [],
      pipelines: [],
      enterpriseConnectors: [],
      connectorSyncLogs: [],
      pipelineSchedules: [],
      pipelineStageRuns: [],
      pipelineRules: [],
      workflowIntelligence: [],
      accessRequests: [],
      userCompanyAssignments: []
    };
  }

  const store = JSON.parse(await readFile(devStoreFile, 'utf8'));
  return {
    companies: store.companies ?? [],
    users: store.users ?? [],
    sessions: store.sessions ?? [],
    datasets: store.datasets ?? [],
    dashboards: store.dashboards ?? [],
    reports: store.reports ?? [],
    moduleRecords: store.moduleRecords ?? [],
    accountTokens: store.accountTokens ?? [],
    auditLogs: store.auditLogs ?? [],
    emailLogs: store.emailLogs ?? [],
    invitations: store.invitations ?? [],
    cleanupJobs: store.cleanupJobs ?? [],
    notifications: store.notifications ?? [],
    analytics: store.analytics ?? [],
    pipelines: store.pipelines ?? [],
    enterpriseConnectors: store.enterpriseConnectors ?? [],
    connectorSyncLogs: store.connectorSyncLogs ?? [],
    pipelineSchedules: store.pipelineSchedules ?? [],
    pipelineStageRuns: store.pipelineStageRuns ?? [],
    pipelineRules: store.pipelineRules ?? [],
    workflowIntelligence: store.workflowIntelligence ?? [],
    accessRequests: store.accessRequests ?? [],
    userCompanyAssignments: store.userCompanyAssignments ?? []
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
