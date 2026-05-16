# Metenova AI Platform

Metenova AI is a modular AI-powered business operations platform for company workspaces, analytics, reporting, user management, data processing, and enterprise workflows.

## Structure

- `frontend`: React/Vite executive dashboard, workspace modules, CSV/Excel analysis, charts, reports, admin controls, support UI, and session timeout handling.
- `backend`: Express API with JWT authentication, role-based access, company-scoped records, invitations, support delivery logs, Railway PostgreSQL persistence, and CSV/Excel processing.

## Local Development

```bash
npm install
npm run dev
```

Open `http://localhost:5174`. The backend runs on `http://localhost:4000`.

For local development, the app can run without production storage credentials. Production deployments must connect protected workspace storage before saving uploads, dashboards, reports, module records, role changes, profile updates, and invitations.

## Required Production Environment

Configure these variables in the backend Vercel project. Railway provides `DATABASE_URL` automatically when you connect the PostgreSQL service.

```env
JWT_SECRET=replace-with-long-random-secret
OWNER_EMAIL=melakue@metenovaai.com
CLIENT_ORIGIN=https://your-frontend-domain
APP_BASE_URL=https://your-frontend-domain
SESSION_TTL_MINUTES=15
SESSION_WARNING_SECONDS=60

DATABASE_URL=postgresql://...

RESEND_API_KEY=your-resend-api-key
EMAIL_FROM=NewFuture Business Platform <support@metenovaai.com>
```

Production email delivery requires a verified sender/domain in the email provider. Support requests, invitations, verification messages, username recovery, and password reset emails are logged and can be retried from the admin area.

## Roles

Supported roles:

- Owner / Super Admin
- Admin
- Manager
- Employee
- Viewer

The permanent owner is `melakue@metenovaai.com`. Owner permissions cannot be downgraded, disabled, deleted, or overwritten.

## API Highlights

- `GET /health`
- `GET /api/config`
- `POST /api/auth/signup`
- `POST /api/auth/login`
- `GET /api/auth/me`
- `POST /api/auth/refresh`
- `POST /api/auth/logout`
- `GET /api/admin/users`
- `PATCH /api/admin/users/:id`
- `POST /api/admin/invitations`
- `GET /api/modules/:module/records`
- `POST /api/modules/:module/records`
- `PATCH /api/modules/:module/records/:id`
- `DELETE /api/modules/:module/records/:id`
- `POST /api/files/upload`
- `GET /api/datasets`
- `POST /api/datasets/:id/chat`
- `GET /api/dashboards`
- `POST /api/dashboards`
- `GET /api/reports`
- `POST /api/reports`
- `POST /api/contact`

## Validation

```bash
node --check backend/src/server.js
npm run build
```
