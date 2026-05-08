# Business AI Platform

A starter full-stack app with a React/Vite frontend and an Express backend.

## Structure

- `frontend`: React dashboard with dark/light mode, drag-and-drop CSV upload, preview tables, chart controls, PDF report download, and an AI data chat
- `backend`: Express API server with local/JWT auth, stored login sessions, CSV parsing, SQL Server persistence, saved dashboards, report history, and dataset-aware chat responses

## Run locally

```bash
npm install
npm run dev
```

Open `http://localhost:5173` to use the app. The frontend proxies API calls to `http://localhost:4000`.

For local MVP development, SQL Server is optional. If SQL Server environment variables are not set, the backend uses `backend/data/dev-store.json` and keeps a simple local login/signup system backed by that file.

Temporary demo login for local testing:

- Email: `admin@businessai.com`
- Password: `admin123`

Roles are stored with each user as `admin` or `user`. Users can only view their own datasets, dashboards, and reports. Admins can view all saved dashboards and reports. Configure initial admin accounts with `ADMIN_EMAILS` in `backend/.env`:

```env
ADMIN_EMAILS=admin@businessai.com,ops-lead@example.com
```

To enable SQL Server mode, copy `backend/.env.example` to `backend/.env`, set the `SQLSERVER_*` credentials and `JWT_SECRET`, then start SQL Server:

```bash
cp backend/.env.example backend/.env
docker compose up -d sqlserver
npm run dev
```

Default local SQL Server credentials in `docker-compose.yml` and `backend/.env.example`:

- Host: `localhost`
- Port: `1433`
- Database: `business_ai_platform`
- User: `sa`
- Password: `YourStrong!Passw0rd`

Tables are created automatically on startup. If `backend/data/uploads/datasets.json` exists from an older local build, those datasets are imported during startup.
SQL Server mode creates `users`, `sessions`, `datasets`, `dashboards`, and `reports` tables. Local mode stores the same model shape in `backend/data/dev-store.json`.

## API

- `GET /health`
- `POST /api/auth/signup`
- `POST /api/auth/login`
- `GET /api/auth/me`
- `GET /api/auth/sessions`
- `POST /api/auth/logout`
- `GET /api/admin/workspace`
- `GET /api/insights`
- `GET /api/workflows`
- `POST /api/csv/upload` with a `file` form field containing a `.csv`
- `GET /api/datasets`
- `GET /api/datasets/:id`
- `POST /api/datasets/:id/chat`
- `GET /api/dashboards`
- `POST /api/dashboards`
- `GET /api/reports`
- `POST /api/reports`

Dataset, dashboard, report, and assistant routes require a bearer token from the login/signup flow. Admin routes also require the `admin` role. Login sessions are persisted and checked on protected requests. Uploaded CSV metadata, saved dashboards, generated reports, and report history are stored by user in SQL Server or in the local JSON fallback.
