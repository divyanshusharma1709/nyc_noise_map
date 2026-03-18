This frontend renders the NYC noise complaint map against the FastAPI backend in the sibling backend project.

## Environment

Create `.env.local`:

```bash
NEXT_PUBLIC_API_BASE=http://127.0.0.1:8000
```

## Development

First, run the development server:

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

The app reads:

- `GET /complaints`
- `GET /grid-summary`
- `GET /stats/overview`
- `GET /sync/status`

Make sure the backend API is running before starting the frontend.

## Deploy

Set `NEXT_PUBLIC_API_BASE` to the deployed FastAPI URL in Railway or your target environment.

Example:

```bash
NEXT_PUBLIC_API_BASE=https://your-backend-service.up.railway.app
```
