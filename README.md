# MotoGear Shop

## Apply database migrations

The migration command uses SQLx to apply any pending files from `migrations/`.
It reads the PostgreSQL credentials from the ignored `.env` file and is safe to
run again: SQLx records which migrations have already been applied.

1. Copy `.env.example` to `.env` and set a local PostgreSQL password.
2. Start the PostgreSQL service:

   ```powershell
   docker compose up -d postgres
   ```

3. Apply the migrations:

   ```powershell
   cargo run -p motogear-persistence --bin migrate
   ```

For a PostgreSQL server running somewhere else, set `POSTGRES_HOST` and
`POSTGRES_PORT` in `.env`. The defaults target the Compose service from the
Windows host (`127.0.0.1:5433`).

## Seed demo data

After applying the migrations, insert the fictional customers and products:

```powershell
cargo run -p motogear-persistence --bin seed
```

The command inserts three customers and eight products in one transaction. It
only inserts missing rows: rerunning it does not change existing product data
or restore stock that has already been sold. The phone mount starts with one
unit when it is first inserted.

## Start the API

After the database migrations have been applied, start the API:

```powershell
cargo run -p motogear-api
```

The API currently exposes `GET /health/live` for process liveness and
`GET /health/ready` for database schema readiness. The live route does not
depend on PostgreSQL; readiness returns HTTP 503 until all migrations included
in this build have been applied.
