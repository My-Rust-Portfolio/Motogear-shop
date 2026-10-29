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
