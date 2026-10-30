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

Open the browser shop at <http://127.0.0.1:3000/>. Axum serves the HTML, CSS,
and vanilla JavaScript from the same process as the API, with no frontend build
step. The page loads the customer selector and catalogue from the API, remembers
the selected customer per tab, and refreshes shared product stock every two
seconds while the tab is visible. The customer selection is demo state only at
this stage; it does not affect any shop operation because checkout is not
implemented yet. Product quantity controls and the cart are also browser-only:
they show a draft selection but do not create an order or reserve stock. The
browser requests up to 100 active products so the current demo catalogue fits
in one response; pagination controls can replace this when the catalogue grows.

The read-only catalogue endpoints are:

- `GET /api/v1/customers?limit=20&offset=0` — demo identities for the customer selector.
- `GET /api/v1/products?limit=20&offset=0` — active products, ordered by SKU.
  `limit` defaults to 20 and must be between 1 and 100; `offset` defaults to 0
  and may not exceed 100000. Product prices are returned as integer EUR cents.

For example, in PowerShell:

```powershell
Invoke-RestMethod http://127.0.0.1:3000/api/v1/products
```

## Database-backed tests

The API and persistence integration tests use PostgreSQL. Set
`TEST_DATABASE_URL` to a database where the test role can create schemas, then
run:

```powershell
cargo test -p motogear-api
cargo test -p motogear-persistence
```

Database-backed tests create a uniquely named schema, apply the real migrations
and seed data there, and drop that schema when the test finishes. They do not
change the database's `public` schema. Persistence tests cover order submission,
idempotency, purchase-time prices, outbox atomicity, and concurrent retries.
