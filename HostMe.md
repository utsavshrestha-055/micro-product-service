# Local setup and hosting

## Requirements

- Rust toolchain with edition 2024 support
- PostgreSQL reachable from this machine or deployment environment
- `protoc` (Protocol Buffers compiler)
- Optional: `grpcurl` for exercising the reflected API

## Local setup

1. Copy `.env.example` to `.env`.
2. Set `DATABASE_URL` to the development database. For this project, the requested local connection is:

   ```dotenv
   DATABASE_URL=postgres://batman:iron@psql.batman:5432/product_service
   ```

   Keep `.env` out of source control; `.env.example` deliberately contains only a placeholder. Ensure the `product_service` database exists and the role can create the schema objects used by the migration.

3. Install SQLx CLI if needed and apply the schema:

   ```sh
   cargo install sqlx-cli --no-default-features --features rustls,postgres
   sqlx migrate run
   ```

4. Start the service:

   ```sh
   cargo run
   ```

   It binds to `127.0.0.1:50052`, checks the PostgreSQL connection, and enables reflection by default. Override with `GRPC_ADDR`, for example `0.0.0.0:50052` for a container listener. Set `GRPC_REFLECTION=false` in production.

5. Check the API:

   ```sh
   grpcurl -plaintext localhost:50052 list
   grpcurl -plaintext localhost:50052 describe orders.OrderService
   ```

## Hosting

1. Provision PostgreSQL and create a dedicated database role with only the privileges needed for the service schema and migrations. Configure network access from the service host.
2. Build a release binary in CI or on a Rust build host:

   ```sh
   cargo build --release
   ```

3. Provide `APP_NAME`, `GRPC_ADDR=0.0.0.0:50052`, `DATABASE_URL`, `GRPC_REFLECTION=false`, and `RUST_LOG` through the hosting platform's secret/configuration manager. Do not bake credentials into the image or commit `.env`.
4. Run `sqlx migrate run` as a controlled deployment step before starting/restarting the application. Back up PostgreSQL and rehearse schema upgrades against a staging copy.
5. Run `target/release/catalog-order-service` under a supervisor or container orchestrator with restart policy and health monitoring. Keep the gRPC port private; route gateway traffic over a protected private network or terminate TLS at a trusted gRPC-capable proxy.
6. Restrict admin order RPC access at the gateway. This service treats all callers as trusted and does not authenticate or authorize them.

The `orders.user_id` column intentionally has no foreign key to the separate user service. Orders and their user IDs remain in this database if a user is later deleted elsewhere.

## Container example

Build in an environment with `protoc` installed, then copy the release binary and migrations into a minimal runtime image. Mount configuration through environment variables and run migrations as a separate release job. Do not expose PostgreSQL publicly. Configure graceful termination and connection pool limits to match the database's connection capacity.
