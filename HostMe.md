# Local setup and hosting

## Requirements

- Rust toolchain with edition 2024 support
- PostgreSQL reachable from this machine or deployment environment
- `protoc` (Protocol Buffers compiler)
- Optional: `grpcurl` for exercising the reflected API

## Local setup

1. Copy `.env.example` to `.env`.
2. Set the required `DATABASE_URL` to a PostgreSQL database reachable from the service. For example:

   ```dotenv
   DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/product_service
   ```

   Keep `.env` out of source control. Ensure the database exists and the migration role can create the schema objects. Optional runtime settings and their defaults are listed in `.env.example`.

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
   grpcurl -plaintext localhost:50052 describe catalog.ProductImageService
   grpcurl -plaintext localhost:50052 describe orders.OrderService
   ```

## Hosting

1. Provision PostgreSQL and create a dedicated database role with only the privileges needed for the service schema and migrations. Configure network access from the service host.
2. Build a release binary in CI or on a Rust build host:

   ```sh
   cargo build --release
   ```

3. Set `DATABASE_URL` and `GRPC_ADDR=0.0.0.0:50052` in the hosting platform; the listener must bind to the container interface. Optionally set `APP_NAME`, `GRPC_REFLECTION=false`, and `RUST_LOG`. Store database credentials in the platform's secret manager; do not bake credentials into the image or commit `.env`.
4. Run `sqlx migrate run` as a controlled deployment step before starting/restarting the application. This applies the product image storage migration, which adds `image_data` (`BYTEA`) and `content_type` columns to `product_images`. Back up PostgreSQL and rehearse schema upgrades against a staging copy.
5. Run `target/release/catalog-order-service` under a supervisor or container orchestrator with restart policy and health monitoring. Keep the gRPC port private; route gateway traffic over a protected private network or terminate TLS at a trusted gRPC-capable proxy.
6. Restrict admin order RPC access at the gateway. This service treats all callers as trusted and does not authenticate or authorize them.

Uploaded JPEG, PNG, and WebP images up to 5 MiB are stored in PostgreSQL by the product service. The admin gateway sends the bytes through gRPC and serves retrieved image data to browser clients; no image filesystem or object-storage environment setting is required. Keep the gRPC port private and expose image retrieval through an appropriately protected or public gateway route for the consuming application.

The `orders.user_id` column intentionally has no foreign key to the separate user service. Orders and their user IDs remain in this database if a user is later deleted elsewhere.

## Container example

Build in an environment with `protoc` installed, then copy the release binary and migrations into a minimal runtime image. Mount configuration through environment variables and run migrations as a separate release job. Do not expose PostgreSQL publicly. Configure graceful termination and connection pool limits to match the database's connection capacity.
