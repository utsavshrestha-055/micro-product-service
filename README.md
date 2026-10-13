# Catalog and Order Service

Rust gRPC service for product catalog, product page data, images, and customer orders. It uses tonic 0.14, `tonic-prost-build`, SQLx/PostgreSQL, ordered SQL migrations, and optional gRPC reflection.

The service trusts its callers. A gateway must authenticate users, authorize operations, supply `user_id` for customer order RPCs, and restrict access to the admin order RPCs. This service does not parse tokens or check permissions. Order `user_id` values are retained indefinitely as plain UUIDs without a foreign key; deleting a user in another service does not delete or invalidate their orders.

## gRPC endpoints

Default local listener: `127.0.0.1:50052`. Package/API definitions live in [`proto/catalog.proto`](proto/catalog.proto) and [`proto/orders.proto`](proto/orders.proto). Reflection is enabled by default for development and can be disabled with `GRPC_REFLECTION=false`.

| Service | RPCs |
| --- | --- |
| `catalog.CategoryService` | `CreateCategory`, `GetCategory`, `ListCategories`, `UpdateCategory`, `DeleteCategory` |
| `catalog.ProductService` | `CreateProduct`, `GetProduct`, `ListProducts`, `UpdateProduct`, `DeleteProduct`, `CreateProductDetails`, `GetProductDetails`, `UpdateProductDetails`, `DeleteProductDetails`, `GetProductPage` |
| `catalog.ProductImageService` | `CreateProductImage`, `GetProductImage`, `ListProductImages`, `UpdateProductImage`, `DeleteProductImage` |
| `orders.OrderService` | `CreateOrder`, `GetOrder`, `ListOrders`, `CancelOrder`, `GetOrderAdmin`, `ListOrdersAdmin`, `UpdateOrderStatus` |

`GetProductPage` returns product, optional product details, its category ancestry, and images. Product image URLs and metadata are stored; this service does not accept binary uploads. When the first image is added it becomes primary automatically. Setting an image primary transfers primary status atomically; deleting a product's only image is rejected to preserve a primary image whenever images exist.

All list APIs are paginated using `page_size` and an offset-valued `page_token`; page sizes default to 25 and are capped at 100. Products support category, active, price bounds, and name search filters. Order list APIs support status and date bounds; admin listing also accepts user ID.

## Examples

Install `grpcurl` and start the service, then inspect each reflected service:

```sh
grpcurl -plaintext localhost:50052 list
grpcurl -plaintext localhost:50052 describe catalog.CategoryService
grpcurl -plaintext localhost:50052 describe catalog.ProductService
grpcurl -plaintext localhost:50052 describe catalog.ProductImageService
grpcurl -plaintext localhost:50052 describe orders.OrderService
```

Example RPC invocations (IDs must be existing UUIDs where applicable):

```sh
grpcurl -plaintext -d '{"name":"Books","slug":"books","description":"Books","is_active":true}' localhost:50052 catalog.CategoryService/CreateCategory
grpcurl -plaintext -d '{"page_size":20,"name_search":"book"}' localhost:50052 catalog.ProductService/ListProducts
grpcurl -plaintext -d '{"id":"PRODUCT_UUID"}' localhost:50052 catalog.ProductService/GetProductPage
grpcurl -plaintext -d '{"product_id":"PRODUCT_UUID","url":"https://cdn.example.test/book.jpg","alt_text":"Book cover","is_primary":true}' localhost:50052 catalog.ProductImageService/CreateProductImage
grpcurl -plaintext -d '{"user_id":"USER_UUID","idempotency_key":"checkout-2026-001","currency":"USD","shipping_address_json":"{\"line1\":\"1 Main St\",\"city\":\"Gotham\"}","items":[{"product_id":"PRODUCT_UUID","quantity":2}]}' localhost:50052 orders.OrderService/CreateOrder
grpcurl -plaintext -d '{"id":"ORDER_UUID","user_id":"USER_UUID"}' localhost:50052 orders.OrderService/GetOrder
grpcurl -plaintext -d '{"id":"ORDER_UUID","status":"CONFIRMED","changed_by":"GATEWAY_ACTOR_ID","note":"payment confirmed"}' localhost:50052 orders.OrderService/UpdateOrderStatus
```

Customer `GetOrder`, `ListOrders`, and `CancelOrder` scope queries by both order and user ID and report `NOT_FOUND` on ownership mismatch. `GetOrderAdmin`, `ListOrdersAdmin`, and `UpdateOrderStatus` do not perform authorization; only expose these through a gateway-protected private route.

## Data behavior

- Prices and totals are integer minor units (`i64`); the server calculates order totals from locked product records.
- `CreateOrder` uses one transaction, locks products in UUID order, checks active status and stock, decrements stock, snapshots name/SKU/price, stores the order and initial history row, and uses a per-user idempotency key.
- Cancelling a pending or confirmed order restores item stock in the same transaction. Status updates record history.
- Allowed transitions: `PENDING -> CONFIRMED/CANCELLED`, `CONFIRMED -> PAID/CANCELLED`, `PAID -> SHIPPED/REFUNDED`, `SHIPPED -> DELIVERED`, `DELIVERED -> REFUNDED`. Terminal statuses have no outgoing transitions.
- Products and categories are soft deleted. Category deletion fails while it has active products or children. Orders are never deleted.
- Database constraints enforce unique slugs/SKUs/idempotency keys and at most one primary image per product.

## Logging

Logs are written to **stderr**, so they appear in the terminal where `cargo run` is running. The service does not create or update a log file by itself. Each completed gRPC call logs the operation path, the incoming `x-request-id` (or a generated ID when that header is absent), duration in milliseconds, and the gRPC status. Startup and database errors are logged there as well.

Set `RUST_LOG` to control verbosity; for example:

```sh
RUST_LOG=debug cargo run
```

To also keep a local log file, capture stderr yourself:

```sh
cargo run 2>&1 | tee -a catalog-order-service.log
```

In production, configure the process supervisor or container platform to collect stderr. This keeps log rotation and retention under the hosting environment's control.

## Configuration and migrations

Copy `.env.example` to `.env`, set `DATABASE_URL`, and follow [HostMe.md](HostMe.md) for local setup and hosting. Apply migrations with:

```sh
cargo install sqlx-cli --no-default-features --features rustls,postgres
sqlx migrate run
```

Environment variables: `APP_NAME`, `GRPC_ADDR` (default `127.0.0.1:50052`), `DATABASE_URL`, `GRPC_REFLECTION` (default `true`), and `RUST_LOG`.
