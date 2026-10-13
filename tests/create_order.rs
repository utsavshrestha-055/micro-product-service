use catalog_order_service::{
    database,
    grpc::orders::OrderGrpc,
    proto::orders::{CreateOrderRequest, OrderItemRequest, order_service_server::OrderService},
};
use tonic::Request;
use uuid::Uuid;

#[tokio::test]
async fn create_order_success_out_of_stock_and_idempotent_retry() {
    dotenvy::dotenv().ok();
    let Ok(url) = std::env::var("TEST_DATABASE_URL").or_else(|_| std::env::var("DATABASE_URL"))
    else {
        eprintln!(
            "skipping database integration test: TEST_DATABASE_URL or DATABASE_URL is not set"
        );
        return;
    };
    let pool = database::create_pool(&url)
        .await
        .expect("connect test database");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("apply migrations");
    let suffix = Uuid::new_v4().to_string().replace('-', "");
    let user = Uuid::new_v4();
    let category: Uuid = sqlx::query_scalar(
        "INSERT INTO categories(name,slug) VALUES('integration','integration-'||$1) RETURNING id",
    )
    .bind(&suffix)
    .fetch_one(&pool)
    .await
    .expect("create category");
    let product: Uuid = sqlx::query_scalar("INSERT INTO products(category_id,name,slug,sku,price_minor,currency,stock_quantity) VALUES($1,'Integration Item',$2,$3,175,'USD',5) RETURNING id")
        .bind(category).bind(format!("integration-{suffix}")).bind(format!("SKU-{suffix}")).fetch_one(&pool).await.expect("create product");
    let empty_stock: Uuid = sqlx::query_scalar("INSERT INTO products(category_id,name,slug,sku,price_minor,currency,stock_quantity) VALUES($1,'Empty Item',$2,$3,25,'USD',0) RETURNING id")
        .bind(category).bind(format!("empty-{suffix}")).bind(format!("EMPTY-{suffix}")).fetch_one(&pool).await.expect("create empty-stock product");
    let service = OrderGrpc::new(pool.clone());
    let request = || CreateOrderRequest {
        user_id: user.to_string(),
        idempotency_key: format!("integration-{suffix}"),
        currency: "USD".into(),
        shipping_address_json: r#"{"city":"Gotham"}"#.into(),
        items: vec![OrderItemRequest {
            product_id: product.to_string(),
            quantity: 2,
        }],
    };
    let first = OrderService::create_order(&service, Request::new(request()))
        .await
        .expect("create order")
        .into_inner()
        .order
        .expect("order response");
    assert_eq!(first.total_minor, 350);
    let stock: i64 = sqlx::query_scalar("SELECT stock_quantity FROM products WHERE id=$1")
        .bind(product)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(stock, 3);
    let retry = OrderService::create_order(&service, Request::new(request()))
        .await
        .expect("idempotent retry")
        .into_inner()
        .order
        .expect("retry response");
    assert_eq!(retry.id, first.id);
    let stock_after_retry: i64 =
        sqlx::query_scalar("SELECT stock_quantity FROM products WHERE id=$1")
            .bind(product)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stock_after_retry, 3);
    let out = CreateOrderRequest {
        items: vec![OrderItemRequest {
            product_id: empty_stock.to_string(),
            quantity: 1,
        }],
        idempotency_key: format!("empty-{suffix}"),
        ..request()
    };
    let error = OrderService::create_order(&service, Request::new(out))
        .await
        .expect_err("out of stock should fail");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    sqlx::query("DELETE FROM order_status_history WHERE order_id IN (SELECT id FROM orders WHERE user_id=$1)").bind(user).execute(&pool).await.unwrap();
    sqlx::query(
        "DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE user_id=$1)",
    )
    .bind(user)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("DELETE FROM orders WHERE user_id=$1")
        .bind(user)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM products WHERE id=ANY($1)")
        .bind(vec![product, empty_stock])
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM categories WHERE id=$1")
        .bind(category)
        .execute(&pool)
        .await
        .unwrap();
}
