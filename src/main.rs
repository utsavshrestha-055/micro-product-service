use micro_product_service::{config::Config, database, grpc, proto};
use proto::orders::order_service_server::OrderServiceServer;
use proto::{
    category_service_server::CategoryServiceServer,
    product_image_service_server::ProductImageServiceServer,
    product_service_server::ProductServiceServer,
};

const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("catalog_order_descriptor");
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let config = Config::from_env()?;
    let pool = database::create_pool(&config.database_url).await?;
    sqlx::query("SELECT 1").execute(&pool).await?;
    let reflection = if config.reflection {
        Some(
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
                .build_v1()?,
        )
    } else {
        None
    };
    tracing::info!(service=%config.app_name, address=%config.grpc_addr, reflection=config.reflection, "gRPC server starting");
    tonic::transport::Server::builder()
        .layer(grpc::logging::RpcLogLayer)
        .add_optional_service(reflection)
        .add_service(CategoryServiceServer::new(
            grpc::categories::CategoryGrpc::new(pool.clone()),
        ))
        .add_service(ProductServiceServer::new(grpc::products::ProductGrpc::new(
            pool.clone(),
        )))
        .add_service(
            ProductImageServiceServer::new(grpc::images::ImageGrpc::new(pool.clone()))
                .max_decoding_message_size(6 * 1024 * 1024)
                .max_encoding_message_size(6 * 1024 * 1024),
        )
        .add_service(OrderServiceServer::new(grpc::orders::OrderGrpc::new(pool)))
        .serve(config.grpc_addr)
        .await?;
    Ok(())
}
