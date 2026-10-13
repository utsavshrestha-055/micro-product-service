use std::{env, net::SocketAddr};

#[derive(Clone, Debug)]
pub struct Config {
    pub app_name: String,
    pub grpc_addr: SocketAddr,
    pub database_url: String,
    pub reflection: bool,
}
impl Config {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        dotenvy::dotenv().ok();
        Ok(Self {
            app_name: env::var("APP_NAME").unwrap_or_else(|_| "catalog-order-service".into()),
            grpc_addr: env::var("GRPC_ADDR")
                .unwrap_or_else(|_| "127.0.0.1:50052".into())
                .parse()?,
            database_url: env::var("DATABASE_URL")?,
            reflection: env::var("GRPC_REFLECTION")
                .unwrap_or_else(|_| "true".into())
                .parse()?,
        })
    }
}
