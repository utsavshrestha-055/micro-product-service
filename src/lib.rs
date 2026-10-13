pub mod proto {
    tonic::include_proto!("catalog");
    pub mod orders {
        tonic::include_proto!("orders");
    }
}
pub mod config;
pub mod database;
mod errors;
pub mod grpc;
mod repositories;
