pub mod categories;
pub mod images;
pub mod logging;
pub mod orders;
pub mod products;

pub fn parse_uuid(value: &str, field: &str) -> Result<uuid::Uuid, tonic::Status> {
    uuid::Uuid::parse_str(value)
        .map_err(|_| tonic::Status::invalid_argument(format!("{field} must be a UUID")))
}
pub fn page_size(n: i32) -> i64 {
    if n <= 0 { 25 } else { n.min(100) as i64 }
}
pub fn page_offset(token: &str) -> Result<i64, tonic::Status> {
    if token.is_empty() {
        return Ok(0);
    }
    let offset = token
        .parse::<i64>()
        .map_err(|_| tonic::Status::invalid_argument("page_token must be an offset"))?;
    if offset < 0 {
        return Err(tonic::Status::invalid_argument(
            "page_token must be non-negative",
        ));
    }
    Ok(offset)
}
pub fn log_rpc(
    op: &str,
    id: &str,
    started: std::time::Instant,
    result: &Result<(), tonic::Status>,
) {
    let code = result
        .as_ref()
        .err()
        .map(|e| format!("{:?}", e.code()))
        .unwrap_or_else(|| "OK".into());
    tracing::info!(operation=op, request_id=id, duration_ms=started.elapsed().as_millis() as u64, status=%code, "RPC completed");
}
