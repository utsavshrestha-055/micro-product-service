use tonic::Status;
pub fn db(e: sqlx::Error) -> Status {
    if let sqlx::Error::Database(d) = &e {
        match d.code().as_deref() {
            Some("23505") => return Status::already_exists("resource already exists"),
            Some("23503" | "23514" | "23502") => {
                return Status::failed_precondition("database constraint rejected the operation");
            }
            _ => {}
        }
    }
    tracing::error!(error=%e, "database operation failed");
    Status::internal("service database error")
}
pub fn invalid(s: impl Into<String>) -> Status {
    Status::invalid_argument(s.into())
}
