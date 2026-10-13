use bytes::Bytes;
use http::{Request, Response};
use http_body::{Body, Frame};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};
use tower::{Layer, Service};

#[derive(Clone, Copy, Default)]
pub struct RpcLogLayer;
impl<S> Layer<S> for RpcLogLayer {
    type Service = RpcLogService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        RpcLogService { inner }
    }
}

#[derive(Clone)]
pub struct RpcLogService<S> {
    inner: S,
}
impl<S, B> Service<Request<tonic::body::Body>> for RpcLogService<S>
where
    S: Service<Request<tonic::body::Body>, Response = Response<B>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Send + 'static,
{
    type Response = Response<RpcLogBody<B>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, req: Request<tonic::body::Body>) -> Self::Future {
        let operation = req.uri().path().to_owned();
        let request_id = req
            .headers()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let started = Instant::now();
        let future = self.inner.call(req);
        Box::pin(async move {
            let response = future.await?;
            let http_status = response.status().as_u16();
            let (parts, body) = response.into_parts();
            Ok(Response::from_parts(
                parts,
                RpcLogBody {
                    inner: Box::pin(body),
                    operation,
                    request_id,
                    started,
                    http_status,
                    grpc_status: None,
                    logged: false,
                },
            ))
        })
    }
}

pub struct RpcLogBody<B> {
    inner: Pin<Box<B>>,
    operation: String,
    request_id: String,
    started: Instant,
    http_status: u16,
    grpc_status: Option<String>,
    logged: bool,
}
impl<B> Unpin for RpcLogBody<B> {}
fn grpc_status_name(code: &str) -> &str {
    match code {
        "0" => "OK",
        "1" => "CANCELLED",
        "2" => "UNKNOWN",
        "3" => "INVALID_ARGUMENT",
        "4" => "DEADLINE_EXCEEDED",
        "5" => "NOT_FOUND",
        "6" => "ALREADY_EXISTS",
        "7" => "PERMISSION_DENIED",
        "8" => "RESOURCE_EXHAUSTED",
        "9" => "FAILED_PRECONDITION",
        "10" => "ABORTED",
        "11" => "OUT_OF_RANGE",
        "12" => "UNIMPLEMENTED",
        "13" => "INTERNAL",
        "14" => "UNAVAILABLE",
        "15" => "DATA_LOSS",
        "16" => "UNAUTHENTICATED",
        _ => "UNKNOWN_GRPC_STATUS",
    }
}

impl<B> Body for RpcLogBody<B>
where
    B: Body<Data = Bytes>,
{
    type Data = Bytes;
    type Error = B::Error;
    fn is_end_stream(&self) -> bool {
        self.inner.as_ref().get_ref().is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.as_ref().get_ref().size_hint()
    }
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.as_mut().get_mut();
        match this.inner.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if frame.is_trailers() {
                    let trailers = frame.into_trailers().expect("frame is trailers");
                    this.grpc_status = trailers
                        .get("grpc-status")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned);
                    Poll::Ready(Some(Ok(Frame::trailers(trailers))))
                } else {
                    Poll::Ready(Some(Ok(frame)))
                }
            }
            Poll::Ready(None) => {
                if !this.logged {
                    let status = this
                        .grpc_status
                        .as_deref()
                        .map(grpc_status_name)
                        .map(str::to_owned)
                        .unwrap_or_else(|| format!("http_{}", this.http_status));
                    tracing::info!(operation=%this.operation, request_id=%this.request_id, duration_ms=this.started.elapsed().as_millis() as u64, status=%status, "RPC completed");
                    this.logged = true;
                }
                Poll::Ready(None)
            }
            other => other,
        }
    }
}
