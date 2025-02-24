use actix_service::{Service, Transform};
use actix_web::{
    dev::{ServiceRequest, ServiceResponse},
    Error,
};
use futures::future::{ok, Ready, LocalBoxFuture};
use std::task::{Context, Poll};
use std::rc::Rc;
use std::time::Instant;
use crate::metrics::{HTTP_REQUEST_COUNTER, HTTP_REQUEST_DURATION};

pub struct MetricsMiddleware;

impl<S, B> Transform<S, ServiceRequest> for MetricsMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = MetricsMiddlewareService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(MetricsMiddlewareService {
            service: Rc::new(service),
        })
    }
}

pub struct MetricsMiddlewareService<S> {
    service: Rc<S>,
}

impl<S, B> Service<ServiceRequest> for MetricsMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }
    
    fn call(&self, req: ServiceRequest) -> Self::Future {
        let start = Instant::now();
        let endpoint = req.path().to_string();
        let method = req.method().to_string();
        HTTP_REQUEST_COUNTER.with_label_values(&[&endpoint, &method]).inc();
        let fut = self.service.call(req);
        Box::pin(async move {
            let res = fut.await?;
            let duration = start.elapsed().as_secs_f64();
            HTTP_REQUEST_DURATION.with_label_values(&[&endpoint, &method]).observe(duration);
            Ok(res)
        })
    }
}
