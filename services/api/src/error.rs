use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub(crate) struct ApiError {
    pub(crate) code: String,
    pub(crate) message: String,
}

impl ApiError {
    pub(crate) fn status(&self) -> StatusCode {
        match self.code.as_str() {
            "unauthorized" => StatusCode::UNAUTHORIZED,
            "forbidden" => StatusCode::FORBIDDEN,
            "not_found" => StatusCode::NOT_FOUND,
            "conflict" => StatusCode::CONFLICT,
            "rate_limited" => StatusCode::TOO_MANY_REQUESTS,
            "service_overloaded"
            | "ticketing_unavailable"
            | "upstream_unavailable"
            | "payment_provider_unavailable" => StatusCode::SERVICE_UNAVAILABLE,
            "request_timeout" | "upstream_timeout" => StatusCode::GATEWAY_TIMEOUT,
            "payment_not_settled" | "payment_verification_failed" => StatusCode::PAYMENT_REQUIRED,
            "internal_error"
            | "ticketing_configuration_error"
            | "upstream_authentication_error" => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let rate_limited = self.code == "rate_limited";
        let mut response = (self.status(), Json(self)).into_response();
        if rate_limited {
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                axum::http::HeaderValue::from_static("60"),
            );
        }
        response
    }
}
