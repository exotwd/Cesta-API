use crate::*;
use axum::{body::{Body,to_bytes},http::Request};
use tower::ServiceExt;

async fn payload(response:Response)->Value { serde_json::from_slice(&to_bytes(response.into_body(),2*1024*1024).await.unwrap()).unwrap() }
fn get(path:&str)->Request<Body>{ Request::builder().uri(path).body(Body::empty()).unwrap() }

#[tokio::test]
async fn invalid_expensive_inputs_are_rejected_before_work_starts(){
    let app=build_router(app_state().await.unwrap());
    for path in ["/departures?stopId=x&limit=999999","/stops/nearby?lat=91&lon=14","/stops/nearby?lat=50&lon=14&radius=999999","/stops/search?q=a&limit=10000"] {
        let response=app.clone().oneshot(get(path)).await.unwrap();assert_eq!(response.status(),StatusCode::BAD_REQUEST);assert_eq!(payload(response).await["code"],"validation_error");
    }
    let request=Request::builder().method("POST").uri("/journeys/search").header("content-type","application/json").body(Body::from(json!({"from":{"type":"coordinate","lat":50.0,"lon":14.0},"to":{"type":"stop","id":"stop-brno-hl-n"},"datetime":"2026-10-05T10:00:00+02:00","mode":"depart_at","transport_modes":["train"],"max_transfers":999999,"walking_speed":"normal","prefer_reliable_transfers":true,"offline_compatible":false}).to_string())).unwrap();
    assert_eq!(app.oneshot(request).await.unwrap().status(),StatusCode::BAD_REQUEST);
    let error=internal_error("password=secret SELECT private_table");assert_eq!(error.message,"An internal service error occurred");
}
#[tokio::test]
async fn rate_limits_are_observed_and_health_survives_saturation(){
    let mut state=app_state().await.unwrap();let mut config=(*state.config).clone();config.public_requests_per_minute=1;state.config=Arc::new(config);
    let app=build_router(state.clone());assert_eq!(app.clone().oneshot(get("/stops/search?q=a")).await.unwrap().status(),StatusCode::OK);
    let limited=app.clone().oneshot(get("/stops/search?q=b")).await.unwrap();assert_eq!(limited.status(),StatusCode::TOO_MANY_REQUESTS);assert!(limited.headers().contains_key(header::RETRY_AFTER));
    assert_eq!(app.oneshot(get("/health")).await.unwrap().status(),StatusCode::OK);
    assert!(state.telemetry.snapshot()["rows"].as_array().unwrap().iter().any(|row|row["status"]==429));
}
#[tokio::test]
async fn protected_placeholders_and_observability_require_authentication(){
    let app=build_router(app_state().await.unwrap());
    for path in ["/admin/analytics","/admin/metrics","/admin/operations","/me/favorite-routes","/me/notification-preferences"] {assert_eq!(app.clone().oneshot(get(path)).await.unwrap().status(),StatusCode::UNAUTHORIZED);}
    let response=app.oneshot(Request::builder().method("PATCH").uri(format!("/me/saved-places/{}",Uuid::new_v4())).body(Body::empty()).unwrap()).await.unwrap();assert_eq!(response.status(),StatusCode::UNAUTHORIZED);
}
#[tokio::test]
async fn normal_users_cannot_read_analytics_and_other_accounts_places(){
    let state=app_state().await.unwrap();
    let a=create_user_record("owner-a@example.invalid","password-a",None,vec!["user".into()]).unwrap();
    let b=create_user_record("owner-b@example.invalid","password-b",None,vec!["user".into()]).unwrap();
    state.users.write().await.insert(a.id,a.clone());state.users.write().await.insert(b.id,b.clone());
    let token_a=auth_response(&state,&a).await.unwrap().access_token;let token_b=auth_response(&state,&b).await.unwrap().access_token;
    let app=build_router(state.clone());
    let create=Request::builder().method("POST").uri("/me/saved-places").header("authorization",format!("Bearer {token_a}")).header("content-type","application/json").body(Body::from(json!({"name":"Home","place_type":"stop","stop_id":"stop-praha-hl-n"}).to_string())).unwrap();
    let place=payload(app.clone().oneshot(create).await.unwrap()).await;
    let remove=Request::builder().method("DELETE").uri(format!("/me/saved-places/{}",place["id"].as_str().unwrap())).header("authorization",format!("Bearer {token_b}")).body(Body::empty()).unwrap();
    assert_eq!(app.clone().oneshot(remove).await.unwrap().status(),StatusCode::OK);
    assert_eq!(state.saved_places.read().await[&a.id].len(),1);
    let list=Request::builder().uri("/me/saved-places").header("authorization",format!("Bearer {token_b}")).body(Body::empty()).unwrap();assert_eq!(payload(app.clone().oneshot(list).await.unwrap()).await["saved_places"],json!([]));
    for path in ["/admin/analytics","/admin/metrics","/admin/operations"] { let req=Request::builder().uri(path).header("authorization",format!("Bearer {token_a}")).body(Body::empty()).unwrap();assert_eq!(app.clone().oneshot(req).await.unwrap().status(),StatusCode::FORBIDDEN); }
}
#[tokio::test]
async fn refresh_rotation_is_atomic_under_concurrency(){
    let state=app_state().await.unwrap();let user=create_user_record("refresh-race@example.invalid","password",None,vec!["user".into()]).unwrap();state.users.write().await.insert(user.id,user.clone());
    let token=auth_response(&state,&user).await.unwrap().refresh_token;let app=build_router(state);
    let request=||Request::builder().method("POST").uri("/auth/refresh").header("content-type","application/json").body(Body::from(json!({"refresh_token":token}).to_string())).unwrap();
    let (a,b)=tokio::join!(app.clone().oneshot(request()),app.oneshot(request()));let statuses=[a.unwrap().status(),b.unwrap().status()];assert!(statuses.contains(&StatusCode::OK));assert!(statuses.contains(&StatusCode::UNAUTHORIZED));
}
#[tokio::test]
async fn read_deadlines_and_capacity_have_distinct_errors(){
    let mut state=app_state().await.unwrap();let mut config=(*state.config).clone();config.read_request_timeout=std::time::Duration::from_millis(5);state.config=Arc::new(config);
    let slow=Router::new().route("/stops/slow",axum::routing::get(||async{tokio::time::sleep(std::time::Duration::from_secs(1)).await;StatusCode::OK})).layer(axum::middleware::from_fn_with_state(state.clone(),http::security::protect));
    let response=slow.oneshot(get("/stops/slow")).await.unwrap();assert_eq!(response.status(),StatusCode::GATEWAY_TIMEOUT);assert_eq!(payload(response).await["code"],"request_timeout");
    state.security=http::security::Security::new(1,1);
    let app=build_router(state);let response=app.oneshot(Request::builder().uri("/health").header("x-request-id","password=secret").body(Body::empty()).unwrap()).await.unwrap();
    assert!(Uuid::parse_str(response.headers()["x-request-id"].to_str().unwrap()).is_ok());
}

pub(crate) async fn isolated_database()->(PgPool,PgPool,String){
    let url=env::var("CESTA_TEST_DATABASE_URL").expect("Use the disposable test database documented in docs/api-operations.md");
    let admin=PgPool::connect(&url).await.unwrap();let schema=format!("api_test_{}",Uuid::new_v4().simple());sqlx::query(&format!("CREATE SCHEMA {schema}")).execute(&admin).await.unwrap();
    let set_path=format!("SET search_path TO {schema},public");
    let pool=sqlx::postgres::PgPoolOptions::new().max_connections(4).after_connect(move |connection,_|{let sql=set_path.clone();Box::pin(async move{sqlx::query(&sql).execute(connection).await?;Ok(())})}).connect(&url).await.unwrap();
    (pool,admin,schema)
}
