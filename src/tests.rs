//! Request-path tests that need no running MongoDB: the database handle points at a closed port,
//! so anything that reaches storage must come back as UNAVAILABLE (which the iOS SDK retries),
//! while auth, validation, routing and throttling are decided before storage is touched.

use prost::Message;
use tonic::Code;

use crate::config::Config;
use crate::db::Db;
use crate::dm::{AppState, RequestMeta};
use crate::pb_dm::{
    ClientContext, CrashKind, CrashReport, FetchConfigRequest, UpdateTopicSubscriptionRequest, UploadCrashReportsRequest,
};
use crate::proxy::dispatch;

const DEAD_MONGO: &str = "mongodb://127.0.0.1:9/?serverSelectionTimeoutMS=200&connectTimeoutMS=200";

async fn state(fetch_limit: u32) -> AppState {
    let mut config = Config::from_env();
    config.api_keys = vec![(Some("dmart".into()), "key-dmart".into()), (None, "key-any".into())];
    config.rc_fetches_per_hour = fetch_limit;
    config.apns = None;
    AppState::new(Db::lazy(DEAD_MONGO).await, config).expect("state")
}

fn meta(key: Option<&str>) -> RequestMeta {
    RequestMeta { api_key: key.map(str::to_string), ..Default::default() }
}

fn context(app: &str) -> Option<ClientContext> {
    Some(ClientContext { app_id: app.into(), installation_id: "install-1".into(), platform: "ios".into(), ..Default::default() })
}

fn fetch(app: &str) -> Vec<u8> {
    FetchConfigRequest { context: context(app), ..Default::default() }.encode_to_vec()
}

const FETCH: &str = "/dm.sdk.v1.RemoteConfigService/FetchConfig";

#[tokio::test]
async fn rejects_missing_and_wrong_api_keys() {
    let s = state(100).await;
    let err = dispatch(&s, &meta(None), FETCH, &fetch("dmart")).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated);
    let err = dispatch(&s, &meta(Some("key-dmart")), FETCH, &fetch("other-app")).await.unwrap_err();
    assert_eq!(err.code(), Code::Unauthenticated, "app-scoped key must not work for another app");
}

#[tokio::test]
async fn storage_outage_is_retryable_unavailable() {
    let s = state(100).await;
    // Global key works for any app; the call then needs MongoDB, which is down.
    let err = dispatch(&s, &meta(Some("key-any")), FETCH, &fetch("dmart")).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);

    let upload = UploadCrashReportsRequest {
        context: context("dmart"),
        reports: vec![CrashReport { id: "r1".into(), kind: CrashKind::Signal as i32, is_fatal: true, ..Default::default() }],
    };
    let err = dispatch(&s, &meta(Some("key-dmart")), "/dm.sdk.v1.CrashlyticsService/UploadCrashReports", &upload.encode_to_vec())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
}

#[tokio::test]
async fn validation_happens_before_storage() {
    let s = state(100).await;
    let key = meta(Some("key-dmart"));

    let no_context = FetchConfigRequest::default().encode_to_vec();
    assert_eq!(dispatch(&s, &key, FETCH, &no_context).await.unwrap_err().code(), Code::InvalidArgument);

    let garbage = [0xff, 0xff, 0xff];
    assert_eq!(dispatch(&s, &key, FETCH, &garbage).await.unwrap_err().code(), Code::InvalidArgument);

    let bad_topic = UpdateTopicSubscriptionRequest {
        context: context("dmart"),
        registration_token: "t".into(),
        topic: "has space".into(),
        subscribe: true,
    };
    let err = dispatch(&s, &key, "/dm.sdk.v1.MessagingService/UpdateTopicSubscription", &bad_topic.encode_to_vec())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    assert_eq!(dispatch(&s, &key, "/dm.sdk.v1.Nope/Nope", &[]).await.unwrap_err().code(), Code::Unimplemented);
}

#[tokio::test]
async fn remote_config_throttling_sends_retry_after() {
    let s = state(1).await;
    let key = meta(Some("key-dmart"));
    let first = dispatch(&s, &key, FETCH, &fetch("dmart")).await.unwrap_err();
    assert_eq!(first.code(), Code::Unavailable); // admitted, then storage is down
    let second = dispatch(&s, &key, FETCH, &fetch("dmart")).await.unwrap_err();
    assert_eq!(second.code(), Code::ResourceExhausted);
    let retry: u64 = second.metadata().get("retry-after-seconds").unwrap().to_str().unwrap().parse().unwrap();
    assert!(retry > 3000 && retry <= 3600, "retry-after {retry}");
}

#[tokio::test]
async fn grpc_web_response_carries_status_trailer() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let s = state(100).await;
    let body = fetch("dmart");
    let mut framed = vec![0u8];
    framed.extend_from_slice(&(body.len() as u32).to_be_bytes());
    framed.extend_from_slice(&body);

    let response = crate::proxy::router(s)
        .oneshot(
            Request::post(FETCH)
                .header("content-type", "application/grpc-web+proto")
                .header("origin", "https://example.com")
                .body(Body::from(framed))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
    assert_eq!(bytes[0], 0x80, "trailers-only response");
    let trailers = String::from_utf8_lossy(&bytes[5..]);
    assert!(trailers.starts_with("grpc-status:16\r\n"), "{trailers}"); // UNAUTHENTICATED (no key)
}

#[tokio::test]
async fn admin_api_requires_key() {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    let mut s_config = Config::from_env();
    s_config.admin_key = Some("admin-secret".into());
    let s = AppState::new(Db::lazy(DEAD_MONGO).await, s_config).unwrap();
    let app = crate::proxy::router(s);

    let unauthorized = app
        .clone()
        .oneshot(Request::get("/admin/crashlytics/dmart/issues").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), 401);

    let bad_template = app
        .oneshot(
            Request::put("/admin/remote-config/dmart")
                .header("x-admin-key", "admin-secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"parameters":{"x":{"conditional_values":[{"condition":"missing","value":1}]}}}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad_template.status(), 400, "templates are validated before storage");
}

#[test]
fn issue_grouping_ignores_exception_reason_and_prefers_in_app_frames() {
    use crate::dm::crashlytics::{issue_id, signature};
    use crate::dm::symbols::SymFrame;

    let frames = vec![
        SymFrame { index: 0, address: "0x1".into(), image: Some("libsystem_kernel.dylib".into()), image_offset: Some("0x10".into()), ..Default::default() },
        SymFrame { index: 1, address: "0x2".into(), image: Some("DMart".into()), image_offset: Some("0x200".into()), in_app: true, symbol: Some("CartViewModel.checkout()".into()), ..Default::default() },
    ];
    let a = CrashReport { exception_name: "NSRangeException".into(), exception_reason: "index 5 beyond bounds [0 .. 3]".into(), ..Default::default() };
    let b = CrashReport { exception_name: "NSRangeException".into(), exception_reason: "index 9 beyond bounds [0 .. 1]".into(), ..Default::default() };
    let sa = signature(&a, CrashKind::Exception, &frames, None);
    let sb = signature(&b, CrashKind::Exception, &frames, None);
    assert_eq!(sa.signature, sb.signature);
    assert!(sa.signature.contains("CartViewModel.checkout()"));
    assert!(!sa.signature.contains("libsystem_kernel"));
    assert_ne!(issue_id("dmart", &sa.signature), issue_id("other", &sa.signature));
}

#[test]
fn metric_kit_crash_signature_uses_attributed_thread() {
    use crate::dm::crashlytics::signature;
    let payload = serde_json::json!({
        "crashDiagnostics": [{
            "diagnosticMetaData": {"signal": 11, "exceptionType": 1},
            "callStackTree": {"callStacks": [
                {"threadAttributed": false, "callStackRootFrames": [{"binaryName": "Other", "offsetIntoBinaryTextSegment": 1}]},
                {"threadAttributed": true, "callStackRootFrames": [{"binaryName": "DMart", "offsetIntoBinaryTextSegment": 4096}]}
            ]}
        }]
    });
    let s = signature(&CrashReport::default(), CrashKind::MetricKitDiagnostic, &[], Some(&payload));
    assert_eq!(s.subtitle, "DMart+0x1000");
    assert!(s.title.starts_with("MetricKit crash"));
}

#[test]
fn stored_stack_round_trips_hex_addresses() {
    use mongodb::bson::doc;
    let report = doc! {
        "kind": "signal",
        "return_addresses": ["0x104a8c123", "0x104a8d000"],
        "binary_images": [{"name": "DMart", "uuid": "A", "load_address": "0x104a80000", "size": 65536_i64, "is_main_executable": true}],
    };
    let (images, addresses, first_is_pc) = crate::dm::crashlytics::stored_stack(&report);
    assert_eq!(addresses, vec![0x104a8c123, 0x104a8d000]);
    assert_eq!(images[0].load_address, 0x104a80000);
    assert!(first_is_pc);
}

#[test]
fn uuid_formatting_matches_dwarfdump() {
    let bytes = [0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef];
    assert_eq!(crate::dm::symbols::format_uuid(&bytes), "12345678-9ABC-DEF0-0123-456789ABCDEF");
}

/// Same reference bytes as the iOS package's DMSDKTests (produced by Google's protobuf runtime),
/// so server and app agree on the wire format byte-for-byte.
#[test]
fn wire_format_matches_ios_reference_bytes() {
    use crate::pb_dm::{Experiment, FetchConfigResponse, FetchState};
    let response = FetchConfigResponse {
        state: FetchState::Updated as i32,
        entries: [("show_banner".to_string(), "true".to_string())].into(),
        template_version: 7,
        experiments: vec![Experiment { experiment_id: "exp1".into(), variant_id: "B".into() }],
    };
    assert_eq!(hex::encode(response.encode_to_vec()), "080112130a0b73686f775f62616e6e6572120474727565180722090a0465787031120142");

    let report = CrashReport::decode(hex::decode("0a0272311001180198010bba010480208040").unwrap().as_slice()).unwrap();
    assert_eq!(report.id, "r1");
    assert_eq!(report.kind, CrashKind::Signal as i32);
    assert!(report.is_fatal);
    assert_eq!(report.signal_number, 11);
    assert_eq!(report.return_addresses, vec![4096, 8192]);
}
