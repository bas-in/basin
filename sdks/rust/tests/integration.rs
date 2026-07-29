//! Integration tests against a mock HTTP server (`wiremock`).
//!
//! These exercise URL/body construction, response normalisation, error
//! decoding, and the auto-refresh path end-to-end through `reqwest`.

use basin::{BasinError, Client};
use serde_json::json;
use wiremock::matchers::{body_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn server() -> MockServer {
    MockServer::start().await
}

fn client(url: &str) -> Client {
    Client::builder(url)
        .token("test-key")
        .project_id("01PROJ")
        .build()
        .unwrap()
}

#[tokio::test]
async fn health_returns_body() {
    let s = server().await;
    Mock::given(method("GET"))
        .and(path("/health"))
        .respond_with(ResponseTemplate::new(200).set_body_string("ok"))
        .mount(&s)
        .await;
    assert_eq!(client(&s.uri()).health().await.unwrap(), "ok");
}

#[tokio::test]
async fn query_get_bare_array() {
    let s = server().await;
    Mock::given(method("GET"))
        .and(path("/rest/v1/orders"))
        .and(query_param("select", "id,total"))
        .and(query_param("status", "eq.paid"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"id":1,"total":10}])))
        .mount(&s)
        .await;
    let result = client(&s.uri())
        .table("orders")
        .select("id,total")
        .eq("status", "paid")
        .run()
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(result.next_cursor.is_none());
}

#[tokio::test]
async fn query_get_paged_shape_and_typed() {
    let s = server().await;
    Mock::given(method("GET"))
        .and(path("/rest/v1/orders"))
        .and(query_param("limit", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "rows": [{"id": 1}, {"id": 2}],
            "next_cursor": "abc"
        })))
        .mount(&s)
        .await;

    #[derive(serde::Deserialize)]
    struct Row {
        id: i64,
    }
    let result = client(&s.uri()).table("orders").limit(2).run().await.unwrap();
    assert_eq!(result.next_cursor.as_deref(), Some("abc"));
    let typed: Vec<Row> = result.into_typed().unwrap();
    assert_eq!(typed[1].id, 2);
}

#[tokio::test]
async fn insert_sends_post_body() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/rest/v1/orders"))
        .and(body_json(json!({"total": 5})))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"ok": true, "tag": "INSERT 0 1"})))
        .mount(&s)
        .await;
    let res = client(&s.uri())
        .table("orders")
        .insert(&json!({"total": 5}))
        .await
        .unwrap();
    assert_eq!(res["ok"], true);
}

#[tokio::test]
async fn update_sends_patch_with_filters() {
    let s = server().await;
    Mock::given(method("PATCH"))
        .and(path("/rest/v1/orders"))
        .and(query_param("id", "eq.1"))
        .and(body_json(json!({"status": "paid"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true, "tag": "UPDATE 1"})))
        .mount(&s)
        .await;
    let res = client(&s.uri())
        .table("orders")
        .eq("id", 1i64)
        .update(&json!({"status": "paid"}))
        .await
        .unwrap();
    assert_eq!(res["ok"], true);
}

#[tokio::test]
async fn delete_unsupported_surfaces_typed_error() {
    let s = server().await;
    Mock::given(method("DELETE"))
        .and(path("/rest/v1/orders"))
        .respond_with(ResponseTemplate::new(501).set_body_json(json!({
            "code": "E_ENGINE_UNSUPPORTED",
            "message": "delete not supported"
        })))
        .mount(&s)
        .await;
    let err = client(&s.uri()).table("orders").delete().await.unwrap_err();
    match err {
        BasinError::Api(e) => {
            assert_eq!(e.code, "E_ENGINE_UNSUPPORTED");
            assert_eq!(e.status, 501);
        }
        other => panic!("expected Api error, got {other:?}"),
    }
}

#[tokio::test]
async fn error_envelope_with_sqlstate() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/rest/v1/orders"))
        .respond_with(ResponseTemplate::new(409).set_body_json(json!({
            "code": "E_INVALID_REQUEST",
            "message": "duplicate key",
            "sqlstate": "23505"
        })))
        .mount(&s)
        .await;
    let err = client(&s.uri())
        .table("orders")
        .insert(&json!({"id": 1}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("E_INVALID_REQUEST"));
    assert_eq!(err.sqlstate(), Some("23505"));
}

#[tokio::test]
async fn sign_in_stores_session_and_uses_token() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/auth/v1/signin"))
        .and(body_json(json!({
            "project_id": "01PROJ",
            "email": "a@b.com",
            "password": "pw"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "atk",
            "refresh_token": "rtk",
            "access_expires_at": "2999-01-01T00:00:00Z",
            "refresh_expires_at": "2999-01-01T00:00:00Z"
        })))
        .mount(&s)
        .await;
    // After sign-in, a query must present the session access token, not the key.
    Mock::given(method("GET"))
        .and(path("/rest/v1/t"))
        .and(header("authorization", "Bearer atk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(&s)
        .await;

    let c = client(&s.uri());
    let session = c.auth().sign_in("a@b.com", "pw", None).await.unwrap();
    assert_eq!(session.access_token, "atk");
    c.table("t").select("*").run().await.unwrap();
}

#[tokio::test]
async fn expired_session_auto_refreshes_once() {
    let s = server().await;
    // Sign in with an already-expired access token.
    Mock::given(method("POST"))
        .and(path("/auth/v1/signin"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "old",
            "refresh_token": "rtk",
            "access_expires_at": "2000-01-01T00:00:00Z",
            "refresh_expires_at": "2999-01-01T00:00:00Z"
        })))
        .mount(&s)
        .await;
    // Refresh returns a fresh, far-future token. Expect exactly one refresh
    // even under a burst of concurrent requests.
    Mock::given(method("POST"))
        .and(path("/auth/v1/refresh"))
        .and(body_json(json!({"refresh_token": "rtk"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "fresh",
            "refresh_token": "rtk2",
            "access_expires_at": "2999-01-01T00:00:00Z",
            "refresh_expires_at": "2999-01-01T00:00:00Z"
        })))
        .expect(1)
        .mount(&s)
        .await;
    Mock::given(method("GET"))
        .and(path("/rest/v1/t"))
        .and(header("authorization", "Bearer fresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(&s)
        .await;

    let c = client(&s.uri());
    c.auth().sign_in("a@b.com", "pw", None).await.unwrap();

    // Fire several concurrent requests; they should share a single refresh.
    let mut handles = Vec::new();
    for _ in 0..5 {
        let cc = c.clone();
        handles.push(tokio::spawn(async move {
            cc.table("t").select("*").run().await
        }));
    }
    for h in handles {
        h.await.unwrap().unwrap();
    }
    // wiremock verifies expect(1) on the refresh mock at drop.
}

#[tokio::test]
async fn sign_out_tolerates_404() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/auth/v1/signin"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "atk",
            "refresh_token": "rtk",
            "access_expires_at": "2999-01-01T00:00:00Z",
            "refresh_expires_at": "2999-01-01T00:00:00Z"
        })))
        .mount(&s)
        .await;
    Mock::given(method("POST"))
        .and(path("/auth/v1/signout"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "code": "E_NOT_FOUND", "message": "no route"
        })))
        .mount(&s)
        .await;

    let c = client(&s.uri());
    c.auth().sign_in("a@b.com", "pw", None).await.unwrap();
    // 404 is tolerated; local session is cleared regardless.
    c.auth().sign_out().await.unwrap();
    assert!(c.auth().get_session().await.is_none());
}

#[tokio::test]
async fn rpc_posts_named_args() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/rest/v1/rpc/sum_orders"))
        .and(body_json(json!({"since": "2024-01-01"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!(42)))
        .mount(&s)
        .await;
    let v = client(&s.uri())
        .rpc("sum_orders", Some(&json!({"since": "2024-01-01"})))
        .await
        .unwrap();
    assert_eq!(v, json!(42));
}

#[tokio::test]
async fn invoke_function_proxies_response() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/fn/v1/thumb"))
        .respond_with(
            ResponseTemplate::new(202)
                .insert_header("content-type", "application/json")
                .set_body_json(json!({"queued": true})),
        )
        .mount(&s)
        .await;
    let res = client(&s.uri())
        .functions()
        .invoke(
            "thumb",
            reqwest::Method::POST,
            Some(basin::InvokeBody::Json(json!({"w": 64}))),
            &[],
        )
        .await
        .unwrap();
    assert_eq!(res.status, 202);
    assert_eq!(res.data["queued"], true);
}

#[tokio::test]
async fn storage_bucket_and_object_roundtrip() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/storage/v1/bucket"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "id": "b1", "name": "media", "public": true,
            "file_size_limit": null, "allowed_mime_types": [],
            "created_at": "t", "updated_at": "t"
        })))
        .mount(&s)
        .await;
    Mock::given(method("POST"))
        .and(path("/storage/v1/object/media/a/b.png"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "id": "o1", "bucket_id": "b1", "path": "a/b.png", "size": 3,
            "mime_type": "image/png", "metadata": null, "owner": null,
            "etag": "e", "created_at": "t", "updated_at": "t"
        })))
        .mount(&s)
        .await;
    Mock::given(method("GET"))
        .and(path("/storage/v1/object/media/a/b.png"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "image/png")
                .insert_header("etag", "e")
                .set_body_bytes(vec![1u8, 2, 3]),
        )
        .mount(&s)
        .await;

    let c = client(&s.uri());
    let bucket = c.storage().create_bucket("media", true, None, &[]).await.unwrap();
    assert_eq!(bucket.name, "media");

    let media = c.storage().from_bucket("media");
    let obj = media.upload("a/b.png", vec![1, 2, 3], Some("image/png")).await.unwrap();
    assert_eq!(obj.size, 3);

    let dl = media.download("a/b.png").await.unwrap();
    assert_eq!(dl.data, vec![1, 2, 3]);
    assert_eq!(dl.content_type.as_deref(), Some("image/png"));
    assert_eq!(dl.etag.as_deref(), Some("e"));
}

#[tokio::test]
async fn signed_url_builds_absolute() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/storage/v1/object/sign/upload/media/a.png"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "signedUrl": "/storage/v1/object/sign/01PROJ/media/a.png?token=xyz",
            "expiresAt": "2999-01-01T00:00:00Z"
        })))
        .mount(&s)
        .await;
    let url = client(&s.uri())
        .storage()
        .from_bucket("media")
        .create_signed_url("a.png", 3600)
        .await
        .unwrap();
    assert!(url.absolute_url.starts_with(&s.uri()));
    assert!(url.absolute_url.contains("token=xyz"));
}

#[tokio::test]
async fn create_api_key_jwt_gated() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/auth/v1/api-keys"))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "id": 1, "name": "ci", "secret": "sek", "created_at": "t"
        })))
        .mount(&s)
        .await;
    let issued = client(&s.uri()).auth().create_api_key("ci").await.unwrap();
    assert_eq!(issued.secret, "sek");
    assert_eq!(issued.id, 1);
}

#[tokio::test]
async fn oauth_authorize_url() {
    let s = server().await;
    Mock::given(method("GET"))
        .and(path("/auth/v1/oauth/github/authorize"))
        .and(query_param("project_id", "01PROJ"))
        .and(query_param("redirect_to", "/done"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "redirect_url": "https://github.com/login/oauth/authorize?x=1",
            "state": "csrf123"
        })))
        .mount(&s)
        .await;
    let r = client(&s.uri())
        .auth()
        .get_oauth_authorize_url("github", "/done", None)
        .await
        .unwrap();
    assert_eq!(r.state, "csrf123");
    assert!(r.redirect_url.contains("github.com"));
}

#[tokio::test]
async fn enroll_totp_factor_untagged() {
    let s = server().await;
    Mock::given(method("POST"))
        .and(path("/auth/v1/factors"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "factor_id": "f1", "factor_type": "totp",
            "secret_b32": "ABCDEF", "otpauth_uri": "otpauth://totp/x"
        })))
        .mount(&s)
        .await;
    let r = client(&s.uri())
        .auth()
        .enroll_factor("totp", "Authenticator")
        .await
        .unwrap();
    match r {
        basin::types::MfaEnrollResult::Totp(t) => assert_eq!(t.secret_b32, "ABCDEF"),
        _ => panic!("expected TOTP"),
    }
}

#[tokio::test]
async fn missing_project_id_is_client_side_error() {
    // Build with a raw key (no JWT) and no project_id; signup must fail before
    // any network round-trip.
    let c = Client::builder("http://127.0.0.1:1").token("rawkey").build().unwrap();
    let err = c.auth().sign_up("a@b.com", "pw", None).await.unwrap_err();
    assert!(matches!(err, BasinError::InvalidRequest(_)));
}
