//! Registry validation tests. Cheap, no HTTP server needed.

use basin_common::{TableName, ProjectId};
use basin_webhooks::{
    SubscriptionError, WebhookOps, WebhookRegistry, WebhookSubscription, WebhookSubscriptionId,
};

fn template(project: ProjectId) -> WebhookSubscription {
    WebhookSubscription {
        id: WebhookSubscriptionId::new(),
        project,
        table: TableName::new("orders").unwrap(),
        url: "https://example.com/hook".into(),
        ops: WebhookOps::INSERT,
        when_predicate: None,
        max_retries: 4,
        paused: false,
    }
}

#[tokio::test]
async fn add_rejects_predicate() {
    let r = WebhookRegistry::new();
    let mut s = template(ProjectId::new());
    s.when_predicate = Some("NEW.status = 'paid'".into());
    let err = r.add(s).await.unwrap_err();
    assert!(matches!(err, SubscriptionError::PredicateNotSupported));
}

#[tokio::test]
async fn add_rejects_empty_ops() {
    let r = WebhookRegistry::new();
    let mut s = template(ProjectId::new());
    s.ops = WebhookOps::empty();
    let err = r.add(s).await.unwrap_err();
    assert!(matches!(err, SubscriptionError::NoOps));
}

#[tokio::test]
async fn add_rejects_zero_retries() {
    let r = WebhookRegistry::new();
    let mut s = template(ProjectId::new());
    s.max_retries = 0;
    let err = r.add(s).await.unwrap_err();
    assert!(matches!(err, SubscriptionError::InvalidMaxRetries));
}

#[tokio::test]
async fn add_rejects_garbage_url() {
    let r = WebhookRegistry::new();
    let mut s = template(ProjectId::new());
    s.url = "not a url".into();
    let err = r.add(s).await.unwrap_err();
    assert!(matches!(err, SubscriptionError::InvalidUrl(_)));
}

#[tokio::test]
async fn list_filters_by_project() {
    let r = WebhookRegistry::new();
    let a = ProjectId::new();
    let b = ProjectId::new();
    r.add(template(a)).await.unwrap();
    r.add(template(a)).await.unwrap();
    r.add(template(b)).await.unwrap();
    assert_eq!(r.list(&a).await.len(), 2);
    assert_eq!(r.list(&b).await.len(), 1);
}

#[tokio::test]
async fn pause_and_resume_round_trip() {
    let r = WebhookRegistry::new();
    let id = r.add(template(ProjectId::new())).await.unwrap();
    assert!(!r.get(id).await.unwrap().paused);
    assert!(r.pause(id).await);
    assert!(r.get(id).await.unwrap().paused);
    assert!(r.resume(id).await);
    assert!(!r.get(id).await.unwrap().paused);
}
