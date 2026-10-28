//! Crash alerts: always logged; also POSTed as {"text": …} to DM_ALERT_WEBHOOK_URL when set
//! (works with Slack incoming webhooks, Google Chat, Microsoft Teams workflows, and most bots).

use serde_json::json;

use super::AppState;

pub fn notify(state: &AppState, text: String) {
    println!("[Alert] {text}");
    let Some(url) = state.config.alert_webhook_url.clone() else { return };
    let http = state.http.clone();
    tokio::spawn(async move {
        if let Err(err) = http.post(&url).json(&json!({ "text": text })).send().await {
            eprintln!("[Alert] webhook failed: {err}");
        }
    });
}
