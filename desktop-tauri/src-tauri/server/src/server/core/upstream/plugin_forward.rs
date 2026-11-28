use std::sync::Arc;

use axum::http::HeaderMap;
use serde_json::Value;

use crate::server::core::account_store::AccountStore;
use crate::server::core::proxies::ResolvedProxy;
use crate::server::core::providers::adapter::{ProviderAdapter, RetryDecision};
use crate::server::errors::GatewayError;

use super::{error_policy, ForwardOutcome, usage::RequestTelemetry};

pub(super) async fn forward(
    adapter: &dyn ProviderAdapter,
    store: &AccountStore,
    account: &str,
    body: &Value,
    headers: &HeaderMap,
    proxy: Option<ResolvedProxy>,
    stream: bool,
    telemetry: &Arc<RequestTelemetry>,
    budget: &mut super::provider_loop::RetryBudget,
) -> Result<ForwardOutcome, GatewayError> {
    let mut refreshed = false;
    let mut last_error = None;
    let model = body["model"].as_str().unwrap_or("");
    loop {
        if telemetry.replay_guard().blocked() { return Err(last_error.unwrap_or_else(super::replay_guard::blocked_error)); }
        let deadline_ms = telemetry
            .attempt_deadline_ms()
            .filter(|_| !adapter.is_stateful() && !telemetry.response_started());
        let call = adapter.forward_conversation(store, account, body, headers, proxy.clone(), stream, telemetry);
        tokio::pin!(call);
        let result = if let Some(ms) = deadline_ms {
            let mut timer = std::pin::pin!(tokio::time::sleep(std::time::Duration::from_millis(ms)));
            tokio::select! {
                biased;
                res = &mut call => res,
                _ = &mut timer => {
                    if !telemetry.replay_guard().blocked() && !telemetry.response_started() {
                        return Err(GatewayError::with_status(
                            504,
                            format!("上游首包响应超过救援时限({ms}ms)，自动切换备选账号"),
                        )
                        .with_code("ttft_deadline_exceeded"));
                    }
                    call.await
                }
            }
        } else {
            call.await
        };
        let Err(mut error) = result else { return result; };
        if !telemetry.response_started() && is_explicit_pre_exec_rejection(&error) {
            telemetry.replay_guard().unfreeze_on_explicit_rejection();
        }
        if telemetry.replay_guard().blocked() || telemetry.response_started() { return Err(error); }
        if telemetry.is_cancelled() { return Err(super::cancellation::cancelled_error()); }
        let status = u16::try_from(error.status_code).unwrap_or(502);
        let class = adapter.classify_conversation_error(&error);
        let detail = error_policy::detail(&error);
        let policy = adapter.error_policy_async(status, &detail).await;
        let stop = error_policy::plugin_stops(adapter.provider_id(), &policy, &class, status, &detail);
        let decision = error_policy::decide_with(&budget.policy, adapter.provider_id(), model, &class, status, &detail,
            error.conversation_class.is_none() && error.native_response.is_none(), stop);
        error_policy::attach(&mut error, &decision);
        error_policy::note(telemetry, &decision, None);
        if policy.suspend_account && !account.is_empty() {
            crate::server::core::blocking::in_place(|| store.suspend_upstream_account(account, &policy.reason));
        } else if decision.hard_stop && !account.is_empty() {
            if let Some(until) = policy.account_cooldown_until {
                crate::server::core::blocking::in_place(|| store.cooldown_upstream_account(account, until, &policy.reason));
            }
        }
        if decision.hard_stop || policy.suspend_account { return Err(error); }
        // 私有持久会话由上游控制续接，不在宿主里从头重复会话。
        if adapter.is_stateful() || !adapter.forwards_response() { return Err(error); }
        if decision.refreshes() {
            if refreshed || !adapter.supports_refresh() || account.is_empty() || !budget.can_wait(decision.retry_delay(0)) { return Err(error); }
            let _refresh_guard = super::scheduler_bridge::acquire_refresh_guard(account).await;
            let refresh = adapter.refresh_access_token(store, account);
            let refreshed_result = if let Some(cancel) = telemetry.cancel_token() {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(super::cancellation::cancelled_error()),
                    result = refresh => result,
                }
            } else { refresh.await };
            if telemetry.replay_guard().blocked() || telemetry.response_started() { return Err(error); }
            let delay = decision.retry_delay(0);
            if refreshed_result.is_err() || !budget.take_retry(delay) { return Err(error); }
            telemetry.note_attempt_retry("登录凭证已刷新，在原账号重试一次", Some(401), delay);
            super::error_policy_send::sleep_or_cancel(telemetry, delay).await;
            refreshed = true; last_error = Some(error); continue;
        }
        if !decision.retries() || budget.remaining == 0 { return Err(error); }
        let suggested = adapter.retry_decision_async(status, &detail, budget.used(), budget.total).await;
        let (delay, reason) = match suggested {
            RetryDecision::Retry { delay_ms, reason } => (decision.retry_delay(delay_ms), reason),
            RetryDecision::Default => (decision.retry_delay(0), "按统一错误规则在原账号重发".to_string()),
            RetryDecision::Stop { reason } => {
                error_policy::note(telemetry, &decision, Some(&reason)); return Err(error);
            }
        };
        if !budget.take_retry(delay) {
            error_policy::note(telemetry, &decision, Some("重发次数或累计等待已用尽，不缩短上游恢复时间"));
            return Err(error);
        }
        telemetry.note_attempt_retry(&reason, Some(i64::from(status)), delay);
        if let Some(cancel) = telemetry.cancel_token() {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(super::cancellation::cancelled_error()),
                _ = tokio::time::sleep(std::time::Duration::from_millis(delay)) => {}
            }
        } else { tokio::time::sleep(std::time::Duration::from_millis(delay)).await; }
        last_error = Some(error);
    }
}

fn is_explicit_pre_exec_rejection(error: &GatewayError) -> bool {
    if matches!(error.status_code, 401 | 402 | 429) {
        return true;
    }
    if matches!(error.status_code, 400 | 409)
        && (error.message.contains("TM.00001041")
            || error.message.contains("Concurrent chats have reached"))
    {
        return true;
    }
    error.status_code == 400
        && (error.message.contains("TM.00001050")
            || error.message.contains("credits have been exhausted")
            || error.message.contains("免费额度已用完"))
}

#[cfg(test)]
mod tests {
    use super::is_explicit_pre_exec_rejection;
    use crate::server::errors::GatewayError;

    #[test]
    fn codearts_concurrent_limit_allows_replay_unfreeze() {
        let err_409 = GatewayError::with_status(
            409,
            "上游返回 400（TM.00001041）：Concurrent chats have reached the upper limit (3).",
        );
        let err_400 = GatewayError::with_status(
            400,
            "Concurrent chats have reached the upper limit (3). Close some chats and try again.",
        );
        let generic_409 = GatewayError::with_status(409, "conflict");
        let err_502 = GatewayError::with_status(502, "TM.00001041");

        assert!(is_explicit_pre_exec_rejection(&err_409));
        assert!(is_explicit_pre_exec_rejection(&err_400));
        assert!(!is_explicit_pre_exec_rejection(&generic_409));
        assert!(!is_explicit_pre_exec_rejection(&err_502));
    }
}
