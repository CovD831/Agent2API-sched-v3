//! Bridge connecting `agent2api-server` (`UpstreamService` / `AccountStore` / `RequestTelemetry`)
//! to the standalone `llm-pool-scheduler` crate, plus single-flight per-account token refresh locks.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use llm_pool_scheduler::{
    AttemptFeedback, AttemptFeedbackKind, ConcurrencyLease, HostAction,
    HysteresisLoadBalancePolicy, NodeDescriptor, QuotaBand, QuotaHysteresisConfig, RejectReason,
    ScheduleRequest, Scheduler, SchedulerConfig,
};
use serde_json::{json, Value};

use crate::server::core::account_store::AccountStore;
use crate::server::core::limiter;
use crate::server::core::providers::readiness;
use crate::server::core::routing;
use crate::server::errors::{self, GatewayError};
use crate::server::logging;

use super::route_context::RequestRouting;
use super::{rotate, RouteTarget, UpstreamService};

const BALANCE_RESYNC_INTERVAL_MS: i64 = 10_000;

/// Global registry of active `GatewayScheduler` instances (typically 1 in production) so
/// background balance updates (`limiter::sources::note`) immediately advance 4-band quota hysteresis.
fn active_schedulers() -> &'static Mutex<Vec<Weak<GatewayScheduler>>> {
    static LIST: OnceLock<Mutex<Vec<Weak<GatewayScheduler>>>> = OnceLock::new();
    LIST.get_or_init(Mutex::default)
}

pub(crate) fn notify_global_quota_update(account_id: &str, remaining: Option<f64>, unlimited: bool) {
    let Some(rem) = (if unlimited { Some(1_000_000.0) } else { remaining }) else {
        return;
    };
    let Ok(mut list) = active_schedulers().lock() else {
        return;
    };
    list.retain(|weak| {
        let Some(sched) = weak.upgrade() else {
            return false;
        };
        sched.inner.update_quota(account_id, rem, None);
        true
    });
}

pub(crate) fn notify_global_cooldown_cleared(account_id: &str, model: Option<&str>) {
    if account_id.is_empty() {
        return;
    }
    let Ok(mut list) = active_schedulers().lock() else {
        return;
    };
    list.retain(|weak| {
        let Some(sched) = weak.upgrade() else {
            return false;
        };
        sched.inner.clear_cooldown(account_id, model);
        true
    });
}

pub(crate) fn notify_global_provider_cooldown_cleared(provider: &str) {
    if provider.is_empty() {
        return;
    }
    let Ok(mut list) = active_schedulers().lock() else {
        return;
    };
    list.retain(|weak| {
        let Some(sched) = weak.upgrade() else {
            return false;
        };
        sched.inner.clear_provider_cooldowns(provider);
        true
    });
}

/// Per-account single-flight async mutex preventing concurrent token refreshes from burning single-use refresh tokens.
pub(crate) async fn acquire_refresh_guard(account_id: &str) -> Option<tokio::sync::OwnedMutexGuard<()>> {
    if account_id.is_empty() {
        return None;
    }
    static LOCKS: OnceLock<Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    let arc_mutex = {
        let mut map = LOCKS
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = map.get(account_id).and_then(Weak::upgrade) {
            existing
        } else {
            if map.len() >= 2048 {
                map.retain(|_, v| v.strong_count() > 0);
            }
            let created = Arc::new(tokio::sync::Mutex::new(()));
            map.insert(account_id.to_string(), Arc::downgrade(&created));
            created
        }
    };
    tokio::time::timeout(Duration::from_secs(15), arc_mutex.lock_owned())
        .await
        .ok()
}

/// Host wrapper around [`llm_pool_scheduler::Scheduler`] maintaining incremental sync watermarks.
pub(crate) struct GatewayScheduler {
    pub(crate) inner: Scheduler<HysteresisLoadBalancePolicy>,
    synced_account_rev: AtomicU64,
    synced_catalog_gen: AtomicU64,
    last_sync_ms: AtomicI64,
    sync_lock: Mutex<()>,
}

impl GatewayScheduler {
    pub(crate) fn new_registered() -> Arc<Self> {
        let inst = Arc::new(Self {
            inner: Scheduler::new(
                HysteresisLoadBalancePolicy::default(),
                SchedulerConfig::default(),
            ),
            synced_account_rev: AtomicU64::new(u64::MAX),
            synced_catalog_gen: AtomicU64::new(u64::MAX),
            last_sync_ms: AtomicI64::new(0),
            sync_lock: Mutex::new(()),
        });
        if let Ok(mut list) = active_schedulers().lock() {
            list.retain(|w| w.strong_count() > 0);
            list.push(Arc::downgrade(&inst));
        }
        inst
    }

    /// Ensures the compact slot arena reflects the latest `AccountStore` revision without per-request scanning.
    pub(crate) fn ensure_synced(&self, store: &AccountStore, now_ms: i64) {
        let rev = store.account_revision_hint();
        let gen = crate::server::core::providers::catalog::generation();
        let last_ms = self.last_sync_ms.load(Ordering::Acquire);
        if self.synced_account_rev.load(Ordering::Acquire) == rev
            && self.synced_catalog_gen.load(Ordering::Acquire) == gen
            && now_ms.saturating_sub(last_ms) < BALANCE_RESYNC_INTERVAL_MS
        {
            return;
        }
        let Ok(_guard) = self.sync_lock.try_lock() else {
            // Another worker is syncing right now; serve immediately from the live arena if already populated.
            if self.last_sync_ms.load(Ordering::Acquire) > 0 {
                return;
            }
            let _blocking_guard = self.sync_lock.lock().unwrap_or_else(|e| e.into_inner());
            self.rebuild_descriptors(store, rev, gen, now_ms);
            return;
        };
        self.rebuild_descriptors(store, rev, gen, now_ms);
    }

    fn rebuild_descriptors(&self, store: &AccountStore, rev: u64, gen: u64, now_ms: i64) {
        let snapshot = store.routing_snapshot();
        let balance_facts = limiter::sources::facts();
        let usage_records_map = crate::server::core::usage_records::load_all();

        // Derive provider priority tiers in O(N) without cloning JSON Values, matching `account_order::manual_groups`:
        // each provider's queue position is determined by the minimum `(priority, addedAt)` among its accounts.
        let mut provider_min_key: HashMap<&str, (i64, i64)> = HashMap::new();
        for acc in &snapshot.accounts {
            let provider = routing::provider_of(acc);
            if !crate::server::core::account_store::store_view::account_visible(provider) {
                continue;
            }
            let prio = acc.get("priority").and_then(Value::as_i64).unwrap_or(i64::MAX);
            let added = acc.get("addedAt").and_then(Value::as_i64).unwrap_or(0);
            let key = (prio, added);
            provider_min_key
                .entry(provider)
                .and_modify(|cur| {
                    if key < *cur {
                        *cur = key;
                    }
                })
                .or_insert(key);
        }
        let mut ordered_providers: Vec<(&str, (i64, i64))> = provider_min_key.into_iter().collect();
        ordered_providers.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(b.0)));
        let provider_rank: HashMap<&str, i32> = ordered_providers
            .into_iter()
            .enumerate()
            .map(|(idx, (prov, _))| (prov, idx as i32))
            .collect();

        let mut descriptors = Vec::with_capacity(snapshot.accounts.len());
        let mut restore_apig_ids: Vec<String> = Vec::new();
        for acc in &snapshot.accounts {
            let Some(id) = routing::account_id(acc) else {
                continue;
            };
            let provider = routing::provider_of(acc);
            if !crate::server::core::account_store::store_view::account_visible(provider) {
                continue;
            }
            let enabled = acc.get("enabled").and_then(Value::as_bool) != Some(false);
            let usage_dead = usage_records_map.get(id).is_some_and(usage_record_is_dead);
            let raw_needs_relogin = acc.get("needsRelogin").and_then(Value::as_bool) == Some(true);
            let is_wrong_apig = raw_needs_relogin
                && acc
                    .get("needsReloginReason")
                    .and_then(Value::as_str)
                    .is_some_and(|r| {
                        r.contains("自动摘除坏号")
                            && (r.contains("APIG.1009")
                                || r.contains("\"code\":1009")
                                || r.contains("\"credentialScope\":\"model\""))
                    });
            if is_wrong_apig {
                restore_apig_ids.push(id.to_string());
            }
            let has_creds = acc.get("hasCredentials").and_then(Value::as_bool) != Some(false)
                && (!raw_needs_relogin || is_wrong_apig)
                && !usage_dead;
            let max_concurrent = routing::max_concurrent_of(acc).min(u32::MAX as u64) as u32;
            let cooldown_until_ms = acc
                .get("upstreamCooldownUntil")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let proxy_group = acc
                .pointer("/proxy/label")
                .or_else(|| acc.pointer("/proxy/host"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let priority_tier = provider_rank.get(provider).copied().unwrap_or(0);

            let (rem_quota, total_quota, quota_cfg) = extract_quota_params(provider, id, acc, &balance_facts);

            descriptors.push(NodeDescriptor {
                id: id.to_string(),
                provider: provider.to_string(),
                proxy_group,
                priority_tier,
                max_concurrent,
                enabled,
                has_credentials: has_creds,
                cooldown_until_ms,
                quota_config: quota_cfg,
                initial_remaining_quota: rem_quota,
                initial_total_quota: total_quota,
            });
        }

        self.inner.sync_nodes(&descriptors, now_ms);
        if !restore_apig_ids.is_empty() {
            let mut restored = 0usize;
            for aid in &restore_apig_ids {
                if store.set_needs_relogin(aid, None) {
                    restored += 1;
                }
            }
            if restored > 0 {
                logging::log_with_level(
                    "[Scheduler]",
                    &format!("✅ 已自动恢复 {restored} 个被 APIG.1009 模型级错误误隔离的账号"),
                    "info",
                );
            }
        }
        self.synced_account_rev.store(rev, Ordering::Release);
        self.synced_catalog_gen.store(gen, Ordering::Release);
        self.last_sync_ms.store(now_ms, Ordering::Release);
    }
}

fn extract_quota_params(
    provider: &str,
    id: &str,
    account: &Value,
    facts: &limiter::sources::Facts,
) -> (Option<f64>, Option<f64>, Option<QuotaHysteresisConfig>) {
    let ref_full = match provider {
        "loomy" => Some(200.0),
        "workbuddy" | "workbuddy-intl" => Some(500.0),
        "codearts" => Some(100.0),
        "officeace" => Some(100.0),
        "raccoon" => Some(200.0),
        "trae" => Some(300.0),
        _ => None,
    };
    let cfg = ref_full.map(|reference_full_credits| QuotaHysteresisConfig {
        reference_full_credits: Some(reference_full_credits),
        ..QuotaHysteresisConfig::default()
    });
    let Some(sf) = facts.get(id).filter(|f| limiter::sources::current_for(account, f)) else {
        return (None, None, cfg);
    };
    if sf.unlimited {
        return (Some(1.0), Some(1.0), cfg);
    }
    if let Some(rem) = sf.remaining.filter(|r| r.is_finite() && *r > 0.0) {
        let matched_total = sf.sources.iter().find_map(|s| {
            s.total
                .filter(|t| t.is_finite() && *t > 0.0 && s.remaining.is_some_and(|sr| (sr - rem).abs() < 1e-6))
        });
        let capped_total = matched_total.map(|t| ref_full.map_or(t, |rf| t.min(rf)));
        return (Some(rem), capped_total, cfg);
    }
    if let Some(primary) = sf.sources.first() {
        if let Some(pct) = primary.remaining_percent.filter(|p| p.is_finite()) {
            return (Some((pct / 100.0).clamp(0.0, 1.0)), Some(1.0), cfg);
        }
        if let (Some(rem), Some(limit)) = (primary.remaining, primary.total) {
            if limit > 0.0 {
                return (Some(rem), Some(limit), cfg);
            }
        }
    }
    (sf.remaining, None, cfg)
}

/// Fast-path $O(1)$ / $O(k)$ account selection backed by `llm-pool-scheduler`.
///
/// Returns `Some(Result<(RouteTarget, Option<ConcurrencyLease>), GatewayError>)` when handled,
/// or `None` to delegate complex virtual routing-group / special test cases to legacy `select_target_account_sync`.
pub(super) fn try_select_fast(
    service: &UpstreamService,
    providers: &[&str],
    keys: &routing::CooldownKeys<'_>,
    tried_ids: &[String],
    pinned: Option<&str>,
    conv_key: &str,
    request_routing: &RequestRouting,
) -> Option<Result<(RouteTarget, Option<ConcurrencyLease>), GatewayError>> {
    // Virtual routing groups or pinned admin tests keep exact legacy projection/diagnostic behavior.
    if request_routing.group.is_some()
        || pinned.is_some()
        || request_routing.strict_vision
        || request_routing.requires_account()
    {
        return None;
    }

    let now = logging::now_ms();
    service.scheduler.ensure_synced(&service.store, now);
    let snapshot = service.store.routing_snapshot();

    // Filter providers by profile scope + local readiness gate (unless all providers are gated)
    let profile = request_routing.profile.as_ref();
    let scoped_providers: Vec<&str> = providers
        .iter()
        .copied()
        .filter(|p| snapshot.by_provider.get(*p).is_some_and(|list| !list.is_empty()))
        .filter(|p| profile.is_none_or(|prof| prof.includes_provider(p)))
        .collect();

    if scoped_providers.is_empty() {
        // Delegate empty-provider edge case (e.g. anonymous env-var token fallback or 503 message) to legacy handler
        return None;
    }

    let ungated: Vec<&str> = scoped_providers
        .iter()
        .copied()
        .filter(|p| !readiness::gated(p))
        .collect();
    let active_providers = if ungated.is_empty() {
        scoped_providers
    } else {
        ungated
    };

    let route_excluded: Option<HashSet<String>> = profile
        .filter(|prof| !prof.excluded_accounts.is_empty())
        .map(|prof| prof.excluded_accounts.iter().cloned().collect());

    let settings = request_routing.settings;
    let sticky_ttl_ms = routing::sticky_ttl_ms(settings.sticky);

    let mut local_excluded: Vec<String> = tried_ids.to_vec();

    // Up to 32 microsecond re-picks if a sampled candidate hits a custom user limiter rule or persistent rateLimits JSON
    for _ in 0..32 {
        let req = ScheduleRequest {
            model: keys.request_model(),
            allowed_providers: &active_providers,
            sticky_key: conv_key,
            sticky_ttl_ms,
            excluded_node_ids: &local_excluded,
            route_excluded_ids: route_excluded.as_ref(),
            route_allowed_ids: None,
            pinned_node_id: None,
            attempt_index: (tried_ids.len().min(u8::MAX as usize)) as u8,
            now_ms: now,
        };

        match service.scheduler.inner.schedule(&req) {
            Ok((decision, mut lease)) => {
                let Some(pub_acc) = snapshot.by_id.get(&decision.node_id) else {
                    lease.release();
                    local_excluded.push(decision.node_id);
                    continue;
                };
                // Check persisted model-level cooldowns & user custom limiter rules ONLY on the 1 chosen candidate!
                let wire_model = keys.for_account(pub_acc);
                let persisted_reset = routing::rate_limit_reset_at(pub_acc, keys, now);
                let policy_cd = service
                    .store
                    .policy_cooldown_for(&decision.provider, &wire_model)
                    .and_then(|v| v.get("resetAt").and_then(Value::as_i64))
                    .unwrap_or(0);
                let effective_reset = persisted_reset.max(policy_cd);
                if effective_reset > now {
                    lease.release();
                    service.scheduler.inner.record_feedback(AttemptFeedback {
                        node_id: decision.node_id.clone(),
                        model: keys.request_model().to_string(),
                        sticky_key: String::new(),
                        sticky_ttl_ms: 0,
                        kind: AttemptFeedbackKind::RateLimited {
                            reset_at_ms: Some(effective_reset),
                        },
                        now_ms: now,
                    });
                    local_excluded.push(decision.node_id);
                    continue;
                }
                if super::super::plugin_host::wasm::catalog::route_allowed(pub_acc, &wire_model)
                    == Some(false)
                {
                    lease.release();
                    service.scheduler.inner.record_feedback(AttemptFeedback {
                        node_id: decision.node_id.clone(),
                        model: keys.request_model().to_string(),
                        sticky_key: String::new(),
                        sticky_ttl_ms: 0,
                        kind: AttemptFeedbackKind::RateLimited {
                            reset_at_ms: Some(now + 60_000),
                        },
                        now_ms: now,
                    });
                    local_excluded.push(decision.node_id);
                    continue;
                }
                if is_single_account_limiter_blocked(pub_acc, &wire_model, now) {
                    lease.release();
                    service.scheduler.inner.record_feedback(AttemptFeedback {
                        node_id: decision.node_id.clone(),
                        model: keys.request_model().to_string(),
                        sticky_key: String::new(),
                        sticky_ttl_ms: 0,
                        kind: AttemptFeedbackKind::RateLimited {
                            reset_at_ms: Some(now + 30_000),
                        },
                        now_ms: now,
                    });
                    local_excluded.push(decision.node_id);
                    continue;
                }

                let Some(picked_session) = service.store.get_session_by_id(&decision.node_id) else {
                    lease.release();
                    local_excluded.push(decision.node_id);
                    continue;
                };

                // Keep legacy StickySessions UI panel mirrored on hit/bind without O(M) scan overhead
                let mut dec_json = json!({
                    "strategy": settings.strategy.as_str(),
                    "sticky": settings.sticky.as_str(),
                    "stickyHit": decision.sticky_hit,
                    "picked": decision.node_id,
                    "orderedTotal": decision.telemetry.pool_candidates,
                    "blockedTotal": local_excluded.len(),
                    "scope": request_routing.scope,
                    "configSnapshot": request_routing.json(),
                    "scheduler": {
                        "engine": "llm-pool-scheduler",
                        "slot": decision.telemetry.picked_slot,
                        "priorityTier": decision.telemetry.priority_tier,
                        "stickyRequested": decision.telemetry.sticky_requested,
                        "stickyHit": decision.telemetry.sticky_hit,
                        "stickyMigrated": decision.telemetry.sticky_migrated,
                        "quotaBand": decision.telemetry.quota_band.as_str(),
                        "quotaRatio": decision.telemetry.quota_ratio,
                        "quotaWeight": decision.telemetry.quota_weight,
                        "healthState": decision.telemetry.health_state.as_str(),
                        "inflightAtPick": decision.telemetry.inflight_at_pick,
                        "maxConcurrent": decision.telemetry.max_concurrent,
                        "ewmaTtftMs": decision.telemetry.ewma_ttft_ms,
                        "attemptDeadlineMs": decision.telemetry.attempt_deadline_ms,
                        "costScore": decision.telemetry.cost_score,
                        "poolCandidates": decision.telemetry.pool_candidates,
                        "sampledK": decision.telemetry.sampled_k,
                        "dualShadowPrice": decision.telemetry.dual_shadow_price,
                    }
                });
                if !conv_key.is_empty() {
                    let sid = super::route_sticky::entry_id(conv_key, keys.request_model());
                    dec_json["sessionId"] = Value::String(sid.clone());
                    if sticky_ttl_ms > 0 {
                        dec_json["stickyId"] = Value::String(sid);
                    }
                }

                let mut target = rotate::with_proxy_notice(
                    pub_acc.as_ref().clone(),
                    picked_session.proxy,
                    picked_session.proxy_error,
                    decision.node_id,
                    Some(dec_json),
                );
                target.attempt_deadline_ms = Some(decision.attempt_ttft_deadline_ms);
                return Some(Ok((target, Some(lease))));
            }
            Err(RejectReason::AllBusy) => {
                return Some(Err(
                    GatewayError::with_status(503, "当前路由内的账号均已达到并发上限")
                        .with_code("account_busy"),
                ));
            }
            Err(RejectReason::AllCoolingDown { earliest_reset_ms }) => {
                if local_excluded.len() > tried_ids.len() {
                    return None;
                }
                let secs = earliest_reset_ms.saturating_sub(now).max(0) as u64 / 1000 + 1;
                return Some(Err(GatewayError::with_status(
                    429,
                    format!(
                        "当前路由对象处于冷却期，最早 {} 恢复；未提前重发",
                        errors::format_reset_at_text(earliest_reset_ms)
                    ),
                )
                .with_code("routing_cooldown")
                .with_retry_after_seconds(secs)));
            }
            Err(RejectReason::AllQuotaExhausted) => {
                return Some(Err(GatewayError::with_status(
                    503,
                    "所有启用中的账号均处于低额度保护波段（DrainOnly/Exhausted），暂无可承接新会话的账号",
                )
                .with_code("quota_band_exhausted")));
            }
            Err(RejectReason::EmptyScope | RejectReason::AllDisabledOrQuarantined | RejectReason::PinnedMismatch) => {
                // Fall back to legacy diagnostic formatter so exact 503 reason text (disabled vs limiter vs empty) is preserved
                return None;
            }
        }
    }
    None
}

fn is_single_account_limiter_blocked(account: &Value, wire_model: &str, now_ms: i64) -> bool {
    let facts = limiter::sources::facts();
    let counts = limiter::counter::facts_for_account(routing::account_id(account).unwrap_or(""));
    limiter::evaluate::blocked(account, wire_model, &facts, &counts, now_ms)
}

/// Classifies upstream failure into `llm-pool-scheduler` feedback and enforces `WaveGuard` anti-mass-quarantine.
pub(super) fn report_attempt_failure(
    service: &UpstreamService,
    account_id: Option<&str>,
    request_model: &str,
    conv_key: &str,
    sticky_ttl_ms: i64,
    attempt_deadline_ms: Option<u64>,
    error: &GatewayError,
) -> HostAction {
    let Some(node_id) = account_id.filter(|id| !id.is_empty()) else {
        return HostAction::None;
    };
    let now_ms = logging::now_ms();
    let msg = error.message.as_str();
    let kind = if error.code.as_deref() == Some("ttft_deadline_exceeded") {
        AttemptFeedbackKind::TtftTimeout {
            elapsed_ms: attempt_deadline_ms.unwrap_or(20_000),
        }
    } else if is_dead_account_error(error.status_code, msg) {
        AttemptFeedbackKind::CredentialDead {
            reason: msg.chars().take(160).collect(),
        }
    } else if error.status_code == 402
        || msg.contains("余额不足")
        || msg.contains("积分不足")
        || msg.contains("配额耗尽")
        || msg.contains("quota exceeded")
        || msg.contains("TM.00001050")
        || msg.contains("credits have been exhausted")
        || msg.contains("免费额度已用完")
    {
        service.scheduler.inner.update_quota(node_id, 0.0, None);
        let reset = errors::parse_quota_reset_at(msg);
        AttemptFeedbackKind::QuotaExhausted {
            reset_at_ms: (reset > now_ms).then_some(reset),
        }
    } else if error.status_code == 401
        && (msg.contains("APIG.1009") || msg.contains("credentialScope"))
    {
        service.scheduler.inner.update_quota(node_id, 0.0, None);
        AttemptFeedbackKind::QuotaExhausted {
            reset_at_ms: Some(now_ms + 1_800_000),
        }
    } else if error.status_code == 409
        || msg.contains("TM.00001041")
        || msg.contains("Concurrent chats have reached")
    {
        AttemptFeedbackKind::RateLimited {
            reset_at_ms: Some(now_ms + 8_000),
        }
    } else if error.status_code == 429 {
        let reset = error
            .retry_after_secs
            .map(|s| now_ms + (s as i64) * 1000)
            .or_else(|| {
                let parsed = errors::parse_quota_reset_at(msg);
                (parsed > now_ms).then_some(parsed)
            });
        AttemptFeedbackKind::RateLimited { reset_at_ms: reset }
    } else if error.status_code >= 500 || error.status_code == 408 {
        AttemptFeedbackKind::TransientFailure {
            is_network_or_proxy: msg.contains("代理")
                || msg.contains("连接")
                || msg.contains("connection")
                || msg.contains("timeout")
                || msg.contains("超时"),
        }
    } else {
        AttemptFeedbackKind::NeutralAbort
    };

    let action = service.scheduler.inner.record_feedback(AttemptFeedback {
        node_id: node_id.to_string(),
        model: request_model.to_string(),
        sticky_key: conv_key.to_string(),
        sticky_ttl_ms,
        kind,
        now_ms,
    });

    match &action {
        HostAction::QuarantineAccount {
            node_id,
            provider,
            reason,
        } => {
            // Persist quarantine via needsRelogin only for verified isolated dead accounts (WaveGuard passed!)
            let store = service.store.clone();
            let id_owned = node_id.clone();
            let reason_owned = format!("自动摘除坏号：{reason}");
            crate::server::core::blocking::in_place(|| {
                if !store.needs_relogin(&id_owned) {
                    store.set_needs_relogin(&id_owned, Some(&reason_owned));
                }
            });
            logging::log_with_level(
                "[Scheduler]",
                &format!("⛔ 账号 {node_id} ({provider}) 命中坏号特征且未触发浪涌，已自动隔离：{reason}"),
                "warn",
            );
        }
        HostAction::WaveAlert {
            provider,
            proxy_group,
            recent_failures,
        } => {
            logging::log_with_level(
                "[WaveGuard]",
                &format!(
                    "⚠️ 检测到渠道 {provider} (出口={}) 30s 内 {recent_failures} 个不同账号集中报错，已触发浪涌保护（仅临时退避，禁止批量停用账号）",
                    if proxy_group.is_empty() { "直连/默认" } else { proxy_group }
                ),
                "warn",
            );
        }
        HostAction::None => {}
    }
    action
}

fn is_dead_account_error(status: i32, msg: &str) -> bool {
    if msg.contains("\"credentialScope\":\"model\"") || msg.contains("\"credentialScope\": \"model\"") {
        return false;
    }
    const DEAD_MARKERS: &[&str] = &[
        "账号已被停用",
        "账户已被封禁",
        "账号已注销",
        "登录已失效",
        "登录态已失效",
        "临时凭据已到期",
        "需重新登录",
        "codearts_access_expired",
        "缺少原授权材料",
        "缺少 public_key_jwk",
        "refresh token has been used",
        "STS5.1806",
        "refresh token is invalid",
        "invalid_grant",
        "authorization_verify_error",
        "TM.00001005",
        "Account banned",
    ];
    if DEAD_MARKERS.iter().any(|m| msg.contains(m)) {
        return true;
    }
    status == 401
        && (msg.contains("请重新登录")
            || msg.contains("需重新登录")
            || msg.contains("临时凭据已到期")
            || msg.contains("Token 无效")
            || msg.contains("续期失败"))
}

fn usage_record_is_dead(rec: &crate::server::core::usage_records::UsageRecord) -> bool {
    if rec.code.as_deref() == Some("officeace_refresh_uncertain") {
        return false;
    }
    if rec.code.as_deref().is_some_and(|c| c.eq_ignore_ascii_case("codearts_access_expired")) {
        return true;
    }
    rec.error.as_deref().is_some_and(|err| is_dead_account_error(401, err))
}

/// Callback invoked from `record_persist::persist` when an entire request (including SSE stream) finishes.
pub struct SchedulerSettleContext {
    pub scheduler: Arc<GatewayScheduler>,
    pub request_model: String,
    pub sticky_key: String,
    pub sticky_ttl_ms: i64,
}

impl SchedulerSettleContext {
    pub fn settle(
        &self,
        account_id: &str,
        status: i64,
        ttft_ms: Option<i64>,
        duration_ms: i64,
        prompt_tokens: i64,
        completion_tokens: i64,
        cached_tokens: i64,
        credit: Option<f64>,
        now_ms: i64,
    ) {
        if account_id.is_empty() {
            return;
        }
        if (200..300).contains(&status) {
            self.scheduler.inner.record_feedback(AttemptFeedback {
                node_id: account_id.to_string(),
                model: self.request_model.clone(),
                sticky_key: self.sticky_key.clone(),
                sticky_ttl_ms: self.sticky_ttl_ms,
                kind: AttemptFeedbackKind::Success {
                    ttft_ms: ttft_ms.filter(|&v| v >= 0).map(|v| v as u64),
                    total_ms: duration_ms.max(0) as u64,
                    prompt_tokens: prompt_tokens.max(0) as u64,
                    completion_tokens: completion_tokens.max(0) as u64,
                    cached_tokens: cached_tokens.max(0) as u64,
                    credit_used: credit,
                },
                now_ms,
            });
        } else if status == 402 {
            let _ = self.scheduler.inner.update_quota(account_id, 0.0, Some(100.0));
        } else if status == 408 && ttft_ms.is_none() && duration_ms >= 15_000 {
            let _ = self.scheduler.inner.record_feedback(AttemptFeedback {
                node_id: account_id.to_string(),
                model: self.request_model.clone(),
                sticky_key: String::new(),
                sticky_ttl_ms: 0,
                kind: AttemptFeedbackKind::NeutralAbort,
                now_ms,
            });
        }
        let _ = QuotaBand::Full;
    }
}

#[cfg(test)]
mod tests {
    use super::is_dead_account_error;

    #[test]
    fn tm_00001005_account_banned_is_classified_as_dead_account() {
        assert!(is_dead_account_error(
            403,
            r#"CodeArts 上游返回 HTTP 403：{"text":"[DONE]","error_code":"TM.00001005","error_msg":"Account banned"}"#
        ));
    }
}
