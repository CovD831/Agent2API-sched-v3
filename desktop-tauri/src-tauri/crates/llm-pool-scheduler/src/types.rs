//! Core types, identifiers, descriptors, and telemetry records for `llm-pool-scheduler`.

use serde::{Deserialize, Serialize};

use crate::health::HealthState;
use crate::hysteresis::{QuotaBand, QuotaHysteresisConfig};

/// Compact dense slot identifier inside [`crate::Scheduler`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SlotId(pub u32);

/// Static/semi-static node (account) configuration pushed by the host gateway.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeDescriptor {
    /// Globally unique account/node ID.
    pub id: String,
    /// Upstream provider/channel identifier (e.g. `"codearts"`, `"loomy"`).
    pub provider: String,
    /// Optional egress proxy group key used by WaveGuard to distinguish proxy failure from account death.
    pub proxy_group: String,
    /// Priority tier (`0` is highest priority; lower numbers are preferred before overflow).
    pub priority_tier: i32,
    /// Hard per-account concurrent in-flight limit (`0` = uncapped).
    pub max_concurrent: u32,
    /// Administrative enabled flag.
    pub enabled: bool,
    /// Whether valid credentials currently exist (`false` when missing token or flagged `needsRelogin`).
    pub has_credentials: bool,
    /// Optional upstream/admin static cooldown deadline (epoch ms).
    pub cooldown_until_ms: i64,
    /// Optional custom hysteresis override for this node/provider.
    pub quota_config: Option<QuotaHysteresisConfig>,
    /// Initial known remaining quota/credits (if available).
    pub initial_remaining_quota: Option<f64>,
    /// Initial known total quota capacity (if percentage-based).
    pub initial_total_quota: Option<f64>,
}

impl NodeDescriptor {
    pub fn new(id: impl Into<String>, provider: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            provider: provider.into(),
            proxy_group: String::new(),
            priority_tier: 0,
            max_concurrent: 2,
            enabled: true,
            has_credentials: true,
            cooldown_until_ms: 0,
            quota_config: None,
            initial_remaining_quota: None,
            initial_total_quota: None,
        }
    }

    pub fn is_routable_static(&self, now_ms: i64) -> bool {
        self.enabled && self.has_credentials && self.cooldown_until_ms <= now_ms
    }
}

/// Read-only runtime view of a candidate node passed to [`crate::policy::SchedulingPolicy`].
#[derive(Clone, Debug)]
pub struct NodeRuntimeView<'a> {
    pub slot_id: SlotId,
    pub id: &'a str,
    pub provider: &'a str,
    pub proxy_group: &'a str,
    pub priority_tier: i32,
    pub max_concurrent: u32,
    pub inflight: u32,
    pub health: HealthState,
    pub quota_band: QuotaBand,
    /// Normalized remaining quota in `[0.0, 1.0]` (`1.0` when unconstrained/unknown).
    pub quota_ratio: f64,
    /// Smooth new-flow admission weight in `[0.0, 1.0]` derived from 4-band hysteresis.
    pub quota_new_flow_weight: f64,
    /// Exponentially weighted moving average TTFT in ms (`0.0` when cold).
    pub ewma_ttft_ms: f64,
    /// Consecutive slow-TTFT count.
    pub slow_streak: u32,
    /// Online dual shadow price $\mu_i(t) \ge 0$ for Phase-2 primal-dual pacing.
    pub dual_shadow_price: f64,
    /// Total dispatched requests on this slot.
    pub total_dispatched: u64,
}

/// Provider/channel aggregate runtime view passed to [`crate::policy::SchedulingPolicy`].
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProviderRuntimeView {
    pub provider: String,
    pub active_inflight: u32,
    pub healthy_nodes: u32,
    pub total_nodes: u32,
    pub wave_degraded: bool,
    pub provider_ewma_ttft_ms: f64,
}

/// Per-request scheduling query.
#[derive(Clone, Debug)]
pub struct ScheduleRequest<'a> {
    /// Requested logical model identifier.
    pub model: &'a str,
    /// Candidate providers allowed to serve this request, ordered by preference.
    pub allowed_providers: &'a [&'a str],
    /// Composite conversation sticky key (empty string disables sticky lookup).
    pub sticky_key: &'a str,
    /// Sticky binding TTL in milliseconds (`0` disables sticky binding).
    pub sticky_ttl_ms: i64,
    /// Node IDs already attempted in earlier retries of this same request.
    pub excluded_node_ids: &'a [String],
    /// Optional additional per-route excluded node IDs (e.g. route profile exclusions).
    pub route_excluded_ids: Option<&'a std::collections::HashSet<String>>,
    /// Optional explicit allowlist of node IDs (e.g. `Selected` scope or routing group).
    pub route_allowed_ids: Option<&'a std::collections::HashSet<String>>,
    /// Optional pinned node ID (used by admin single-account model test).
    pub pinned_node_id: Option<&'a str>,
    /// Attempt index within this request (`0` = initial attempt).
    pub attempt_index: u8,
    /// Current epoch timestamp in milliseconds.
    pub now_ms: i64,
}

/// High-dimensional telemetry emitted on every scheduling decision for Phase-1 monitoring & Phase-2 thesis fitting.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecisionTelemetry {
    pub picked_slot: u32,
    pub picked_provider: String,
    pub priority_tier: i32,
    pub sticky_requested: bool,
    pub sticky_hit: bool,
    pub sticky_migrated: bool,
    pub quota_band: QuotaBand,
    pub quota_ratio: f64,
    pub quota_weight: f64,
    pub health_state: HealthState,
    pub inflight_at_pick: u32,
    pub max_concurrent: u32,
    pub ewma_ttft_ms: f64,
    pub attempt_deadline_ms: u64,
    pub cost_score: f64,
    pub pool_candidates: usize,
    pub sampled_k: usize,
    pub dual_shadow_price: f64,
}

/// Successful scheduling decision + atomically reserved concurrency lease token.
#[derive(Clone, Debug)]
pub struct ScheduleDecision {
    pub slot_id: SlotId,
    pub node_id: String,
    pub provider: String,
    /// Per-attempt TTFT rescue deadline in ms; caller aborts & switches account if no first byte arrives in time.
    pub attempt_ttft_deadline_ms: u64,
    pub sticky_hit: bool,
    pub telemetry: DecisionTelemetry,
}

/// Why no account could be scheduled right now.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectReason {
    /// No accounts exist in the requested providers/scope.
    EmptyScope,
    /// Pinned account does not exist or is outside allowed providers.
    PinnedMismatch,
    /// All enabled accounts are currently at `max_concurrent` capacity (caller may queue briefly).
    AllBusy,
    /// All accounts are in rate-limit / slow / wave backoff; includes earliest recovery timestamp if known.
    AllCoolingDown { earliest_reset_ms: i64 },
    /// All enabled accounts are blocked by `Exhausted` quota band.
    AllQuotaExhausted,
    /// All accounts are administratively disabled or quarantined (`needsRelogin` / dead credentials).
    AllDisabledOrQuarantined,
}

/// Classification of an upstream attempt result reported back to the scheduler.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AttemptFeedbackKind {
    /// Stream/completion succeeded cleanly.
    Success {
        ttft_ms: Option<u64>,
        total_ms: u64,
        prompt_tokens: u64,
        completion_tokens: u64,
        cached_tokens: u64,
        credit_used: Option<f64>,
    },
    /// Upstream failed to emit first byte within `attempt_ttft_deadline_ms` (rescued by switching account).
    TtftTimeout {
        elapsed_ms: u64,
    },
    /// Upstream rate limited (`429` or soft concurrency/window cap).
    RateLimited {
        reset_at_ms: Option<i64>,
    },
    /// Upstream quota / credit hard exhausted (`402` or body quota marker).
    QuotaExhausted {
        reset_at_ms: Option<i64>,
    },
    /// Transient upstream or network/proxy error (`5xx`, reset, timeout).
    TransientFailure {
        is_network_or_proxy: bool,
    },
    /// Credential/account permanently dead (`401` after refresh failure, `"账号已被停用"`, `STS5.1806`, etc.).
    CredentialDead {
        reason: String,
    },
    /// Client cancelled or content policy refusal (neutral to account health, just release inflight).
    NeutralAbort,
}

/// Feedback event reported when an attempt finishes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AttemptFeedback {
    pub node_id: String,
    /// Model key for per-model cooldown tracking (optional).
    pub model: String,
    /// Sticky key to bind/update on success or clear/migrate on failure.
    pub sticky_key: String,
    pub sticky_ttl_ms: i64,
    pub kind: AttemptFeedbackKind,
    pub now_ms: i64,
}

/// Side-effect advisory returned by `record_feedback` so the host gateway can persist durable state if desired.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HostAction {
    /// No persistent DB action needed (state updated purely in scheduler memory).
    None,
    /// Account was quarantined as dead (and NOT part of an upstream/proxy wave); host may persist `needsRelogin` or disable.
    QuarantineAccount {
        node_id: String,
        provider: String,
        reason: String,
    },
    /// Channel/proxy wave guard tripped! Host MUST NOT disable accounts; upstream/proxy is experiencing a mass outage.
    WaveAlert {
        provider: String,
        proxy_group: String,
        recent_failures: usize,
    },
}

/// Per-provider real-time macro telemetry snapshot for live monitoring & Phase-2 thesis analysis.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProviderProbeSummary {
    pub provider: String,
    pub priority_tier: i32,
    pub total_nodes: usize,
    pub enabled_routable: usize,
    pub inflight: u32,
    pub total_dispatched: u64,
    pub load_gini: f64,
    pub mean_ewma_ttft_ms: f64,
    pub wave_degraded: bool,
    pub band_full: usize,
    pub band_throttle: usize,
    pub band_drain_only: usize,
    pub band_exhausted: usize,
    pub health_active: usize,
    pub health_slow_cooldown: usize,
    pub health_temp_backoff: usize,
    pub health_probation: usize,
    pub health_quarantined: usize,
}

/// Global scheduler pool snapshot returned by `GET /api/routing/scheduler`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PoolProbeSummary {
    pub sampled_at_ms: i64,
    pub total_slots: usize,
    pub enabled_routable_slots: usize,
    pub total_inflight: u32,
    pub total_dispatched: u64,
    pub active_sticky_sessions: usize,
    pub providers: Vec<ProviderProbeSummary>,
}
