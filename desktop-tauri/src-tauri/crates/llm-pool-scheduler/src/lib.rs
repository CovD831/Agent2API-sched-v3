//! `llm-pool-scheduler` — Standalone, zero-IO multi-provider account pool scheduler.
//!
//! Designed for high-concurrency LLM aggregation gateways (thousands of heterogeneous accounts)
//! and two-phase mathematical optimization research:
//! - **O(1) Sticky Fast-Path & O(k) Sampled Load Balancing**: Eliminates per-request $O(N)$ full-pool scans,
//!   JSON cloning, and global lock contention.
//! - **Atomic Global Concurrency Leases**: Guarantees per-account in-flight count strictly respects
//!   `max_concurrent` across all protocol streams and routing groups.
//! - **4-Band Quota Hysteresis**: Smoothly transitions accounts across `Full -> Throttle -> DrainOnly -> Exhausted`
//!   with asymmetric recovery thresholds ($B^\uparrow > B^\downarrow$) to preserve prompt cache stickiness
//!   while preventing boundary oscillation.
//! - **5-State Health FSM + Per-Attempt TTFT Rescue + Channel WaveGuard**: Isolates slow or dead accounts
//!   without mass-disabling pools during upstream or proxy outages.

pub mod health;
pub mod hysteresis;
pub mod policy;
pub mod replay;
pub mod sticky;
pub mod types;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

pub use health::{HealthConfig, HealthState, WaveGuard};
pub use hysteresis::{QuotaBand, QuotaHysteresisConfig};
pub use policy::{
    HysteresisLoadBalancePolicy, HysteresisPolicyConfig, PrimalDualCongestionPolicy,
    SchedulingPolicy,
};
pub use sticky::{build_isolated_sticky_key, ShardedStickyTable};
pub use types::{
    AttemptFeedback, AttemptFeedbackKind, DecisionTelemetry, HostAction, NodeDescriptor,
    NodeRuntimeView, PoolProbeSummary, ProviderProbeSummary, ProviderRuntimeView, RejectReason,
    ScheduleDecision, ScheduleRequest, SlotId,
};

#[derive(Debug)]
struct QuotaRuntime {
    config: QuotaHysteresisConfig,
    band: QuotaBand,
    ratio: f64,
    remaining: Option<f64>,
    total: Option<f64>,
    dual_shadow_price: f64,
}

impl QuotaRuntime {
    fn from_descriptor(desc: &NodeDescriptor, default_cfg: &QuotaHysteresisConfig) -> Self {
        let cfg = desc
            .quota_config
            .as_ref()
            .unwrap_or(default_cfg)
            .sanitized();
        let ratio = desc
            .initial_remaining_quota
            .and_then(|rem| cfg.normalize_quota(rem, desc.initial_total_quota))
            .unwrap_or(1.0);
        let band = if desc.initial_remaining_quota.is_some() {
            cfg.transition(QuotaBand::Full, ratio)
        } else {
            QuotaBand::Full
        };
        Self {
            config: cfg,
            band,
            ratio,
            remaining: desc.initial_remaining_quota,
            total: desc.initial_total_quota,
            dual_shadow_price: 0.0,
        }
    }
}

struct NodeSlot {
    slot_id: SlotId,
    /// Immutable ID for lifetime of slot.
    id: String,
    meta: RwLock<NodeDescriptor>,
    /// Strictly atomic global per-account in-flight lease counter (0028 invariant).
    inflight: AtomicU32,
    dispatched: AtomicU64,
    health: Mutex<health::NodeHealthRuntime>,
    quota: Mutex<QuotaRuntime>,
}

impl NodeSlot {
    fn new(slot_id: SlotId, desc: NodeDescriptor, default_quota: &QuotaHysteresisConfig) -> Self {
        let id = desc.id.clone();
        let quota = QuotaRuntime::from_descriptor(&desc, default_quota);
        Self {
            slot_id,
            id,
            meta: RwLock::new(desc),
            inflight: AtomicU32::new(0),
            dispatched: AtomicU64::new(0),
            health: Mutex::new(health::NodeHealthRuntime::default()),
            quota: Mutex::new(quota),
        }
    }

    /// Atomically reserves 1 in-flight slot iff `inflight < effective_cap` (`0` = unlimited).
    fn try_acquire_slot(&self, max_concurrent: u32, is_probation: bool) -> Option<u32> {
        let effective_cap = if is_probation {
            1
        } else {
            max_concurrent
        };
        loop {
            let cur = self.inflight.load(Ordering::Acquire);
            if effective_cap > 0 && cur >= effective_cap {
                return None;
            }
            if self
                .inflight
                .compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.dispatched.fetch_add(1, Ordering::Relaxed);
                return Some(cur + 1);
            }
        }
    }

    #[inline]
    fn release_slot(&self) {
        let mut cur = self.inflight.load(Ordering::Acquire);
        loop {
            let next = cur.saturating_sub(1);
            match self.inflight.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }
}

#[derive(Default)]
struct ArenaIndex {
    slots: Vec<Arc<NodeSlot>>,
    by_id: HashMap<String, SlotId>,
    /// Provider -> PriorityTier (ascending) -> dense list of SlotIds.
    by_provider_tiers: HashMap<String, BTreeMap<i32, Vec<SlotId>>>,
}

/// RAII concurrency lease guard guaranteeing every scheduled request decrements `inflight` on drop.
pub struct ConcurrencyLease {
    slot: Option<Arc<NodeSlot>>,
}

impl std::fmt::Debug for ConcurrencyLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrencyLease")
            .field("node_id", &self.node_id())
            .finish()
    }
}

impl ConcurrencyLease {
    pub fn empty() -> Self {
        Self { slot: None }
    }

    pub fn node_id(&self) -> Option<&str> {
        self.slot.as_ref().map(|s| s.id.as_str())
    }

    /// Releases the current slot early (idempotent).
    pub fn release(&mut self) {
        if let Some(slot) = self.slot.take() {
            slot.release_slot();
        }
    }
}

impl Drop for ConcurrencyLease {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            slot.release_slot();
        }
    }
}

/// Configuration bundle for [`Scheduler`].
#[derive(Clone, Debug, Default)]
pub struct SchedulerConfig {
    pub quota: QuotaHysteresisConfig,
    pub health: HealthConfig,
    pub sticky_capacity: usize,
    /// Step size $\eta \ge 0$ for online dual shadow price pacing (`0.01` default).
    pub dual_step_size: f64,
}

/// Standalone, thread-safe account pool scheduler parameterized by [`SchedulingPolicy`].
pub struct Scheduler<P: SchedulingPolicy = HysteresisLoadBalancePolicy> {
    arena: RwLock<ArenaIndex>,
    sticky: ShardedStickyTable,
    wave_guard: WaveGuard,
    provider_ttft: RwLock<HashMap<String, f64>>,
    policy: RwLock<P>,
    config: RwLock<SchedulerConfig>,
    rr_cursor: AtomicU64,
}

impl Default for Scheduler<HysteresisLoadBalancePolicy> {
    fn default() -> Self {
        Self::new(
            HysteresisLoadBalancePolicy::default(),
            SchedulerConfig {
                quota: QuotaHysteresisConfig::default(),
                health: HealthConfig::default(),
                sticky_capacity: 131_072,
                dual_step_size: 0.01,
            },
        )
    }
}

impl<P: SchedulingPolicy> Scheduler<P> {
    pub fn new(policy: P, mut config: SchedulerConfig) -> Self {
        if config.sticky_capacity == 0 {
            config.sticky_capacity = 131_072;
        }
        config.quota = config.quota.sanitized();
        let sticky = ShardedStickyTable::with_capacity(config.sticky_capacity);
        Self {
            arena: RwLock::new(ArenaIndex::default()),
            sticky,
            wave_guard: WaveGuard::new(),
            provider_ttft: RwLock::new(HashMap::new()),
            policy: RwLock::new(policy),
            config: RwLock::new(config),
            rr_cursor: AtomicU64::new(0),
        }
    }

    /// Incrementally synchronizes the node registry while preserving live in-flight counts,
    /// EWMA TTFT statistics, health states, and 4-band quota hysteresis states for existing nodes!
    pub fn sync_nodes(&self, descriptors: &[NodeDescriptor], _now_ms: i64) {
        let default_quota = self
            .config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .quota
            .clone();
        let mut arena = self.arena.write().unwrap_or_else(|e| e.into_inner());
        let mut by_provider_tiers: HashMap<String, BTreeMap<i32, Vec<SlotId>>> = HashMap::new();

        for desc in descriptors {
            let slot_id = if let Some(&existing_id) = arena.by_id.get(&desc.id) {
                let slot = &arena.slots[existing_id.0 as usize];
                let was_re_enabled = {
                    let mut meta = slot.meta.write().unwrap_or_else(|e| e.into_inner());
                    let became_valid = (!meta.enabled || !meta.has_credentials)
                        && desc.enabled
                        && desc.has_credentials;
                    *meta = desc.clone();
                    became_valid
                };
                if was_re_enabled {
                    let mut h = slot.health.lock().unwrap_or_else(|e| e.into_inner());
                    if matches!(h.state, HealthState::Quarantined) {
                        h.state = HealthState::Probation;
                        h.quarantine_reason = None;
                        h.failure_streak = 0;
                    }
                }
                if let Some(rem) = desc.initial_remaining_quota {
                    let mut q = slot.quota.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(custom) = &desc.quota_config {
                        q.config = custom.sanitized();
                    }
                    q.remaining = Some(rem);
                    if desc.initial_total_quota.is_some() {
                        q.total = desc.initial_total_quota;
                    }
                    if let Some(ratio) = q.config.normalize_quota(rem, q.total) {
                        q.ratio = ratio;
                        q.band = q.config.transition(q.band, ratio);
                    }
                }
                existing_id
            } else {
                let next_id = SlotId(arena.slots.len() as u32);
                let slot = Arc::new(NodeSlot::new(next_id, desc.clone(), &default_quota));
                arena.slots.push(slot);
                arena.by_id.insert(desc.id.clone(), next_id);
                next_id
            };

            if desc.enabled && desc.has_credentials {
                by_provider_tiers
                    .entry(desc.provider.clone())
                    .or_default()
                    .entry(desc.priority_tier)
                    .or_default()
                    .push(slot_id);
            }
        }
        arena.by_provider_tiers = by_provider_tiers;
    }

    /// Updates an account's real-time quota reading (e.g. from background usage query, checkin, or response header),
    /// advancing the 4-band hysteresis state machine (`Full <-> Throttle <-> DrainOnly <-> Exhausted`).
    pub fn update_quota(&self, node_id: &str, remaining: f64, total: Option<f64>) -> Option<QuotaBand> {
        let arena = self.arena.read().unwrap_or_else(|e| e.into_inner());
        let &slot_id = arena.by_id.get(node_id)?;
        let slot = &arena.slots[slot_id.0 as usize];
        let mut q = slot.quota.lock().unwrap_or_else(|e| e.into_inner());
        let previous_band = q.band;
        q.remaining = Some(remaining);
        if total.is_some() {
            q.total = total;
        }
        if let Some(ratio) = q.config.normalize_quota(remaining, q.total) {
            q.ratio = ratio;
            q.band = q.config.transition(q.band, ratio);
            if matches!(previous_band, QuotaBand::Exhausted) && !matches!(q.band, QuotaBand::Exhausted) {
                // Recovered across upward hysteresis threshold B2_up / B1_up -> enter half-open Probation!
                let mut h = slot.health.lock().unwrap_or_else(|e| e.into_inner());
                if matches!(h.state, HealthState::Active) {
                    h.state = HealthState::Probation;
                }
            }
        }
        Some(q.band)
    }

    /// Immediately clears in-memory rate-limit / temporary backoff cooldowns for `node_id`
    /// when the host store explicitly clears a rate limit (`clear_rate_limit` / `clear_all_rate_limits`).
    pub fn clear_cooldown(&self, node_id: &str, model: Option<&str>) -> bool {
        let arena = self.arena.read().unwrap_or_else(|e| e.into_inner());
        let Some(&slot_id) = arena.by_id.get(node_id) else {
            return false;
        };
        let slot = &arena.slots[slot_id.0 as usize];
        let mut h = slot.health.lock().unwrap_or_else(|e| e.into_inner());
        match model.filter(|m| !m.is_empty()) {
            Some(m) => {
                // Remove exact model key; if absent (e.g. request used a model alias while store cleared wire_model),
                // clear cached model cooldowns so host store remains authoritative on subsequent picks.
                if h.model_cooldowns.remove(m).is_none() {
                    h.model_cooldowns.clear();
                }
            }
            None => {
                h.model_cooldowns.clear();
            }
        }
        if h.model_cooldowns.is_empty()
            && matches!(h.state, HealthState::TempBackoff | HealthState::SlowCooldown)
        {
            h.state = HealthState::Active;
            h.state_until_ms = 0;
            h.failure_streak = 0;
            h.slow_streak = 0;
        }
        true
    }

    /// Immediately clears in-memory temporary cooldowns across all nodes belonging to `provider`.
    pub fn clear_provider_cooldowns(&self, provider: &str) {
        let arena = self.arena.read().unwrap_or_else(|e| e.into_inner());
        let Some(tiers) = arena.by_provider_tiers.get(provider) else {
            return;
        };
        for slot_ids in tiers.values() {
            for &slot_id in slot_ids {
                let slot = &arena.slots[slot_id.0 as usize];
                let mut h = slot.health.lock().unwrap_or_else(|e| e.into_inner());
                h.model_cooldowns.clear();
                if matches!(h.state, HealthState::TempBackoff | HealthState::SlowCooldown) {
                    h.state = HealthState::Active;
                    h.state_until_ms = 0;
                    h.failure_streak = 0;
                    h.slow_streak = 0;
                }
            }
        }
    }

    /// Primary $O(1)$ sticky / $O(k)$ sampled scheduling entry point with atomic concurrency lease reservation.
    pub fn schedule(
        &self,
        req: &ScheduleRequest<'_>,
    ) -> Result<(ScheduleDecision, ConcurrencyLease), RejectReason> {
        let arena = self.arena.read().unwrap_or_else(|e| e.into_inner());
        let policy = self.policy.read().unwrap_or_else(|e| e.into_inner());
        let cfg = self.config.read().unwrap_or_else(|e| e.into_inner());

        // 0. Pinned account fast path (model test mode)
        if let Some(pinned_id) = req.pinned_node_id {
            let Some(&slot_id) = arena.by_id.get(pinned_id) else {
                return Err(RejectReason::PinnedMismatch);
            };
            let slot = &arena.slots[slot_id.0 as usize];
            let meta = slot.meta.read().unwrap_or_else(|e| e.into_inner());
            if !req.allowed_providers.is_empty()
                && !req.allowed_providers.contains(&meta.provider.as_str())
            {
                return Err(RejectReason::PinnedMismatch);
            }
            let (view, prov_view) = self.snapshot_slot_views(&arena, slot, &meta, req.now_ms, &cfg.health);
            let deadline = policy.attempt_ttft_deadline_ms(&view, &prov_view, req.attempt_index);
            let Some(inflight_after) = slot.try_acquire_slot(meta.max_concurrent, false) else {
                return Err(RejectReason::AllBusy);
            };
            let decision = build_decision(slot, &view, deadline, false, false, 0.0, 1, 1);
            let mut dec = decision;
            dec.telemetry.inflight_at_pick = inflight_after.saturating_sub(1);
            return Ok((dec, ConcurrencyLease { slot: Some(slot.clone()) }));
        }

        // 1. O(1) Sticky fast path (preserves prompt cache even in `DrainOnly` band!)
        let mut sticky_migrated = false;
        if !req.sticky_key.is_empty() && req.sticky_ttl_ms > 0 && req.excluded_node_ids.is_empty() {
            if let Some((sticky_slot_id, _)) =
                self.sticky
                    .lookup_and_touch(req.sticky_key, req.model, req.now_ms, req.sticky_ttl_ms)
            {
                if let Some(slot) = arena.slots.get(sticky_slot_id.0 as usize) {
                    if self.slot_in_scope(slot, req) {
                        let meta = slot.meta.read().unwrap_or_else(|e| e.into_inner());
                        if meta.is_routable_static(req.now_ms) {
                            let model_cooling = slot
                                .health
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .model_cooling_until(req.model, req.now_ms)
                                .is_some();
                            let slow_degraded = self.wave_guard.is_provider_slow_degraded(
                                &meta.provider,
                                req.now_ms,
                                &cfg.health,
                            );
                            if !model_cooling && !slow_degraded {
                                let (view, prov_view) =
                                    self.snapshot_slot_views(&arena, slot, &meta, req.now_ms, &cfg.health);
                                if let Some(score) = policy.evaluate_cost(&view, &prov_view, req, true) {
                                    let is_prob = matches!(view.health, HealthState::Probation);
                                    if let Some(inflight_after) =
                                        slot.try_acquire_slot(meta.max_concurrent, is_prob)
                                    {
                                        let deadline = policy.attempt_ttft_deadline_ms(
                                            &view,
                                            &prov_view,
                                            req.attempt_index,
                                        );
                                        let mut dec = build_decision(
                                            slot, &view, deadline, true, false, score, 1, 1,
                                        );
                                        dec.telemetry.inflight_at_pick = inflight_after.saturating_sub(1);
                                        return Ok((
                                            dec,
                                            ConcurrencyLease {
                                                slot: Some(slot.clone()),
                                            },
                                        ));
                                    }
                                }
                            }
                        }
                    }
                    // Bound sticky account is busy, cooling, quarantined, or quota-exhausted -> migrate cleanly
                    sticky_migrated = true;
                }
            }
        }

        // 2. O(k) Generalized Power-of-k Choices across priority tiers
        let sample_k = policy.sample_size();
        let mut saw_any_in_scope = false;
        let mut saw_busy = false;
        let mut saw_quota_exhausted = false;
        let mut earliest_cooldown: Option<i64> = None;

        // Collect distinct priority tiers across allowed providers in ascending order
        let mut tiers: Vec<i32> = Vec::with_capacity(4);
        for &prov in req.allowed_providers {
            if let Some(tier_map) = arena.by_provider_tiers.get(prov) {
                for &tier in tier_map.keys() {
                    if !tiers.contains(&tier) {
                        tiers.push(tier);
                    }
                }
            }
        }
        tiers.sort_unstable();

        // Precompute lightweight provider views once for the allowed providers
        let mut prov_views: HashMap<&str, ProviderRuntimeView> =
            HashMap::with_capacity(req.allowed_providers.len());
        let mut slow_degraded_map: HashMap<&str, bool> =
            HashMap::with_capacity(req.allowed_providers.len());
        for &prov in req.allowed_providers {
            let is_slow = self
                .wave_guard
                .is_provider_slow_degraded(prov, req.now_ms, &cfg.health);
            slow_degraded_map.insert(prov, is_slow);
            prov_views.insert(
                prov,
                ProviderRuntimeView {
                    provider: prov.to_string(),
                    wave_degraded: self
                        .wave_guard
                        .is_provider_degraded(prov, req.now_ms, &cfg.health)
                        || is_slow,
                    provider_ewma_ttft_ms: self.get_provider_ttft(prov),
                    ..Default::default()
                },
            );
        }

        let mut drain_fallback: Option<(f64, Arc<NodeSlot>, u32, bool, DecisionTelemetry)> = None;
        let mut slow_wave_fallback: Option<(f64, Arc<NodeSlot>, u32, bool, DecisionTelemetry)> =
            None;
        let mut soft_cooldown_fallback: Option<(i64, Arc<NodeSlot>, u32, DecisionTelemetry)> = None;

        for tier in tiers {
            // Gather slice references for this tier across allowed providers without cloning Accounts
            let mut tier_slices: Vec<&[SlotId]> = Vec::with_capacity(req.allowed_providers.len());
            let mut total_in_tier = 0usize;
            for &prov in req.allowed_providers {
                if let Some(list) = arena.by_provider_tiers.get(prov).and_then(|m| m.get(&tier)) {
                    if !list.is_empty() {
                        total_in_tier += list.len();
                        tier_slices.push(list.as_slice());
                    }
                }
            }
            if total_in_tier == 0 {
                continue;
            }

            let seed = self.rr_cursor.fetch_add(1, Ordering::Relaxed);
            // Strictly coprime stride guarantees visiting every slot in the tier without repetition for any odd/even N.
            let stride = coprime_stride(total_in_tier);
            let start = (seed as usize) % total_in_tier;

            let mut best_candidates: Vec<(f64, Arc<NodeSlot>, u32, bool, DecisionTelemetry)> =
                Vec::with_capacity(sample_k);
            let mut evaluated = 0usize;

            for step in 0..total_in_tier {
                let flat_idx = (start + step.wrapping_mul(stride)) % total_in_tier;
                let slot_id = index_across_slices(&tier_slices, flat_idx);
                let slot = &arena.slots[slot_id.0 as usize];
                if !self.slot_in_scope(slot, req) {
                    continue;
                }
                let meta = slot.meta.read().unwrap_or_else(|e| e.into_inner());
                if !meta.enabled || !meta.has_credentials {
                    continue;
                }
                saw_any_in_scope = true;
                if meta.cooldown_until_ms > req.now_ms {
                    update_min(&mut earliest_cooldown, meta.cooldown_until_ms);
                    continue;
                }

                // Check health & per-model cooldown
                let (health_state, state_until, ewma_ttft, slow_streak, model_until) = {
                    let mut h = slot.health.lock().unwrap_or_else(|e| e.into_inner());
                    let st = h.refresh_clock(req.now_ms);
                    let m_until = h.model_cooling_until(req.model, req.now_ms);
                    (st, h.state_until_ms, h.ewma_ttft_ms, h.slow_streak, m_until)
                };
                if matches!(health_state, HealthState::Quarantined) {
                    continue;
                }
                let is_slow_degraded = slow_degraded_map
                    .get(meta.provider.as_str())
                    .copied()
                    .unwrap_or(false);

                let inflight_now = slot.inflight.load(Ordering::Acquire);
                let is_soft_cooling = model_until.is_some()
                    || matches!(health_state, HealthState::SlowCooldown | HealthState::TempBackoff);
                if is_soft_cooling {
                    let soft_until = match (model_until, matches!(health_state, HealthState::SlowCooldown | HealthState::TempBackoff)) {
                        (Some(m), true) if state_until > req.now_ms => m.max(state_until),
                        (Some(m), _) => m,
                        (None, _) => state_until,
                    };
                    if soft_until > req.now_ms {
                        update_min(&mut earliest_cooldown, soft_until);
                    }
                    let allow_soft_probe = !(is_slow_degraded
                        && (req.attempt_index > 0 || matches!(health_state, HealthState::SlowCooldown)));
                    if allow_soft_probe
                        && inflight_now == 0
                        && (meta.max_concurrent == 0 || inflight_now < meta.max_concurrent)
                        && soft_until > req.now_ms
                        && soft_until.saturating_sub(req.now_ms) <= 180_000
                    {
                        let (band, ratio, weight, shadow) = {
                            let q = slot.quota.lock().unwrap_or_else(|e| e.into_inner());
                            (
                                q.band,
                                q.ratio,
                                q.config.new_flow_weight(q.band, q.ratio),
                                q.dual_shadow_price,
                            )
                        };
                        if !matches!(band, QuotaBand::Exhausted)
                            && soft_cooldown_fallback
                                .as_ref()
                                .is_none_or(|(best_until, _, _, _)| soft_until < *best_until)
                        {
                            let prov_view = prov_views
                                .get(meta.provider.as_str())
                                .cloned()
                                .unwrap_or_default();
                            let view = NodeRuntimeView {
                                slot_id: slot.slot_id,
                                id: &slot.id,
                                provider: &meta.provider,
                                proxy_group: &meta.proxy_group,
                                priority_tier: meta.priority_tier,
                                max_concurrent: meta.max_concurrent,
                                inflight: inflight_now,
                                health: health_state,
                                quota_band: band,
                                quota_ratio: ratio,
                                quota_new_flow_weight: weight,
                                ewma_ttft_ms: ewma_ttft,
                                slow_streak,
                                dual_shadow_price: shadow,
                                total_dispatched: slot.dispatched.load(Ordering::Relaxed),
                                proxy_wave_degraded: self.wave_guard.is_proxy_group_degraded(
                                    &meta.provider,
                                    &meta.proxy_group,
                                    req.now_ms,
                                    &cfg.health,
                                ),
                            };
                            let deadline =
                                policy.attempt_ttft_deadline_ms(&view, &prov_view, req.attempt_index);
                            let tel = DecisionTelemetry {
                                picked_slot: slot.slot_id.0,
                                picked_provider: meta.provider.clone(),
                                priority_tier: meta.priority_tier,
                                sticky_requested: !req.sticky_key.is_empty(),
                                sticky_hit: false,
                                sticky_migrated,
                                quota_band: band,
                                quota_ratio: ratio,
                                quota_weight: weight,
                                health_state,
                                inflight_at_pick: inflight_now,
                                max_concurrent: meta.max_concurrent,
                                ewma_ttft_ms: ewma_ttft,
                                attempt_deadline_ms: deadline,
                                cost_score: (meta.priority_tier as f64) * 10.0 + 5.0,
                                pool_candidates: total_in_tier,
                                sampled_k: 1,
                                dual_shadow_price: shadow,
                            };
                            soft_cooldown_fallback =
                                Some((soft_until, slot.clone(), meta.max_concurrent, tel));
                        }
                    }
                    continue;
                }

                let effective_cap = if matches!(health_state, HealthState::Probation) {
                    1
                } else {
                    meta.max_concurrent
                };
                if effective_cap > 0 && inflight_now >= effective_cap {
                    saw_busy = true;
                    continue;
                }

                let (band, ratio, weight, shadow) = {
                    let q = slot.quota.lock().unwrap_or_else(|e| e.into_inner());
                    let w = q.config.new_flow_weight(q.band, q.ratio);
                    (q.band, q.ratio, w, q.dual_shadow_price)
                };
                if !band.allows_new_flow() {
                    saw_quota_exhausted = true;
                    if matches!(band, QuotaBand::DrainOnly)
                        && !(is_slow_degraded && req.attempt_index > 0)
                    {
                        let cap = meta.max_concurrent.max(4) as f64;
                        let util = (inflight_now as f64 + 1.0) / cap;
                        let drain_cost = (meta.priority_tier as f64) * 10.0
                            + if is_slow_degraded { 10_000.0 } else { 0.0 }
                            + (1.0 - ratio)
                            + util
                            + if matches!(health_state, HealthState::Probation) {
                                0.35
                            } else {
                                0.0
                            };
                        if drain_fallback
                            .as_ref()
                            .is_none_or(|(best_cost, _, _, _, _)| drain_cost < *best_cost)
                        {
                            let prov_view = prov_views
                                .get(meta.provider.as_str())
                                .cloned()
                                .unwrap_or_default();
                            let view = NodeRuntimeView {
                                slot_id: slot.slot_id,
                                id: &slot.id,
                                provider: &meta.provider,
                                proxy_group: &meta.proxy_group,
                                priority_tier: meta.priority_tier,
                                max_concurrent: meta.max_concurrent,
                                inflight: inflight_now,
                                health: health_state,
                                quota_band: band,
                                quota_ratio: ratio,
                                quota_new_flow_weight: weight,
                                ewma_ttft_ms: ewma_ttft,
                                slow_streak,
                                dual_shadow_price: shadow,
                                total_dispatched: slot.dispatched.load(Ordering::Relaxed),
                                proxy_wave_degraded: self.wave_guard.is_proxy_group_degraded(
                                    &meta.provider,
                                    &meta.proxy_group,
                                    req.now_ms,
                                    &cfg.health,
                                ),
                            };
                            let deadline =
                                policy.attempt_ttft_deadline_ms(&view, &prov_view, req.attempt_index);
                            let tel = DecisionTelemetry {
                                picked_slot: slot.slot_id.0,
                                picked_provider: meta.provider.clone(),
                                priority_tier: meta.priority_tier,
                                sticky_requested: !req.sticky_key.is_empty(),
                                sticky_hit: false,
                                sticky_migrated,
                                quota_band: band,
                                quota_ratio: ratio,
                                quota_weight: weight,
                                health_state,
                                inflight_at_pick: inflight_now,
                                max_concurrent: meta.max_concurrent,
                                ewma_ttft_ms: ewma_ttft,
                                attempt_deadline_ms: deadline,
                                cost_score: drain_cost,
                                pool_candidates: total_in_tier,
                                sampled_k: 1,
                                dual_shadow_price: shadow,
                            };
                            drain_fallback = Some((
                                drain_cost,
                                slot.clone(),
                                meta.max_concurrent,
                                matches!(health_state, HealthState::Probation),
                                tel,
                            ));
                        }
                    }
                    continue;
                }

                let view = NodeRuntimeView {
                    slot_id: slot.slot_id,
                    id: &slot.id,
                    provider: &meta.provider,
                    proxy_group: &meta.proxy_group,
                    priority_tier: meta.priority_tier,
                    max_concurrent: meta.max_concurrent,
                    inflight: inflight_now,
                    health: health_state,
                    quota_band: band,
                    quota_ratio: ratio,
                    quota_new_flow_weight: weight,
                    ewma_ttft_ms: ewma_ttft,
                    slow_streak,
                    dual_shadow_price: shadow,
                    total_dispatched: slot.dispatched.load(Ordering::Relaxed),
                    proxy_wave_degraded: self.wave_guard.is_proxy_group_degraded(
                        &meta.provider,
                        &meta.proxy_group,
                        req.now_ms,
                        &cfg.health,
                    ),
                };
                let prov_view = prov_views
                    .get(meta.provider.as_str())
                    .cloned()
                    .unwrap_or_default();

                if is_slow_degraded {
                    if req.attempt_index > 0 {
                        update_min(&mut earliest_cooldown, req.now_ms.saturating_add(30_000));
                        continue;
                    }
                    if let Some(cost) = policy.evaluate_cost(&view, &prov_view, req, false) {
                        if slow_wave_fallback
                            .as_ref()
                            .is_none_or(|(best_cost, _, _, _, _)| cost < *best_cost)
                        {
                            let deadline =
                                policy.attempt_ttft_deadline_ms(&view, &prov_view, req.attempt_index);
                            let tel = DecisionTelemetry {
                                picked_slot: slot.slot_id.0,
                                picked_provider: meta.provider.clone(),
                                priority_tier: meta.priority_tier,
                                sticky_requested: !req.sticky_key.is_empty(),
                                sticky_hit: false,
                                sticky_migrated,
                                quota_band: band,
                                quota_ratio: ratio,
                                quota_weight: weight,
                                health_state,
                                inflight_at_pick: inflight_now,
                                max_concurrent: meta.max_concurrent,
                                ewma_ttft_ms: ewma_ttft,
                                attempt_deadline_ms: deadline,
                                cost_score: cost,
                                pool_candidates: total_in_tier,
                                sampled_k: 1,
                                dual_shadow_price: shadow,
                            };
                            slow_wave_fallback = Some((
                                cost,
                                slot.clone(),
                                meta.max_concurrent,
                                matches!(health_state, HealthState::Probation),
                                tel,
                            ));
                        }
                    }
                    continue;
                }

                if let Some(cost) = policy.evaluate_cost(&view, &prov_view, req, false) {
                    evaluated += 1;
                    let deadline =
                        policy.attempt_ttft_deadline_ms(&view, &prov_view, req.attempt_index);
                    let tel = DecisionTelemetry {
                        picked_slot: slot.slot_id.0,
                        picked_provider: meta.provider.clone(),
                        priority_tier: meta.priority_tier,
                        sticky_requested: !req.sticky_key.is_empty(),
                        sticky_hit: false,
                        sticky_migrated,
                        quota_band: band,
                        quota_ratio: ratio,
                        quota_weight: weight,
                        health_state,
                        inflight_at_pick: inflight_now,
                        max_concurrent: meta.max_concurrent,
                        ewma_ttft_ms: ewma_ttft,
                        attempt_deadline_ms: deadline,
                        cost_score: cost,
                        pool_candidates: total_in_tier,
                        sampled_k: evaluated,
                        dual_shadow_price: shadow,
                    };
                    best_candidates.push((
                        cost,
                        slot.clone(),
                        meta.max_concurrent,
                        matches!(health_state, HealthState::Probation),
                        tel,
                    ));
                    if evaluated >= sample_k || (evaluated > 0 && step + 1 >= (sample_k * 8).max(64)) {
                        break;
                    }
                }
            }

            if !best_candidates.is_empty() {
                best_candidates
                    .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                let sampled_count = best_candidates.len();
                for (_cost, slot, max_conc, is_prob, mut tel) in best_candidates {
                    if let Some(inflight_after) = slot.try_acquire_slot(max_conc, is_prob) {
                        tel.sampled_k = sampled_count;
                        tel.inflight_at_pick = inflight_after.saturating_sub(1);
                        let decision = ScheduleDecision {
                            slot_id: slot.slot_id,
                            node_id: slot.id.clone(),
                            provider: tel.picked_provider.clone(),
                            attempt_ttft_deadline_ms: tel.attempt_deadline_ms,
                            sticky_hit: false,
                            telemetry: tel,
                        };
                        return Ok((
                            decision,
                            ConcurrencyLease {
                                slot: Some(slot),
                            },
                        ));
                    } else {
                        saw_busy = true;
                    }
                }
            }
        }

        // 2b. Two-stage safe fallbacks before returning an error when no Full/Throttle healthy slot exists:
        // (1) DrainOnly fallback (never touches Exhausted), preserving service continuity on thin pools.
        if let Some((_cost, slot, max_conc, is_prob, mut tel)) = drain_fallback {
            if let Some(inflight_after) = slot.try_acquire_slot(max_conc, is_prob) {
                tel.inflight_at_pick = inflight_after.saturating_sub(1);
                let decision = ScheduleDecision {
                    slot_id: slot.slot_id,
                    node_id: slot.id.clone(),
                    provider: tel.picked_provider.clone(),
                    attempt_ttft_deadline_ms: tel.attempt_deadline_ms,
                    sticky_hit: false,
                    telemetry: tel,
                };
                return Ok((
                    decision,
                    ConcurrencyLease {
                        slot: Some(slot),
                    },
                ));
            } else {
                saw_busy = true;
            }
        }
        // (2) Slow-wave single probe fallback when NO non-slow-degraded provider is available (attempt 0 only).
        if let Some((_cost, slot, max_conc, is_prob, mut tel)) = slow_wave_fallback {
            if let Some(inflight_after) = slot.try_acquire_slot(max_conc, is_prob) {
                tel.inflight_at_pick = inflight_after.saturating_sub(1);
                let decision = ScheduleDecision {
                    slot_id: slot.slot_id,
                    node_id: slot.id.clone(),
                    provider: tel.picked_provider.clone(),
                    attempt_ttft_deadline_ms: tel.attempt_deadline_ms,
                    sticky_hit: false,
                    telemetry: tel,
                };
                return Ok((
                    decision,
                    ConcurrencyLease {
                        slot: Some(slot),
                    },
                ));
            } else {
                saw_busy = true;
            }
        }
        // (3) Earliest soft-cooldown single-flight probe fallback when pool isn't capacity-busy.
        if !saw_busy {
            if let Some((_until, slot, max_conc, mut tel)) = soft_cooldown_fallback {
                if let Some(inflight_after) = slot.try_acquire_slot(max_conc, true) {
                    tel.inflight_at_pick = inflight_after.saturating_sub(1);
                    let decision = ScheduleDecision {
                        slot_id: slot.slot_id,
                        node_id: slot.id.clone(),
                        provider: tel.picked_provider.clone(),
                        attempt_ttft_deadline_ms: tel.attempt_deadline_ms,
                        sticky_hit: false,
                        telemetry: tel,
                    };
                    return Ok((
                        decision,
                        ConcurrencyLease {
                            slot: Some(slot),
                        },
                    ));
                } else {
                    saw_busy = true;
                }
            }
        }

        // 3. Classify rejection reason accurately for gateway HTTP status mapping
        if !saw_any_in_scope {
            let any_registered = req
                .allowed_providers
                .iter()
                .any(|p| arena.by_provider_tiers.get(*p).is_some_and(|m| !m.is_empty()));
            if any_registered {
                Err(RejectReason::AllDisabledOrQuarantined)
            } else {
                Err(RejectReason::EmptyScope)
            }
        } else if saw_busy {
            Err(RejectReason::AllBusy)
        } else if let Some(reset) = earliest_cooldown {
            Err(RejectReason::AllCoolingDown {
                earliest_reset_ms: reset,
            })
        } else if saw_quota_exhausted {
            Err(RejectReason::AllQuotaExhausted)
        } else {
            Err(RejectReason::AllDisabledOrQuarantined)
        }
    }

    /// Processes attempt outcome feedback: updates EWMA TTFT, slow cooldown, 4-band quota hysteresis,
    /// online dual shadow price, sticky bindings, and WaveGuard dead-account quarantine.
    pub fn record_feedback(&self, feedback: AttemptFeedback) -> HostAction {
        let arena = self.arena.read().unwrap_or_else(|e| e.into_inner());
        let Some(&slot_id) = arena.by_id.get(&feedback.node_id) else {
            return HostAction::None;
        };
        let slot = &arena.slots[slot_id.0 as usize];
        let cfg = self.config.read().unwrap_or_else(|e| e.into_inner());
        let (provider, proxy_group) = {
            let meta = slot.meta.read().unwrap_or_else(|e| e.into_inner());
            (meta.provider.clone(), meta.proxy_group.clone())
        };

        match feedback.kind {
            AttemptFeedbackKind::Success {
                ttft_ms,
                credit_used,
                ..
            } => {
                if let Some(ms) = ttft_ms.filter(|&ms| ms > 0) {
                    self.update_provider_ttft(&provider, ms as f64, cfg.health.ttft_ewma_alpha);
                }
                {
                    let mut h = slot.health.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(ms) = ttft_ms.filter(|&ms| ms >= cfg.health.slow_ttft_threshold_ms) {
                        // Succeeded, but TTFT exceeded slow threshold -> track streak
                        h.record_slow_event(ms, false, feedback.now_ms, &cfg.health);
                    } else {
                        h.record_fast_success(ttft_ms, &feedback.model, &cfg.health);
                        self.wave_guard.clear_slow_wave(&provider);
                    }
                }
                let band_allows_sticky = {
                    let mut q = slot.quota.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(used) = credit_used.filter(|c| c.is_finite() && *c > 0.0) {
                        if let Some(rem) = q.remaining {
                            let next_rem = (rem - used).max(0.0);
                            q.remaining = Some(next_rem);
                            if let Some(ratio) = q.config.normalize_quota(next_rem, q.total) {
                                q.ratio = ratio;
                                q.band = q.config.transition(q.band, ratio);
                            }
                        }
                        // Online primal-dual shadow price subgradient step
                        let denom = q
                            .total
                            .or(q.config.reference_full_credits)
                            .unwrap_or(100.0)
                            .max(1.0);
                        q.dual_shadow_price =
                            (q.dual_shadow_price + cfg.dual_step_size * (used / denom)).min(10.0);
                    } else {
                        q.dual_shadow_price = (q.dual_shadow_price * 0.995).max(0.0);
                    }
                    q.band.allows_sticky()
                };
                if band_allows_sticky && !feedback.sticky_key.is_empty() && feedback.sticky_ttl_ms > 0 {
                    self.sticky.bind(
                        &feedback.sticky_key,
                        &feedback.model,
                        slot.slot_id,
                        &slot.id,
                        feedback.now_ms,
                        feedback.sticky_ttl_ms,
                    );
                }
                HostAction::None
            }

            AttemptFeedbackKind::TtftTimeout { elapsed_ms } => {
                if elapsed_ms > 0 {
                    self.update_provider_ttft(
                        &provider,
                        elapsed_ms as f64,
                        cfg.health.ttft_ewma_alpha,
                    );
                }
                let wave = self.wave_guard.observe_slow_timeout(
                    &provider,
                    &slot.id,
                    feedback.now_ms,
                    &cfg.health,
                );
                {
                    let mut h = slot.health.lock().unwrap_or_else(|e| e.into_inner());
                    h.record_slow_event(elapsed_ms, true, feedback.now_ms, &cfg.health);
                }
                self.sticky.remove(&feedback.sticky_key, &feedback.model);
                if let Some(recent_failures) = wave {
                    return HostAction::WaveAlert {
                        provider,
                        proxy_group,
                        recent_failures,
                    };
                }
                HostAction::None
            }

            AttemptFeedbackKind::RateLimited { reset_at_ms } => {
                let mut h = slot.health.lock().unwrap_or_else(|e| e.into_inner());
                h.record_backoff(reset_at_ms, &feedback.model, feedback.now_ms, &cfg.health);
                self.sticky.remove(&feedback.sticky_key, &feedback.model);
                HostAction::None
            }

            AttemptFeedbackKind::QuotaExhausted { reset_at_ms } => {
                {
                    let mut q = slot.quota.lock().unwrap_or_else(|e| e.into_inner());
                    q.band = QuotaBand::Exhausted;
                    q.ratio = 0.0;
                    q.remaining = Some(0.0);
                }
                if reset_at_ms.is_some() {
                    let mut h = slot.health.lock().unwrap_or_else(|e| e.into_inner());
                    h.record_backoff(reset_at_ms, "", feedback.now_ms, &cfg.health);
                }
                self.sticky.remove(&feedback.sticky_key, &feedback.model);
                HostAction::None
            }

            AttemptFeedbackKind::TransientFailure { is_network_or_proxy } => {
                let wave = if is_network_or_proxy {
                    self.wave_guard.observe_failure(
                        &provider,
                        &proxy_group,
                        &slot.id,
                        feedback.now_ms,
                        &cfg.health,
                    )
                } else {
                    None
                };
                let mut h = slot.health.lock().unwrap_or_else(|e| e.into_inner());
                h.record_backoff(None, "", feedback.now_ms, &cfg.health);
                if let Some(recent_failures) = wave {
                    return HostAction::WaveAlert {
                        provider,
                        proxy_group,
                        recent_failures,
                    };
                }
                HostAction::None
            }

            AttemptFeedbackKind::CredentialDead { reason } => {
                self.sticky.remove(&feedback.sticky_key, &feedback.model);
                // Critical WaveGuard check: if >= K accounts in this provider/proxy failed in the last 30s,
                // DO NOT quarantine/disable the account — treat as protective backoff!
                if let Some(recent_failures) = self.wave_guard.observe_failure(
                    &provider,
                    &proxy_group,
                    &slot.id,
                    feedback.now_ms,
                    &cfg.health,
                ) {
                    let mut h = slot.health.lock().unwrap_or_else(|e| e.into_inner());
                    h.state = HealthState::TempBackoff;
                    h.state_until_ms = feedback
                        .now_ms
                        .saturating_add(cfg.health.wave_protective_backoff_ms);
                    return HostAction::WaveAlert {
                        provider,
                        proxy_group,
                        recent_failures,
                    };
                }
                let mut h = slot.health.lock().unwrap_or_else(|e| e.into_inner());
                h.state = HealthState::Quarantined;
                h.quarantine_reason = Some(reason.clone());
                HostAction::QuarantineAccount {
                    node_id: slot.id.clone(),
                    provider,
                    reason,
                }
            }

            AttemptFeedbackKind::NeutralAbort => HostAction::None,
        }
    }

    /// Snapshot of current non-zero in-flight counts (`node_id -> inflight`).
    pub fn inflight_snapshot(&self) -> Vec<(String, usize)> {
        let arena = self.arena.read().unwrap_or_else(|e| e.into_inner());
        let mut out = Vec::new();
        for slot in &arena.slots {
            let count = slot.inflight.load(Ordering::Relaxed) as usize;
            if count > 0 {
                out.push((slot.id.clone(), count));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// Snapshot of EWMA TTFT (`node_id -> ms`) for observability panels.
    pub fn ttft_snapshot(&self) -> HashMap<String, i64> {
        let arena = self.arena.read().unwrap_or_else(|e| e.into_inner());
        let mut out = HashMap::new();
        for slot in &arena.slots {
            let ms = slot
                .health
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .ewma_ttft_ms;
            if ms > 0.0 {
                out.insert(slot.id.clone(), ms.round() as i64);
            }
        }
        out
    }

    /// Total dispatched counts per registered slot (used by replay / Gini fairness tests).
    pub fn dispatched_counts(&self) -> Vec<u64> {
        let arena = self.arena.read().unwrap_or_else(|e| e.into_inner());
        arena
            .slots
            .iter()
            .map(|s| s.dispatched.load(Ordering::Relaxed))
            .collect()
    }

    pub fn clear_sticky(&self) {
        self.sticky.clear_all();
    }

    /// Captures a lock-light macro telemetry summary across all registered accounts & channels.
    pub fn probe_summary(&self, now_ms: i64) -> PoolProbeSummary {
        let arena = self.arena.read().unwrap_or_else(|e| e.into_inner());
        let cfg = self.config.read().unwrap_or_else(|e| e.into_inner());
        let mut by_prov: BTreeMap<String, (ProviderProbeSummary, Vec<u64>, f64, usize)> =
            BTreeMap::new();
        let mut total_inflight = 0u32;
        let mut total_dispatched = 0u64;
        let mut enabled_routable_slots = 0usize;

        for slot in &arena.slots {
            let meta = slot.meta.read().unwrap_or_else(|e| e.into_inner()).clone();
            let inflight = slot.inflight.load(Ordering::Relaxed);
            let disp = slot.dispatched.load(Ordering::Relaxed);
            total_inflight = total_inflight.saturating_add(inflight);
            total_dispatched = total_dispatched.saturating_add(disp);

            let entry = by_prov.entry(meta.provider.clone()).or_insert_with(|| {
                (
                    ProviderProbeSummary {
                        provider: meta.provider.clone(),
                        priority_tier: meta.priority_tier,
                        wave_degraded: self.wave_guard.is_provider_degraded(
                            &meta.provider,
                            now_ms,
                            &cfg.health,
                        ) || self.wave_guard.is_provider_slow_degraded(
                            &meta.provider,
                            now_ms,
                            &cfg.health,
                        ),
                        ..Default::default()
                    },
                    Vec::new(),
                    0.0,
                    0,
                )
            });
            if meta.priority_tier < entry.0.priority_tier {
                entry.0.priority_tier = meta.priority_tier;
            }
            entry.0.total_nodes += 1;
            entry.0.inflight = entry.0.inflight.saturating_add(inflight);
            entry.0.total_dispatched = entry.0.total_dispatched.saturating_add(disp);

            if !meta.enabled || !meta.has_credentials {
                continue;
            }
            enabled_routable_slots += 1;
            entry.0.enabled_routable += 1;
            entry.1.push(disp);

            let (h_state, ewma) = {
                let mut h = slot.health.lock().unwrap_or_else(|e| e.into_inner());
                let st = h.refresh_clock(now_ms);
                (st, h.ewma_ttft_ms)
            };
            if ewma > 0.0 {
                entry.2 += ewma;
                entry.3 += 1;
            }
            match h_state {
                HealthState::Active => entry.0.health_active += 1,
                HealthState::SlowCooldown => entry.0.health_slow_cooldown += 1,
                HealthState::TempBackoff => entry.0.health_temp_backoff += 1,
                HealthState::Probation => entry.0.health_probation += 1,
                HealthState::Quarantined => entry.0.health_quarantined += 1,
            }

            let band = slot.quota.lock().unwrap_or_else(|e| e.into_inner()).band;
            match band {
                QuotaBand::Full => entry.0.band_full += 1,
                QuotaBand::Throttle => entry.0.band_throttle += 1,
                QuotaBand::DrainOnly => entry.0.band_drain_only += 1,
                QuotaBand::Exhausted => entry.0.band_exhausted += 1,
            }
        }

        let mut providers: Vec<ProviderProbeSummary> = by_prov
            .into_values()
            .map(|(mut sum, disp_vec, ttft_sum, ttft_n)| {
                sum.load_gini = replay::gini_coefficient(&disp_vec);
                sum.mean_ewma_ttft_ms = if ttft_n > 0 {
                    (ttft_sum / ttft_n as f64 * 10.0).round() / 10.0
                } else {
                    0.0
                };
                sum
            })
            .collect();
        providers.sort_by(|a, b| {
            a.priority_tier
                .cmp(&b.priority_tier)
                .then_with(|| a.provider.cmp(&b.provider))
        });

        PoolProbeSummary {
            sampled_at_ms: now_ms,
            total_slots: arena.slots.len(),
            enabled_routable_slots,
            total_inflight,
            total_dispatched,
            active_sticky_sessions: self.sticky.active_count(now_ms),
            providers,
        }
    }

    fn slot_in_scope(&self, slot: &NodeSlot, req: &ScheduleRequest<'_>) -> bool {
        if req.excluded_node_ids.iter().any(|id| id == &slot.id) {
            return false;
        }
        if let Some(ex) = req.route_excluded_ids {
            if ex.contains(&slot.id) {
                return false;
            }
        }
        if let Some(allow) = req.route_allowed_ids {
            if !allow.contains(&slot.id) {
                return false;
            }
        }
        let meta = slot.meta.read().unwrap_or_else(|e| e.into_inner());
        req.allowed_providers.is_empty()
            || req.allowed_providers.contains(&meta.provider.as_str())
    }

    fn update_provider_ttft(&self, provider: &str, sample_ms: f64, alpha: f64) {
        if provider.is_empty() || sample_ms <= 0.0 || !sample_ms.is_finite() {
            return;
        }
        let alpha = alpha.clamp(0.01, 1.0);
        let mut map = self.provider_ttft.write().unwrap_or_else(|e| e.into_inner());
        let entry = map.entry(provider.to_string()).or_insert(sample_ms);
        *entry = (1.0 - alpha) * *entry + alpha * sample_ms;
    }

    fn get_provider_ttft(&self, provider: &str) -> f64 {
        self.provider_ttft
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(provider)
            .copied()
            .unwrap_or(0.0)
    }

    fn snapshot_slot_views<'a>(
        &self,
        _arena: &ArenaIndex,
        slot: &'a NodeSlot,
        meta: &'a NodeDescriptor,
        now_ms: i64,
        health_cfg: &HealthConfig,
    ) -> (NodeRuntimeView<'a>, ProviderRuntimeView) {
        let (health_state, ewma_ttft, slow_streak) = {
            let mut h = slot.health.lock().unwrap_or_else(|e| e.into_inner());
            let st = h.refresh_clock(now_ms);
            (st, h.ewma_ttft_ms, h.slow_streak)
        };
        let (band, ratio, weight, shadow) = {
            let q = slot.quota.lock().unwrap_or_else(|e| e.into_inner());
            let w = q.config.new_flow_weight(q.band, q.ratio);
            (q.band, q.ratio, w, q.dual_shadow_price)
        };
        let view = NodeRuntimeView {
            slot_id: slot.slot_id,
            id: &slot.id,
            provider: &meta.provider,
            proxy_group: &meta.proxy_group,
            priority_tier: meta.priority_tier,
            max_concurrent: meta.max_concurrent,
            inflight: slot.inflight.load(Ordering::Acquire),
            health: health_state,
            quota_band: band,
            quota_ratio: ratio,
            quota_new_flow_weight: weight,
            ewma_ttft_ms: ewma_ttft,
            slow_streak,
            dual_shadow_price: shadow,
            total_dispatched: slot.dispatched.load(Ordering::Relaxed),
            proxy_wave_degraded: self.wave_guard.is_proxy_group_degraded(
                &meta.provider,
                &meta.proxy_group,
                now_ms,
                health_cfg,
            ),
        };
        let prov_view = ProviderRuntimeView {
            provider: meta.provider.clone(),
            wave_degraded: self
                .wave_guard
                .is_provider_degraded(&meta.provider, now_ms, health_cfg)
                || self
                    .wave_guard
                    .is_provider_slow_degraded(&meta.provider, now_ms, health_cfg),
            provider_ewma_ttft_ms: self.get_provider_ttft(&meta.provider),
            ..Default::default()
        };
        (view, prov_view)
    }
}

#[allow(clippy::too_many_arguments)]
fn build_decision(
    slot: &NodeSlot,
    view: &NodeRuntimeView<'_>,
    deadline_ms: u64,
    sticky_hit: bool,
    sticky_migrated: bool,
    score: f64,
    pool_candidates: usize,
    sampled_k: usize,
) -> ScheduleDecision {
    ScheduleDecision {
        slot_id: slot.slot_id,
        node_id: slot.id.clone(),
        provider: view.provider.to_string(),
        attempt_ttft_deadline_ms: deadline_ms,
        sticky_hit,
        telemetry: DecisionTelemetry {
            picked_slot: slot.slot_id.0,
            picked_provider: view.provider.to_string(),
            priority_tier: view.priority_tier,
            sticky_requested: sticky_hit || sticky_migrated,
            sticky_hit,
            sticky_migrated,
            quota_band: view.quota_band,
            quota_ratio: view.quota_ratio,
            quota_weight: view.quota_new_flow_weight,
            health_state: view.health,
            inflight_at_pick: view.inflight,
            max_concurrent: view.max_concurrent,
            ewma_ttft_ms: view.ewma_ttft_ms,
            attempt_deadline_ms: deadline_ms,
            cost_score: score,
            pool_candidates,
            sampled_k,
            dual_shadow_price: view.dual_shadow_price,
        },
    }
}

fn index_across_slices(slices: &[&[SlotId]], mut idx: usize) -> SlotId {
    for slice in slices {
        if idx < slice.len() {
            return slice[idx];
        }
        idx -= slice.len();
    }
    slices[0][0]
}

fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    a
}

fn coprime_stride(n: usize) -> usize {
    if n <= 1 {
        return 1;
    }
    let mut s = ((n as f64) * 0.618_033_988_75).round() as usize;
    s = s.clamp(1, n - 1);
    for _ in 0..n {
        if gcd(s, n) == 1 {
            return s;
        }
        s += 1;
        if s >= n {
            s = 1;
        }
    }
    1
}

fn update_min(slot: &mut Option<i64>, candidate: i64) {
    if candidate > 0 && slot.is_none_or(|cur| candidate < cur) {
        *slot = Some(candidate);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::{run_trace_replay, TraceItem};

    #[test]
    fn four_band_quota_hysteresis_prevents_boundary_oscillation() {
        let scheduler = Scheduler::default();
        let mut node = NodeDescriptor::new("acc-1", "codearts");
        node.initial_remaining_quota = Some(100.0);
        node.initial_total_quota = Some(100.0);
        let mut full_backup = NodeDescriptor::new("acc-full", "codearts");
        full_backup.initial_remaining_quota = Some(90.0);
        full_backup.initial_total_quota = Some(100.0);
        scheduler.sync_nodes(&[node, full_backup], 1_000);

        // 100% -> Full
        assert_eq!(scheduler.update_quota("acc-1", 25.0, Some(100.0)), Some(QuotaBand::Full));
        // Drop to 15% (<= B1_down 20%) -> Throttle
        assert_eq!(scheduler.update_quota("acc-1", 15.0, Some(100.0)), Some(QuotaBand::Throttle));
        // Slight bounce to 25% (< B1_up 30%) -> stays in Throttle (no chattering!)
        assert_eq!(scheduler.update_quota("acc-1", 25.0, Some(100.0)), Some(QuotaBand::Throttle));
        // Drop to 4% (<= B2_down 5%) -> DrainOnly (sticky allowed, new flow prefers Full backup!)
        assert_eq!(scheduler.update_quota("acc-1", 4.0, Some(100.0)), Some(QuotaBand::DrainOnly));

        let providers = ["codearts"];
        let new_req = ScheduleRequest {
            model: "glm-5",
            allowed_providers: &providers,
            sticky_key: "",
            sticky_ttl_ms: 0,
            excluded_node_ids: &[],
            route_excluded_ids: None,
            route_allowed_ids: None,
            pinned_node_id: None,
            attempt_index: 0,
            now_ms: 2_000,
        };
        // With acc-full healthy, new flow strictly avoids DrainOnly acc-1
        let (dec_full, lease_full) = scheduler.schedule(&new_req).expect("picks full backup");
        assert_eq!(dec_full.node_id, "acc-full");
        drop(lease_full);

        // When only acc-1 (DrainOnly) remains, drain_fallback still serves instead of 503
        assert_eq!(scheduler.update_quota("acc-full", 0.0, Some(100.0)), Some(QuotaBand::Exhausted));
        let (dec_drain, lease_drain) = scheduler.schedule(&new_req).expect("drain_fallback serves");
        assert_eq!(dec_drain.node_id, "acc-1");
        assert_eq!(dec_drain.telemetry.quota_band, QuotaBand::DrainOnly);
        drop(lease_drain);

        // Drop acc-1 to 0.2% (<= B3_down 0.5%) -> Exhausted -> strictly rejects!
        assert_eq!(scheduler.update_quota("acc-1", 0.2, Some(100.0)), Some(QuotaBand::Exhausted));
        assert_eq!(scheduler.schedule(&new_req).unwrap_err(), RejectReason::AllQuotaExhausted);
        // Small refund to 6% (< B2_up 10%) -> MUST remain Exhausted (enablement hysteresis!)
        assert_eq!(scheduler.update_quota("acc-1", 6.0, Some(100.0)), Some(QuotaBand::Exhausted));
        // Replenish to 12% (>= B2_up 10%) -> recovers to Throttle (in Probation single-flight mode!)
        assert_eq!(scheduler.update_quota("acc-1", 12.0, Some(100.0)), Some(QuotaBand::Throttle));
        let (dec, _lease) = scheduler.schedule(&new_req).expect("recovered node accepts probe");
        assert_eq!(dec.telemetry.health_state, HealthState::Probation);
    }

    #[test]
    fn drain_only_preserves_existing_sticky_session_while_blocking_new_sessions() {
        let scheduler = Scheduler::default();
        let mut n1 = NodeDescriptor::new("acc-sticky", "loomy");
        n1.initial_remaining_quota = Some(80.0);
        n1.initial_total_quota = Some(100.0);
        let mut n2 = NodeDescriptor::new("acc-fresh", "loomy");
        n2.initial_remaining_quota = Some(90.0);
        n2.initial_total_quota = Some(100.0);
        scheduler.sync_nodes(&[n1, n2], 1_000);

        let providers = ["loomy"];
        let req1 = ScheduleRequest {
            model: "claude-4",
            allowed_providers: &providers,
            sticky_key: "ctx:userA",
            sticky_ttl_ms: 300_000,
            excluded_node_ids: &[],
            route_excluded_ids: None,
            route_allowed_ids: None,
            pinned_node_id: None,
            attempt_index: 0,
            now_ms: 1_000,
        };
        let (dec1, lease1) = scheduler.schedule(&req1).unwrap();
        let sticky_node = dec1.node_id.clone();
        let other_node = if sticky_node == "acc-sticky" {
            "acc-fresh"
        } else {
            "acc-sticky"
        };
        drop(lease1);
        scheduler.record_feedback(AttemptFeedback {
            node_id: sticky_node.clone(),
            model: "claude-4".into(),
            sticky_key: "ctx:userA".into(),
            sticky_ttl_ms: 300_000,
            kind: AttemptFeedbackKind::Success {
                ttft_ms: Some(1_200),
                total_ms: 3_000,
                prompt_tokens: 2_000,
                completion_tokens: 300,
                cached_tokens: 0,
                credit_used: None,
            },
            now_ms: 4_000,
        });

        // Push sticky_node into DrainOnly (3% remaining <= B2_down 5%)
        assert_eq!(scheduler.update_quota(&sticky_node, 3.0, Some(100.0)), Some(QuotaBand::DrainOnly));

        // Existing sticky userA STILL hits sticky_node to preserve prompt cache!
        let (dec_sticky, _lease_s) = scheduler.schedule(&req1).expect("sticky preserved in DrainOnly");
        assert!(dec_sticky.sticky_hit);
        assert_eq!(dec_sticky.node_id, sticky_node);
        assert_eq!(dec_sticky.telemetry.quota_band, QuotaBand::DrainOnly);
        drop(_lease_s);

        // Brand-new userB is routed to Full other_node instead of DrainOnly sticky_node!
        let req_new = ScheduleRequest {
            sticky_key: "ctx:userB",
            ..req1
        };
        for _ in 0..10 {
            let (dec_b, lease_b) = scheduler.schedule(&req_new).expect("new session avoids DrainOnly");
            assert_eq!(dec_b.node_id, other_node);
            drop(lease_b);
        }

        // If other_node also becomes Exhausted, DrainOnly fallback still keeps userB alive
        assert_eq!(scheduler.update_quota(other_node, 0.0, Some(100.0)), Some(QuotaBand::Exhausted));
        let (dec_fallback, lease_fb) = scheduler.schedule(&req_new).expect("drain_fallback keeps serving");
        assert_eq!(dec_fallback.node_id, sticky_node);
        drop(lease_fb);

        // Once sticky_node drops to Exhausted (0.2%), new session is rejected
        assert_eq!(scheduler.update_quota(&sticky_node, 0.2, Some(100.0)), Some(QuotaBand::Exhausted));
        assert_eq!(scheduler.schedule(&req_new).unwrap_err(), RejectReason::AllQuotaExhausted);
    }

    #[test]
    fn global_atomic_max_concurrent_never_exceeded_under_contention() {
        let scheduler = Arc::new(Scheduler::default());
        let mut node = NodeDescriptor::new("acc-cap2", "codearts");
        node.max_concurrent = 2;
        scheduler.sync_nodes(&[node], 1_000);

        let providers = ["codearts"];
        let req = ScheduleRequest {
            model: "glm-5",
            allowed_providers: &providers,
            sticky_key: "",
            sticky_ttl_ms: 0,
            excluded_node_ids: &[],
            route_excluded_ids: None,
            route_allowed_ids: None,
            pinned_node_id: None,
            attempt_index: 0,
            now_ms: 1_000,
        };
        let (_d1, l1) = scheduler.schedule(&req).expect("slot 1");
        let (_d2, l2) = scheduler.schedule(&req).expect("slot 2");
        assert_eq!(scheduler.schedule(&req).unwrap_err(), RejectReason::AllBusy);
        drop(l1);
        let (_d3, _l3) = scheduler.schedule(&req).expect("slot freed after drop");
        drop(l2);
    }

    #[test]
    fn wave_guard_prevents_mass_quarantine_on_upstream_outage() {
        let scheduler = Scheduler::default();
        let nodes: Vec<_> = (0..10)
            .map(|i| NodeDescriptor::new(format!("loomy-{i}"), "loomy"))
            .collect();
        scheduler.sync_nodes(&nodes, 10_000);

        // First 4 isolated dead-credential errors quarantine individual accounts
        for i in 0..4 {
            let action = scheduler.record_feedback(AttemptFeedback {
                node_id: format!("loomy-{i}"),
                model: "m".into(),
                sticky_key: String::new(),
                sticky_ttl_ms: 0,
                kind: AttemptFeedbackKind::CredentialDead {
                    reason: "登录已失效".into(),
                },
                now_ms: 10_100 + i * 50,
            });
            assert!(matches!(action, HostAction::QuarantineAccount { .. }));
        }

        // 5th distinct account within 30s trips WaveGuard -> protects 5..10 with TempBackoff instead of Quarantine!
        for i in 4..8 {
            let action = scheduler.record_feedback(AttemptFeedback {
                node_id: format!("loomy-{i}"),
                model: "m".into(),
                sticky_key: String::new(),
                sticky_ttl_ms: 0,
                kind: AttemptFeedbackKind::CredentialDead {
                    reason: "登录已失效".into(),
                },
                now_ms: 10_500 + i * 50,
            });
            assert!(matches!(action, HostAction::WaveAlert { .. }));
        }
    }

    #[test]
    fn power_of_k_spreads_load_evenly_across_thousand_account_pool() {
        let scheduler = Scheduler::default();
        let nodes: Vec<_> = (0..1_000)
            .map(|i| NodeDescriptor::new(format!("ca-{i:04}"), "codearts"))
            .collect();
        let trace: Vec<_> = (0..5_000)
            .map(|t| TraceItem {
                arrival_ms: 1_000 + t * 10,
                model: "glm-5.1".into(),
                providers: vec!["codearts".into()],
                sticky_key: String::new(),
                simulated_ttft_ms: 800,
                simulated_duration_ms: 2_000,
                credit_cost: 0.1,
            })
            .collect();

        let summary = run_trace_replay(&scheduler, &nodes, &trace);
        assert_eq!(summary.scheduled_ok, 5_000);
        assert!(
            summary.load_gini < 0.15,
            "expected uniform load spread across 1000 accounts, got Gini={:.4}",
            summary.load_gini
        );
    }

    #[test]
    fn priority_tier_failover_and_clear_cooldown_restores_primary() {
        let scheduler = Scheduler::default();
        let mut first = NodeDescriptor::new("acct-first", "prov-first");
        first.priority_tier = 0;
        let mut second = NodeDescriptor::new("acct-second", "prov-second");
        second.priority_tier = 1;
        scheduler.sync_nodes(&[first, second], 1_000);

        let providers = ["prov-first", "prov-second"];
        let req = ScheduleRequest {
            model: "fallback-model",
            allowed_providers: &providers,
            sticky_key: "",
            sticky_ttl_ms: 0,
            excluded_node_ids: &[],
            route_excluded_ids: None,
            route_allowed_ids: None,
            pinned_node_id: None,
            attempt_index: 0,
            now_ms: 1_000,
        };

        let (d1, _l1) = scheduler.schedule(&req).expect("tier 0 selected");
        assert_eq!(d1.node_id, "acct-first");

        // Rate-limit primary -> fails over to tier 1
        scheduler.record_feedback(AttemptFeedback {
            node_id: "acct-first".into(),
            model: "fallback-model".into(),
            sticky_key: String::new(),
            sticky_ttl_ms: 0,
            kind: AttemptFeedbackKind::RateLimited {
                reset_at_ms: Some(31_000),
            },
            now_ms: 1_000,
        });
        let (d2, _l2) = scheduler.schedule(&req).expect("tier 1 fallback selected");
        assert_eq!(d2.node_id, "acct-second");

        // Explicit admin clear_cooldown immediately restores tier 0 before 31_000ms
        assert!(scheduler.clear_cooldown("acct-first", Some("fallback-model")));
        let (d3, _l3) = scheduler.schedule(&req).expect("tier 0 restored");
        assert_eq!(d3.node_id, "acct-first");
    }

    #[test]
    fn odd_pool_size_visits_all_slots_and_reroutes_around_cooldown_and_exhausted() {
        for pool_n in [65usize, 93usize] {
            let scheduler = Scheduler::default();
            let nodes: Vec<_> = (0..pool_n)
                .map(|i| NodeDescriptor::new(format!("odd-{i}"), "raccoon"))
                .collect();
            scheduler.sync_nodes(&nodes, 1_000);

            // Put first pool_n - 1 nodes into cooldown or exhausted, leaving only the last one healthy.
            for i in 0..(pool_n - 1) {
                if i % 2 == 0 {
                    scheduler.record_feedback(AttemptFeedback {
                        node_id: format!("odd-{i}"),
                        model: "deepseek-v4.1-flash".into(),
                        sticky_key: String::new(),
                        sticky_ttl_ms: 0,
                        kind: AttemptFeedbackKind::RateLimited {
                            reset_at_ms: Some(60_000),
                        },
                        now_ms: 1_000,
                    });
                } else {
                    scheduler.update_quota(&format!("odd-{i}"), 0.0, Some(100.0));
                }
            }

            let providers = ["raccoon"];
            let req = ScheduleRequest {
                model: "deepseek-v4.1-flash",
                allowed_providers: &providers,
                sticky_key: "",
                sticky_ttl_ms: 0,
                excluded_node_ids: &[],
                route_excluded_ids: None,
                route_allowed_ids: None,
                pinned_node_id: None,
                attempt_index: 0,
                now_ms: 2_000,
            };
            let expected = format!("odd-{}", pool_n - 1);
            for _ in 0..100 {
                let (dec, lease) = scheduler
                    .schedule(&req)
                    .expect("coprime stride must find the sole healthy slot");
                assert_eq!(dec.node_id, expected);
                drop(lease);
            }
        }
    }

    #[test]
    fn even_pool_size_finds_healthy_slots_past_contiguous_exhausted_run() {
        let scheduler = Scheduler::default();
        let nodes: Vec<_> = (0..42)
            .map(|i| {
                let mut d = NodeDescriptor::new(format!("ca-{i:02}"), "codearts");
                d.initial_remaining_quota = Some(if i < 30 { 0.0 } else { 80.0 });
                d.initial_total_quota = Some(100.0);
                d
            })
            .collect();
        scheduler.sync_nodes(&nodes, 1_000);

        let providers = ["codearts"];
        let req = ScheduleRequest {
            model: "glm-5.3",
            allowed_providers: &providers,
            sticky_key: "",
            sticky_ttl_ms: 0,
            excluded_node_ids: &[],
            route_excluded_ids: None,
            route_allowed_ids: None,
            pinned_node_id: None,
            attempt_index: 0,
            now_ms: 2_000,
        };
        let mut counts = vec![0u64; 12];
        for _ in 0..240 {
            let (dec, lease) = scheduler
                .schedule(&req)
                .expect("must skip past 30 contiguous exhausted slots");
            let idx: usize = dec.node_id.strip_prefix("ca-").unwrap().parse().unwrap();
            assert!(idx >= 30);
            counts[idx - 30] += 1;
            drop(lease);
        }
        let gini = replay::gini_coefficient(&counts);
        assert!(gini < 0.15, "expected uniform spread across 12 healthy slots, got gini={gini:.4}");
    }

    #[test]
    fn soft_cooldown_fallback_picks_earliest_recovery_when_all_cooling() {
        let scheduler = Scheduler::default();
        let nodes: Vec<_> = (0..3)
            .map(|i| NodeDescriptor::new(format!("cool-{i}"), "codearts"))
            .collect();
        scheduler.sync_nodes(&nodes, 1_000);

        for (i, reset) in [(0, 90_000), (1, 15_000), (2, 45_000)] {
            scheduler.record_feedback(AttemptFeedback {
                node_id: format!("cool-{i}"),
                model: "glm-5.3".into(),
                sticky_key: String::new(),
                sticky_ttl_ms: 0,
                kind: AttemptFeedbackKind::RateLimited {
                    reset_at_ms: Some(reset),
                },
                now_ms: 1_000,
            });
        }

        let providers = ["codearts"];
        let req = ScheduleRequest {
            model: "glm-5.3",
            allowed_providers: &providers,
            sticky_key: "",
            sticky_ttl_ms: 0,
            excluded_node_ids: &[],
            route_excluded_ids: None,
            route_allowed_ids: None,
            pinned_node_id: None,
            attempt_index: 0,
            now_ms: 2_000,
        };
        let (dec1, lease1) = scheduler
            .schedule(&req)
            .expect("soft_cooldown_fallback should pick earliest recovery");
        assert_eq!(dec1.node_id, "cool-1");
        let (dec2, lease2) = scheduler
            .schedule(&req)
            .expect("second concurrent request probes next earliest recovery");
        assert_eq!(dec2.node_id, "cool-2");
        let (dec3, lease3) = scheduler
            .schedule(&req)
            .expect("third concurrent request probes remaining cooling node");
        assert_eq!(dec3.node_id, "cool-0");
        // Once every cooling node already has an in-flight probe, further requests get AllCoolingDown.
        assert!(matches!(
            scheduler.schedule(&req).unwrap_err(),
            RejectReason::AllCoolingDown { .. }
        ));
        drop(lease1);
        let (dec_reprobe, lease_reprobe) = scheduler
            .schedule(&req)
            .expect("freed earliest node accepts next probe");
        assert_eq!(dec_reprobe.node_id, "cool-1");
        drop((lease2, lease3, lease_reprobe));
    }

    #[test]
    fn slow_wave_spills_over_to_backup_provider_and_recovers_on_fast_success() {
        let scheduler = Scheduler::default();
        let nodes = (0..3)
            .map(|i| {
                let mut d = NodeDescriptor::new(format!("loomy-{i}"), "loomy");
                d.priority_tier = 0;
                d
            })
            .chain((0..2).map(|i| {
                let mut d = NodeDescriptor::new(format!("backup-{i}"), "codearts");
                d.priority_tier = 1;
                d
            }))
            .collect::<Vec<_>>();
        scheduler.sync_nodes(&nodes, 1_000);

        let providers = ["loomy", "codearts"];
        // Two distinct loomy accounts time out -> trips provider slow-wave guard.
        let a1 = scheduler.record_feedback(AttemptFeedback {
            node_id: "loomy-0".into(),
            model: "glm-5.3-flash".into(),
            sticky_key: "s1".into(),
            sticky_ttl_ms: 60_000,
            kind: AttemptFeedbackKind::TtftTimeout { elapsed_ms: 35_000 },
            now_ms: 10_000,
        });
        assert_eq!(a1, HostAction::None);

        let a2 = scheduler.record_feedback(AttemptFeedback {
            node_id: "loomy-1".into(),
            model: "glm-5.3-flash".into(),
            sticky_key: "s2".into(),
            sticky_ttl_ms: 60_000,
            kind: AttemptFeedbackKind::TtftTimeout { elapsed_ms: 35_000 },
            now_ms: 11_000,
        });
        assert!(matches!(
            a2,
            HostAction::WaveAlert {
                ref provider,
                recent_failures: 2,
                ..
            } if provider == "loomy"
        ));

        // Even though loomy-2 is Active and tier 0, slow-wave spills immediately over to tier 1 codearts!
        let req = ScheduleRequest {
            model: "glm-5.3-flash",
            allowed_providers: &providers,
            sticky_key: "",
            sticky_ttl_ms: 0,
            excluded_node_ids: &[],
            route_excluded_ids: None,
            route_allowed_ids: None,
            pinned_node_id: None,
            attempt_index: 0,
            now_ms: 12_000,
        };
        let (dec, lease) = scheduler.schedule(&req).expect("spills to healthy tier 1 backup");
        assert_eq!(dec.provider, "codearts");
        drop(lease);

        // Once a loomy probe succeeds fast, slow wave clears and tier 0 resumes.
        scheduler.record_feedback(AttemptFeedback {
            node_id: "loomy-2".into(),
            model: "glm-5.3-flash".into(),
            sticky_key: String::new(),
            sticky_ttl_ms: 0,
            kind: AttemptFeedbackKind::Success {
                ttft_ms: Some(1_500),
                total_ms: 3_000,
                prompt_tokens: 100,
                completion_tokens: 20,
                cached_tokens: 0,
                credit_used: None,
            },
            now_ms: 15_000,
        });
        let (dec_rec, _lease_rec) = scheduler.schedule(&req).expect("tier 0 loomy restored");
        assert_eq!(dec_rec.provider, "loomy");
    }

    #[test]
    fn slow_wave_without_backup_probes_once_and_fails_fast_on_retry() {
        let scheduler = Scheduler::default();
        let nodes: Vec<_> = (0..4)
            .map(|i| NodeDescriptor::new(format!("loomy-{i}"), "loomy"))
            .collect();
        scheduler.sync_nodes(&nodes, 1_000);

        for i in 0..2 {
            scheduler.record_feedback(AttemptFeedback {
                node_id: format!("loomy-{i}"),
                model: "qwen3.8-flash".into(),
                sticky_key: String::new(),
                sticky_ttl_ms: 0,
                kind: AttemptFeedbackKind::TtftTimeout { elapsed_ms: 35_000 },
                now_ms: 10_000 + i * 500,
            });
        }

        let providers = ["loomy"];
        let req_probe = ScheduleRequest {
            model: "qwen3.8-flash",
            allowed_providers: &providers,
            sticky_key: "",
            sticky_ttl_ms: 0,
            excluded_node_ids: &[],
            route_excluded_ids: None,
            route_allowed_ids: None,
            pinned_node_id: None,
            attempt_index: 0,
            now_ms: 12_000,
        };
        let (probe_dec, probe_lease) = scheduler
            .schedule(&req_probe)
            .expect("attempt 0 allows one probe when no backup provider exists");
        assert_eq!(probe_dec.provider, "loomy");
        drop(probe_lease);

        let excluded = [probe_dec.node_id];
        let req_retry = ScheduleRequest {
            excluded_node_ids: &excluded,
            attempt_index: 1,
            ..req_probe
        };
        assert!(matches!(
            scheduler.schedule(&req_retry).unwrap_err(),
            RejectReason::AllCoolingDown { .. }
        ));
    }

    #[test]
    fn cold_account_on_slow_provider_does_not_beat_warm_fast_provider() {
        let scheduler = Scheduler::default();
        let mut fast = NodeDescriptor::new("fast-0", "autoclaw");
        fast.priority_tier = 0;
        let mut slow_warm = NodeDescriptor::new("slow-0", "loomy");
        slow_warm.priority_tier = 0;
        let mut slow_cold = NodeDescriptor::new("slow-1", "loomy");
        slow_cold.priority_tier = 0;
        scheduler.sync_nodes(&[fast, slow_warm, slow_cold], 1_000);

        scheduler.record_feedback(AttemptFeedback {
            node_id: "fast-0".into(),
            model: "m".into(),
            sticky_key: String::new(),
            sticky_ttl_ms: 0,
            kind: AttemptFeedbackKind::Success {
                ttft_ms: Some(12_000),
                total_ms: 14_000,
                prompt_tokens: 10,
                completion_tokens: 10,
                cached_tokens: 0,
                credit_used: None,
            },
            now_ms: 2_000,
        });
        scheduler.record_feedback(AttemptFeedback {
            node_id: "slow-0".into(),
            model: "m".into(),
            sticky_key: String::new(),
            sticky_ttl_ms: 0,
            kind: AttemptFeedbackKind::Success {
                ttft_ms: Some(24_000),
                total_ms: 26_000,
                prompt_tokens: 10,
                completion_tokens: 10,
                cached_tokens: 0,
                credit_used: None,
            },
            now_ms: 2_500,
        });

        let providers = ["loomy", "autoclaw"];
        let req = ScheduleRequest {
            model: "m",
            allowed_providers: &providers,
            sticky_key: "",
            sticky_ttl_ms: 0,
            excluded_node_ids: &[],
            route_excluded_ids: None,
            route_allowed_ids: None,
            pinned_node_id: None,
            attempt_index: 0,
            now_ms: 3_000,
        };
        for _ in 0..8 {
            let (dec, lease) = scheduler
                .schedule(&req)
                .expect("should pick verified faster provider over cold/slow provider");
            assert_eq!(dec.provider, "autoclaw");
            assert_eq!(dec.node_id, "fast-0");
            drop(lease);
        }
    }

    #[test]
    fn adaptive_ttft_deadline_tightens_for_fast_provider_without_shrinking_below_floor() {
        let scheduler = Scheduler::default();
        scheduler.sync_nodes(&[NodeDescriptor::new("l0", "loomy")], 1_000);

        let providers = ["loomy"];
        let req = ScheduleRequest {
            model: "m",
            allowed_providers: &providers,
            sticky_key: "",
            sticky_ttl_ms: 0,
            excluded_node_ids: &[],
            route_excluded_ids: None,
            route_allowed_ids: None,
            pinned_node_id: None,
            attempt_index: 0,
            now_ms: 2_000,
        };
        let (d_cold, l_cold) = scheduler.schedule(&req).unwrap();
        assert_eq!(d_cold.attempt_ttft_deadline_ms, 35_000);
        drop(l_cold);

        // Ultra-fast TTFT (2_000ms) -> 2_000 * 2.5 + 8_000 = 13_000 -> clamped to 15_000 floor.
        scheduler.record_feedback(AttemptFeedback {
            node_id: "l0".into(),
            model: "m".into(),
            sticky_key: String::new(),
            sticky_ttl_ms: 0,
            kind: AttemptFeedbackKind::Success {
                ttft_ms: Some(2_000),
                total_ms: 3_000,
                prompt_tokens: 10,
                completion_tokens: 10,
                cached_tokens: 0,
                credit_used: None,
            },
            now_ms: 3_000,
        });
        let (d_floor, l_floor) = scheduler.schedule(&req).unwrap();
        assert_eq!(d_floor.attempt_ttft_deadline_ms, 15_000);
        drop(l_floor);

        // Fresh scheduler with 6_000ms EWMA -> 6_000 * 2.5 + 8_000 = 23_000 (tighter than 35_000 base).
        let scheduler2 = Scheduler::default();
        scheduler2.sync_nodes(&[NodeDescriptor::new("l1", "loomy")], 1_000);
        scheduler2.record_feedback(AttemptFeedback {
            node_id: "l1".into(),
            model: "m".into(),
            sticky_key: String::new(),
            sticky_ttl_ms: 0,
            kind: AttemptFeedbackKind::Success {
                ttft_ms: Some(6_000),
                total_ms: 9_000,
                prompt_tokens: 10,
                completion_tokens: 10,
                cached_tokens: 0,
                credit_used: None,
            },
            now_ms: 2_000,
        });
        let (d_mid, _l_mid) = scheduler2.schedule(&req).unwrap();
        assert_eq!(d_mid.attempt_ttft_deadline_ms, 23_000);
    }

    #[test]
    fn proxy_group_wave_penalizes_healthy_nodes_sharing_degraded_egress() {
        let scheduler = Scheduler::default();
        let mut nodes = Vec::new();
        for i in 0..6 {
            let mut d = NodeDescriptor::new(format!("bad-{i}"), "codearts");
            d.proxy_group = "bad-egress".into();
            d.max_concurrent = 4;
            nodes.push(d);
        }
        for i in 0..4 {
            let mut d = NodeDescriptor::new(format!("good-{i}"), "codearts");
            d.proxy_group = "good-egress".into();
            d.max_concurrent = 4;
            nodes.push(d);
        }
        scheduler.sync_nodes(&nodes, 1_000);

        // 5 distinct accounts failing on bad-egress trip WaveGuard on x:codearts:bad-egress;
        // bad-5 remains Active, but inherits the proxy_wave_degraded routing penalty.
        for i in 0..5 {
            scheduler.record_feedback(AttemptFeedback {
                node_id: format!("bad-{i}"),
                model: "glm-5.3".into(),
                sticky_key: String::new(),
                sticky_ttl_ms: 0,
                kind: AttemptFeedbackKind::TransientFailure {
                    is_network_or_proxy: true,
                },
                now_ms: 10_000 + i * 100,
            });
        }

        let providers = ["codearts"];
        let req = ScheduleRequest {
            model: "glm-5.3",
            allowed_providers: &providers,
            sticky_key: "",
            sticky_ttl_ms: 0,
            excluded_node_ids: &[],
            route_excluded_ids: None,
            route_allowed_ids: None,
            pinned_node_id: None,
            attempt_index: 0,
            now_ms: 11_000,
        };
        for _ in 0..40 {
            let (dec, lease) = scheduler.schedule(&req).expect("selects healthy egress node");
            assert!(
                dec.node_id.starts_with("good-"),
                "expected proxy_wave_degraded bad-5 to lose to good-egress nodes, got {}",
                dec.node_id
            );
            drop(lease);
        }
    }
}
