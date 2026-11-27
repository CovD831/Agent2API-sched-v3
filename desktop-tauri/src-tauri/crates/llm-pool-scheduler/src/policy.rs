//! Pluggable scheduling policies:
//! - [`HysteresisLoadBalancePolicy`] (Phase 1): Generalized Power-of-$k$ load balancing with
//!   4-band quota hysteresis, EWMA TTFT penalty, and per-provider rescue deadlines.
//! - [`PrimalDualCongestionPolicy`] (Phase 2 thesis): Wardrop/Beckmann marginal congestion cost
//!   paired with online Lagrangian shadow prices $\mu_i(t)$ for quota pacing.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::health::HealthState;
use crate::types::{NodeRuntimeView, ProviderRuntimeView, ScheduleRequest};

/// Trait implemented by pluggable routing & stopping-time controllers.
pub trait SchedulingPolicy: Send + Sync + 'static {
    /// Number of active candidates sampled per tier (`k` in Power-of-$k$ Choices).
    fn sample_size(&self) -> usize {
        8
    }

    /// Computes instantaneous routing cost $C_i$ (**lower is better**), or `None` if rejected.
    fn evaluate_cost(
        &self,
        node: &NodeRuntimeView<'_>,
        provider: &ProviderRuntimeView,
        req: &ScheduleRequest<'_>,
        is_sticky_hit: bool,
    ) -> Option<f64>;

    /// Computes per-attempt first-byte (TTFT) rescue deadline $D_p$ in milliseconds.
    fn attempt_ttft_deadline_ms(
        &self,
        node: &NodeRuntimeView<'_>,
        provider: &ProviderRuntimeView,
        attempt_index: u8,
    ) -> u64;
}

/// Configuration for Phase-1 [`HysteresisLoadBalancePolicy`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HysteresisPolicyConfig {
    /// Sample size $k$ for Generalized Power-of-$k$ selection (default `8`).
    pub sample_k: usize,
    /// Weight $\alpha$ on normalized in-flight utilization $\frac{\text{inflight}+1}{\text{cap}}$ (default `1.0`).
    pub load_weight: f64,
    /// Weight $\beta$ on normalized EWMA TTFT $\frac{\widehat{\text{TTFT}}}{D_p}$ (default `0.65`).
    pub latency_weight: f64,
    /// Weight $\eta$ on quota throttle penalty $(1 - w_{\text{quota}}(q))$ (default `0.85`).
    pub quota_penalty_weight: f64,
    /// Bonus subtracted on sticky session hit to preserve prompt cache unless overloaded (default `2.5`).
    pub sticky_affinity_bonus: f64,
    /// Penalty added when `Probation` half-open state is active so healthy nodes take bulk load while probes still trickle through (default `0.35`).
    pub probation_penalty: f64,
    /// Penalty added when provider WaveGuard is degraded (default `1.5`).
    pub wave_degraded_penalty: f64,
    /// Default per-attempt TTFT rescue deadline in ms when provider has no custom override (default `22_000`).
    pub default_ttft_deadline_ms: u64,
    /// Per-provider base TTFT rescue deadlines in ms (e.g. fast channels `12_000`, deep-thinking channels `28_000`).
    pub provider_ttft_deadlines_ms: HashMap<String, u64>,
}

impl Default for HysteresisPolicyConfig {
    fn default() -> Self {
        let mut map = HashMap::new();
        map.insert("autoclaw".to_string(), 18_000);
        map.insert("loomy".to_string(), 35_000);
        map.insert("minimax-code".to_string(), 22_000);
        map.insert("trae".to_string(), 24_000);
        map.insert("officeace".to_string(), 30_000);
        map.insert("codearts".to_string(), 35_000);
        map.insert("raccoon".to_string(), 35_000);
        Self {
            sample_k: 8,
            load_weight: 1.0,
            latency_weight: 0.65,
            quota_penalty_weight: 0.85,
            sticky_affinity_bonus: 2.5,
            probation_penalty: 0.35,
            wave_degraded_penalty: 1.5,
            default_ttft_deadline_ms: 30_000,
            provider_ttft_deadlines_ms: map,
        }
    }
}

/// Phase-1 production controller combining Power-of-$k$ load spreading, 4-band quota hysteresis, and TTFT rescue.
#[derive(Clone, Debug, Default)]
pub struct HysteresisLoadBalancePolicy {
    pub config: HysteresisPolicyConfig,
}

impl HysteresisLoadBalancePolicy {
    pub fn new(config: HysteresisPolicyConfig) -> Self {
        Self { config }
    }
}

impl SchedulingPolicy for HysteresisLoadBalancePolicy {
    fn sample_size(&self) -> usize {
        self.config.sample_k.max(2)
    }

    fn evaluate_cost(
        &self,
        node: &NodeRuntimeView<'_>,
        provider: &ProviderRuntimeView,
        req: &ScheduleRequest<'_>,
        is_sticky_hit: bool,
    ) -> Option<f64> {
        if matches!(node.health, HealthState::Quarantined | HealthState::SlowCooldown | HealthState::TempBackoff) {
            return None;
        }
        if matches!(node.health, HealthState::Probation) && node.inflight >= 1 {
            return None;
        }
        if node.max_concurrent > 0 && node.inflight >= node.max_concurrent {
            return None;
        }
        if is_sticky_hit {
            if !node.quota_band.allows_sticky() {
                return None;
            }
        } else if !node.quota_band.allows_new_flow() {
            return None;
        }

        let cap = node.max_concurrent.max(4) as f64;
        let util = (node.inflight as f64 + 1.0) / cap;
        let deadline = self
            .attempt_ttft_deadline_ms(node, provider, req.attempt_index)
            .max(1_000) as f64;
        let norm_ttft = if node.ewma_ttft_ms > 0.0 {
            (node.ewma_ttft_ms / deadline).min(3.0)
        } else {
            // Unobserved cold account gets a true optimistic prior so unused pool accounts warm up smoothly.
            0.0
        };
        let quota_penalty = if is_sticky_hit {
            0.0
        } else {
            1.0 - node.quota_new_flow_weight.clamp(0.0, 1.0)
        };

        let mut cost = (node.priority_tier as f64) * 10.0
            + self.config.load_weight * util
            + self.config.latency_weight * norm_ttft
            + self.config.quota_penalty_weight * quota_penalty;

        if matches!(node.health, HealthState::Probation) {
            cost += self.config.probation_penalty;
        }
        if provider.wave_degraded {
            cost += self.config.wave_degraded_penalty;
        }
        if is_sticky_hit {
            cost -= self.config.sticky_affinity_bonus;
        }
        Some(cost)
    }

    fn attempt_ttft_deadline_ms(
        &self,
        _node: &NodeRuntimeView<'_>,
        provider: &ProviderRuntimeView,
        attempt_index: u8,
    ) -> u64 {
        let base = self
            .config
            .provider_ttft_deadlines_ms
            .get(&provider.provider)
            .copied()
            .unwrap_or(self.config.default_ttft_deadline_ms);
        // Slightly relax deadline on final fallback attempt to avoid premature abort when all fast channels failed.
        if attempt_index == 0 {
            base
        } else {
            base.saturating_add(u64::from(attempt_index) * 10_000).min(65_000)
        }
    }
}

/// Phase-2 mathematical optimization controller: Primal-Dual Shadow-Price Pacing + Beckmann Marginal Congestion Cost.
///
/// Minimizes $\sum_i \int_0^{x_i} d_i(u)\,du + \sum_i \mu_i(t) x_i - \Delta c_{\text{cache}} \mathbb{I}_{\text{sticky}}$,
/// where $d_i(u) = d_{0,i}\bigl(1 + \kappa (\frac{u}{c_i})^p\bigr)$ is the BPR/M/M/c road-network link delay function
/// and $\mu_i(t)$ is the online dual variable updated via mirror descent / subgradient pacing.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrimalDualCongestionPolicy {
    pub sample_k: usize,
    /// BPR congestion scale $\kappa$ (default `0.8`).
    pub bpr_kappa: f64,
    /// BPR power exponent $p$ (default `2.0`, road-network / queueing convexity).
    pub bpr_power: f64,
    /// Cache preservation value $\Delta c_{\text{cache}}$ in equivalent ms cost.
    pub cache_value_ms: f64,
    /// Scaling factor converting dual shadow price $\mu_i$ into ms-equivalent marginal cost.
    pub shadow_price_scale: f64,
    pub base_deadline_ms: u64,
}

impl Default for PrimalDualCongestionPolicy {
    fn default() -> Self {
        Self {
            sample_k: 12,
            bpr_kappa: 0.8,
            bpr_power: 2.0,
            cache_value_ms: 8_000.0,
            shadow_price_scale: 5_000.0,
            base_deadline_ms: 20_000,
        }
    }
}

impl SchedulingPolicy for PrimalDualCongestionPolicy {
    fn sample_size(&self) -> usize {
        self.sample_k.max(2)
    }

    fn evaluate_cost(
        &self,
        node: &NodeRuntimeView<'_>,
        provider: &ProviderRuntimeView,
        _req: &ScheduleRequest<'_>,
        is_sticky_hit: bool,
    ) -> Option<f64> {
        if matches!(node.health, HealthState::Quarantined | HealthState::SlowCooldown | HealthState::TempBackoff) {
            return None;
        }
        if matches!(node.health, HealthState::Probation) && node.inflight >= 1 {
            return None;
        }
        if node.max_concurrent > 0 && node.inflight >= node.max_concurrent {
            return None;
        }
        if is_sticky_hit {
            if !node.quota_band.allows_sticky() {
                return None;
            }
        } else if !node.quota_band.allows_new_flow() {
            return None;
        }

        let d0 = if node.ewma_ttft_ms > 0.0 {
            node.ewma_ttft_ms
        } else if provider.provider_ewma_ttft_ms > 0.0 {
            provider.provider_ewma_ttft_ms
        } else {
            4_000.0
        };
        let cap = node.max_concurrent.max(4) as f64;
        let flow_ratio = (node.inflight as f64 + 1.0) / cap;
        // System-optimal marginal delay cost: d(x) + x * d'(x)
        let bpr_term = self.bpr_kappa * flow_ratio.powf(self.bpr_power);
        let marginal_delay_ms = d0 * (1.0 + (1.0 + self.bpr_power) * bpr_term);
        let shadow_cost = self.shadow_price_scale * node.dual_shadow_price
            + if !is_sticky_hit {
                self.shadow_price_scale * (1.0 - node.quota_new_flow_weight)
            } else {
                0.0
            };
        let tier_offset = (node.priority_tier as f64) * 25_000.0;
        let cache_rebate = if is_sticky_hit { self.cache_value_ms } else { 0.0 };
        Some(tier_offset + marginal_delay_ms + shadow_cost - cache_rebate)
    }

    fn attempt_ttft_deadline_ms(
        &self,
        node: &NodeRuntimeView<'_>,
        provider: &ProviderRuntimeView,
        attempt_index: u8,
    ) -> u64 {
        let mu = if provider.provider_ewma_ttft_ms > 0.0 {
            provider.provider_ewma_ttft_ms
        } else if node.ewma_ttft_ms > 0.0 {
            node.ewma_ttft_ms
        } else {
            self.base_deadline_ms as f64 * 0.4
        };
        // Optimal stopping rule approximation: E[T_switch] + tail buffer
        let opt = (mu * 2.5 + 6_000.0) as u64;
        opt.clamp(10_000, 45_000)
            .saturating_add(u64::from(attempt_index) * 5_000)
    }
}
