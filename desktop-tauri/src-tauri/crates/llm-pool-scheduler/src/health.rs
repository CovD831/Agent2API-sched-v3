//! Five-state account health FSM (`Active`, `SlowCooldown`, `TempBackoff`, `Probation`, `Quarantined`)
//! paired with `WaveGuard` channel/proxy surge circuit breaking to prevent mass false quarantines.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// Operational health state of a node (orthogonal to [`crate::hysteresis::QuotaBand`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    /// Healthy and eligible for full `max_concurrent` traffic.
    #[default]
    Active,
    /// Cooling down due to repeated slow TTFT or per-attempt TTFT rescue timeout.
    SlowCooldown,
    /// Cooling down due to 429 rate-limit, transient 5xx, or wave-protected failure.
    TempBackoff,
    /// Half-open recovery probe state: allows at most `1` concurrent request until a clean fast success confirms health.
    Probation,
    /// Removed from routing due to verified account-specific credential death / ban.
    Quarantined,
}

impl HealthState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::SlowCooldown => "slow_cooldown",
            Self::TempBackoff => "temp_backoff",
            Self::Probation => "probation",
            Self::Quarantined => "quarantined",
        }
    }
}

/// Parameters governing TTFT slow-cooldown, backoff, probation, and wave guard.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthConfig {
    /// EWMA smoothing factor $\alpha \in (0, 1]$ for TTFT updates (default `0.25`).
    pub ttft_ewma_alpha: f64,
    /// Soft TTFT threshold (ms) above which an attempt increments `slow_streak` (default `15_000`).
    pub slow_ttft_threshold_ms: u64,
    /// Consecutive slow responses before entering `SlowCooldown` (default `2`).
    pub slow_streak_trigger: u32,
    /// Base cooldown duration (ms) on `SlowCooldown` entry (default `30_000`, exponential up to `max_cooldown_ms`).
    pub base_slow_cooldown_ms: i64,
    /// Base backoff duration (ms) on transient/429 without explicit `reset_at` (default `15_000`).
    pub base_transient_backoff_ms: i64,
    /// Maximum automatic cooldown duration (ms) (default `600_000` = 10 min).
    pub max_cooldown_ms: i64,
    /// Sliding window (ms) for `WaveGuard` mass-failure detection (default `30_000`).
    pub wave_window_ms: i64,
    /// Minimum distinct accounts failing in the same provider/proxy within `wave_window_ms` to trip WaveGuard (default `5`).
    pub wave_distinct_accounts_trigger: usize,
    /// Backoff applied to an account instead of quarantine when WaveGuard is active (default `45_000`).
    pub wave_protective_backoff_ms: i64,
    /// Sliding window (ms) for provider-wide TTFT slow-wave detection (default `180_000` = 3 min).
    pub slow_wave_window_ms: i64,
    /// Distinct accounts triggering `ttft_deadline_exceeded` within `slow_wave_window_ms` to trip slow-wave mode (default `2`).
    pub slow_wave_distinct_accounts_trigger: usize,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            ttft_ewma_alpha: 0.25,
            slow_ttft_threshold_ms: 25_000,
            slow_streak_trigger: 3,
            base_slow_cooldown_ms: 20_000,
            base_transient_backoff_ms: 15_000,
            max_cooldown_ms: 180_000,
            wave_window_ms: 30_000,
            wave_distinct_accounts_trigger: 5,
            wave_protective_backoff_ms: 45_000,
            slow_wave_window_ms: 180_000,
            slow_wave_distinct_accounts_trigger: 2,
        }
    }
}

/// Mutable per-node health runtime state protected by a lightweight per-slot or shard lock.
#[derive(Clone, Debug, Default)]
pub struct NodeHealthRuntime {
    pub state: HealthState,
    pub state_until_ms: i64,
    pub ewma_ttft_ms: f64,
    pub slow_streak: u32,
    pub failure_streak: u32,
    pub quarantine_reason: Option<String>,
    /// Per-model rate-limit reset timestamps (`model -> reset_at_ms`).
    pub model_cooldowns: HashMap<String, i64>,
}

impl NodeHealthRuntime {
    /// Advances time-based transitions (`SlowCooldown` / `TempBackoff` -> `Probation` once timer expires).
    pub fn refresh_clock(&mut self, now_ms: i64) -> HealthState {
        if matches!(self.state, HealthState::SlowCooldown | HealthState::TempBackoff)
            && now_ms >= self.state_until_ms
        {
            self.state = HealthState::Probation;
            self.state_until_ms = 0;
        }
        self.state
    }

    pub fn model_cooling_until(&self, model: &str, now_ms: i64) -> Option<i64> {
        if model.is_empty() {
            return None;
        }
        self.model_cooldowns
            .get(model)
            .copied()
            .filter(|until| *until > now_ms)
    }

    pub fn record_fast_success(&mut self, ttft_ms: Option<u64>, model: &str, cfg: &HealthConfig) {
        if let Some(ms) = ttft_ms {
            let sample = ms as f64;
            if self.ewma_ttft_ms <= 0.0 {
                self.ewma_ttft_ms = sample;
            } else {
                self.ewma_ttft_ms =
                    (1.0 - cfg.ttft_ewma_alpha) * self.ewma_ttft_ms + cfg.ttft_ewma_alpha * sample;
            }
        }
        self.slow_streak = 0;
        self.failure_streak = 0;
        if !model.is_empty() {
            self.model_cooldowns.remove(model);
        }
        if !matches!(self.state, HealthState::Quarantined) {
            self.state = HealthState::Active;
            self.state_until_ms = 0;
        }
    }

    pub fn record_slow_event(&mut self, elapsed_ms: u64, is_timeout: bool, now_ms: i64, cfg: &HealthConfig) {
        let sample = elapsed_ms as f64;
        if self.ewma_ttft_ms <= 0.0 {
            self.ewma_ttft_ms = sample;
        } else {
            self.ewma_ttft_ms =
                (1.0 - cfg.ttft_ewma_alpha) * self.ewma_ttft_ms + cfg.ttft_ewma_alpha * sample;
        }
        self.slow_streak = self.slow_streak.saturating_add(1);
        if is_timeout || self.slow_streak >= cfg.slow_streak_trigger || matches!(self.state, HealthState::Probation) {
            let shift = self.slow_streak.saturating_sub(1).min(5);
            let dur = (cfg.base_slow_cooldown_ms.saturating_mul(1i64 << shift)).min(cfg.max_cooldown_ms);
            self.state = HealthState::SlowCooldown;
            self.state_until_ms = now_ms.saturating_add(dur);
        }
    }

    pub fn record_backoff(&mut self, reset_at_ms: Option<i64>, model: &str, now_ms: i64, cfg: &HealthConfig) {
        self.failure_streak = self.failure_streak.saturating_add(1);
        let shift = self.failure_streak.saturating_sub(1).min(5);
        let default_dur = (cfg.base_transient_backoff_ms.saturating_mul(1i64 << shift)).min(cfg.max_cooldown_ms);
        let until = reset_at_ms
            .filter(|at| *at > now_ms)
            .unwrap_or_else(|| now_ms.saturating_add(default_dur));
        if !model.is_empty() && reset_at_ms.is_some_and(|at| at > now_ms) {
            // Model-scoped rate limit: keep account open for other models unless already in Probation.
            self.model_cooldowns.insert(model.to_string(), until);
            if matches!(self.state, HealthState::Probation) {
                self.state = HealthState::TempBackoff;
                self.state_until_ms = until.min(now_ms.saturating_add(cfg.max_cooldown_ms));
            }
        } else {
            self.state = HealthState::TempBackoff;
            self.state_until_ms = until;
        }
    }
}

#[derive(Clone, Debug)]
struct FailureSample {
    at_ms: i64,
    node_hash: u64,
}

/// Sliding-window surge detector preventing upstream/proxy outages from mass-disabling accounts.
#[derive(Default)]
pub struct WaveGuard {
    by_scope: Mutex<HashMap<String, VecDeque<FailureSample>>>,
    slow_by_provider: Mutex<HashMap<String, VecDeque<FailureSample>>>,
}

impl WaveGuard {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a TTFT rescue timeout on `(provider, node_id)` and returns `Some(distinct_accounts)`
    /// if a provider-wide slow wave is currently active.
    pub fn observe_slow_timeout(
        &self,
        provider: &str,
        node_id: &str,
        now_ms: i64,
        cfg: &HealthConfig,
    ) -> Option<usize> {
        let node_hash = fnv1a_64(node_id.as_bytes());
        let mut map = self.slow_by_provider.lock().unwrap_or_else(|e| e.into_inner());
        let cutoff = now_ms.saturating_sub(cfg.slow_wave_window_ms);
        let distinct = record_in_window(
            map.entry(provider.to_string()).or_default(),
            cutoff,
            now_ms,
            node_hash,
        );
        (distinct >= cfg.slow_wave_distinct_accounts_trigger).then_some(distinct)
    }

    pub fn clear_slow_wave(&self, provider: &str) {
        let mut map = self.slow_by_provider.lock().unwrap_or_else(|e| e.into_inner());
        map.remove(provider);
    }

    pub fn is_provider_slow_degraded(&self, provider: &str, now_ms: i64, cfg: &HealthConfig) -> bool {
        let map = self.slow_by_provider.lock().unwrap_or_else(|e| e.into_inner());
        let Some(queue) = map.get(provider) else {
            return false;
        };
        let cutoff = now_ms.saturating_sub(cfg.slow_wave_window_ms);
        let mut seen = Vec::with_capacity(cfg.slow_wave_distinct_accounts_trigger);
        for item in queue.iter().rev() {
            if item.at_ms < cutoff {
                break;
            }
            if !seen.contains(&item.node_hash) {
                seen.push(item.node_hash);
                if seen.len() >= cfg.slow_wave_distinct_accounts_trigger {
                    return true;
                }
            }
        }
        false
    }

    /// Records an error on `(provider, proxy_group, node_id)` and returns `Some(distinct_accounts)`
    /// if a failure wave is currently active.
    pub fn observe_failure(
        &self,
        provider: &str,
        proxy_group: &str,
        node_id: &str,
        now_ms: i64,
        cfg: &HealthConfig,
    ) -> Option<usize> {
        let node_hash = fnv1a_64(node_id.as_bytes());
        let mut map = self.by_scope.lock().unwrap_or_else(|e| e.into_inner());
        let cutoff = now_ms.saturating_sub(cfg.wave_window_ms);

        let provider_key = format!("p:{provider}");
        let distinct_provider = record_in_window(
            map.entry(provider_key).or_default(),
            cutoff,
            now_ms,
            node_hash,
        );
        let distinct_proxy = if !proxy_group.is_empty() {
            let proxy_key = format!("x:{provider}:{proxy_group}");
            record_in_window(map.entry(proxy_key).or_default(), cutoff, now_ms, node_hash)
        } else {
            0
        };
        let peak = distinct_provider.max(distinct_proxy);
        (peak >= cfg.wave_distinct_accounts_trigger).then_some(peak)
    }

    pub fn is_provider_degraded(&self, provider: &str, now_ms: i64, cfg: &HealthConfig) -> bool {
        let map = self.by_scope.lock().unwrap_or_else(|e| e.into_inner());
        let key = format!("p:{provider}");
        let Some(queue) = map.get(&key) else {
            return false;
        };
        let cutoff = now_ms.saturating_sub(cfg.wave_window_ms);
        let mut seen = Vec::with_capacity(cfg.wave_distinct_accounts_trigger);
        for item in queue.iter().rev() {
            if item.at_ms < cutoff {
                break;
            }
            if !seen.contains(&item.node_hash) {
                seen.push(item.node_hash);
                if seen.len() >= cfg.wave_distinct_accounts_trigger {
                    return true;
                }
            }
        }
        false
    }
}

fn record_in_window(
    queue: &mut VecDeque<FailureSample>,
    cutoff: i64,
    now_ms: i64,
    node_hash: u64,
) -> usize {
    while queue.front().is_some_and(|front| front.at_ms < cutoff) {
        queue.pop_front();
    }
    if queue.len() >= 512 {
        queue.pop_front();
    }
    queue.push_back(FailureSample {
        at_ms: now_ms,
        node_hash,
    });
    let mut distinct = Vec::with_capacity(16);
    for sample in queue.iter() {
        if !distinct.contains(&sample.node_hash) {
            distinct.push(sample.node_hash);
        }
    }
    distinct.len()
}

#[inline]
pub(crate) fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
