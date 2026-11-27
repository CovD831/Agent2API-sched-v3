//! Four-band quota hysteresis automaton (`Full` <-> `Throttle` <-> `DrainOnly` <-> `Exhausted`).
//!
//! Eliminates hard-cutoff chattering at low quota boundaries by separating downward exit
//! thresholds ($B_1^\downarrow > B_2^\downarrow > B_3^\downarrow$) from upward recovery
//! thresholds ($B_1^\uparrow > B_1^\downarrow$, $B_2^\uparrow > B_2^\downarrow$).

use serde::{Deserialize, Serialize};

/// Discrete quota operating band of an account node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuotaBand {
    /// $q > B_1^\downarrow$: accepts both sticky and new sessions at full weight ($1.0$).
    #[default]
    Full,
    /// $B_2^\downarrow < q \le B_1^\downarrow$: preserves sticky sessions at $1.0$;
    /// smoothly throttles new session admission weight $w(q) \in [\underline{w}, 1.0)$.
    Throttle,
    /// $B_3^\downarrow < q \le B_2^\downarrow$: rejects new sessions ($w_{\text{new}} = 0$);
    /// continues serving existing sticky sessions to preserve upstream prompt cache.
    DrainOnly,
    /// $q \le B_3^\downarrow$ (or explicit 402/exhausted): blocks all traffic until quota
    /// replenishes above $B_2^\uparrow$ (asymmetric recovery hysteresis).
    Exhausted,
}

impl QuotaBand {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Throttle => "throttle",
            Self::DrainOnly => "drain_only",
            Self::Exhausted => "exhausted",
        }
    }

    #[inline]
    pub fn allows_sticky(self) -> bool {
        !matches!(self, Self::Exhausted)
    }

    #[inline]
    pub fn allows_new_flow(self) -> bool {
        matches!(self, Self::Full | Self::Throttle)
    }
}

/// Dual-mode threshold specification (normalized ratio in `[0, 1]` + optional absolute credit scale).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotaHysteresisConfig {
    /// Downward entry into `Throttle` (default `0.30`).
    pub b1_down: f64,
    /// Upward recovery back to `Full` (default `0.40`, must be `> b1_down`).
    pub b1_up: f64,
    /// Downward entry into `DrainOnly` (default `0.12`).
    pub b2_down: f64,
    /// Upward recovery from `Exhausted` / `DrainOnly` into `Throttle` (default `0.20`, must be `> b2_down`).
    pub b2_up: f64,
    /// Hard floor entry into `Exhausted` (default `0.03`).
    pub b3_down: f64,
    /// Minimum new-flow weight at the bottom of `Throttle` (default `0.15`).
    pub min_throttle_weight: f64,
    /// Shape exponent $\gamma > 0$ for throttle curve (default `1.2`).
    pub throttle_gamma: f64,
    /// Reference full-scale credits used when an upstream only reports absolute remaining credits
    /// without a total capacity denominator (`None` = only ratio mode applies unless total is given).
    pub reference_full_credits: Option<f64>,
}

impl Default for QuotaHysteresisConfig {
    fn default() -> Self {
        Self {
            b1_down: 0.20,
            b1_up: 0.30,
            b2_down: 0.05,
            b2_up: 0.10,
            b3_down: 0.005,
            min_throttle_weight: 0.25,
            throttle_gamma: 1.2,
            reference_full_credits: None,
        }
    }
}

impl QuotaHysteresisConfig {
    /// Sanitizes configuration so ordering invariants $0 \le B_3^\downarrow < B_2^\downarrow < B_2^\uparrow < B_1^\downarrow < B_1^\uparrow \le 1$ always hold.
    pub fn sanitized(&self) -> Self {
        let b3_down = self.b3_down.clamp(0.0, 0.50);
        let b2_down = self.b2_down.max(b3_down + 0.005).min(0.70);
        let b2_up = self.b2_up.max(b2_down + 0.01).min(0.85);
        let b1_down = self.b1_down.max(b2_up + 0.02).min(0.92);
        let b1_up = self.b1_up.max(b1_down + 0.02).min(0.99);
        Self {
            b1_down,
            b1_up,
            b2_down,
            b2_up,
            b3_down,
            min_throttle_weight: self.min_throttle_weight.clamp(0.02, 0.95),
            throttle_gamma: if self.throttle_gamma.is_finite() && self.throttle_gamma > 0.1 {
                self.throttle_gamma.min(5.0)
            } else {
                1.0
            },
            reference_full_credits: self
                .reference_full_credits
                .filter(|v| v.is_finite() && *v > 0.0),
        }
    }

    /// Normalizes `(remaining, total)` into a `[0.0, 1.0]` quota ratio.
    /// Returns `None` if neither `total` nor `reference_full_credits` is available.
    pub fn normalize_quota(&self, remaining: f64, total: Option<f64>) -> Option<f64> {
        if !remaining.is_finite() {
            return None;
        }
        if remaining <= 0.0 {
            return Some(0.0);
        }
        let denom = total
            .filter(|t| t.is_finite() && *t > 0.0)
            .or(self.reference_full_credits)?;
        Some((remaining / denom).clamp(0.0, 1.0))
    }

    /// Evaluates the next [`QuotaBand`] given the current band and normalized ratio $q \in [0, 1]$.
    pub fn transition(&self, current: QuotaBand, q: f64) -> QuotaBand {
        let q = q.clamp(0.0, 1.0);
        match current {
            QuotaBand::Full => {
                if q <= self.b3_down {
                    QuotaBand::Exhausted
                } else if q <= self.b2_down {
                    QuotaBand::DrainOnly
                } else if q <= self.b1_down {
                    QuotaBand::Throttle
                } else {
                    QuotaBand::Full
                }
            }
            QuotaBand::Throttle => {
                if q <= self.b3_down {
                    QuotaBand::Exhausted
                } else if q <= self.b2_down {
                    QuotaBand::DrainOnly
                } else if q >= self.b1_up {
                    QuotaBand::Full
                } else {
                    QuotaBand::Throttle
                }
            }
            QuotaBand::DrainOnly => {
                if q <= self.b3_down {
                    QuotaBand::Exhausted
                } else if q >= self.b1_up {
                    QuotaBand::Full
                } else if q >= self.b2_up {
                    QuotaBand::Throttle
                } else {
                    QuotaBand::DrainOnly
                }
            }
            QuotaBand::Exhausted => {
                // Asymmetric enablement guard: NEVER re-enable until quota recovers >= b2_up!
                if q >= self.b1_up {
                    QuotaBand::Full
                } else if q >= self.b2_up {
                    QuotaBand::Throttle
                } else {
                    QuotaBand::Exhausted
                }
            }
        }
    }

    /// Computes smooth new-flow admission weight $w_{\text{quota}}(q) \in [0.0, 1.0]$.
    pub fn new_flow_weight(&self, band: QuotaBand, q: f64) -> f64 {
        match band {
            QuotaBand::Full => 1.0,
            QuotaBand::Throttle => {
                let span = (self.b1_up - self.b2_down).max(1e-6);
                let pos = ((q - self.b2_down) / span).clamp(0.0, 1.0);
                self.min_throttle_weight
                    + (1.0 - self.min_throttle_weight) * pos.powf(self.throttle_gamma)
            }
            QuotaBand::DrainOnly | QuotaBand::Exhausted => 0.0,
        }
    }
}
