//! Discrete-event trace replay harness for offline benchmarking & thesis experiments.

use serde::{Deserialize, Serialize};

use crate::policy::SchedulingPolicy;
use crate::types::{AttemptFeedback, AttemptFeedbackKind, NodeDescriptor, ScheduleRequest};
use crate::Scheduler;

/// One synthetic or recorded workload request in a trace.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceItem {
    pub arrival_ms: i64,
    pub model: String,
    pub providers: Vec<String>,
    pub sticky_key: String,
    pub simulated_ttft_ms: u64,
    pub simulated_duration_ms: u64,
    pub credit_cost: f64,
}

/// Summary metrics produced by [`run_trace_replay`].
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplaySummary {
    pub total_requests: usize,
    pub scheduled_ok: usize,
    pub rejected: usize,
    pub sticky_hits: usize,
    pub avg_ttft_ms: f64,
    /// Gini coefficient $\in [0, 1]$ of request counts across eligible nodes (`0.0` = perfectly uniform load spread).
    pub load_gini: f64,
}

/// Replays a sequence of [`TraceItem`] arrivals against a fresh [`Scheduler<P>`] and computes aggregate metrics.
pub fn run_trace_replay<P: SchedulingPolicy>(
    scheduler: &Scheduler<P>,
    nodes: &[NodeDescriptor],
    trace: &[TraceItem],
) -> ReplaySummary {
    scheduler.sync_nodes(nodes, trace.first().map(|t| t.arrival_ms).unwrap_or(0));
    let mut summary = ReplaySummary {
        total_requests: trace.len(),
        ..Default::default()
    };
    let mut ttft_sum = 0.0;

    for item in trace {
        let providers_ref: Vec<&str> = item.providers.iter().map(String::as_str).collect();
        let req = ScheduleRequest {
            model: &item.model,
            allowed_providers: &providers_ref,
            sticky_key: &item.sticky_key,
            sticky_ttl_ms: 300_000,
            excluded_node_ids: &[],
            route_excluded_ids: None,
            route_allowed_ids: None,
            pinned_node_id: None,
            attempt_index: 0,
            now_ms: item.arrival_ms,
        };
        match scheduler.schedule(&req) {
            Ok((decision, lease)) => {
                summary.scheduled_ok += 1;
                if decision.sticky_hit {
                    summary.sticky_hits += 1;
                }
                ttft_sum += item.simulated_ttft_ms as f64;
                drop(lease);
                scheduler.record_feedback(AttemptFeedback {
                    node_id: decision.node_id,
                    model: item.model.clone(),
                    sticky_key: item.sticky_key.clone(),
                    sticky_ttl_ms: 300_000,
                    kind: AttemptFeedbackKind::Success {
                        ttft_ms: Some(item.simulated_ttft_ms),
                        total_ms: item.simulated_duration_ms,
                        prompt_tokens: 1_000,
                        completion_tokens: 250,
                        cached_tokens: if decision.sticky_hit { 800 } else { 0 },
                        credit_used: Some(item.credit_cost),
                    },
                    now_ms: item.arrival_ms + item.simulated_duration_ms as i64,
                });
            }
            Err(_) => {
                summary.rejected += 1;
            }
        }
    }

    if summary.scheduled_ok > 0 {
        summary.avg_ttft_ms = ttft_sum / summary.scheduled_ok as f64;
    }
    let counts = scheduler.dispatched_counts();
    summary.load_gini = gini_coefficient(&counts);
    summary
}

/// Computes the Gini coefficient of a slice of non-negative load counts.
pub fn gini_coefficient(values: &[u64]) -> f64 {
    if values.len() <= 1 {
        return 0.0;
    }
    let mut sorted: Vec<f64> = values.iter().map(|&v| v as f64).collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = sorted.len() as f64;
    let sum: f64 = sorted.iter().sum();
    if sum <= 0.0 {
        return 0.0;
    }
    let mut weighted_sum = 0.0;
    for (idx, &val) in sorted.iter().enumerate() {
        weighted_sum += (2.0 * (idx as f64 + 1.0) - n - 1.0) * val;
    }
    (weighted_sum / (n * sum)).clamp(0.0, 1.0)
}
