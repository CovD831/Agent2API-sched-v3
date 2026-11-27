//! 64-shard, $O(1)$ sticky session table with composite collision-free key builder.
//!
//! Replaces full-table `HashMap::retain()` scans with sharded constant-time lookups and
//! bounded 2-sample eviction, supporting 130,000+ active conversations with near-zero lock contention.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::health::fnv1a_64;
use crate::types::SlotId;

const SHARD_COUNT: usize = 64;
const DEFAULT_MAX_ENTRIES: usize = 131_072;

#[derive(Clone, Debug)]
struct StickyEntry {
    slot_id: SlotId,
    node_id: String,
    updated_at_ms: i64,
    expires_at_ms: i64,
}

#[derive(Default)]
struct StickyShard {
    entries: HashMap<u64, StickyEntry>,
    evict_cursor: u64,
}

/// High-concurrency sharded sticky binding table (`u64` composite key -> `SlotId`).
pub struct ShardedStickyTable {
    shards: Box<[Mutex<StickyShard>; SHARD_COUNT]>,
    max_per_shard: usize,
}

impl Default for ShardedStickyTable {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_MAX_ENTRIES)
    }
}

impl ShardedStickyTable {
    pub fn with_capacity(total_capacity: usize) -> Self {
        let max_per_shard = (total_capacity / SHARD_COUNT).max(64);
        let shards = std::array::from_fn(|_| Mutex::new(StickyShard::default()));
        Self {
            shards: Box::new(shards),
            max_per_shard,
        }
    }

    #[inline]
    fn locate(&self, key_hash: u64) -> &Mutex<StickyShard> {
        let idx = ((key_hash ^ (key_hash >> 32)) as usize) & (SHARD_COUNT - 1);
        &self.shards[idx]
    }

    /// Looks up a sticky binding in $O(1)$ and refreshes its sliding TTL if alive.
    pub fn lookup_and_touch(
        &self,
        sticky_key: &str,
        model: &str,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Option<(SlotId, String)> {
        if sticky_key.is_empty() || ttl_ms <= 0 {
            return None;
        }
        let hash = composite_hash(sticky_key, model);
        let mut shard = self.locate(hash).lock().unwrap_or_else(|e| e.into_inner());
        let expired = match shard.entries.get_mut(&hash) {
            Some(entry) if entry.expires_at_ms > now_ms => {
                entry.updated_at_ms = now_ms;
                entry.expires_at_ms = now_ms.saturating_add(ttl_ms);
                return Some((entry.slot_id, entry.node_id.clone()));
            }
            Some(_) => true,
            None => false,
        };
        if expired {
            shard.entries.remove(&hash);
        }
        None
    }

    /// Binds or migrates `(sticky_key, model)` to `(slot_id, node_id)` in $O(1)$ amortized time.
    pub fn bind(
        &self,
        sticky_key: &str,
        model: &str,
        slot_id: SlotId,
        node_id: &str,
        now_ms: i64,
        ttl_ms: i64,
    ) {
        if sticky_key.is_empty() || node_id.is_empty() || ttl_ms <= 0 {
            return;
        }
        let hash = composite_hash(sticky_key, model);
        let mut shard = self.locate(hash).lock().unwrap_or_else(|e| e.into_inner());
        if !shard.entries.contains_key(&hash) && shard.entries.len() >= self.max_per_shard {
            evict_bounded(&mut shard, now_ms);
        }
        shard.entries.insert(
            hash,
            StickyEntry {
                slot_id,
                node_id: node_id.to_string(),
                updated_at_ms: now_ms,
                expires_at_ms: now_ms.saturating_add(ttl_ms),
            },
        );
    }

    /// Unbinds a sticky session immediately (e.g. when its bound account hits `Exhausted` or `Quarantined`).
    pub fn remove(&self, sticky_key: &str, model: &str) {
        if sticky_key.is_empty() {
            return;
        }
        let hash = composite_hash(sticky_key, model);
        let mut shard = self.locate(hash).lock().unwrap_or_else(|e| e.into_inner());
        shard.entries.remove(&hash);
    }

    pub fn clear_all(&self) {
        for shard in self.shards.iter() {
            shard.lock().unwrap_or_else(|e| e.into_inner()).entries.clear();
        }
    }

    /// Counts non-expired sticky bindings across all 64 shards without mutating TTLs.
    pub fn active_count(&self, now_ms: i64) -> usize {
        self.shards
            .iter()
            .map(|s| {
                s.lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .entries
                    .values()
                    .filter(|e| e.expires_at_ms > now_ms)
                    .count()
            })
            .sum()
    }
}

/// Bounded eviction: scans at most 8 entries to drop expired items or the oldest sampled item.
fn evict_bounded(shard: &mut StickyShard, now_ms: i64) {
    shard.evict_cursor = shard.evict_cursor.wrapping_add(1);
    let skip = (shard.evict_cursor as usize) % shard.entries.len().max(1);
    let mut expired_key = None;
    let mut oldest: Option<(u64, i64)> = None;

    for (&k, v) in shard.entries.iter().skip(skip).take(8) {
        if v.expires_at_ms <= now_ms {
            expired_key = Some(k);
            break;
        }
        if oldest.is_none_or(|(_, old_ts)| v.updated_at_ms < old_ts) {
            oldest = Some((k, v.updated_at_ms));
        }
    }
    if expired_key.is_none() && oldest.is_none() {
        if let Some((&k, _)) = shard.entries.iter().next() {
            oldest = Some((k, 0));
        }
    }
    if let Some(k) = expired_key.or_else(|| oldest.map(|(k, _)| k)) {
        shard.entries.remove(&k);
    }
}

#[inline]
fn composite_hash(sticky_key: &str, model: &str) -> u64 {
    let mut h = fnv1a_64(sticky_key.as_bytes());
    h ^= 0xff;
    h = h.wrapping_mul(0x100000001b3);
    for &b in model.as_bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100000001b3);
    }
    // SplitMix64 finalizer for uniform shard distribution
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58476d1ce4e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d049bb133111eb);
    h ^ (h >> 31)
}

/// Builds a collision-resistant conversation anchor from tenant identity, system prompt, and conversation anchor.
///
/// Unlike legacy content hashing (which hashed only `model + first_user_message` and caused unrelated
/// tenants sharing an opening prompt to collide onto a single hotspot account), this builder mixes:
/// - `tenant_scope` (API key ID / user ID / client IP hash),
/// - `explicit_session_id` (if supplied by headers/metadata), OR
/// - `system_prompt` + `first_user_message` + `second_turn_anchor` (so distinct system prompts or diverged histories separate cleanly).
pub fn build_isolated_sticky_key(
    tenant_scope: &str,
    explicit_session_id: Option<&str>,
    system_prompt: &str,
    first_user_msg: &str,
    assistant_anchor: &str,
) -> String {
    if let Some(exp) = explicit_session_id.map(str::trim).filter(|s| !s.is_empty()) {
        let mut h = fnv1a_64(tenant_scope.as_bytes());
        h ^= fnv1a_64(exp.as_bytes()).rotate_left(13);
        return format!("exp:{h:016x}");
    }
    if first_user_msg.trim().is_empty() {
        return String::new();
    }
    let mut h = fnv1a_64(tenant_scope.as_bytes());
    h = h.wrapping_mul(0x100000001b3) ^ fnv1a_64(take_prefix(system_prompt, 4096).as_bytes());
    h = h.wrapping_mul(0x100000001b3) ^ fnv1a_64(take_prefix(first_user_msg, 4096).as_bytes());
    if !assistant_anchor.is_empty() {
        h = h.wrapping_mul(0x100000001b3) ^ fnv1a_64(take_prefix(assistant_anchor, 1024).as_bytes());
    }
    format!("ctx:{h:016x}")
}

fn take_prefix(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}
