use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};

use super::{read_rows, AccountStore};

#[derive(Default)]
pub(super) struct SnapshotCache {
    build: Mutex<()>,
    state: RwLock<Option<CachedSnapshot>>,
}

struct CachedSnapshot {
    revision: u64,
    metadata: u64,
    beijing_day: String,
    checked_at: Instant,
    rows: Arc<BTreeMap<i64, Arc<Value>>>,
    snapshot: Arc<RoutingSnapshot>,
}

pub(crate) struct RoutingSnapshot {
    pub accounts: Vec<Arc<Value>>,
    pub by_id: HashMap<String, Arc<Value>>,
    pub by_provider: HashMap<String, Vec<Arc<Value>>>,
    pub public_accounts: Vec<Arc<Value>>,
    pub public_by_id: HashMap<String, Arc<Value>>,
    pub available_providers: HashSet<String>,
    pub forwarding_providers: HashSet<String>,
    pub(crate) projected_routes: std::sync::Mutex<HashMap<String, Arc<crate::server::core::upstream::route_context::ProjectedRoute>>>,
}

impl RoutingSnapshot {
    fn from_rows(rows: &BTreeMap<i64, Arc<Value>>) -> Self {
        let public_accounts: Vec<_> = rows.values().cloned().collect();
        let public_by_id = public_accounts.iter().filter_map(|account| {
            Some((account.get("id")?.as_str()?.to_string(), account.clone()))
        }).collect();
        let accounts: Vec<_> = public_accounts.iter().map(|account| Arc::new(route_account(account))).collect();
        let by_id = accounts.iter().filter_map(|account| {
            Some((account.get("id")?.as_str()?.to_string(), account.clone()))
        }).collect();
        let available_providers = accounts.iter().filter(|account| {
            account.get("enabled").and_then(Value::as_bool) != Some(false)
                && account.get("hasCredentials").and_then(Value::as_bool) == Some(true)
        }).filter_map(|account| account.get("provider")?.as_str().map(str::to_string)).collect();
        let forwarding_providers = accounts.iter().filter_map(|account| {
            account.get("provider")?.as_str().map(str::to_string)
        }).collect::<HashSet<_>>().into_iter()
            .filter(|provider| super::store::provider_forwards_requests(provider)).collect();
        let mut by_provider: HashMap<String, Vec<Arc<Value>>> = HashMap::new();
        for account in &accounts {
            let provider = crate::server::core::routing::provider_of(account);
            by_provider.entry(provider.to_string()).or_default().push(account.clone());
        }
        Self {
            accounts,
            by_id,
            by_provider,
            public_accounts,
            public_by_id,
            available_providers,
            forwarding_providers,
            projected_routes: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

impl AccountStore {
    pub(crate) fn routing_snapshot(&self) -> Arc<RoutingSnapshot> {
        let Some(db) = self.database() else { return empty() };
        let cache = self.snapshot_cache();
        let metadata = crate::server::core::providers::catalog::generation();
        let today_day = crate::server::core::beijing::today_key();
        if let Some(snapshot) = fresh(cache, db.account_revision(), metadata, &today_day) { return snapshot; }
        // 0027 stale-while-revalidate：已有旧快照时非阻塞抢 build 锁，未抢到者立即返回旧快照，彻底消除重建惊群。
        if let Some(stale) = cached_snapshot(cache) {
            return crate::server::core::blocking::in_place(|| self.build_or_stale(db, stale));
        }
        crate::server::core::blocking::in_place(|| self.build_routing_snapshot(db))
    }

    /// 强一致快照读取（供管理界面列表、余额查询、签到与选路校验等调用）：
    /// `account_revision` / `catalog::generation` / 北京日期未变时零 SQLite 零解密命中缓存；
    /// 发生变化时仅增量读取变更行（`read_rows::changed`），保证写后立即可读。
    pub(crate) fn routing_snapshot_fresh(&self) -> Arc<RoutingSnapshot> {
        let Some(db) = self.database() else { return empty() };
        let cache = self.snapshot_cache();
        let metadata = crate::server::core::providers::catalog::generation();
        let today_day = crate::server::core::beijing::today_key();
        if let Some(snapshot) = fresh(cache, db.account_revision(), metadata, &today_day) {
            return snapshot;
        }
        crate::server::core::blocking::in_place(|| self.build_routing_snapshot(db))
    }

    fn build_or_stale(&self, db: &crate::server::db::Db, stale: Arc<RoutingSnapshot>) -> Arc<RoutingSnapshot> {
        let cache = self.snapshot_cache();
        match cache.build.try_lock() {
            Ok(guard) => self.build_locked(db, guard),
            Err(_) => stale,
        }
    }

    fn build_routing_snapshot(&self, db: &crate::server::db::Db) -> Arc<RoutingSnapshot> {
        let cache = self.snapshot_cache();
        let guard = cache.build.lock().unwrap_or_else(|error| error.into_inner());
        self.build_locked(db, guard)
    }

    fn build_locked(&self, db: &crate::server::db::Db, _build: std::sync::MutexGuard<'_, ()>) -> Arc<RoutingSnapshot> {
        let cache = self.snapshot_cache();
        let metadata = crate::server::core::providers::catalog::generation();
        let today_day = crate::server::core::beijing::today_key();
        if let Some(snapshot) = fresh(cache, db.account_revision(), metadata, &today_day) { return snapshot; }
        let previous = cache.state.read().ok().and_then(|state| state.as_ref().map(|cached| {
            (cached.revision, cached.metadata, cached.beijing_day.clone(), cached.rows.clone())
        }));
        // 0027：不再依赖全库 PRAGMA data_version（requests 表写入会令其持续失效导致反复全量解密），
        // 仅依赖 accounts 表专属的 account_revision + 行级 update_hook journal。
        let batch = db.with(|conn| -> rusqlite::Result<_> {
            let since = previous.as_ref().filter(|(_, old_metadata, old_day, _)| {
                *old_metadata == metadata && *old_day == today_day
            }).map(|(revision, _, _, _)| *revision);
            let changes = db.account_changes(since);
            let (replace, rows, unchanged) = match changes.rows {
                Some(ids) => {
                    let unchanged = ids.is_empty();
                    (false, read_rows::changed(conn, &ids)?, unchanged)
                }
                None => (true, read_rows::all(conn)?.into_iter().map(|row| (row.rowid, Some(row))).collect(), false),
            };
            Ok((changes.revision, replace, rows, unchanged))
        }).and_then(Result::ok);
        let Some((revision, replace, changes, unchanged)) = batch else { return empty() };
        if unchanged && crate::server::core::providers::catalog::generation() == metadata {
            if let Ok(mut state) = cache.state.write() {
                if let Some(cached) = state.as_mut() {
                    cached.revision = revision;
                    cached.checked_at = Instant::now();
                    return cached.snapshot.clone();
                }
            }
        }
        let mut rows = if replace { BTreeMap::new() } else {
            previous.map(|(_, _, _, rows)| (*rows).clone()).unwrap_or_default()
        };
        let mut failed = 0;
        for (rowid, raw) in changes {
            rows.remove(&rowid);
            if let Some(raw) = raw {
                match read_rows::decode(raw) {
                    Ok((_, record)) => { rows.insert(rowid, Arc::new(self.public_account(&record))); }
                    Err(_) => failed += 1,
                }
            }
        }
        if failed > 0 {
            super::sql::warn_load_failure("读取选路账号", &format!("{failed} 条账号记录解密或解析失败，已跳过；原始数据未修改"));
        }
        let snapshot = Arc::new(RoutingSnapshot::from_rows(&rows));
        // 0027：按构建取数时的 revision 入缓存，不再因构建期间新到变更作废整批成果。
        if let Ok(mut state) = cache.state.write() {
            *state = Some(CachedSnapshot {
                revision,
                metadata,
                beijing_day: today_day,
                checked_at: Instant::now(),
                rows: Arc::new(rows),
                snapshot: snapshot.clone(),
            });
        }
        snapshot
    }

    pub(crate) fn route_account_ids_for_provider(&self, provider: &str) -> Vec<String> {
        let snapshot = self.routing_snapshot();
        let mut accounts: Vec<_> = snapshot.by_provider.get(provider).into_iter().flatten().filter(|account| {
            account.get("enabled").and_then(Value::as_bool) != Some(false)
                && account.get("hasCredentials").and_then(Value::as_bool) == Some(true)
        }).collect();
        accounts.sort_by_key(|account| super::store::route_order_key(account));
        accounts.into_iter().filter_map(|account| account.get("id")?.as_str().map(str::to_string)).collect()
    }

    pub(crate) fn route_account(&self, id: &str) -> Option<Arc<Value>> {
        self.routing_snapshot().by_id.get(id).cloned()
    }
}

fn fresh(cache: &SnapshotCache, revision: u64, metadata: u64, today_day: &str) -> Option<Arc<RoutingSnapshot>> {
    let state = cache.state.read().ok()?;
    let cached = state.as_ref()?;
    (cached.revision == revision
        && cached.metadata == metadata
        && cached.beijing_day == today_day
        && cached.checked_at.elapsed() < Duration::from_secs(10))
        .then(|| cached.snapshot.clone())
}

fn cached_snapshot(cache: &SnapshotCache) -> Option<Arc<RoutingSnapshot>> {
    cache.state.read().ok()?.as_ref().map(|cached| cached.snapshot.clone())
}

fn empty() -> Arc<RoutingSnapshot> {
    Arc::new(RoutingSnapshot::from_rows(&BTreeMap::new()))
}

// 选路只消费所需字段；完整公开投影复用同一份按账号增量更新的缓存。
fn route_account(account: &Value) -> Value {
    let mut public = Map::new();
    for key in [
        "id", "provider", "name", "nickname", "email", "userId", "uid", "account",
        "displayName", "priority", "addedAt", "enabled", "hasCredentials", "nameCustom",
        "needsRelogin", "needsReloginReason", "allowRouteSkip", "maxConcurrent", "rateLimits", "routingCooldowns", "limiterGroups",
        "upstreamCooldownUntil", "zcodePlan", "pool", "quotaChannel", "pluginModelPermissions", "usageConfigKey",
    ] {
        if let Some(value) = account.get(key) { public.insert(key.to_string(), value.clone()); }
    }
    let source = account.pointer("/proxy/config/source").and_then(Value::as_str).unwrap_or("");
    public.insert("proxy".to_string(), json!({ "config": { "source": source } }));
    Value::Object(public)
}
