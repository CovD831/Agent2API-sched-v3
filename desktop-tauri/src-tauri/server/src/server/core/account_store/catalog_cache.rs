//! 账号目录状态 (`host.catalog.v1`) 的按行增量内存缓存。
//!
//! 解决高并发请求路径中每次 `wasm::catalog::snapshot(provider)` 都要持 `AccountStore` 锁
//! 对同提供商全部账号做 SQLite 扫描 + AES-GCM 全量解密 + JSON 反序列化 + 全量模型去重合并的性能瓶颈。
//! 仅在启用且有凭证账号的 `host.catalog.v1` 实际变化时增量更新受影响的提供商并递增 `catalog_revision`。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, OnceLock, RwLock};

use agent2api_provider_protocol as proto;
use serde_json::Value;

use super::{read_rows, state::StoredAccount, AccountStore};
use crate::server::core::plugin_host::wasm::catalog::{
    aggregate_account_catalogs, parse_account_catalog,
};

const CATALOG_STATE_KEY: &str = "host.catalog.v1";

pub(crate) struct ProviderCatalogSnapshot {
    pub accounts: BTreeMap<String, Arc<proto::ModelList>>,
    pub aggregated: Arc<proto::ModelList>,
}

struct AccountCatalogEntry {
    provider: String,
    account_id: String,
    state_revision: u64,
    parsed: Arc<proto::ModelList>,
}

#[derive(Default)]
pub(super) struct CatalogCache {
    build: std::sync::Mutex<()>,
    state: RwLock<CatalogCacheState>,
}

#[derive(Default)]
struct CatalogCacheState {
    initialized: bool,
    synced_revision: u64,
    catalog_revision: u64,
    by_rowid: HashMap<i64, AccountCatalogEntry>,
    by_provider: HashMap<String, Arc<ProviderCatalogSnapshot>>,
}

fn empty_provider_snapshot() -> Arc<ProviderCatalogSnapshot> {
    static EMPTY: OnceLock<Arc<ProviderCatalogSnapshot>> = OnceLock::new();
    EMPTY
        .get_or_init(|| {
            Arc::new(ProviderCatalogSnapshot {
                accounts: BTreeMap::new(),
                aggregated: Arc::new(proto::ModelList {
                    models: Vec::new(),
                    meta: proto::CatalogMeta {
                        remote: true,
                        refreshed_at: 0,
                    },
                }),
            })
        })
        .clone()
}

fn model_list_eq(left: &proto::ModelList, right: &proto::ModelList) -> bool {
    left.meta.remote == right.meta.remote
        && left.meta.refreshed_at == right.meta.refreshed_at
        && left.models == right.models
}

fn entry_eq(left: Option<&AccountCatalogEntry>, right: Option<&AccountCatalogEntry>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            a.provider == b.provider
                && a.account_id == b.account_id
                && a.state_revision == b.state_revision
                && (Arc::ptr_eq(&a.parsed, &b.parsed) || model_list_eq(&a.parsed, &b.parsed))
        }
        _ => false,
    }
}

fn extract_entry(record: &StoredAccount) -> Option<AccountCatalogEntry> {
    if !record.enabled() || !record.has_credentials() {
        return None;
    }
    let state = record.get("pluginState")?.get(CATALOG_STATE_KEY)?;
    let state_revision = state.get("revision").and_then(Value::as_u64).unwrap_or(0);
    let parsed = parse_account_catalog(state.get("value")?)?;
    Some(AccountCatalogEntry {
        provider: record.provider(),
        account_id: record.id().to_string(),
        state_revision,
        parsed: Arc::new(parsed),
    })
}

fn build_provider_snapshot(
    by_rowid: &HashMap<i64, AccountCatalogEntry>,
    provider: &str,
) -> Option<Arc<ProviderCatalogSnapshot>> {
    let mut accounts = BTreeMap::new();
    for entry in by_rowid.values() {
        if entry.provider == provider {
            accounts.insert(entry.account_id.clone(), entry.parsed.clone());
        }
    }
    if accounts.is_empty() {
        return None;
    }
    let aggregated = Arc::new(aggregate_account_catalogs(&accounts));
    Some(Arc::new(ProviderCatalogSnapshot {
        accounts,
        aggregated,
    }))
}

impl AccountStore {
    pub(crate) fn catalog_state_revision(&self) -> u64 {
        self.sync_catalog_cache()
    }

    pub(crate) fn provider_catalog(&self, provider: &str) -> Option<Arc<ProviderCatalogSnapshot>> {
        if self.database().is_none() {
            return None;
        }
        let _ = self.sync_catalog_cache();
        let state = self.catalog_cache().state.read().ok()?;
        Some(
            state
                .by_provider
                .get(provider)
                .cloned()
                .unwrap_or_else(empty_provider_snapshot),
        )
    }

    fn sync_catalog_cache(&self) -> u64 {
        let Some(db) = self.database() else {
            return 0;
        };
        let current_rev = db.account_revision();
        if let Ok(state) = self.catalog_cache().state.read() {
            if state.initialized && state.synced_revision == current_rev {
                return state.catalog_revision;
            }
        }
        crate::server::core::blocking::in_place(|| self.sync_catalog_cache_slow(db))
    }

    fn sync_catalog_cache_slow(&self, db: &crate::server::db::Db) -> u64 {
        let cache = self.catalog_cache();
        let _build_guard = cache.build.lock().unwrap_or_else(|error| error.into_inner());
        let current_rev = db.account_revision();
        let (since, fallback_revision) = {
            let Ok(state) = cache.state.read() else {
                return 0;
            };
            if state.initialized && state.synced_revision == current_rev {
                return state.catalog_revision;
            }
            (
                state.initialized.then_some(state.synced_revision),
                state.catalog_revision,
            )
        };

        // 与 `snapshot.rs` 同口径：直接读已提交表快照而不抢账号写锁，并在锁外解码变更行。
        let batch = db
            .with(|conn| -> rusqlite::Result<_> {
                let changes = db.account_changes(since);
                let (replace, rows) = match changes.rows {
                    Some(ids) => (false, read_rows::changed(conn, &ids)?),
                    None => (
                        true,
                        read_rows::all(conn)?
                            .into_iter()
                            .map(|row| (row.rowid, Some(row)))
                            .collect(),
                    ),
                };
                Ok((changes.revision, replace, rows))
            })
            .and_then(Result::ok);

        let Some((revision, replace, raw_rows)) = batch else {
            return fallback_revision;
        };

        let decoded: Vec<(i64, Option<AccountCatalogEntry>)> = raw_rows
            .into_iter()
            .map(|(rowid, raw)| {
                let entry = raw
                    .and_then(|row| read_rows::decode(row).ok())
                    .and_then(|(_, record)| extract_entry(&record));
                (rowid, entry)
            })
            .collect();

        let Ok(mut state) = cache.state.write() else {
            return fallback_revision;
        };

        if replace {
            let next_by_rowid: HashMap<i64, AccountCatalogEntry> = decoded
                .into_iter()
                .filter_map(|(rowid, entry)| entry.map(|value| (rowid, value)))
                .collect();
            let changed_any = !state.initialized
                || next_by_rowid.len() != state.by_rowid.len()
                || next_by_rowid
                    .iter()
                    .any(|(rowid, entry)| !entry_eq(state.by_rowid.get(rowid), Some(entry)));
            if changed_any {
                let providers: HashSet<String> = next_by_rowid
                    .values()
                    .map(|entry| entry.provider.clone())
                    .collect();
                let mut next_by_provider = HashMap::new();
                for provider in providers {
                    if let Some(snapshot) = build_provider_snapshot(&next_by_rowid, &provider) {
                        next_by_provider.insert(provider, snapshot);
                    }
                }
                state.by_rowid = next_by_rowid;
                state.by_provider = next_by_provider;
                state.catalog_revision = state.catalog_revision.saturating_add(1);
            }
            state.initialized = true;
            state.synced_revision = revision;
            return state.catalog_revision;
        }

        let mut dirty_providers = HashSet::<String>::new();
        let mut changed_any = false;
        for (rowid, next_entry) in decoded {
            if entry_eq(state.by_rowid.get(&rowid), next_entry.as_ref()) {
                continue;
            }
            changed_any = true;
            if let Some(previous) = state.by_rowid.remove(&rowid) {
                dirty_providers.insert(previous.provider);
            }
            if let Some(entry) = next_entry {
                dirty_providers.insert(entry.provider.clone());
                state.by_rowid.insert(rowid, entry);
            }
        }
        if changed_any {
            for provider in dirty_providers {
                match build_provider_snapshot(&state.by_rowid, &provider) {
                    Some(snapshot) => {
                        state.by_provider.insert(provider, snapshot);
                    }
                    None => {
                        state.by_provider.remove(&provider);
                    }
                }
            }
            state.catalog_revision = state.catalog_revision.saturating_add(1);
        }
        state.initialized = true;
        state.synced_revision = revision;
        state.catalog_revision
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::db::Db;
    use serde_json::json;

    fn catalog_payload(model_id: &str, refreshed_at: i64) -> Value {
        json!({
            "models": [{ "id": model_id, "name": model_id }],
            "meta": { "remote": true, "refreshed_at": refreshed_at }
        })
    }

    #[test]
    fn unrelated_account_mutations_do_not_bump_catalog_revision() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(&dir.path().join("accounts.db")).unwrap();
        let store = AccountStore::with_db(Some(db));

        let a1 = store
            .plugin_upsert_account(
                "loomy-acc-1",
                "loomy",
                "L1",
                &json!({ "uid": "l1", "accessToken": "tok-1" }),
                &json!({ "nickname": "L1" }),
                false,
            )
            .unwrap();
        let id1 = a1["id"].as_str().unwrap().to_string();

        let a2 = store
            .plugin_upsert_account(
                "codearts-acc-1",
                "codearts",
                "C1",
                &json!({ "uid": "c1", "accessToken": "tok-2" }),
                &json!({ "nickname": "C1" }),
                false,
            )
            .unwrap();
        let id2 = a2["id"].as_str().unwrap().to_string();

        let rev_empty = store.catalog_state_revision();
        assert!(rev_empty >= 1);

        let r1 = store
            .plugin_state_compare_exchange(
                "loomy",
                &id1,
                CATALOG_STATE_KEY,
                0,
                1,
                catalog_payload("glm-5.3", 100),
            )
            .unwrap();
        assert_eq!(r1["updated"], true);

        let r2 = store
            .plugin_state_compare_exchange(
                "codearts",
                &id2,
                CATALOG_STATE_KEY,
                0,
                1,
                catalog_payload("deepseek-v4.1-flash", 100),
            )
            .unwrap();
        assert_eq!(r2["updated"], true);

        let rev_after_seed = store.catalog_state_revision();
        assert!(rev_after_seed > rev_empty);

        let loomy_snap_1 = store.provider_catalog("loomy").unwrap();
        let codearts_snap_1 = store.provider_catalog("codearts").unwrap();
        assert_eq!(loomy_snap_1.aggregated.models.len(), 1);
        assert_eq!(codearts_snap_1.aggregated.models.len(), 1);

        let db_rev_before = store.account_revision_hint();
        store
            .update_account(&id1, &json!({ "priority": 42, "name": "L1-updated" }))
            .unwrap();
        assert!(store.account_revision_hint() > db_rev_before);

        assert_eq!(store.catalog_state_revision(), rev_after_seed);
        let loomy_snap_2 = store.provider_catalog("loomy").unwrap();
        let codearts_snap_2 = store.provider_catalog("codearts").unwrap();
        assert!(Arc::ptr_eq(&loomy_snap_1, &loomy_snap_2));
        assert!(Arc::ptr_eq(&codearts_snap_1, &codearts_snap_2));
    }

    #[test]
    fn catalog_updates_and_account_disable_or_removal_reflect_incrementally() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(&dir.path().join("accounts.db")).unwrap();
        let store = AccountStore::with_db(Some(db));

        let a1 = store
            .plugin_upsert_account(
                "loomy-acc-2",
                "loomy",
                "L1",
                &json!({ "uid": "l1", "accessToken": "tok-1" }),
                &json!({}),
                false,
            )
            .unwrap();
        let id1 = a1["id"].as_str().unwrap().to_string();

        let a2 = store
            .plugin_upsert_account(
                "codearts-acc-2",
                "codearts",
                "C1",
                &json!({ "uid": "c1", "accessToken": "tok-2" }),
                &json!({}),
                false,
            )
            .unwrap();
        let id2 = a2["id"].as_str().unwrap().to_string();

        store
            .plugin_state_compare_exchange(
                "loomy",
                &id1,
                CATALOG_STATE_KEY,
                0,
                1,
                catalog_payload("glm-5.3", 100),
            )
            .unwrap();
        store
            .plugin_state_compare_exchange(
                "codearts",
                &id2,
                CATALOG_STATE_KEY,
                0,
                1,
                catalog_payload("qwen3.8-flash", 100),
            )
            .unwrap();

        let rev1 = store.catalog_state_revision();
        let codearts_snap_1 = store.provider_catalog("codearts").unwrap();

        // 更新 loomy 的 catalog：仅 loomy 应重建，codearts 保持 Arc::ptr_eq
        let res = store
            .plugin_state_compare_exchange(
                "loomy",
                &id1,
                CATALOG_STATE_KEY,
                1,
                1,
                catalog_payload("glm-5.4", 200),
            )
            .unwrap();
        assert_eq!(res["updated"], true);

        let rev2 = store.catalog_state_revision();
        assert!(rev2 > rev1);
        let loomy_snap_2 = store.provider_catalog("loomy").unwrap();
        let codearts_snap_2 = store.provider_catalog("codearts").unwrap();
        assert_eq!(loomy_snap_2.aggregated.models[0]["id"], "glm-5.4");
        assert!(Arc::ptr_eq(&codearts_snap_1, &codearts_snap_2));

        // 禁用 loomy 账号：目录立即清空，codearts 仍不受影响
        store
            .update_account(&id1, &json!({ "enabled": false }))
            .unwrap();
        let rev3 = store.catalog_state_revision();
        assert!(rev3 > rev2);
        let loomy_snap_3 = store.provider_catalog("loomy").unwrap();
        let codearts_snap_3 = store.provider_catalog("codearts").unwrap();
        assert!(loomy_snap_3.aggregated.models.is_empty());
        assert!(Arc::ptr_eq(&codearts_snap_1, &codearts_snap_3));

        // 重新启用再删除：目录先恢复后消失
        store
            .update_account(&id1, &json!({ "enabled": true }))
            .unwrap();
        assert_eq!(
            store.provider_catalog("loomy").unwrap().aggregated.models[0]["id"],
            "glm-5.4"
        );
        store.remove_account(&id1).unwrap();
        assert!(
            store
                .provider_catalog("loomy")
                .unwrap()
                .aggregated
                .models
                .is_empty()
        );
    }
}
