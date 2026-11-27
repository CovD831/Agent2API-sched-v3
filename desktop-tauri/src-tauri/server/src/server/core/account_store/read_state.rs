use std::collections::HashMap;
use std::sync::Arc;

use super::{
    read_rows,
    state::{AccountState, StoredAccount},
    AccountStore,
};

/// 按 `Db::account_revision()` 与行级变更日志同步的解密账号内存缓存。
///
/// 解决千号池热路径上每次 `get_session_by_id` / `plugin_account` 都要抢 `AccountStore::lock` + `Db` 锁
/// 并重复做 `enc1:` AES-GCM 解密与 JSON 解析的性能瓶颈。
#[derive(Default)]
pub(super) struct RecordCache {
    revision: u64,
    initialized: bool,
    by_id: HashMap<String, Option<Arc<StoredAccount>>>,
    id_by_rowid: HashMap<i64, String>,
}

impl AccountStore {
    pub(super) fn read_record(&self, id: &str) -> Option<StoredAccount> {
        if id.is_empty() {
            return None;
        }
        let db = self.database()?;
        let current_rev = db.account_revision();

        // ① 读锁快路径：revision 一致且已缓存时，零 SQLite 访问、零解密直接返回
        if let Ok(cache) = self.record_cache().read() {
            if cache.initialized && cache.revision == current_rev {
                if let Some(hit) = cache.by_id.get(id) {
                    return hit.as_ref().map(|arc| (**arc).clone());
                }
            }
        }

        // ② Revision 推进时先按行级 journal 增量淘汰脏行，未变更的数千账号继续保留缓存
        if let Ok(mut cache) = self.record_cache().write() {
            if !cache.initialized {
                cache.revision = current_rev;
                cache.initialized = true;
            } else if cache.revision != current_rev {
                let since = Some(cache.revision);
                let changes = db.account_changes(since);
                match changes.rows {
                    Some(changed_rowids) => {
                        for rowid in changed_rowids {
                            if let Some(old_id) = cache.id_by_rowid.remove(&rowid) {
                                cache.by_id.remove(&old_id);
                            }
                        }
                        // 若有按 id 新增（此前缓存过 None），清理负缓存以确保新插入立即可见
                        cache.by_id.retain(|_, v| v.is_some());
                        cache.revision = changes.revision;
                    }
                    None => {
                        cache.by_id.clear();
                        cache.id_by_rowid.clear();
                        cache.revision = changes.revision;
                    }
                }
            }
            if cache.revision == db.account_revision() {
                if let Some(hit) = cache.by_id.get(id) {
                    return hit.as_ref().map(|arc| (**arc).clone());
                }
            }
        }

        // ③ 缓存未命中：单行主键读取并在锁外完成解密回填
        let (rev_before, raw_opt) = db.with(|conn| {
            let rev = db.account_revision();
            let row = read_rows::by_id(conn, id)?;
            Ok::<_, rusqlite::Error>((rev, row))
        }).and_then(Result::ok)?;

        let decoded = match raw_opt {
            Some(raw) => match read_rows::decode(raw) {
                Ok((rowid, record)) => Some((rowid, Arc::new(record))),
                Err(error) => {
                    super::sql::warn_load_failure(
                        "按 id 读取账号",
                        &format!("账号 {id} 读不出来：{}", error.describe()),
                    );
                    return None;
                }
            },
            None => None,
        };

        if let Ok(mut cache) = self.record_cache().write() {
            if cache.initialized && cache.revision == rev_before && db.account_revision() == rev_before {
                match &decoded {
                    Some((rowid, arc)) => {
                        cache.id_by_rowid.insert(*rowid, id.to_string());
                        cache.by_id.insert(id.to_string(), Some(arc.clone()));
                    }
                    None => {
                        if cache.by_id.len() < 16_384 {
                            cache.by_id.insert(id.to_string(), None);
                        }
                    }
                }
            }
        }

        decoded.map(|(_, arc)| (*arc).clone())
    }

    pub(super) fn read_provider(&self, provider: &str) -> Vec<StoredAccount> {
        let Some(db) = self.database() else { return Vec::new() };
        let rows = {
            let _guard = self.guard();
            db.with(|conn| read_rows::provider(conn, provider))
                .and_then(Result::ok)
                .unwrap_or_default()
        };
        read_rows::decode_many(rows, &format!("读取 {provider} 的账号"))
            .into_iter()
            .map(|(_, record)| record)
            .collect()
    }

    pub(super) fn read_state(&self) -> AccountState {
        let Some(db) = self.database() else {
            return AccountState::default();
        };
        let result = {
            let _guard = self.guard();
            db.with(|conn| {
                Ok::<_, rusqlite::Error>((
                    read_rows::all(conn)?,
                    super::sql::load_priority_scope(conn)?,
                ))
            })
            .and_then(Result::ok)
        };
        let Some((rows, priority_scope)) = result else {
            return AccountState::default();
        };
        let accounts = read_rows::decode_many(rows, "读取全部账号")
            .into_iter()
            .map(|(_, record)| record)
            .collect();
        AccountState {
            accounts,
            priority_scope,
        }
    }
}
