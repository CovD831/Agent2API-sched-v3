use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use super::{database, row_to_record, UsageRecord, RECORD_COLUMNS};

struct CachedRecords {
    revision: u64,
    database: u64,
    external: i64,
    checked_at: Instant,
    records: Arc<HashMap<String, Arc<UsageRecord>>>,
}

#[derive(Default)]
pub(super) struct Cache {
    build: Mutex<()>,
    state: RwLock<Option<CachedRecords>>,
}

pub(super) fn global() -> &'static Cache {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE.get_or_init(Cache::default)
}

fn fresh(cache: &Cache, revision: u64, database: u64) -> Option<Arc<HashMap<String, Arc<UsageRecord>>>> {
    let state = cache.state.read().ok()?;
    let cached = state.as_ref()?;
    (cached.revision == revision && cached.database == database
        && cached.checked_at.elapsed() < Duration::from_secs(10)).then(|| cached.records.clone())
}

impl Cache {
    pub(super) fn snapshot_with(&self, db: &crate::server::db::Db) -> Arc<HashMap<String, Arc<UsageRecord>>> {
        let database = crate::server::db::query_cache_generation();
        if let Some(records) = fresh(self, db.usage_revision(), database) { return records; }
        let _build = self.build.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(records) = fresh(self, db.usage_revision(), database) { return records; }
        let previous = self.state.read().ok().and_then(|state| state.as_ref().map(|cached| {
            (cached.revision, cached.database, cached.external, cached.records.clone())
        }));
        let loaded = db.with(|conn| -> rusqlite::Result<_> {
            let external: i64 = conn.pragma_query_value(None, "data_version", |row| row.get(0))?;
            let revision = db.usage_revision();
            if let Some((old_revision, old_database, old_external, records)) = &previous {
                if *old_revision == revision && *old_database == database && *old_external == external {
                    return Ok((revision, external, records.clone()));
                }
            }
            let mut statement = conn.prepare(&format!("SELECT account_id, {RECORD_COLUMNS} FROM account_usage_records"))?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>("account_id")?, Arc::new(row_to_record(row)?)))
            })?;
            let records = rows.collect::<rusqlite::Result<HashMap<_, _>>>()?;
            Ok((revision, external, Arc::new(records)))
        }).and_then(Result::ok);
        let Some((revision, external, records)) = loaded else { return Arc::new(HashMap::new()); };
        if db.usage_revision() == revision && crate::server::db::query_cache_generation() == database {
            if let Ok(mut state) = self.state.write() {
                *state = Some(CachedRecords { revision, database, external, checked_at: Instant::now(), records: records.clone() });
            }
        }
        records
    }

    fn mutate_if_sequential(
        &self,
        db: &crate::server::db::Db,
        rev_before: u64,
        rev_after: u64,
        apply: impl FnOnce(&mut HashMap<String, Arc<UsageRecord>>),
    ) {
        let database = crate::server::db::query_cache_generation();
        let Ok(mut state) = self.state.write() else { return };
        let Some(cached) = state.as_mut() else { return };
        if cached.database != database {
            *state = None;
            return;
        }
        if cached.revision == rev_before && db.usage_revision() == rev_after {
            apply(Arc::make_mut(&mut cached.records));
            cached.revision = rev_after;
            cached.checked_at = Instant::now();
        } else if cached.revision != rev_after {
            *state = None;
        }
    }

    pub(super) fn note_updated_with(
        &self,
        db: &crate::server::db::Db,
        account_id: &str,
        record: UsageRecord,
        rev_before: u64,
        rev_after: u64,
    ) {
        self.mutate_if_sequential(db, rev_before, rev_after, |map| {
            map.insert(account_id.to_string(), Arc::new(record));
        });
    }

    pub(super) fn note_attempt_with(
        &self,
        db: &crate::server::db::Db,
        account_id: &str,
        now: i64,
        rev_before: u64,
        rev_after: u64,
    ) {
        self.mutate_if_sequential(db, rev_before, rev_after, |map| {
            let updated = match map.get(account_id) {
                Some(existing) => {
                    let mut next = (**existing).clone();
                    next.last_attempt_at = now;
                    next
                }
                None => UsageRecord {
                    usage: None,
                    error: None,
                    diagnostics: None,
                    code: None,
                    remaining: None,
                    unlimited: false,
                    last_success_at: 0,
                    last_attempt_at: now,
                    query_config: String::new(),
                    query_request_id: String::new(),
                },
            };
            map.insert(account_id.to_string(), Arc::new(updated));
        });
    }
    #[cfg(test)]
    pub(super) fn is_cached(&self) -> bool {
        self.state.read().is_ok_and(|guard| guard.is_some())
    }
}

pub(super) fn snapshot() -> Arc<HashMap<String, Arc<UsageRecord>>> {
    let Some(db) = database() else { return Arc::new(HashMap::new()); };
    global().snapshot_with(&db)
}

/// 单账号写回后在库锁外增量同步缓存，避免 4,000+ 行全表重扫；若并发写入错序则清缓存交由下次安全重载。
pub(super) fn note_updated(account_id: &str, record: UsageRecord, rev_before: u64, rev_after: u64) {
    let Some(db) = database() else { return };
    global().note_updated_with(&db, account_id, record, rev_before, rev_after);
}

pub(super) fn note_attempt(account_id: &str, now: i64, rev_before: u64, rev_after: u64) {
    let Some(db) = database() else { return };
    global().note_attempt_with(&db, account_id, now, rev_before, rev_after);
}
