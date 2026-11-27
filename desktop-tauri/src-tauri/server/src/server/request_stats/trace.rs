use rusqlite::params;
use serde_json::{json, Value};

use super::{entry_json, sql, RequestEntry, RequestStats};

fn trace_json(entry: &RequestEntry, trace_key: String) -> Value {
    let mut value = entry_json(entry);
    value["traceKey"] = Value::String(trace_key);
    value
}

impl RequestStats {
    pub fn routing_trace(
        &self,
        model: Option<&str>,
        offset: usize,
        limit: usize,
        status: &str,
        search: &str,
        bounds: Option<(i64, i64)>,
    ) -> Value {
        let model = model.unwrap_or("");
        let limit = limit.clamp(1, 100);
        let offset = offset.min(i64::MAX as usize);
        let (start, end) = bounds.unwrap_or((0, i64::MAX));
        let status_clause = match status {
            "error" => " AND (r.error IS NOT NULL OR (r.status <> 0 AND (r.status < 200 OR r.status >= 300)))",
            "ok" => " AND r.status >= 200 AND r.status < 300 AND r.error IS NULL",
            "running" => " AND r.status = 0 AND r.error IS NULL",
            "switched" => " AND r.attempts > 1",
            "sticky" => " AND CASE WHEN (r.detail_flags & 32) <> 0 THEN ((r.detail_flags & 16) <> 0) WHEN json_valid(r.route) THEN json_extract(r.route, '$.stickyHit') = 1 ELSE 0 END = 1",
            _ => "",
        };
        let where_clause = format!(
            "WHERE (?1 = '' OR r.model = ?1 COLLATE NOCASE OR r.client_model = ?1 COLLATE NOCASE OR r.upstream_model = ?1 COLLATE NOCASE)
             AND r.ts >= ?2 AND r.ts < ?3
             AND (?4 = '' OR instr(lower(r.model), lower(?4)) > 0 OR instr(lower(r.client_model), lower(?4)) > 0
                 OR instr(lower(r.upstream_model), lower(?4)) > 0 OR instr(lower(r.id), lower(?4)) > 0){status_clause}",
        );
        let loaded = self.with_read_conn(|conn| {
            let matched: i64 = conn.query_row(
                &format!("SELECT COUNT(*) FROM requests r {where_clause}"),
                params![model, start, end, search], |row| row.get(0),
            )?;
            let query = format!(
                "SELECT {}, r.row_id AS trace_key FROM {} {where_clause}
                 ORDER BY r.ts DESC, r.row_id DESC LIMIT ?5 OFFSET ?6",
                sql::REQUEST_JOIN_COLUMNS,
                sql::REQUEST_DETAILS_JOIN,
            );
            let mut stmt = conn.prepare(&query)?;
            let mut rows = stmt.query(params![model, start, end, search, limit as i64, offset as i64])?;
            let mut entries = Vec::new();
            while let Some(row) = rows.next()? {
                entries.push((sql::decode_request(row)?, row.get::<_, i64>("trace_key")?.to_string()));
            }
            Ok((matched, entries))
        });
        let Some((matched, rows)) = loaded else {
            return json!({ "available": false, "entries": [], "matched": 0 });
        };
        let entries: Vec<Value> = rows.into_iter().map(|(entry, key)| trace_json(&entry, key)).collect();
        json!({ "available": true, "entries": entries, "matched": matched, "offset": offset, "limit": limit })
    }

    pub fn routing_trace_request(&self, id: &str, ts: Option<i64>) -> Option<Value> {
        let loaded = self.with_read_conn(|conn| {
            let query = format!(
                "SELECT {}, r.row_id AS trace_key FROM {} WHERE r.id = ?1 AND (?2 IS NULL OR r.ts = ?2)
                 ORDER BY (r.status = 0) DESC, r.ts DESC, r.row_id DESC LIMIT 1",
                sql::REQUEST_JOIN_COLUMNS,
                sql::REQUEST_DETAILS_JOIN,
            );
            let mut stmt = conn.prepare(&query)?;
            let mut rows = stmt.query(params![id, ts])?;
            let Some(row) = rows.next()? else { return Ok(None) };
            Ok(Some((sql::decode_request(row)?, row.get::<_, i64>("trace_key")?.to_string())))
        });
        let (entry, key) = loaded??;
        Some(trace_json(&entry, key))
    }
}
