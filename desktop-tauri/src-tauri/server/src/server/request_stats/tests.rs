use super::{
    ApiKeyIdentity, NewRequestEntry, RequestQuery, RequestSource, RequestStats, Retention,
};
use crate::server::db::Db;
use serde_json::{json, Value};

fn temp_stats() -> (tempfile::TempDir, RequestStats) {
    crate::server::logging::process_started_at();
    let dir = tempfile::tempdir().expect("应当能创建临时目录");
    let db = Db::open(&dir.path().join("agent2api.db")).expect("应当能创建临时数据库");
    (dir, RequestStats::with_db(Some(db), Retention::default))
}

fn source(client: &str, key: &str) -> RequestSource {
    RequestSource {
        client: client.into(),
        api_key: ApiKeyIdentity::from_filter(key).expect("合法的密钥筛选标识"),
        ..Default::default()
    }
}

fn entry(id: &str, ts: i64) -> NewRequestEntry {
    let mut entry = NewRequestEntry::new("public-model", 200);
    entry.id = id.into();
    entry.ts = Some(ts);
    entry.provider = Some("test-provider".into());
    entry.upstream_model = "wire-model".into();
    entry.source = source("zcode", "key:a");
    entry.prompt_tokens = 8;
    entry.completion_tokens = 2;
    entry.total_tokens = 10;
    entry
}

fn ids(stats: &RequestStats, query: RequestQuery) -> Vec<String> {
    let page = stats.query_requests(&query);
    let rows = page["entries"].as_array().expect("请求记录应为数组");
    rows.iter()
        .map(|row| row["id"].as_str().unwrap().to_string())
        .collect()
}

fn seed() -> (tempfile::TempDir, RequestStats, i64) {
    let (dir, stats) = temp_stats();
    let now = crate::server::logging::now_ms();
    stats.record(entry("ok", now));
    let mut test = entry("test", now);
    test.is_test = true;
    stats.record(test);
    stats.record_started(
        "running",
        now,
        "public-model",
        "public-model",
        "",
        &source("zcode", "key:a"),
        false,
    );
    let mut failed = entry("failed", now);
    failed.error = Some("回答中途断开".into());
    failed.source = source("codex", "env");
    stats.record(failed);
    let mut unknown = entry("unknown", now);
    unknown.source = RequestSource::default();
    unknown.provider = Some("other-provider".into());
    stats.record(unknown);
    (dir, stats, now)
}

#[test]
fn default_query_keeps_test_running_and_completed_rows() {
    let (_dir, stats, _) = seed();
    let page = stats.query_requests(&RequestQuery::default());
    assert_eq!(page["total"], 5);
    assert_eq!(page["matched"], 5);
    assert_eq!(page["running"], 1);
    assert_eq!(
        ids(&stats, RequestQuery::default()),
        ["unknown", "failed", "running", "test", "ok"]
    );
}

#[test]
fn client_filter_distinguishes_missing_identity_from_no_filter() {
    let (_dir, stats, _) = seed();
    for (client, expected) in [
        ("zcode", vec!["running", "test", "ok"]),
        ("codex", vec!["failed"]),
        ("", vec!["unknown"]),
    ] {
        assert_eq!(
            ids(
                &stats,
                RequestQuery {
                    client: Some(client.into()),
                    ..Default::default()
                }
            ),
            expected
        );
    }
}

#[test]
fn api_key_filters_distinguish_managed_environment_anonymous_and_unknown() {
    let (_dir, stats) = temp_stats();
    let now = crate::server::logging::now_ms();
    for (i, key) in ["key:a", "key:b", "env", "none", "unknown"]
        .iter()
        .enumerate()
    {
        let mut item = entry(key, now + i as i64);
        item.source = source("zcode", key);
        stats.record(item);
    }
    for key in ["key:a", "key:b", "env", "none", "unknown"] {
        assert_eq!(
            ids(
                &stats,
                RequestQuery {
                    api_key: Some(key.into()),
                    ..Default::default()
                }
            ),
            [key]
        );
    }
}

#[test]
fn production_and_completed_filters_work_independently_and_together() {
    let (_dir, stats, _) = seed();
    assert_eq!(
        ids(
            &stats,
            RequestQuery {
                production_only: true,
                ..Default::default()
            }
        ),
        ["unknown", "failed", "running", "ok"]
    );
    assert_eq!(
        ids(
            &stats,
            RequestQuery {
                completed_only: true,
                ..Default::default()
            }
        ),
        ["unknown", "failed", "test", "ok"]
    );
    let query = RequestQuery {
        production_only: true,
        completed_only: true,
        ..Default::default()
    };
    let page = stats.query_requests(&query);
    assert_eq!(page["total"], 5);
    assert_eq!(page["matched"], 3);
    assert_eq!(page["running"], 0);
    assert_eq!(ids(&stats, query), ["unknown", "failed", "ok"]);
}

#[test]
fn all_filters_compose_without_losing_the_new_fields() {
    let (_dir, stats, now) = seed();
    let query = RequestQuery {
        offset: 0,
        limit: Some(1),
        model: Some("wire-model".into()),
        provider: Some("test-provider".into()),
        client: Some("zcode".into()),
        api_key: Some("key:a".into()),
        production_only: true,
        completed_only: true,
        status: Some("ok".into()),
        start: Some(now),
        end: Some(now + 5),
        list_only: false,
    };
    let page = stats.query_requests(&query);
    assert_eq!(page["total"], 5);
    assert_eq!(page["matched"], 1);
    assert_eq!(page["running"], 0);
    assert_eq!(ids(&stats, query), ["ok"]);
}

#[test]
fn success_error_and_running_filters_match_the_displayed_state() {
    let (_dir, stats, _) = seed();
    for (status, expected) in [
        ("ok", vec!["unknown", "test", "ok"]),
        ("error", vec!["failed"]),
        ("running", vec!["running"]),
    ] {
        assert_eq!(
            ids(
                &stats,
                RequestQuery {
                    status: Some(status.into()),
                    ..Default::default()
                }
            ),
            expected
        );
    }
}

#[test]
fn model_filter_matches_both_public_and_upstream_names() {
    let (_dir, stats, _) = seed();
    let query = |model: &str| RequestQuery {
        model: Some(model.into()),
        completed_only: true,
        ..Default::default()
    };
    assert_eq!(
        ids(&stats, query("public-model")),
        ids(&stats, query("wire-model"))
    );
    assert!(ids(&stats, query("unrelated-model")).is_empty());
}

#[test]
fn time_range_is_start_inclusive_and_end_exclusive() {
    let (_dir, stats) = temp_stats();
    let now = crate::server::logging::now_ms();
    for (id, ts) in [
        ("before", now - 3),
        ("start", now - 2),
        ("inside", now - 1),
        ("end", now),
    ] {
        stats.record(entry(id, ts));
    }
    assert_eq!(
        ids(
            &stats,
            RequestQuery {
                start: Some(now - 2),
                end: Some(now),
                ..Default::default()
            }
        ),
        ["inside", "start"]
    );
}

#[test]
fn pagination_is_stable_for_equal_timestamps_and_preserves_counts() {
    let (_dir, stats) = temp_stats();
    let now = crate::server::logging::now_ms();
    for id in ["first", "second", "third"] {
        stats.record(entry(id, now));
    }
    assert_eq!(
        ids(
            &stats,
            RequestQuery {
                offset: 1,
                limit: Some(1),
                ..Default::default()
            }
        ),
        ["second"]
    );
    assert_eq!(
        ids(
            &stats,
            RequestQuery {
                limit: Some(0),
                ..Default::default()
            }
        ),
        ["third"]
    );
    let page = stats.query_requests(&RequestQuery {
        offset: 9,
        ..Default::default()
    });
    assert_eq!(page["entries"], json!([]));
    assert_eq!(page["matched"], 3);
}

#[test]
fn finishing_a_running_request_reuses_the_row_and_is_idempotent() {
    let (_dir, stats) = temp_stats();
    let now = crate::server::logging::now_ms();
    let item = entry("same-request", now);
    stats.record_started(
        &item.id,
        now,
        &item.model,
        &item.model,
        "",
        &item.source,
        false,
    );
    stats.record_started(
        &item.id,
        now,
        &item.model,
        &item.model,
        "",
        &item.source,
        false,
    );
    let started = stats.query_requests(&RequestQuery::default());
    assert_eq!(started["total"], 1);
    assert_eq!(started["running"], 1);
    stats.record(item.clone());
    stats.record(item);
    let page = stats.query_requests(&RequestQuery::default());
    assert_eq!(page["total"], 1);
    assert_eq!(page["running"], 0);
    assert_eq!(page["entries"][0]["totalTokens"], 10);
    let summary = stats.usage_summary("all", None, None);
    assert_eq!(summary["overview"]["requests"], 1);
    assert_eq!(summary["overview"]["tokens"], 10);
}

#[test]
fn model_tests_and_running_requests_do_not_inflate_usage_reports() {
    let (_dir, stats, _) = seed();
    let summary = stats.usage_summary("all", None, None);
    assert_eq!(summary["overview"]["requests"], 3);
    assert_eq!(summary["overview"]["successful"], 2);
    assert_eq!(summary["overview"]["tokens"], 20);
}

#[test]
fn failed_rows_clear_usage_but_keep_failure_and_first_response() {
    let (_dir, stats) = temp_stats();
    let now = crate::server::logging::now_ms();
    for (id, status, error) in [
        ("http-error", 429, None),
        ("stream-error", 200, Some("中途断流".to_string())),
    ] {
        let mut item = entry(id, now);
        item.status = status;
        item.error = error.clone();
        item.cache_read_tokens = 4;
        item.credit = Some(1.5);
        item.first_response_ms = Some(12);
        stats.record(item);
    }
    let page = stats.query_requests(&RequestQuery::default());
    for row in page["entries"].as_array().unwrap() {
        for field in [
            "promptTokens",
            "completionTokens",
            "totalTokens",
            "cacheReadTokens",
        ] {
            assert_eq!(row[field], 0);
        }
        assert!(row["credit"].is_null());
        assert_eq!(row["firstResponseMs"], 12);
    }
    assert_eq!(page["entries"][0]["error"], "中途断流");
}

#[test]
fn missing_database_returns_the_same_empty_query_shape() {
    let stats = RequestStats::with_db(None, Retention::default);
    let page: Value = stats.query_requests(&RequestQuery::default());
    assert_eq!(
        page,
        json!({"entries": [], "total": 0, "matched": 0, "running": 0})
    );
}

#[test]
fn schema_v31_query_plans_use_targeted_covering_indexes() {
    let (_dir, stats, now) = seed();

    fn explain(
        conn: &rusqlite::Connection,
        sql: &str,
        binds: &[rusqlite::types::Value],
    ) -> String {
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        let rows = stmt
            .query_map(rusqlite::params_from_iter(binds.iter()), |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        rows.join(" | ")
    }

    let running_plan = super::sql::FilterPlan::of(&RequestQuery {
        status: Some("running".into()),
        ..Default::default()
    });
    let error_plan = super::sql::FilterPlan::of(&RequestQuery {
        status: Some("error".into()),
        start: Some(0),
        ..Default::default()
    });
    let provider_plan = super::sql::FilterPlan::of(&RequestQuery {
        provider: Some("test-provider".into()),
        ..Default::default()
    });

    let (eqp_running, eqp_error, eqp_error_page, eqp_provider, eqp_summary_rows, eqp_cache_windows) = stats
        .with_read_conn(|conn| {
            let eqp_running = explain(
                conn,
                &format!(
                    "SELECT row_id FROM requests{} ORDER BY ts DESC, row_id DESC LIMIT 50",
                    running_plan.where_sql_for_test()
                ),
                running_plan.binds_for_test(),
            );
            let eqp_error = explain(
                conn,
                &format!("SELECT COUNT(*) FROM requests{}", error_plan.where_sql_for_test()),
                error_plan.binds_for_test(),
            );
            let eqp_error_page = explain(
                conn,
                &format!(
                    "SELECT row_id FROM requests{} ORDER BY ts DESC, row_id DESC LIMIT 50",
                    error_plan.where_sql_for_test()
                ),
                error_plan.binds_for_test(),
            );
            let eqp_provider = explain(
                conn,
                &format!(
                    "SELECT row_id FROM requests{} ORDER BY ts DESC, row_id DESC LIMIT 50",
                    provider_plan.where_sql_for_test()
                ),
                provider_plan.binds_for_test(),
            );
            let eqp_summary_rows = explain(
                conn,
                &format!(
                    "SELECT ts, model, upstream_model, total_tokens, cache_read_tokens, prompt_tokens, \
                     completion_tokens, (status >= 200 AND status < 300 AND error IS NULL) \
                     FROM requests WHERE ts >= {} AND ts <= {now} AND is_test = 0 AND status <> 0",
                    now - 86_400_000
                ),
                &[],
            );
            let eqp_cache_windows = explain(
                conn,
                &format!(
                    "SELECT COALESCE(SUM(CASE WHEN ts >= {} THEN cache_read_tokens END), 0), \
                     COALESCE(SUM(CASE WHEN ts >= {} THEN prompt_tokens END), 0) \
                     FROM requests WHERE ts >= {} AND ts <= {now} AND is_test = 0",
                    now - 600_000,
                    now - 600_000,
                    now - 7 * 86_400_000
                ),
                &[],
            );
            Ok((
                eqp_running,
                eqp_error,
                eqp_error_page,
                eqp_provider,
                eqp_summary_rows,
                eqp_cache_windows,
            ))
        })
        .unwrap();

    assert!(
        eqp_running.contains("idx_requests_running") && !eqp_running.contains("TEMP B-TREE"),
        "running plan: {eqp_running}"
    );
    assert!(
        eqp_error.contains("COVERING INDEX idx_requests_errors_ts"),
        "error plan: {eqp_error}"
    );
    assert!(
        eqp_error_page.contains("idx_requests_errors_ts") && !eqp_error_page.contains("TEMP B-TREE"),
        "error page plan: {eqp_error_page}"
    );
    assert!(
        eqp_provider.contains("idx_requests_provider_ts") && !eqp_provider.contains("TEMP B-TREE"),
        "provider plan: {eqp_provider}"
    );
    assert!(
        eqp_summary_rows.contains("COVERING INDEX idx_requests_summary"),
        "summary_rows plan: {eqp_summary_rows}"
    );
    assert!(
        eqp_cache_windows.contains("COVERING INDEX idx_requests_summary"),
        "cache_windows plan: {eqp_cache_windows}"
    );
}

#[test]
fn usage_summary_and_today_count_include_unflushed_daily_buffer_while_writer_locked() {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    let (_dir, stats) = temp_stats();
    let stats = Arc::new(stats);
    let now = crate::server::logging::now_ms();
    stats.record(entry("buffered-1", now));

    let writer_stats = Arc::clone(&stats);
    let (locked_tx, locked_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let db = writer_stats.db.as_ref().unwrap();
        db.with_mut(|_| {
            locked_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(Duration::from_secs(5));
        });
    });

    locked_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let reader_stats = Arc::clone(&stats);
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let count = reader_stats.today_request_count();
        let summary = reader_stats.usage_summary("today", None, None);
        let _ = done_tx.send((count, summary));
    });

    let (count, summary) = done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("usage_summary 与 today_request_count 不应等待主写锁");
    assert_eq!(count["overview"]["requests"], 1);
    assert_eq!(summary["overview"]["requests"], 1);
    assert_eq!(summary["overview"]["tokens"], 10);

    let _ = release_tx.send(());
    writer.join().unwrap();
}

#[test]
fn source_filter_options_formats_all_key_sources_accurately() {
    let (_dir, stats) = temp_stats();
    let now = crate::server::logging::now_ms();
    for (i, key) in ["key:alpha", "env", "none", "unknown"].iter().enumerate() {
        let mut item = entry(&format!("k-{i}"), now + i as i64);
        item.source = source("zcode", key);
        if *key == "key:alpha" {
            item.source.api_key.name = "Alpha Key".into();
        }
        stats.record(item);
    }
    let filters = stats.filter_options();
    let keys = filters["apiKeys"].as_array().unwrap();
    let map: std::collections::HashMap<_, _> = keys
        .iter()
        .map(|k| (k["value"].as_str().unwrap(), k["label"].as_str().unwrap()))
        .collect();
    assert_eq!(map.get("key:alpha").copied(), Some("Alpha Key"));
    assert_eq!(map.get("env").copied(), Some("环境变量 Key"));
    assert_eq!(map.get("none").copied(), Some("免鉴权调用"));
    assert_eq!(map.get("unknown").copied(), Some("历史未记录 Key"));
}

#[test]
fn cold_json_is_stored_in_request_details_and_deleted_by_trigger() {
    use super::record::{AttemptDetail, RetryEvent, SensitiveHit};
    use crate::server::core::client::ClientEvidence;
    use super::flow::FlowQuery;

    let (_dir, stats) = temp_stats();
    let now = crate::server::logging::now_ms();
    let mut item = entry("split-1", now);
    item.attempts = 1;
    item.route = json!({
        "stickyHit": true,
        "sessionId": "sess-1",
        "sessionSource": "cookie",
        "visionCompatibility": { "mode": "strip" }
    });
    item.attempt_details = vec![AttemptDetail {
        provider: "test-provider".into(),
        account: "acc".into(),
        account_id: "acc-1".into(),
        candidate_id: String::new(),
        member_id: String::new(),
        model: "wire-model".into(),
        reasoning: String::new(),
        reason: "ok".into(),
        sticky_hit: true,
        started_at: Some(now),
        response_at: Some(now + 5),
        finished_at: Some(now + 10),
        status: Some(200),
        error: None,
        retries: vec![RetryEvent {
            reason: "429".into(),
            status: Some(429),
            delay_ms: 100,
        }],
        notice: None,
        body_bytes: Some(42),
        error_policy: None,
    }];
    item.sensitive_hits = vec![SensitiveHit {
        word: "secret-rule".into(),
        count: 2,
    }];
    item.source.evidence = Some(ClientEvidence {
        method: "header".into(),
        client_name: "Codex CLI".into(),
        user_agent: "codex/1.0".into(),
        ..Default::default()
    });
    stats.record(item);

    // 1. 主表冷 JSON 列必须为极小占位值，热标量与 request_details 必须落库
    let (main_row, detail_row) = stats
        .with_read_conn(|conn| {
            let main_row: (Option<String>, String, String, String, i64, i64, String, String, String, String, String) = conn.query_row(
                "SELECT route, attempt_details, sensitive_hits, client_evidence, detail_flags, internal_retries, \
                        vision_mode, recorded_client_name, client_user_agent, session_id, session_source \
                 FROM requests WHERE id = 'split-1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?, r.get(8)?, r.get(9)?, r.get(10)?)),
            )?;
            let detail_row: (Option<String>, String, String, String) = conn.query_row(
                "SELECT route, attempt_details, sensitive_hits, client_evidence FROM request_details",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
            Ok((main_row, detail_row))
        })
        .unwrap();

    assert_eq!(main_row.0, None);
    assert_eq!(main_row.1, "[]");
    assert_eq!(main_row.2, "[]");
    assert_eq!(main_row.3, "{}");
    assert_ne!(main_row.4 & super::sql::FLAG_DETAILS_SPLIT, 0);
    assert_eq!(main_row.5, 1);
    assert_eq!(main_row.6, "strip");
    assert_eq!(main_row.7, "Codex CLI");
    assert_eq!(main_row.8, "codex/1.0");
    assert_eq!(main_row.9, "sess-1");
    assert_eq!(main_row.10, "cookie");
    assert!(detail_row.0.unwrap().contains("sess-1"));
    assert!(detail_row.1.contains("429"));
    assert!(detail_row.2.contains("secret-rule"));
    assert!(detail_row.3.contains("Codex CLI"));

    // 2. list_only 列表响应零回表返回所有派生标记
    let list_page = stats.query_requests(&RequestQuery {
        list_only: true,
        ..Default::default()
    });
    let row = &list_page["entries"][0];
    assert_eq!(row["hasRoute"], true);
    assert_eq!(row["visionMode"], "strip");
    assert_eq!(row["hasSensitiveHits"], true);
    assert_eq!(row["hasProcessFacts"], true);
    assert_eq!(row["internalRetries"], 1);
    assert_eq!(row["hasClientEvidence"], true);
    assert_eq!(row["clientLabel"], "Codex CLI");
    assert_eq!(row["clientUserAgent"], "codex/1.0");

    // 3. 详情与完整页、flow/trace/replays 均能还原冷表
    let detail = stats.request_detail(row["rowId"].as_i64(), None, None).unwrap();
    assert_eq!(detail["route"]["sessionId"], "sess-1");
    assert_eq!(detail["attemptDetails"][0]["retries"][0]["reason"], "429");
    assert_eq!(detail["sensitiveHits"][0]["word"], "secret-rule");
    assert_eq!(detail["clientEvidence"]["method"], "header");

    let replays = stats.routing_replays(None, 0, 10, "sticky");
    assert_eq!(replays["matched"], 1);
    let trace = stats.routing_trace(None, 0, 10, "sticky", "", None);
    assert_eq!(trace["matched"], 1);
    let flow = stats.routing_flow(
        &FlowQuery {
            model: String::new(),
            minutes: 60,
            running_only: false,
            search: String::new(),
            session_search: String::new(),
            path: String::new(),
            session: String::new(),
            client: None,
            provider: String::new(),
            account: String::new(),
            activity: String::new(),
            offset: 0,
            limit: 20,
        },
        None,
    );
    assert_eq!(flow["paths"][0]["stickyHits"], 1);

    // 4. 删除 requests 行时触发器必须同步级联清理 request_details
    stats.clear_requests(
        &RequestQuery {
            provider: Some("test-provider".into()),
            ..Default::default()
        },
        false,
    );
    let remaining_details: i64 = stats
        .with_read_conn(|conn| conn.query_row("SELECT COUNT(*) FROM request_details", [], |r| r.get(0)))
        .unwrap();
    assert_eq!(remaining_details, 0);
}

#[test]
fn legacy_inline_rows_still_decode_for_list_and_detail() {
    let (_dir, stats) = temp_stats();
    let now = crate::server::logging::now_ms();
    let guard = stats.guard();
    stats
        .with_conn_mut(&guard, |conn| {
            conn.execute(
                "INSERT INTO requests (id, ts, model, status, duration_ms, attempts, provider, route, client, client_evidence, detail_flags) \
                 VALUES ('legacy-1', ?1, 'm', 200, 10, 1, 'p', '{\"stickyHit\":true,\"visionCompatibility\":{\"mode\":\"assist_call\"}}', \
                 'zcode', '{\"method\":\"ua\",\"clientName\":\"Legacy Client\",\"userAgent\":\"legacy/1.0\"}', 0)",
                rusqlite::params![now],
            )?;
            Ok(())
        })
        .unwrap();
    drop(guard);

    let list_page = stats.query_requests(&RequestQuery {
        list_only: true,
        ..Default::default()
    });
    let row = &list_page["entries"][0];
    assert_eq!(row["id"], "legacy-1");
    assert_eq!(row["hasRoute"], true);
    assert_eq!(row["visionMode"], "assist_call");
    assert_eq!(row["hasClientEvidence"], true);
    assert_eq!(row["clientLabel"], "Legacy Client");
    assert_eq!(row["clientUserAgent"], "legacy/1.0");

    let detail = stats.request_detail(row["rowId"].as_i64(), None, None).unwrap();
    assert_eq!(detail["route"]["stickyHit"], true);
    assert_eq!(detail["clientEvidence"]["clientName"], "Legacy Client");
}

#[tokio::test]
async fn enqueue_started_coalesces_with_record_finalize_update_and_query() {
    use std::sync::Arc;
    use super::RunningProgress;

    let now = crate::server::logging::now_ms();

    // 1. Fast completion before flush: record drops pending start and writes 1 terminal row cleanly.
    {
        let (_dir, stats) = temp_stats();
        let stats = Arc::new(stats);
        let item = entry("fast-done", now);
        stats.enqueue_started(&item.id, now, &item.model, &item.model, "", &item.source, false);
        stats.record(item);
        stats.flush_pending_started();
        let page = stats.query_requests(&RequestQuery::default());
        assert_eq!(page["total"], 1);
        assert_eq!(page["running"], 0);
        assert_eq!(page["entries"][0]["status"], 200);
        assert_eq!(page["entries"][0]["totalTokens"], 10);
    }

    // 2. Client disconnect before flush: finalize_interrupted materializes and settles 408.
    {
        let (_dir, stats) = temp_stats();
        let stats = Arc::new(stats);
        stats.enqueue_started("early-abort", now, "m", "m", "", &source("zcode", "key:a"), false);
        stats.finalize_interrupted(
            "early-abort",
            "客户端中断",
            &RunningProgress {
                provider: "codearts".into(),
                account_id: "a1".into(),
                account_name: "A1".into(),
                upstream_model: "m".into(),
                upstream_reasoning: String::new(),
                attempts: 1,
                first_response_ms: None,
                phase: "connecting".into(),
                phase_started_at: Some(now),
                attempt_details: Vec::new(),
                route: None,
                sensitive_hits: Vec::new(),
            },
            now + 15,
        );
        let page = stats.query_requests(&RequestQuery::default());
        assert_eq!(page["total"], 1);
        assert_eq!(page["running"], 0);
        assert_eq!(page["entries"][0]["status"], 408);
        let summary = stats.usage_summary("all", None, None);
        assert_eq!(summary["overview"]["requests"], 1);
    }

    // 3. Live progress update before flush: update_running inserts and updates in one tx.
    {
        let (_dir, stats) = temp_stats();
        let stats = Arc::new(stats);
        stats.enqueue_started("in-flight", now, "m", "m", "", &source("zcode", "key:a"), false);
        stats.update_running(
            "in-flight",
            &RunningProgress {
                provider: "loomy".into(),
                account_id: "a2".into(),
                account_name: "A2".into(),
                upstream_model: "wire-m".into(),
                upstream_reasoning: String::new(),
                attempts: 2,
                first_response_ms: Some(120),
                phase: "streaming".into(),
                phase_started_at: Some(now + 120),
                attempt_details: Vec::new(),
                route: None,
                sensitive_hits: Vec::new(),
            },
        );
        let page = stats.query_requests(&RequestQuery::default());
        assert_eq!(page["total"], 1);
        assert_eq!(page["running"], 1);
        assert_eq!(page["entries"][0]["provider"], "loomy");
        assert_eq!(page["entries"][0]["attempts"], 2);
    }

    // 4. Read query before timer flush forces pending started rows visible.
    {
        let (_dir, stats) = temp_stats();
        let stats = Arc::new(stats);
        stats.enqueue_started("queued-read", now, "m", "m", "", &source("zcode", "key:a"), false);
        let page = stats.query_requests(&RequestQuery::default());
        assert_eq!(page["total"], 1);
        assert_eq!(page["running"], 1);
        assert_eq!(page["entries"][0]["id"], "queued-read");
    }
}
