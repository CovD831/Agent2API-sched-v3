use super::*;

fn stamp(text: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(text)
        .unwrap()
        .timestamp_millis()
}

#[test]
fn fixed_windows_change_at_the_exact_boundary() {
    let window = Window {
        reset: Reset::Fixed,
        seconds: 60,
        starts_at: 10_000,
        ..Default::default()
    };
    assert_eq!(window_bounds(&window, 69_999), (10_000, 70_000));
    assert_eq!(window_bounds(&window, 70_000), (70_000, 130_000));
}

#[test]
fn daily_weekly_and_monthly_windows_use_the_configured_timezone() {
    let now = stamp("2024-03-01T00:00:00+08:00");
    for (reset, start, end) in [
        (
            Reset::Daily,
            "2024-03-01T00:00:00+08:00",
            "2024-03-02T00:00:00+08:00",
        ),
        (
            Reset::Weekly,
            "2024-02-26T00:00:00+08:00",
            "2024-03-04T00:00:00+08:00",
        ),
        (
            Reset::Monthly,
            "2024-03-01T00:00:00+08:00",
            "2024-04-01T00:00:00+08:00",
        ),
    ] {
        let window = Window {
            reset,
            utc_offset_minutes: Some(480),
            ..Default::default()
        };
        assert_eq!(
            window_bounds(&window, now),
            (stamp(start), stamp(end)),
            "{reset:?}"
        );
    }
    let february = Window {
        reset: Reset::Monthly,
        utc_offset_minutes: Some(480),
        ..Default::default()
    };
    assert_eq!(
        window_bounds(&february, now - 1),
        (stamp("2024-02-01T00:00:00+08:00"), now)
    );
}

#[test]
fn rolling_window_expires_out_of_order_events_without_losing_boundary_event() {
    let mut count = Count {
        window: Window {
            reset: Reset::Rolling,
            seconds: 60,
            ..Default::default()
        },
        ..Default::default()
    };
    for (ts, tokens) in [(50_000, 5), (10_000, 1), (40_000, 4)] {
        count.add(Event {
            ts,
            tokens,
            requests: 1,
            model: "MODEL-A".into(),
        });
    }
    count.advance(100_000);
    assert_eq!((count.tokens, count.requests), (9, 2));
    assert_eq!(count.by_model["model-a"].tokens, 9);
    count.advance(100_001);
    assert_eq!((count.tokens, count.requests), (5, 1));
    count.advance(110_001);
    assert_eq!((count.tokens, count.requests), (0, 0));
    assert!(count.events.is_empty());
}

#[test]
fn fixed_window_reset_clears_model_totals() {
    let mut count = Count {
        window: Window {
            reset: Reset::Fixed,
            seconds: 60,
            ..Default::default()
        },
        ..Default::default()
    };
    count.advance(0);
    count.add(Event {
        ts: 10,
        model: "model".into(),
        tokens: 100,
        requests: 1,
    });
    count.advance(60_000);
    assert_eq!((count.tokens, count.requests), (0, 0));
    assert!(count.by_model.is_empty());
}

#[test]
fn duplicate_request_is_counted_once_and_negative_tokens_are_clamped() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    crate::server::db::schema::migrate(&conn).unwrap();
    assert!(
        insert_event(&conn, "account", "request-one", "model", "paid", -1, 100)
            .unwrap()
            .is_some()
    );
    assert!(
        insert_event(&conn, "account", "request-one", "model", "paid", 500, 101)
            .unwrap()
            .is_none()
    );
    let totals: (i64, i64, i64) = conn
        .query_row(
            "SELECT COUNT(*),SUM(tokens),SUM(requests) FROM account_limiter_events",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(totals, (1, 0, 1));
    assert!(insert_event(&conn, "", "request-two", "model", "", 1, 101)
        .unwrap()
        .is_none());
}

#[test]
fn prune_expired_deletes_old_rolling_events_but_protects_total_window_accounts() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    crate::server::db::schema::migrate(&conn).unwrap();

    let now = 2_000_000_000_000i64;
    let old_ts = now - 40 * 86_400_000;
    let recent_ts = now - 5 * 86_400_000;

    insert_event(&conn, "acc-rolling", "old-r", "m", "", 100, old_ts).unwrap();
    insert_event(&conn, "acc-rolling", "new-r", "m", "", 200, recent_ts).unwrap();
    insert_event(&conn, "acc-total", "old-t", "m", "", 300, old_ts).unwrap();

    let accounts = vec![
        serde_json::json!({
            "id": "acc-rolling",
            "limiterGroups": [{
                "enabled": true,
                "conditions": [{
                    "metric": "tokens",
                    "comparison": "gte",
                    "threshold": 1000,
                    "window": { "reset": "rolling", "seconds": 86400 }
                }]
            }]
        }),
        serde_json::json!({
            "id": "acc-total",
            "limiterGroups": [{
                "enabled": true,
                "conditions": [{
                    "metric": "tokens",
                    "comparison": "gte",
                    "threshold": 1000,
                    "window": { "reset": "total", "startsAt": 0, "endsAt": 0 }
                }]
            }]
        }),
    ];

    let eqp: String = conn
        .query_row(
            "EXPLAIN QUERY PLAN SELECT row_id FROM account_limiter_events WHERE ts < ?1 ORDER BY ts ASC LIMIT 100",
            params![now - 35 * 86_400_000],
            |row| row.get(3),
        )
        .unwrap();
    assert!(
        eqp.contains("idx_limiter_events_ts"),
        "expected idx_limiter_events_ts in EQP: {eqp}"
    );

    let deleted = prune_expired_in_conn(&conn, &accounts, now, 100).unwrap();
    assert_eq!(deleted, 1);

    let remaining: Vec<String> = {
        let mut stmt = conn
            .prepare("SELECT id FROM account_limiter_events ORDER BY row_id")
            .unwrap();
        stmt.query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    assert_eq!(remaining, vec!["new-r".to_string(), "old-t".to_string()]);
}
