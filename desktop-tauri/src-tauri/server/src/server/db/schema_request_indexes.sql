-- ── requests 补部分/复合索引并修复报表覆盖索引（schema v31）──────────
-- 1. 进行中行（永远几十行量级）：加速 status=0 计数与 ORDER BY ts DESC, row_id DESC；
--    同时清理被它严格替代的旧全表索引 idx_requests_status，避免无 ANALYZE 时误选旧索引并减少写放大
DROP INDEX IF EXISTS idx_requests_status;
CREATE INDEX IF NOT EXISTS idx_requests_running
  ON requests(ts DESC, row_id DESC)
  WHERE status = 0;

-- 2. 失败行局部索引（侧栏 20s 轮询与 status=error 筛选）：严格匹配 FilterPlan 失败条件，尾随 (status, error) 消除 DeferredSeek，零回表计数与排序
CREATE INDEX IF NOT EXISTS idx_requests_errors_ts
  ON requests(ts DESC, row_id DESC, status, error)
  WHERE NOT (status >= 200 AND status < 300 AND error IS NULL) AND status <> 0;

-- 3. provider 维度按时间倒序索引：消除 provider 筛选全表扫描与回表排序
CREATE INDEX IF NOT EXISTS idx_requests_provider_ts
  ON requests(provider, ts DESC, row_id DESC);

-- 4. 重建报表覆盖索引：补齐 status / completion_tokens / error，恢复 select_summary_rows 零回表
DROP INDEX IF EXISTS idx_requests_summary;
CREATE INDEX IF NOT EXISTS idx_requests_summary ON requests(
  ts, is_test, status, model, upstream_model,
  total_tokens, cache_read_tokens, prompt_tokens, completion_tokens, error
);

-- 5. account_limiter_events 时间索引：支撑按时间窗口安全分批裁剪
CREATE INDEX IF NOT EXISTS idx_limiter_events_ts
  ON account_limiter_events(ts);

-- 6. requests 冷热垂直拆分：4 个冷详情 JSON 移至独立表，列表/流向/追踪轻量标量常驻主表
CREATE TABLE IF NOT EXISTS request_details (
  row_id          INTEGER PRIMARY KEY,
  route           TEXT,
  attempt_details TEXT NOT NULL DEFAULT '[]',
  sensitive_hits  TEXT NOT NULL DEFAULT '[]',
  client_evidence TEXT NOT NULL DEFAULT '{}'
);

ALTER TABLE requests ADD COLUMN detail_flags INTEGER NOT NULL DEFAULT 0;
ALTER TABLE requests ADD COLUMN internal_retries INTEGER NOT NULL DEFAULT 0;
ALTER TABLE requests ADD COLUMN vision_mode TEXT NOT NULL DEFAULT '';
ALTER TABLE requests ADD COLUMN recorded_client_name TEXT NOT NULL DEFAULT '';
ALTER TABLE requests ADD COLUMN client_user_agent TEXT NOT NULL DEFAULT '';
ALTER TABLE requests ADD COLUMN session_id TEXT NOT NULL DEFAULT '';
ALTER TABLE requests ADD COLUMN session_source TEXT NOT NULL DEFAULT '';

CREATE TRIGGER IF NOT EXISTS trg_requests_delete_details
AFTER DELETE ON requests
BEGIN
  DELETE FROM request_details WHERE row_id = OLD.row_id;
END;
