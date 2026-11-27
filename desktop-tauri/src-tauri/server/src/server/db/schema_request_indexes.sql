-- ── requests 补部分/复合索引并修复报表覆盖索引（schema v31）──────────
-- 1. 进行中行（永远几十行量级）：加速 status=0 计数与 ORDER BY ts DESC, row_id DESC；
--    同时清理被它严格替代的旧全表索引 idx_requests_status，避免无 ANALYZE 时误选旧索引并减少写放大
DROP INDEX IF EXISTS idx_requests_status;
CREATE INDEX IF NOT EXISTS idx_requests_running
  ON requests(ts DESC, row_id DESC)
  WHERE status = 0;

-- 2. 失败行局部索引（侧栏 20s 轮询与 status=error 筛选）：严格匹配 FilterPlan 失败条件，零回表计数
CREATE INDEX IF NOT EXISTS idx_requests_errors_ts
  ON requests(ts DESC, row_id DESC)
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
