//! 待机模式（`AGENT2API_STANDBY=1`）：给「同库热备」实例用的只读档。
//!
//! ── 为什么需要这一档 ───────────────────────────────────────────
//! 热备实例与生产实例共享同一个数据库（同一份账号 / 凭证 / 路由状态）。
//! 一个**不接流量**的实例在正常模式下仍会把全部后台活动跑满：定时签到、
//! 凭证自动维护（刷新 token / byPwd 主动重登）、每账号余额查询、模型目录
//! 刷新、云同步、插件宿主及其排程……这些活动有两宗罪：
//!   1. **白烧 CPU**：无流量实例实测仍占约 40% 单核，纯粹是后台循环在转；
//!   2. **与生产抢写**：待机实例的写路径与生产写同一份共享状态，前科明确——
//!      2026-11-16 蓝绿重叠期两个实例竞争消费 refresh token，23 个 codearts
//!      账号真死；旧版实例的配置整份保存（差集删除）会周期性擦掉新版写入的
//!      kv 状态键（`routingProviderCooldowns` 约 8 秒被擦一次）。
//!
//! ── 待机档的语义 ───────────────────────────────────────────────
//! 开启后本进程**只保留 HTTP 服务面与只读状态加载**，全部后台写者与定时器
//! 不启动：
//!   - 定时签到（`auto_checkin`）、成长排程（`growth_scheduler`）不启动；
//!   - 间隔型定时任务（`scheduled_tasks`：凭证自动维护 / 模型目录刷新 /
//!     软件版本检查）不启动；
//!   - 每账号余额查询心跳（`usage_query::spawn_sweeper`）不启动；
//!   - 云同步工作线程（`cloud_sync::auto_sync`）不启动（Db 的 update_hook
//!     仍在，但没有工作线程时 `notify_change` 是无操作）；
//!   - 插件宿主（`plugin_host::spawn_bootstrap`）不启动 —— 插件进程内的
//!     签到 / 刷新 / 主动重登随之全部不存在；
//!   - 请求统计的僵尸行清扫（启动一次 + 300 秒循环）与启动期历史统计整理
//!     （`start_background_maintenance`）不跑；
//!   - 启动期一次性写动作（签到清单补全、账号侧迁移）不跑；
//!   - **配置回写漏斗 `config::save_raw` 直接拒绝**：任何路径（定时任务、
//!     云同步状态记录、面板 API）触发的整份保存 / 差集删除在本进程内
//!     一律不落库，从漏斗处保证「配置保存回写路径不存活」，不依赖各
//!     调用点自觉。
//!
//! ── 接管方式 ───────────────────────────────────────────────────
//! 待机不是运行期可翻转的状态：要接管流量，把容器以**不带**
//! `AGENT2API_STANDBY` 的正常模式重启即可，全部后台活动随正常启动恢复。
//! 本开关只在启动时读一次（`OnceLock` 缓存），运行中改 env 不生效 ——
//! 这是有意的：半路翻转会让「哪些任务已经起、哪些没起」变成不可言说
//! 的中间态，重启是唯一干净的边界。
//!
//! ── 口径 ───────────────────────────────────────────────────────
//! 与本仓既有 env 开关同口径（见 `super::env_flag` 的说明）：只有值
//! 恰为 `"1"` 才算开启，其余（含 `"true"` / `"0"` / 空串 / 未设置）
//! 都按关闭处理 —— 默认关，生产实例不设此变量时行为零变化。

use std::sync::OnceLock;

/// 开关的环境变量名。
pub const ENV_STANDBY: &str = "AGENT2API_STANDBY";

/// 纯判定（供测试）：只有 `"1"`（容忍首尾空白）开启。
fn parse_standby_flag(value: Option<&str>) -> bool {
    value.is_some_and(|value| value.trim() == "1")
}

/// 本进程是否处于待机模式（进程级缓存，只读一次 env）。
pub fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| parse_standby_flag(std::env::var(ENV_STANDBY).ok().as_deref()))
}

#[cfg(test)]
mod tests {
    use super::{parse_standby_flag, ENV_STANDBY};

    #[test]
    fn standby_env_name_is_stable() {
        assert_eq!(ENV_STANDBY, "AGENT2API_STANDBY");
    }

    #[test]
    fn only_literal_1_enables_standby() {
        assert!(parse_standby_flag(Some("1")));
        assert!(parse_standby_flag(Some(" 1 ")));
        assert!(!parse_standby_flag(Some("0")));
        assert!(!parse_standby_flag(Some("true")));
        assert!(!parse_standby_flag(Some("")));
        assert!(!parse_standby_flag(None));
    }
}
