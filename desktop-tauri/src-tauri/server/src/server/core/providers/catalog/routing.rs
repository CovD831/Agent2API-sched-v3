//! 对外模型名到上游模型的解析。路由与发送侧共用本解析，避免选中一家后再换一套规则。

use serde_json::Value;

use crate::server::core::model_rules;
use crate::server::core::models::model_id;
use crate::server::core::providers::adapter::adapter_by_id;

use super::{all_provider_ids, manifest_for_provider};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WireTarget {
    pub model: String,
    pub reasoning: Option<String>,
}

pub(crate) fn entry_id_in_manifest(manifest: &[Value], requested: &str) -> Option<String> {
    let requested = requested.trim();
    if requested.is_empty() {
        return None;
    }
    // 保留展示名兼容，但必须先匹配原始 ID；返回清单里的实际大小写。
    manifest.iter().find(|entry| model_id(entry).eq_ignore_ascii_case(requested))
        .or_else(|| manifest.iter().find(|entry| {
            entry.get("name")
                .map(crate::server::core::models::shape_value_text)
                .is_some_and(|name| name.eq_ignore_ascii_case(requested))
        }))
        .map(model_id)
}

pub(super) fn builtin_target(
    rules: &model_rules::ModelRules,
    provider: &str,
    manifest: &[Value],
    requested: &str,
) -> Option<WireTarget> {
    let requested = requested.trim();
    let exact = manifest.iter()
        .find(|entry| model_id(entry).eq_ignore_ascii_case(requested))
        .map(model_id);
    let mappings: Vec<_> = rules.mappings.iter()
        .filter(|mapping| mapping.alias.eq_ignore_ascii_case(requested))
        .filter(|mapping| mapping.provider.as_deref()
            .map_or(true, |id| id.eq_ignore_ascii_case(provider)))
        .collect();
    let default = exact.or_else(|| {
        if mappings.is_empty() { entry_id_in_manifest(manifest, requested) } else { None }
    });
    if let Some(id) = default {
        if rules.default_enabled(provider, &id) {
            return Some(WireTarget {
                reasoning: rules.binding(provider, &id, &id).and_then(|m| m.reasoning.clone()),
                model: id,
            });
        }
    }
    let mapping = rules.selected_mapping(provider, requested, manifest)?;
    entry_id_in_manifest(manifest, &mapping.target).map(|model| WireTarget {
        model,
        reasoning: mapping.reasoning.clone(),
    })
}

/// 原始能力查询不应用开关，供管理 API 展开旧版全局规则。
/// 别名也算登记过的名字，但必须能解析到该家的真实上游记录。
pub fn providers_for_model(model: &str) -> Vec<String> {
    super::index::current().providers_for_model(model)
}

/// 只返回有开启绑定的提供商，不把关闭的 ID 回落给默认提供商。
pub fn forwarding_providers(model: &str) -> Vec<String> {
    super::index::current().forwarding_providers(model)
}

/// 「这个名字是不是被关掉了」—— 只在广告里没有它时用来区分「承载家全被关闭」
/// 与「根本没有这个名字」两种 404 文案（`pipeline::resolve_model` 与
/// `provider_loop::forward_with_providers` 各判一次）。
///
/// ── 判据为什么只剩 `providers_for_model`（两条别名兜底已删）──────
/// 曾经这里还兜着两条「只要**存在**同名的映射条目就算被关闭」：内建家的
/// `has_alias(model)` 与自定义家 mappings 的 alias 扫描。它们在「上游把某个
/// 模型下线、映射的目标已经不在任何清单里」时会**误命中** —— 别名确实还在
/// 配置里，但它指向的上游已经没了，既调不通，也不该报「已在网关中关闭」：
/// 那句文案会把用户引去模型管理页找一个**已经不在列表里**的开关。
/// 它本来想覆盖的「别名在、目标也仍在清单里、只是开关关了」已经被
/// [`providers_for_model`] 覆盖 —— 那个函数**不过滤 `enabled`**（见它的注释），
/// 只要有承载者就非空，所以这两条兜底整条删掉也不会漏判；剩下的悬空别名
/// 据此归入「模型不存在」，与它实际的处境一致。
pub fn model_blocked_everywhere(model: &str) -> bool {
    if !forwarding_providers(model).is_empty() {
        return false;
    }
    !providers_for_model(model).is_empty()
}

pub fn wire_target_for_provider(model: &str, provider_id: &str, _account: Option<&Value>) -> WireTarget {
    super::index::current().wire_target_for_provider(model, provider_id)
}

pub fn default_model_catalog() -> Vec<Value> {
    let rules = model_rules::shared();
    let mut result = Vec::new();
    for provider in all_provider_ids().into_iter().filter(|provider| {
        adapter_by_id(provider).is_some_and(|adapter| adapter.supports_default_model())
    }) {
        for entry in manifest_for_provider(&provider) {
            let id = model_id(&entry);
            if !id.is_empty() && !super::super::images::model_is_image(&entry)
                && rules.default_enabled(&provider, &id)
                && !result.iter().any(|item| model_id(item).eq_ignore_ascii_case(&id))
            {
                result.push(entry);
            }
        }
    }
    result
}

pub fn default_model_usable(model: &str) -> bool {
    entry_id_in_manifest(&default_model_catalog(), model).is_some()
}
