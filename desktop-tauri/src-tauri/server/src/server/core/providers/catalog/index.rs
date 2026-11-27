//! 聚合目录与模型路由解析的多级内存索引缓存。
//!
//! 将每个请求在 `resolve_client_model` 与 `forward_with_providers` 中原本重复 6+ 轮的
//! 全提供商清单构建、rules 扫描与 `customProviders` 读取收敛为按 `(catalog::generation, store_id, catalog_revision)`
//! 一致化维护的不可变索引 + 有界高频查询缓存。

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use serde_json::Value;

use crate::server::core::account_store::plugin_accounts;
use crate::server::core::models::model_id;
use crate::server::core::{custom_providers, model_rules};

use super::routing::{builtin_target, entry_id_in_manifest, WireTarget};

const MAX_QUERY_ENTRIES: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CacheKey {
    generation: u64,
    store_id: u64,
    catalog_revision: u64,
}

#[derive(Default)]
struct QueryCache {
    forwarding: HashMap<String, Arc<Vec<String>>>,
    providers: HashMap<String, Arc<Vec<String>>>,
    wire_targets: HashMap<(String, String), Option<WireTarget>>,
    is_image: HashMap<(String, String), bool>,
}

pub(super) struct CatalogIndex {
    key: CacheKey,
    provider_order: Vec<String>,
    manifests: HashMap<String, Arc<Vec<Value>>>,
    rules: Arc<model_rules::ModelRules>,
    custom_providers: Vec<Value>,
    custom_ids: HashSet<String>,
    queries: RwLock<QueryCache>,
}

fn cache() -> &'static RwLock<Option<Arc<CatalogIndex>>> {
    static CACHE: OnceLock<RwLock<Option<Arc<CatalogIndex>>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(None))
}

fn build_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn current_key() -> CacheKey {
    let (store_id, catalog_revision) = plugin_accounts::shared()
        .map(|store| (store.instance_id(), store.catalog_state_revision()))
        .unwrap_or((0, 0));
    CacheKey {
        generation: super::generation(),
        store_id,
        catalog_revision,
    }
}

pub(super) fn current() -> Arc<CatalogIndex> {
    let key = current_key();
    if let Ok(guard) = cache().read() {
        if let Some(existing) = guard.as_ref().filter(|index| index.key == key) {
            return existing.clone();
        }
    }

    let _build = build_lock().lock().unwrap_or_else(|error| error.into_inner());
    let key = current_key();
    if let Ok(guard) = cache().read() {
        if let Some(existing) = guard.as_ref().filter(|index| index.key == key) {
            return existing.clone();
        }
    }

    let provider_order = super::all_provider_ids();
    let manifests = provider_order
        .iter()
        .map(|provider| {
            (
                provider.clone(),
                Arc::new(super::build_manifest_for_provider(provider)),
            )
        })
        .collect();
    let rules = model_rules::shared();
    let custom_list = custom_providers::list();
    let built = Arc::new(CatalogIndex::from_parts(
        key,
        provider_order,
        manifests,
        rules,
        custom_list,
    ));

    if current_key() == key {
        if let Ok(mut guard) = cache().write() {
            *guard = Some(built.clone());
        }
    }
    built
}

impl CatalogIndex {
    fn from_parts(
        key: CacheKey,
        provider_order: Vec<String>,
        manifests: HashMap<String, Arc<Vec<Value>>>,
        rules: Arc<model_rules::ModelRules>,
        custom_providers: Vec<Value>,
    ) -> Self {
        let custom_ids = custom_providers
            .iter()
            .filter_map(|item| item.get("id").and_then(Value::as_str))
            .map(|id| id.trim().to_string())
            .filter(|id| !id.is_empty())
            .collect();
        Self {
            key,
            provider_order,
            manifests,
            rules,
            custom_providers,
            custom_ids,
            queries: RwLock::new(QueryCache::default()),
        }
    }

    pub(super) fn manifest_for(&self, provider: &str) -> Arc<Vec<Value>> {
        self.manifests
            .get(provider)
            .cloned()
            .unwrap_or_else(|| Arc::new(super::build_manifest_for_provider(provider)))
    }

    pub(super) fn forwarding_providers(&self, model: &str) -> Vec<String> {
        let norm = model.trim().to_ascii_lowercase();
        if let Ok(queries) = self.queries.read() {
            if let Some(hit) = queries.forwarding.get(&norm) {
                return (**hit).clone();
            }
        }
        let mut result: Vec<String> = self
            .provider_order
            .iter()
            .filter(|provider| {
                let Some(manifest) = self.manifests.get(*provider) else {
                    return false;
                };
                builtin_target(&self.rules, provider, manifest, model).is_some()
            })
            .cloned()
            .collect();
        result.extend(custom_providers::carriers_of_model_in(
            &self.custom_providers,
            model,
        ));
        let cached = Arc::new(result.clone());
        if let Ok(mut queries) = self.queries.write() {
            if queries.forwarding.len() < MAX_QUERY_ENTRIES {
                queries.forwarding.insert(norm, cached);
            }
        }
        result
    }

    pub(super) fn providers_for_model(&self, model: &str) -> Vec<String> {
        let model = model.trim();
        if model.is_empty() {
            return Vec::new();
        }
        let norm = model.to_ascii_lowercase();
        if let Ok(queries) = self.queries.read() {
            if let Some(hit) = queries.providers.get(&norm) {
                return (**hit).clone();
            }
        }
        let exact = self.provider_order.iter().any(|provider| {
            self.manifests.get(provider).is_some_and(|items| {
                items
                    .iter()
                    .any(|item| model_id(item).eq_ignore_ascii_case(model))
            })
        });
        let mut result: Vec<String> = self
            .provider_order
            .iter()
            .filter(|provider| {
                let Some(items) = self.manifests.get(*provider) else {
                    return false;
                };
                let declared = if exact {
                    items
                        .iter()
                        .any(|item| model_id(item).eq_ignore_ascii_case(model))
                } else {
                    entry_id_in_manifest(items, model).is_some()
                };
                let mapped = self.rules.mappings.iter().any(|mapping| {
                    mapping.alias.eq_ignore_ascii_case(model)
                        && mapping
                            .provider
                            .as_deref()
                            .map_or(true, |owner| owner.eq_ignore_ascii_case(provider))
                        && entry_id_in_manifest(items, &mapping.target).is_some()
                });
                declared || mapped
            })
            .cloned()
            .collect();
        result.extend(self.custom_providers.iter().filter_map(|provider| {
            let models = provider.get("models")?.as_array()?;
            let declared = models
                .iter()
                .any(|entry| model_id(entry).eq_ignore_ascii_case(model));
            let mapped = provider
                .get("mappings")
                .and_then(Value::as_array)
                .is_some_and(|mappings| {
                    mappings.iter().any(|mapping| {
                        mapping
                            .get("alias")
                            .and_then(Value::as_str)
                            .is_some_and(|alias| alias.eq_ignore_ascii_case(model))
                    })
                });
            (declared || mapped)
                .then(|| provider.get("id").and_then(Value::as_str).unwrap_or("").to_string())
        }));
        let cached = Arc::new(result.clone());
        if let Ok(mut queries) = self.queries.write() {
            if queries.providers.len() < MAX_QUERY_ENTRIES {
                queries.providers.insert(norm, cached);
            }
        }
        result
    }

    pub(super) fn wire_target_for_provider(&self, model: &str, provider_id: &str) -> WireTarget {
        let requested = model.trim();
        if provider_id.starts_with(custom_providers::ID_PREFIX)
            && self.custom_ids.contains(provider_id)
        {
            return WireTarget {
                model: requested.to_string(),
                reasoning: None,
            };
        }
        let cache_key = (provider_id.to_string(), requested.to_ascii_lowercase());
        if let Ok(queries) = self.queries.read() {
            if let Some(hit) = queries.wire_targets.get(&cache_key) {
                return hit.clone().unwrap_or_else(|| WireTarget {
                    model: requested.to_string(),
                    reasoning: None,
                });
            }
        }
        let resolved = builtin_target(
            &self.rules,
            provider_id,
            &self.manifest_for(provider_id),
            requested,
        );
        if let Ok(mut queries) = self.queries.write() {
            if queries.wire_targets.len() < MAX_QUERY_ENTRIES {
                queries.wire_targets.insert(cache_key, resolved.clone());
            }
        }
        resolved.unwrap_or_else(|| WireTarget {
            model: requested.to_string(),
            reasoning: None,
        })
    }

    pub(super) fn provider_model_is_image(&self, provider: &str, requested: &str) -> bool {
        let cache_key = (provider.to_string(), requested.trim().to_ascii_lowercase());
        if let Ok(queries) = self.queries.read() {
            if let Some(hit) = queries.is_image.get(&cache_key) {
                return *hit;
            }
        }
        let wire = self.wire_target_for_provider(requested, provider);
        let is_image = self
            .manifest_for(provider)
            .iter()
            .find(|item| model_id(item).eq_ignore_ascii_case(&wire.model))
            .is_some_and(super::super::images::model_is_image);
        if let Ok(mut queries) = self.queries.write() {
            if queries.is_image.len() < MAX_QUERY_ENTRIES {
                queries.is_image.insert(cache_key, is_image);
            }
        }
        is_image
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn query_caches_hit_and_preserve_requested_case_on_fallback() {
        let rules_raw = json!({
            "modelRules": {
                "disabled": [{ "provider": "loomy", "id": "glm-disabled" }],
                "mappings": [{
                    "alias": "MyAlias",
                    "target": "glm-5.3",
                    "provider": "loomy",
                    "reasoning": "high"
                }, {
                    "alias": "ImgAlias",
                    "target": "img-gen",
                    "provider": "loomy"
                }]
            }
        });
        let rules = Arc::new(model_rules::ModelRules::from_raw(
            rules_raw.as_object().unwrap(),
        ));
        let mut manifests = HashMap::new();
        manifests.insert(
            "loomy".to_string(),
            Arc::new(vec![
                json!({ "id": "glm-5.3", "name": "GLM 5.3" }),
                json!({ "id": "glm-disabled", "name": "GLM Disabled" }),
                json!({ "id": "img-gen", "kind": "image" }),
            ]),
        );
        let custom = vec![json!({
            "id": "custom-abc",
            "enabled": true,
            "models": [{ "id": "custom-chat", "enabled": true, "reasoning": "" }],
            "mappings": []
        })];
        let index = CatalogIndex::from_parts(
            CacheKey {
                generation: 1,
                store_id: 1,
                catalog_revision: 1,
            },
            vec!["loomy".to_string()],
            manifests,
            rules,
            custom,
        );

        assert_eq!(index.forwarding_providers("myalias"), vec!["loomy"]);
        assert_eq!(index.forwarding_providers("glm-disabled"), Vec::<String>::new());
        assert_eq!(index.providers_for_model("glm-disabled"), vec!["loomy"]);
        assert_eq!(index.forwarding_providers("custom-chat"), vec!["custom-abc"]);
        assert_eq!(index.providers_for_model("custom-chat"), vec!["custom-abc"]);

        // 命中缓存：forwarding 与 providers 缓存同一个 Arc
        let fwd_ptr_1 = Arc::clone(index.queries.read().unwrap().forwarding.get("myalias").unwrap());
        assert_eq!(index.forwarding_providers("MYALIAS"), vec!["loomy"]);
        let fwd_ptr_2 = Arc::clone(index.queries.read().unwrap().forwarding.get("myalias").unwrap());
        assert!(Arc::ptr_eq(&fwd_ptr_1, &fwd_ptr_2));

        // 映射 & 思考等级命中
        assert_eq!(
            index.wire_target_for_provider("myalias", "loomy"),
            WireTarget {
                model: "glm-5.3".to_string(),
                reasoning: Some("high".to_string()),
            }
        );

        // 未知模型回落保持请求端大小写（即使 lowercase 缓存键相同）
        assert_eq!(
            index.wire_target_for_provider("CustomUnknownModel", "loomy"),
            WireTarget {
                model: "CustomUnknownModel".to_string(),
                reasoning: None,
            }
        );
        assert_eq!(
            index.wire_target_for_provider("customunknownmodel", "loomy"),
            WireTarget {
                model: "customunknownmodel".to_string(),
                reasoning: None,
            }
        );
        // 自定义提供商直接原样返回请求名
        assert_eq!(
            index.wire_target_for_provider("custom-chat", "custom-abc"),
            WireTarget {
                model: "custom-chat".to_string(),
                reasoning: None,
            }
        );

        // 图片判定（含映射别名 + 缓存命中）
        assert!(index.provider_model_is_image("loomy", "ImgAlias"));
        assert!(index.provider_model_is_image("loomy", "imgalias"));
        assert!(!index.provider_model_is_image("loomy", "MyAlias"));
    }

    #[test]
    fn global_index_reuses_arc_until_invalidated() {
        let first = current();
        let second = current();
        assert!(Arc::ptr_eq(&first, &second));
        super::super::invalidate();
        let third = current();
        assert!(!Arc::ptr_eq(&first, &third));
    }
}
