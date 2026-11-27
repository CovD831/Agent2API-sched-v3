use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use serde_json::{json, Value};
use crate::server::{config, core::{account_order, routing, routing_groups::{self, GroupProjection, RequestNeeds}}};

pub(crate) struct ProjectedRoute {
    pub profile: Option<Arc<config::RoutingProfile>>,
    pub excluded_set: Option<Arc<HashSet<String>>>,
    pub scope: String,
    pub positions: Arc<HashMap<String, usize>>,
    pub profiles_gen: u64,
    pub sort_gen: u64,
    pub order_gen: u64,
    pub next_expiry_ms: Option<i64>,
}

fn build_excluded_set(profile: Option<&config::RoutingProfile>) -> Option<Arc<HashSet<String>>> {
    profile
        .filter(|prof| !prof.excluded_accounts.is_empty())
        .map(|prof| Arc::new(prof.excluded_accounts.iter().cloned().collect()))
}

#[derive(Clone)]
pub(crate) struct RequestRouting {
    pub settings: config::RoutingSettings,
    pub profile: Option<Arc<config::RoutingProfile>>,
    pub excluded_set: Option<Arc<HashSet<String>>>,
    pub scope: String,
    pub captured_at: i64,
    pub group: Option<config::RoutingGroup>,
    pub group_projection: GroupProjection,
    positions: Arc<HashMap<String, usize>>,
    account_id: Option<String>,
    require_account: bool,
    pub strict_vision: bool,
    vision_model: Option<String>,
}

impl RequestRouting {
    fn from_projected(
        hit: &ProjectedRoute,
        model: &str,
        profiles: &config::RoutingProfiles,
        base: config::RoutingSettings,
        now: i64,
        profiles_gen: u64,
        sort_gen: u64,
        order_gen: u64,
    ) -> Option<Self> {
        if hit.profiles_gen != profiles_gen
            || hit.sort_gen != sort_gen
            || hit.order_gen != order_gen
            || !hit.next_expiry_ms.is_none_or(|exp| now < exp)
        {
            return None;
        }
        Some(Self {
            settings: profiles.settings_for(model, base),
            profile: hit.profile.clone(),
            excluded_set: hit.excluded_set.clone(),
            scope: hit.scope.clone(),
            captured_at: now,
            group: None,
            account_id: None,
            require_account: false,
            strict_vision: false,
            vision_model: None,
            group_projection: Default::default(),
            positions: hit.positions.clone(),
        })
    }

    fn capture_with(
        model: &str,
        store: &crate::server::core::account_store::store::AccountStore,
        profiles: &config::RoutingProfiles,
        base: config::RoutingSettings,
        profiles_gen: u64,
        sort_gen: u64,
    ) -> Self {
        let snapshot = store.routing_snapshot();
        let now = crate::server::logging::now_ms();
        let order_gen = account_order::revision();
        let cache_key = config::normalize_routing_model(model).unwrap_or_else(|_| model.trim().to_ascii_lowercase());
        if let Ok(cache) = snapshot.projected_routes.lock() {
            if let Some(hit) = cache.get(&cache_key) {
                if let Some(routing) = Self::from_projected(hit, model, profiles, base, now, profiles_gen, sort_gen, order_gen) {
                    return routing;
                }
            }
        }
        let _build = snapshot.projected_build.lock().unwrap_or_else(|error| error.into_inner());
        let now = crate::server::logging::now_ms();
        let order_gen = account_order::revision();
        if let Ok(cache) = snapshot.projected_routes.lock() {
            if let Some(hit) = cache.get(&cache_key) {
                if let Some(routing) = Self::from_projected(hit, model, profiles, base, now, profiles_gen, sort_gen, order_gen) {
                    return routing;
                }
            }
        }
        let mut visible_cache: HashMap<&str, bool> = HashMap::new();
        let entries: Vec<account_order::SortAccount<'_>> = snapshot
            .accounts
            .iter()
            .filter(|account| {
                let provider = routing::provider_of(account);
                *visible_cache
                    .entry(provider)
                    .or_insert_with(|| crate::server::core::account_store::store_view::account_visible(provider))
            })
            .map(|account| account_order::SortAccount::from_value(account.as_ref()))
            .collect();
        let sorting = account_order::sorting_lightweight(&entries, now);
        let next_expiry_ms = sorting.credit_expires_at.values().flatten().copied().filter(|&exp| exp > now).min();
        let mut position_map: HashMap<String, usize> = HashMap::with_capacity(entries.len());
        let mut next_index = 0usize;
        if let Some(profile) = profiles.model_profile(model) {
            let mut providers = profile.providers.clone();
            if profile.scope() == config::RoutingProviderScope::All {
                for provider in &sorting.provider_order {
                    if !providers.contains(provider) {
                        providers.push(provider.clone());
                    }
                }
            }
            let excluded_set: HashSet<&str> = profile.excluded_accounts.iter().map(String::as_str).collect();
            for provider in &providers {
                if !profile.includes_provider(provider) {
                    continue;
                }
                let Some(base_ids) = sorting.account_order.get(provider) else {
                    continue;
                };
                let mut members: Vec<&str> = base_ids
                    .iter()
                    .map(String::as_str)
                    .filter(|id| !excluded_set.contains(id))
                    .collect();
                if let Some(ids) = profile.account_order.get(provider) {
                    let order_pos: HashMap<&str, usize> =
                        ids.iter().enumerate().map(|(index, id)| (id.as_str(), index)).collect();
                    members.sort_by_key(|id| order_pos.get(id).copied().unwrap_or(usize::MAX));
                }
                for id in members {
                    position_map.insert(id.to_string(), next_index);
                    next_index += 1;
                }
            }
        } else {
            let global = profiles.global.as_ref();
            let global_excluded: Option<HashSet<&str>> =
                global.map(|profile| profile.excluded_accounts.iter().map(String::as_str).collect());
            for provider in &sorting.provider_order {
                if global.is_some_and(|profile| !profile.includes_provider(provider)) {
                    continue;
                }
                let Some(ids) = sorting.account_order.get(provider) else {
                    continue;
                };
                for id in ids {
                    if global_excluded.as_ref().is_some_and(|set| set.contains(id.as_str())) {
                        continue;
                    }
                    position_map.insert(id.clone(), next_index);
                    next_index += 1;
                }
            }
        }
        let profile = profiles.effective(model).cloned().map(Arc::new);
        let excluded_set = build_excluded_set(profile.as_deref());
        let scope = profiles.scope(model).to_string();
        let positions: Arc<HashMap<String, usize>> = Arc::new(position_map);
        let projected = Arc::new(ProjectedRoute {
            profile: profile.clone(),
            excluded_set: excluded_set.clone(),
            scope: scope.clone(),
            positions: positions.clone(),
            profiles_gen,
            sort_gen,
            order_gen,
            next_expiry_ms,
        });
        if let Ok(mut cache) = snapshot.projected_routes.lock() {
            if cache.len() >= 256 {
                cache.clear();
            }
            cache.insert(cache_key, projected);
        }
        Self {
            settings: profiles.settings_for(model, base),
            profile,
            excluded_set,
            scope,
            captured_at: now,
            group: None,
            account_id: None,
            require_account: false,
            strict_vision: false,
            vision_model: None,
            group_projection: Default::default(),
            positions,
        }
    }

    pub fn capture_request(model: &str, store: &crate::server::core::account_store::store::AccountStore,
        body: &Value, scope: Option<&crate::server::core::key_scope::KeyScope>, ignore: bool) -> Result<Self, crate::server::errors::GatewayError> {
        Self::capture_request_with_vision(model, store, body, scope, ignore, None)
    }

    pub(super) fn capture_request_with_vision(model: &str, store: &crate::server::core::account_store::store::AccountStore,
        body: &Value, scope: Option<&crate::server::core::key_scope::KeyScope>, ignore: bool,
        vision: Option<&config::vision_compat::VisionSettings>) -> Result<Self, crate::server::errors::GatewayError> {
        Self::capture_request_with_group_id(
            model,
            store,
            body.get("_wb_route_group_id").and_then(Value::as_str),
            Some(body),
            scope,
            ignore,
            vision,
        )
    }

    pub(super) fn capture_request_with_group_id(
        model: &str,
        store: &crate::server::core::account_store::store::AccountStore,
        route_group_id: Option<&str>,
        body_for_group: Option<&Value>,
        scope: Option<&crate::server::core::key_scope::KeyScope>,
        ignore: bool,
        vision: Option<&config::vision_compat::VisionSettings>,
    ) -> Result<Self, crate::server::errors::GatewayError> {
        let profiles_gen = config::routing_profiles_generation();
        let sort_gen = config::account_sort_generation();
        let (profiles, base) = config::routing_snapshot();
        if !ignore && route_group_id.is_some_and(|id|
            !profiles.group_for_model(model).is_some_and(|group| group.id == id)) {
            return Err(crate::server::errors::GatewayError::with_status(409, "路由组已删除或入口已变更，请重新发起请求；未回退其他路由"));
        }
        let mut routing = Self::capture_with(model, store, &profiles, base, profiles_gen, sort_gen);
        routing.account_id = scope.and_then(|scope| scope.account_id()).map(str::to_string);
        routing.require_account = scope.is_some_and(|scope| scope.requires_account());
        if let Some(id) = &routing.account_id {
            Arc::make_mut(&mut routing.positions).retain(|account, _| account == id);
        }
        if !ignore {
            if let Some(group) = profiles.group_for_model(model).cloned() {
                if !crate::server::core::key_scope::allows_model(scope, &group.model) {
                    return Err(crate::server::errors::GatewayError::with_status(404, "此路由组不在网关 Key 的可用模型列表里").with_code("model_not_found"));
                }
                let body = body_for_group.unwrap_or(&Value::Null);
                let snapshot = store.routing_snapshot();
                let accounts: Vec<_> = snapshot.accounts.iter().map(|account| account.as_ref().clone()).collect();
                routing.group_projection = routing_groups::project_with_vision(&group, &accounts, &RequestNeeds::from_body(body), scope, vision);
                if let Some(continuation) = body.get("_wb_route_continue") {
                    routing.group_projection.accounts.retain(|account| {
                        continuation.get("candidateId").and_then(Value::as_str) == Some(routing_groups::attempt_key(account).as_str())
                            && continuation.get("accountId").and_then(Value::as_str) == routing::account_id(account)
                            && continuation.get("model").and_then(Value::as_str) == routing_groups::candidate_model(account)
                    });
                }
                routing.settings = group.settings(base);
                let group_profile = Arc::new(group.profile());
                routing.excluded_set = build_excluded_set(Some(group_profile.as_ref()));
                routing.profile = Some(group_profile);
                routing.scope = group.model.clone();
                routing.group = Some(group);
            }
        }
        Ok(routing)
    }

    pub fn providers(&self, available: Vec<String>) -> Vec<String> {
        if let Some(group) = &self.group { return group.profile().providers; }
        let Some(profile) = &self.profile else { return available };
        let mut ordered = if self.scope.is_empty() { available.clone() } else { profile.providers.clone() };
        if profile.scope() == config::RoutingProviderScope::All {
            for id in &available { if !ordered.contains(id) { ordered.push(id.clone()); } }
        }
        ordered.retain(|id| available.contains(id) && profile.includes_provider(id));
        ordered
    }

    pub fn requires_account(&self) -> bool {
        self.require_account
    }

    pub fn account_id(&self) -> Option<&str> {
        self.account_id.as_deref()
    }

    pub fn accounts(&self, accounts: Vec<Value>) -> Vec<Value> {
        let accounts: Vec<_> = accounts.into_iter().filter(|account| self.account_id.as_deref()
            .is_none_or(|id| routing::account_id(account) == Some(id))).collect();
        if self.group.is_some() {
            return self.group_projection.accounts.iter().filter_map(|candidate| {
                let mut current = accounts.iter().find(|account| routing::account_id(account) == routing::account_id(candidate)
                    && routing::provider_of(account) == routing::provider_of(candidate))?.clone();
                for key in ["routingCandidateId", "routingMemberId", "routingModel", "routingPriority"] { current[key] = candidate[key].clone(); }
                Some(current)
            }).collect();
        }
        let mut accounts: Vec<_> = accounts.into_iter().filter(|account| routing::account_id(account)
            .is_some_and(|id| self.positions.contains_key(id))).collect();
        accounts.sort_by_key(|account| routing::account_id(account).and_then(|id| self.positions.get(id)).copied().unwrap_or(usize::MAX));
        for (index, account) in accounts.iter_mut().enumerate() {
            if let Some(object) = account.as_object_mut() {
                object.insert("routingPriority".to_string(), index.into());
                if let Some(model) = &self.vision_model { object.insert("routingModel".into(), model.clone().into()); }
            }
        }
        accounts
    }

    pub(super) fn for_vision_target(&self, store: &crate::server::core::account_store::AccountStore,
        provider: &str, model: &str, scope: Option<&crate::server::core::key_scope::KeyScope>,
        pinned: Option<&str>, candidate: Option<&str>, needs: Option<&RequestNeeds>) -> Result<Self, crate::server::errors::GatewayError> {
        use crate::server::errors::GatewayError;
        let mut next = self.clone();
        if let Some(current_group) = &self.group {
            let profiles = config::routing_profiles();
            if profiles.group_for_model(&current_group.model).is_none_or(|group| group.id != current_group.id) {
                return Err(GatewayError::with_status(409, "所选路由组已删除或入口已变更，图片兼容已停止"));
            }
        }
        next.strict_vision = true;
        next.vision_model = Some(model.to_string());
        if let Some(group) = &self.group {
            if let Some(needs) = needs {
                let snapshot = store.routing_snapshot();
                let accounts: Vec<_> = snapshot.accounts.iter().map(|account| account.as_ref().clone()).collect();
                next.group_projection = routing_groups::project(group, &accounts, needs, scope);
            }
            next.group_projection.accounts.retain(|account| routing::provider_of(account) == provider
                && routing_groups::candidate_model(account).is_some_and(|value| value.eq_ignore_ascii_case(model))
                && pinned.is_none_or(|id| routing::account_id(account) == Some(id))
                && candidate.is_none_or(|id| routing_groups::attempt_key(account) == id));
            if next.group_projection.accounts.is_empty() {
                return Err(GatewayError::with_status(403, "视觉目标不在所选路由组的参与成员内")
                    .with_code("vision_target_outside_route"));
            }
        } else {
            if !self.providers(vec![provider.to_string()]).contains(&provider.to_string()) {
                return Err(GatewayError::with_status(403, "视觉提供商未参与所选路由，请检查目标模型的路由配置")
                    .with_code("vision_target_outside_route"));
            }
            Arc::make_mut(&mut next.positions).retain(|id, _| pinned.is_none_or(|pinned| pinned == id));
            if next.positions.is_empty() {
                return Err(GatewayError::with_status(503, "当前权限和目标路由内没有参与图片处理的账号")
                    .with_code("vision_target_unavailable"));
            }
        }
        Ok(next)
    }

    pub(super) fn cache_identity(&self) -> String {
        let mut accounts: Vec<_> = self.positions.iter().map(|(id, position)| (id, position)).collect();
        accounts.sort_by_key(|(id, _)| *id);
        let candidates: Vec<_> = self.group_projection.accounts.iter().map(|account| routing_groups::attempt_key(account)).collect();
        json!({ "scope": self.scope, "accounts": accounts, "group": self.group, "candidates": candidates,
            "account": self.account_id, "requireAccount": self.require_account }).to_string()
    }

    pub fn json(&self) -> Value {
        json!({ "capturedAt": self.captured_at, "scope": self.scope,
            "strategy": self.settings.strategy.as_str(), "sticky": self.settings.sticky.as_str(),
            "providerScope": self.profile.as_ref().map(|profile| profile.scope()).unwrap_or(config::RoutingProviderScope::All),
            "providerCount": self.profile.as_ref().map(|profile| profile.providers.len()),
            "accountCount": if self.group.is_some() { self.group_projection.accounts.len() } else { self.positions.len() },
            "groupId": self.group.as_ref().map(|group| &group.id), "groupName": self.group.as_ref().map(|group| &group.name),
            "members": self.group.as_ref().map(|group| group.members.iter().map(|member| json!({
                "id": member.id, "provider": member.provider, "model": member.model, "included": member.included,
                "accountCount": member.accounts.len(),
            })).collect::<Vec<_>>()) })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use serde_json::json;
    use crate::server::{
        config,
        core::{account_order, account_store::AccountStore, upstream::route_context::RequestRouting},
        db::Db,
    };

    #[test]
    fn projected_route_cache_hits_and_invalidates_across_dimensions() {
        crate::test_support::isolated(
            concat!(
                module_path!(),
                "::projected_route_cache_hits_and_invalidates_across_dimensions"
            ),
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = Db::open(&dir.path().join("route-cache.db")).unwrap();
                config::init(Some(db.clone()));
                let store = AccountStore::with_db(Some(db));

                let a1 = store
                    .add_custom_account("custom-p1", &json!({"apiKey": "sk-1"}), Some("A1"))
                    .unwrap()
                    .account;
                let id1 = a1["id"].as_str().unwrap().to_string();
                let _a2 = store
                    .add_custom_account("custom-p1", &json!({"apiKey": "sk-2"}), Some("A2"))
                    .unwrap()
                    .account;

                let r1 = RequestRouting::capture_request("gpt-test", &store, &json!({}), None, false)
                    .unwrap();
                let r2 = RequestRouting::capture_request("GPT-Test", &store, &json!({}), None, false)
                    .unwrap();
                assert!(
                    Arc::ptr_eq(&r1.positions, &r2.positions),
                    "repeated normalized model request must reuse cached ProjectedRoute positions"
                );
                assert!(r1.positions.contains_key(&id1));

                let a3 = store
                    .add_custom_account("custom-p1", &json!({"apiKey": "sk-3"}), Some("A3"))
                    .unwrap()
                    .account;
                let id3 = a3["id"].as_str().unwrap().to_string();
                let r3 = RequestRouting::capture_request("gpt-test", &store, &json!({}), None, false)
                    .unwrap();
                assert!(
                    !Arc::ptr_eq(&r2.positions, &r3.positions),
                    "account_revision change must invalidate ProjectedRoute cache"
                );
                assert!(r3.positions.contains_key(&id3));

                let r4 = RequestRouting::capture_request("gpt-test", &store, &json!({}), None, false)
                    .unwrap();
                assert!(Arc::ptr_eq(&r3.positions, &r4.positions));

                assert!(config::update_raw_field(
                    config::KEY_ROUTING_PROFILES,
                    json!({
                        "models": {
                            "gpt-test": {
                                "providerScope": "selected",
                                "providers": ["custom-p1"]
                            }
                        }
                    }),
                ));
                let r5 = RequestRouting::capture_request("gpt-test", &store, &json!({}), None, false)
                    .unwrap();
                assert!(
                    !Arc::ptr_eq(&r4.positions, &r5.positions),
                    "routing_profiles_generation bump must invalidate ProjectedRoute cache"
                );

                let r6 = RequestRouting::capture_request("gpt-test", &store, &json!({}), None, false)
                    .unwrap();
                assert!(Arc::ptr_eq(&r5.positions, &r6.positions));

                assert!(config::update_raw_field("accountSort", json!({"mode": "balance"})));
                let r7 = RequestRouting::capture_request("gpt-test", &store, &json!({}), None, false)
                    .unwrap();
                assert!(
                    !Arc::ptr_eq(&r6.positions, &r7.positions),
                    "account_sort_generation bump must invalidate ProjectedRoute cache"
                );

                let r8 = RequestRouting::capture_request("gpt-test", &store, &json!({}), None, false)
                    .unwrap();
                assert!(Arc::ptr_eq(&r7.positions, &r8.positions));

                account_order::note_usage(&id1, &json!({"totalLeft": 50.0}), "");
                let r9 = RequestRouting::capture_request("gpt-test", &store, &json!({}), None, false)
                    .unwrap();
                assert!(
                    !Arc::ptr_eq(&r8.positions, &r9.positions),
                    "account_order::revision() bump must invalidate ProjectedRoute cache"
                );
            },
        );
    }

    #[test]
    fn identical_note_usage_preserves_cache_and_positions_match_legacy_projection() {
        use std::collections::HashMap;
        use crate::server::core::routing_scope;

        crate::test_support::isolated(
            concat!(
                module_path!(),
                "::identical_note_usage_preserves_cache_and_positions_match_legacy_projection"
            ),
            || {
                let dir = tempfile::tempdir().unwrap();
                let db = Db::open(&dir.path().join("route-p1.db")).unwrap();
                config::init(Some(db.clone()));
                let store = AccountStore::with_db(Some(db));

                let a1 = store.add_custom_account("custom-p1", &json!({"apiKey": "sk-1"}), Some("A1")).unwrap().account;
                let id1 = a1["id"].as_str().unwrap().to_string();
                let a2 = store.add_custom_account("custom-p1", &json!({"apiKey": "sk-2"}), Some("A2")).unwrap().account;
                let id2 = a2["id"].as_str().unwrap().to_string();
                let a3 = store.add_custom_account("custom-p2", &json!({"apiKey": "sk-3"}), Some("A3")).unwrap().account;
                let id3 = a3["id"].as_str().unwrap().to_string();
                let a4 = store.add_custom_account("custom-p2", &json!({"apiKey": "sk-4"}), Some("A4")).unwrap().account;
                let id4 = a4["id"].as_str().unwrap().to_string();
                store.update_account(&id1, &json!({"priority": 20})).unwrap();
                store.update_account(&id2, &json!({"priority": 10})).unwrap();
                store.update_account(&id3, &json!({"priority": 5})).unwrap();
                store.update_account(&id4, &json!({"priority": 15})).unwrap();

                account_order::note_usage(&id1, &json!({"totalLeft": 42.0, "unit": "积分"}), "");
                let r1 = RequestRouting::capture_request("gpt-test", &store, &json!({}), None, false).unwrap();
                account_order::note_usage(&id1, &json!({"totalLeft": 42.0, "unit": "积分"}), "");
                let r2 = RequestRouting::capture_request("gpt-test", &store, &json!({}), None, false).unwrap();
                assert!(
                    Arc::ptr_eq(&r1.positions, &r2.positions),
                    "identical note_usage payload must not invalidate ProjectedRoute"
                );
                account_order::note_usage(&id1, &json!({"totalLeft": 41.5, "unit": "积分"}), "");
                let r3 = RequestRouting::capture_request("gpt-test", &store, &json!({}), None, false).unwrap();
                assert!(
                    !Arc::ptr_eq(&r2.positions, &r3.positions),
                    "changed note_usage payload must invalidate ProjectedRoute"
                );

                let assert_matches_legacy = |label: &str| {
                    let actual = RequestRouting::capture_request("gpt-test", &store, &json!({}), None, false).unwrap();
                    let snapshot = store.routing_snapshot();
                    let now = actual.captured_at;
                    let visible: Vec<serde_json::Value> = snapshot
                        .accounts
                        .iter()
                        .filter(|account| crate::server::core::account_store::store_view::account_visible(crate::server::core::routing::provider_of(account)))
                        .map(|account| account.as_ref().clone())
                        .collect();
                    let sorting = account_order::sorting(&visible, now);
                    let profiles = config::routing_profiles();
                    let ordered = if let Some(profile) = profiles.model_profile("gpt-test") {
                        routing_scope::project_accounts_with_sorting(&visible, profile, &sorting)
                    } else {
                        account_order::ordered_accounts(&visible, &sorting)
                            .into_iter()
                            .filter(|account| {
                                profiles.global.as_ref().map_or(true, |profile| {
                                    profile.includes_provider(crate::server::core::routing::provider_of(account))
                                        && !crate::server::core::routing::account_id(account)
                                            .is_some_and(|id| profile.excluded_accounts.iter().any(|excluded| excluded == id))
                                })
                            })
                            .collect()
                    };
                    let expected: HashMap<String, usize> = ordered
                        .iter()
                        .enumerate()
                        .filter_map(|(idx, account)| {
                            crate::server::core::routing::account_id(account).map(|id| (id.to_string(), idx))
                        })
                        .collect();
                    assert_eq!(*actual.positions, expected, "{label} positions mismatch");
                };

                assert_matches_legacy("global default");

                assert!(config::update_raw_field(
                    config::KEY_ROUTING_PROFILES,
                    json!({
                        "global": {
                            "providerScope": "all",
                            "providers": ["custom-p1", "custom-p2"],
                            "disabledProviders": [],
                            "excludedAccounts": [id4.clone()]
                        }
                    }),
                ));
                assert_matches_legacy("global with excluded account");

                assert!(config::update_raw_field(
                    config::KEY_ROUTING_PROFILES,
                    json!({
                        "models": {
                            "gpt-test": {
                                "providerScope": "selected",
                                "providers": ["custom-p1"],
                                "accountOrder": { "custom-p1": [id1.clone(), id2.clone()] },
                                "excludedAccounts": []
                            }
                        }
                    }),
                ));
                assert_matches_legacy("model selected with custom accountOrder");

                assert!(config::update_raw_field("accountSort", json!({"mode": "balance"})));
                assert!(config::update_raw_field(
                    config::KEY_ROUTING_PROFILES,
                    json!({
                        "models": {
                            "gpt-test": {
                                "providerScope": "all",
                                "providers": ["custom-p2"],
                                "accountOrder": { "custom-p2": [id4.clone()] },
                                "excludedAccounts": [id2.clone()]
                            }
                        }
                    }),
                ));
                assert_matches_legacy("model all with partial order, exclusion & balance sort");
            },
        );
    }
}
