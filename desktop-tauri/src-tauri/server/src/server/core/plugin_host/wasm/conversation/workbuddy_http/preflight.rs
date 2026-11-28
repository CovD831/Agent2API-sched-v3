//! 在唯一对话投递前检查官方入口；检查不带账号凭据，也不提交对话正文。

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use agent2api_provider_protocol as proto;
use tokio::time::Instant;

use crate::server::core::{
    egress::managed::FrozenHttpClient,
    plugin_host::script::HttpDeadline,
    providers::policy::{HttpDnsPolicy, NetworkPolicy},
    proxies::chain::SelectedPath,
    upstream::http_dns,
};

const BACKUP: &str = "https://wb.tencentbuddy.com";
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const CACHE_TTL: Duration = Duration::from_secs(60);
const MAX_CACHE_ENTRIES: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
struct CachedRoute {
    url: String,
    host: String,
    addresses: Vec<SocketAddr>,
    expires_at: Instant,
}

fn route_cache() -> &'static Mutex<HashMap<(String, String), CachedRoute>> {
    static CACHE: OnceLock<Mutex<HashMap<(String, String), CachedRoute>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cache_get(path_key: &str, origin: &str, now: Instant) -> Option<CachedRoute> {
    let mut cache = route_cache().lock().unwrap_or_else(|e| e.into_inner());
    let key = (path_key.to_string(), origin.to_string());
    if let Some(entry) = cache.get(&key) {
        if now < entry.expires_at {
            return Some(entry.clone());
        }
        cache.remove(&key);
    }
    None
}

fn cache_put(path_key: String, origin: String, route: CachedRoute, now: Instant) {
    let mut cache = route_cache().lock().unwrap_or_else(|e| e.into_inner());
    cache.retain(|_, entry| entry.expires_at > now);
    let key = (path_key, origin);
    if !cache.contains_key(&key) && cache.len() >= MAX_CACHE_ENTRIES {
        if let Some(oldest) = cache
            .iter()
            .min_by_key(|(_, entry)| entry.expires_at)
            .map(|(k, _)| k.clone())
        {
            cache.remove(&oldest);
        }
    }
    cache.insert(key, route);
}

pub(super) fn invalidate(path_key: &str, origin: &str) {
    let mut cache = route_cache().lock().unwrap_or_else(|e| e.into_inner());
    cache.remove(&(path_key.to_string(), origin.to_string()));
}

fn policy() -> NetworkPolicy {
    NetworkPolicy {
        trusted_origins: super::ORIGINS.iter().map(|origin| (*origin).into()).collect(),
        backup_origin: BACKUP.into(),
        http_dns: Some(HttpDnsPolicy {
            token: "719980335".into(),
            bootstrap_ips: vec!["119.29.29.99".into(), "119.28.28.99".into()],
        }),
        fallback_addresses: vec!["162.14.3.241".into()],
        ..Default::default()
    }
}

async fn probe(client: &FrozenHttpClient, origin: &str, deadline: HttpDeadline) -> bool {
    if deadline.check().is_err() { return false; }
    let Ok(client) = client.client() else { return false; };
    let request = client.head(format!("{origin}/")).header("Accept", "*/*");
    match tokio::time::timeout_at(Instant::now() + PROBE_TIMEOUT, request.send()).await {
        Ok(Ok(response)) => !matches!(response.status().as_u16(), 408 | 502 | 503 | 504),
        _ => false,
    }
}

pub(super) async fn prepare(path: SelectedPath, url: &str, deadline: HttpDeadline)
    -> Result<(FrozenHttpClient, String), proto::WireError>
{
    deadline.check()?;
    let parsed = url::Url::parse(url).map_err(|_| super::invalid())?;
    let origin = parsed.origin().ascii_serialization();
    if !super::ORIGINS.contains(&origin.as_str()) || parsed.path() != super::PATH {
        return Err(super::invalid());
    }
    let path_key = path.cache_key();
    let now = Instant::now();
    if let Some(cached) = cache_get(&path_key, &origin, now) {
        if cached.addresses.is_empty() {
            let client = FrozenHttpClient::from_path(path.clone()).await.map_err(|_| super::unavailable())?;
            deadline.check()?;
            return Ok((client, cached.url));
        } else if path.is_direct() {
            let client = FrozenHttpClient::from_path_with_addresses(path.clone(), &cached.host, &cached.addresses)
                .await
                .map_err(|_| super::unavailable())?;
            deadline.check()?;
            return Ok((client, cached.url));
        }
    }
    let original = FrozenHttpClient::from_path(path.clone()).await.map_err(|_| super::unavailable())?;
    if probe(&original, &origin, deadline).await {
        deadline.check()?;
        let now = Instant::now();
        cache_put(
            path_key,
            origin,
            CachedRoute {
                url: url.into(),
                host: String::new(),
                addresses: Vec::new(),
                expires_at: now + CACHE_TTL,
            },
            now,
        );
        return Ok((original, url.into()));
    }
    let backup = if origin != BACKUP {
        if probe(&original, BACKUP, deadline).await {
            deadline.check()?;
            let resolved = format!("{BACKUP}{}", super::PATH);
            let now = Instant::now();
            cache_put(
                path_key,
                origin,
                CachedRoute {
                    url: resolved.clone(),
                    host: String::new(),
                    addresses: Vec::new(),
                    expires_at: now + CACHE_TTL,
                },
                now,
            );
            return Ok((original, resolved));
        }
        BACKUP
    } else {
        origin.as_str()
    };
    if path.is_direct() {
        let hostname = url::Url::parse(backup).map_err(|_| super::invalid())?
            .host_str().ok_or_else(super::invalid)?.to_string();
        let policy = policy();
        let mut addresses = http_dns::resolve(&policy, &hostname).await;
        addresses.extend(policy.fallback_addresses.iter().filter_map(|ip| ip.parse::<IpAddr>().ok()));
        addresses.retain(http_dns::public_ip);
        addresses.sort_unstable();
        addresses.dedup();
        for ip in addresses.into_iter().take(4) {
            deadline.check()?;
            let addresses = [SocketAddr::new(ip, 443)];
            let client = FrozenHttpClient::from_path_with_addresses(path.clone(), &hostname, &addresses)
                .await.map_err(|_| super::unavailable())?;
            if probe(&client, backup, deadline).await {
                deadline.check()?;
                let resolved = format!("{backup}{}", super::PATH);
                let now = Instant::now();
                cache_put(
                    path_key,
                    origin,
                    CachedRoute {
                        url: resolved.clone(),
                        host: hostname,
                        addresses: addresses.to_vec(),
                        expires_at: now + CACHE_TTL,
                    },
                    now,
                );
                return Ok((client, resolved));
            }
        }
    }
    deadline.check()?;
    // HEAD 不通不证明对话不通；最后仍只投递原请求一次，不把检查失败变成重放资格。
    Ok((original, url.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_cache_respects_ttl_invalidation_and_capacity() {
        let base = Instant::now();
        let key_prefix = "test-preflight-cache";
        invalidate(key_prefix, "https://www.workbuddy.cn");

        cache_put(
            key_prefix.into(),
            "https://www.workbuddy.cn".into(),
            CachedRoute {
                url: "https://www.workbuddy.cn/v2/chat/completions".into(),
                host: String::new(),
                addresses: Vec::new(),
                expires_at: base + Duration::from_secs(60),
            },
            base,
        );
        assert!(cache_get(key_prefix, "https://www.workbuddy.cn", base + Duration::from_secs(30)).is_some());
        assert!(cache_get(key_prefix, "https://www.workbuddy.cn", base + Duration::from_secs(61)).is_none());

        cache_put(
            key_prefix.into(),
            "https://www.workbuddy.cn".into(),
            CachedRoute {
                url: "https://wb.tencentbuddy.com/v2/chat/completions".into(),
                host: String::new(),
                addresses: Vec::new(),
                expires_at: base + Duration::from_secs(120),
            },
            base,
        );
        invalidate(key_prefix, "https://www.workbuddy.cn");
        assert!(cache_get(key_prefix, "https://www.workbuddy.cn", base).is_none());

        for i in 0..(MAX_CACHE_ENTRIES + 12) {
            cache_put(
                format!("{key_prefix}-cap-{i}"),
                "https://www.workbuddy.cn".into(),
                CachedRoute {
                    url: "https://www.workbuddy.cn/v2/chat/completions".into(),
                    host: String::new(),
                    addresses: Vec::new(),
                    expires_at: base + Duration::from_secs(60 + i as u64),
                },
                base,
            );
        }
        assert!(route_cache().lock().unwrap().len() <= MAX_CACHE_ENTRIES);
        for i in 0..(MAX_CACHE_ENTRIES + 12) {
            invalidate(&format!("{key_prefix}-cap-{i}"), "https://www.workbuddy.cn");
        }
    }
}
