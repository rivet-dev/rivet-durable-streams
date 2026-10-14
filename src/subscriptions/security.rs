use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use ring::hmac;
use serde_json::Value;

pub fn constant_time_equal(a: &[u8], b: &[u8]) -> bool {
    let key = hmac::Key::new(hmac::HMAC_SHA256, b"durable-streams-management-comparison");
    hmac::verify(&key, b, hmac::sign(&key, a).as_ref()).is_ok()
}

fn public_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_broadcast()
        || a == 0
        || a >= 240
        || (a == 100 && (64..=127).contains(&b))
        || (a == 192 && b == 0 && (c == 0 || c == 2))
        || (a == 192 && b == 88 && c == 99)
        || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
        || (a == 203 && b == 0 && c == 113))
}
pub fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => public_ipv4(v4),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return public_ipv4(v4);
            }
            let s = v6.segments();
            // Only global unicast; exclude transition, benchmarking and documentation ranges.
            (s[0] & 0xe000) == 0x2000
                && s[0] != 0x2002
                && !(s[0] == 0x2001 && (s[1] < 0x200 || s[1] == 0xdb8))
                && !(s[0] == 0x3fff && s[1] < 0x1000)
        }
    }
}

pub async fn destination(raw: &str, local: bool) -> Result<(url::Url, Vec<SocketAddr>)> {
    let url = url::Url::parse(raw)?;
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        bail!("Webhook credentials and fragments are not allowed");
    }
    let host = url
        .host_str()
        .context("Webhook host required")?
        .trim_matches(['[', ']']);
    let dev_host = local
        && (host == "localhost"
            || host.parse::<IpAddr>().is_ok_and(|ip| match ip {
                IpAddr::V4(ip) => ip.octets()[..3] == [127, 0, 0],
                IpAddr::V6(ip) => ip.is_loopback(),
            }));
    if url.scheme() != "https" && !(url.scheme() == "http" && dev_host) {
        bail!("Webhook requires HTTPS (localhost HTTP requires development mode)");
    }
    let port = url.port_or_known_default().context("Missing port")?;
    let addresses: Vec<_> = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::net::lookup_host((host, port)),
    )
    .await??
    .collect();
    if addresses.is_empty()
        || addresses
            .iter()
            .any(|a| !(public_ip(a.ip()) || dev_host && a.ip().is_loopback()))
    {
        bail!("Webhook resolves to a non-public address");
    }
    Ok((url, addresses))
}

pub async fn deliver(
    url: &str,
    local: bool,
    signature: String,
    body: bytes::Bytes,
) -> Result<bool> {
    // Resolve on every attempt and pin vetted addresses to this connection. Disable
    // proxies and redirects: neither may bypass address validation or leak tokens.
    let (url, addresses) = destination(url, local).await?;
    let host = url.host_str().unwrap().to_owned();
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5))
        .resolve_to_addrs(&host, &addresses)
        .build()?;
    let response = client
        .post(url)
        .header("content-type", "application/json")
        .header("webhook-signature", signature)
        .body(body)
        .send()
        .await?;
    if !response.status().is_success() {
        bail!("Webhook HTTP {}", response.status());
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if bytes.len() + chunk.len() > 64 * 1024 {
            bail!("Webhook response too large");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice::<Value>(&bytes)
        .ok()
        .is_some_and(|v| v.get("done") == Some(&Value::Bool(true))))
}
