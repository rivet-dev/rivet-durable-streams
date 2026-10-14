//! Verification-only compatibility for artifacts issued before Engine cutover.
use anyhow::{Result, anyhow, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use ring::{
    hmac,
    signature::{Ed25519KeyPair, KeyPair},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::model::{ApiError, ApiResult, Subscription};

pub const TOKEN_RETENTION_MS: i64 = 86_400_000;
const PUBLIC_RETENTION_MS: i64 = 365_000;

#[derive(Clone, Serialize, Deserialize)]
pub struct LegacyKeys {
    pub cutover: i64,
    pub token_expires: i64,
    pub public_expires: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    token_key: Option<Vec<u8>>,
    public_keys: Vec<Value>,
}

impl LegacyKeys {
    pub fn migrate(value: &str, cutover: i64, latest_expiry: i64) -> Result<Self> {
        #[derive(Deserialize)]
        struct OldKeys {
            private_key: Vec<u8>,
            token_key: Vec<u8>,
            old_public_keys: Vec<Value>,
        }
        let old: OldKeys = serde_json::from_str(value)?;
        ensure!(old.token_key.len() == 32, "Invalid legacy capability key");
        let signer = Ed25519KeyPair::from_pkcs8(&old.private_key)
            .map_err(|_| anyhow!("Invalid legacy webhook key"))?;
        let active = public_jwk(signer.public_key().as_ref());
        let mut public_keys = Vec::with_capacity(old.old_public_keys.len() + 1);
        for key in old.old_public_keys {
            ensure!(
                key["kty"] == "OKP" && key["crv"] == "Ed25519",
                "Invalid legacy public key"
            );
            let x = URL_SAFE_NO_PAD.decode(
                key["x"]
                    .as_str()
                    .ok_or_else(|| anyhow!("Missing legacy public key"))?,
            )?;
            ensure!(x.len() == 32, "Invalid legacy public key size");
            let public = public_jwk(&x);
            ensure!(key["kid"] == public["kid"], "Invalid legacy public key id");
            public_keys.push(public);
        }
        public_keys.push(active);
        let token_expires =
            latest_expiry.clamp(cutover, cutover.saturating_add(TOKEN_RETENTION_MS));
        Ok(Self {
            cutover,
            token_expires,
            public_expires: cutover.saturating_add(PUBLIC_RETENTION_MS),
            token_key: (token_expires > cutover).then_some(old.token_key),
            public_keys,
        })
    }

    pub fn public_jwks(&self, now: i64) -> Vec<Value> {
        if now < self.public_expires {
            self.public_keys.clone()
        } else {
            Vec::new()
        }
    }

    pub fn next_deadline(&self) -> Option<i64> {
        self.token_key
            .as_ref()
            .map(|_| self.token_expires)
            .into_iter()
            .chain((!self.public_keys.is_empty()).then_some(self.public_expires))
            .min()
    }

    pub fn cleanup(&mut self, now: i64) {
        if now >= self.token_expires {
            self.token_key = None;
        }
        if now >= self.public_expires {
            self.public_keys.clear();
        }
    }

    pub fn verify(&self, token: &str, sub: &Subscription, now: i64) -> ApiResult<()> {
        let invalid = || ApiError::new(401, "TOKEN_INVALID", "Invalid capability token");
        if token.len() > 6144 || now >= self.token_expires {
            return Err(invalid());
        }
        let key = self.token_key.as_ref().ok_or_else(invalid)?;
        let (input, tag) = token.rsplit_once('.').ok_or_else(invalid)?;
        let tag = URL_SAFE_NO_PAD.decode(tag).map_err(|_| invalid())?;
        hmac::verify(
            &hmac::Key::new(hmac::HMAC_SHA256, key),
            input.as_bytes(),
            &tag,
        )
        .map_err(|_| invalid())?;
        let (header, payload) = input.split_once('.').ok_or_else(invalid)?;
        if header != URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#) {
            return Err(invalid());
        }
        let claims: Claims =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).map_err(|_| invalid())?)
                .map_err(|_| invalid())?;
        if claims.sub != sub.id || claims.root != sub.root {
            return Err(invalid());
        }
        let wake = sub.wake.as_ref().ok_or_else(ApiError::fenced)?;
        if sub.deleted
            || wake.issued_at >= self.cutover
            || claims.generation != wake.generation
            || claims.wake_id != wake.id
        {
            return Err(ApiError::fenced());
        }
        if claims.exp <= now / 1000 {
            return Err(ApiError::new(
                401,
                "TOKEN_EXPIRED",
                "Capability token expired",
            ));
        }
        if claims.exp > self.token_expires / 1000
            || claims.exp > wake.issued_at.saturating_add(TOKEN_RETENTION_MS) / 1000
            || claims.kind
                != if sub.config.webhook.is_some() {
                    "callback"
                } else {
                    "claim"
                }
        {
            return Err(invalid());
        }
        Ok(())
    }
}

fn public_jwk(bytes: &[u8]) -> Value {
    let x = URL_SAFE_NO_PAD.encode(bytes);
    let canonical = format!(r#"{{"crv":"Ed25519","kty":"OKP","x":"{x}"}}"#);
    let kid = format!(
        "ds_{}",
        URL_SAFE_NO_PAD.encode(Sha256::digest(canonical.as_bytes()))
    );
    json!({"kty":"OKP","crv":"Ed25519","kid":kid,"use":"sig","alg":"EdDSA","x":x})
}

#[derive(Serialize, Deserialize)]
struct Claims {
    sub: String,
    root: String,
    generation: i64,
    wake_id: String,
    exp: i64,
    kind: String,
}

#[cfg(test)]
mod tests {
    use super::super::model::{CatalogEntry, Config};
    use super::*;

    fn old_keys() -> Value {
        json!({"private_key":Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).unwrap().as_ref(),"token_key":vec![7;32],"old_public_keys":[]})
    }

    fn fixture() -> (LegacyKeys, Subscription, String) {
        let old = old_keys();
        let config = serde_json::from_value::<Config>(
            json!({"type":"pull-wake","pattern":"**","wake_stream":"wake"}),
        )
        .unwrap()
        .normalize()
        .unwrap();
        let mut sub = Subscription::new(
            "sub".into(),
            "/root".into(),
            "https://streams.gameinc.io/root".into(),
            config,
            0,
            1000,
        );
        sub.observe(&CatalogEntry {
            path: "/root/source".into(),
            version: 1,
            incarnation: "first".into(),
            tail: "0000000000000000_0000000000000001".into(),
            start: crate::protocol::ZERO_OFFSET.into(),
            exists: true,
            wakes: vec![],
        });
        sub.ensure_wake(1000);
        let wake = sub.wake.as_ref().unwrap();
        let claims = Claims {
            sub: sub.id.clone(),
            root: sub.root.clone(),
            generation: wake.generation,
            wake_id: wake.id.clone(),
            exp: wake.token_expires / 1000,
            kind: "claim".into(),
        };
        let input = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#),
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap())
        );
        let tag = hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, &[7; 32]),
            input.as_bytes(),
        );
        let token = format!("{input}.{}", URL_SAFE_NO_PAD.encode(tag.as_ref()));
        (
            LegacyKeys::migrate(&old.to_string(), 2000, wake.token_expires).unwrap(),
            sub,
            token,
        )
    }

    #[test]
    fn cutover_preserves_old_capabilities_without_retaining_private_webhook_keys() {
        let (keys, mut sub, token) = fixture();
        assert!(keys.verify(&token, &sub, 3000).is_ok());
        let serialized = serde_json::to_string(&keys).unwrap();
        assert!(!serialized.contains("private_key"));
        let restored: LegacyKeys = serde_json::from_str(&serialized).unwrap();
        assert_eq!(restored.token_expires, keys.token_expires);
        assert_eq!(restored.public_expires, keys.public_expires);
        assert!(restored.verify(&token, &sub, 3000).is_ok());
        sub.root = "/other".into();
        assert!(keys.verify(&token, &sub, 3000).is_err());
        sub.root = "/root".into();
        sub.wake.as_mut().unwrap().generation += 1;
        assert_eq!(keys.verify(&token, &sub, 3000).unwrap_err().status, 409);
        sub.wake.as_mut().unwrap().generation -= 1;
        sub.wake.as_mut().unwrap().issued_at = keys.cutover;
        assert_eq!(keys.verify(&token, &sub, 3000).unwrap_err().status, 409);
    }

    #[test]
    fn migration_bounds_capability_retention_and_drops_unused_hmac_keys() {
        let old = old_keys().to_string();
        let capped = LegacyKeys::migrate(&old, 2000, i64::MAX).unwrap();
        assert_eq!(capped.token_expires, 2000 + TOKEN_RETENTION_MS);
        let unused = LegacyKeys::migrate(&old, 2000, 1000).unwrap();
        assert_eq!(unused.token_expires, 2000);
        assert!(
            !serde_json::to_string(&unused)
                .unwrap()
                .contains("token_key")
        );
        assert_eq!(unused.next_deadline(), Some(2000 + PUBLIC_RETENTION_MS));
    }

    #[test]
    fn legacy_material_is_removed_at_fixed_deadlines() {
        let (mut keys, sub, token) = fixture();
        assert_eq!(keys.next_deadline(), Some(keys.public_expires));
        assert_eq!(keys.public_jwks(keys.public_expires - 1).len(), 1);
        assert!(keys.public_jwks(keys.public_expires).is_empty());
        keys.cleanup(keys.public_expires);
        assert_eq!(keys.next_deadline(), Some(keys.token_expires));
        assert!(keys.verify(&token, &sub, keys.token_expires).is_err());
        keys.cleanup(keys.token_expires);
        assert_eq!(keys.next_deadline(), None);
        assert!(!serde_json::to_string(&keys).unwrap().contains("token_key"));
    }
}
