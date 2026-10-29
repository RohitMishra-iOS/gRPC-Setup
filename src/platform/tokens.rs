//! Every token the server issues is an HS256 JWT signed with DM_TOKEN_SECRET and tagged with a
//! `typ` claim, so one kind can never be used as another:
//!
//!   fis       installation auth token (7 days)            Authorization: FIS_v2 <token>
//!   appcheck  App Check token (DM_APPCHECK_TTL_SECONDS)    X-DM-AppCheck: <token>
//!   id        Auth ID token (1 hour)                       Authorization: Bearer <token>
//!   custom    custom sign-in token minted by your backend  accounts:signInWithCustomToken
//!
//! Refresh tokens are opaque random strings stored hashed (sha256) in MongoDB.

use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use rand::RngCore;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{Map, Value as Json};
use sha2::{Digest, Sha256};

pub const ID_TOKEN_TTL: i64 = 3600;
pub const FIS_TOKEN_TTL: i64 = 7 * 86400;

pub fn now() -> i64 {
    chrono::Utc::now().timestamp()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FisClaims {
    pub typ: String,
    /// Installation id (FID).
    pub sub: String,
    pub app_id: String,
    pub iat: i64,
    pub exp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppCheckClaims {
    pub typ: String,
    /// App id the token was issued to.
    pub sub: String,
    pub provider: String,
    pub iat: i64,
    pub exp: i64,
    pub jti: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub limited_use: bool,
}

/// Firebase-compatible ID-token claims (user_id == sub).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdClaims {
    pub typ: String,
    pub iss: String,
    pub aud: String,
    pub sub: String,
    pub user_id: String,
    pub auth_time: i64,
    pub iat: i64,
    pub exp: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default)]
    pub email_verified: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub picture: Option<String>,
    /// {"sign_in_provider": "password" | "anonymous" | "custom" | "google.com" | "apple.com"}
    pub firebase: Json,
    /// Custom claims set through the admin API, merged at the top level like Firebase.
    #[serde(flatten)]
    pub custom: Map<String, Json>,
}

impl IdClaims {
    pub fn provider(&self) -> &str {
        self.firebase.get("sign_in_provider").and_then(Json::as_str).unwrap_or("")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomTokenClaims {
    pub typ: String,
    pub uid: String,
    pub iat: i64,
    pub exp: i64,
    #[serde(default)]
    pub claims: Map<String, Json>,
}

#[derive(Clone)]
pub struct Tokens {
    encoding: EncodingKey,
    decoding: DecodingKey,
}

impl Tokens {
    pub fn new(secret: &str) -> Self {
        Self { encoding: EncodingKey::from_secret(secret.as_bytes()), decoding: DecodingKey::from_secret(secret.as_bytes()) }
    }

    pub fn sign<T: Serialize>(&self, claims: &T) -> String {
        encode(&Header::new(Algorithm::HS256), claims, &self.encoding).expect("HS256 signing cannot fail")
    }

    /// Verifies signature + expiry and that `typ` matches.
    pub fn verify<T: DeserializeOwned>(&self, token: &str, typ: &str) -> Result<T, String> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_aud = false;
        validation.leeway = 30;
        let data = decode::<Json>(token, &self.decoding, &validation).map_err(|e| e.to_string())?;
        if data.claims.get("typ").and_then(Json::as_str) != Some(typ) {
            return Err(format!("not a {typ} token"));
        }
        serde_json::from_value(data.claims).map_err(|e| e.to_string())
    }
}

/// 32 random bytes, base64url.
pub fn random_token() -> String {
    use base64::Engine;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

pub fn hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

/// Firebase-style FID: 22 chars of URL-safe base64 over 128 random bits, first 4 bits = 0111.
pub fn new_fid() -> String {
    use base64::Engine;
    let mut bytes = [0u8; 17];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes[0] = 0b0111_0000 | (bytes[0] & 0x0F);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)[..22].to_string()
}

pub fn valid_fid(fid: &str) -> bool {
    fid.len() == 22 && fid.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') && matches!(fid.as_bytes()[0], b'c' | b'd' | b'e' | b'f')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fids_are_firebase_shaped() {
        for _ in 0..200 {
            let fid = new_fid();
            assert!(valid_fid(&fid), "{fid}");
        }
    }

    #[test]
    fn token_types_cannot_be_swapped() {
        let tokens = Tokens::new("secret");
        let fis = tokens.sign(&FisClaims { typ: "fis".into(), sub: new_fid(), app_id: "a".into(), iat: now(), exp: now() + 60 });
        assert!(tokens.verify::<FisClaims>(&fis, "fis").is_ok());
        assert!(tokens.verify::<AppCheckClaims>(&fis, "appcheck").is_err());
        assert!(Tokens::new("other").verify::<FisClaims>(&fis, "fis").is_err());
        let expired = tokens.sign(&FisClaims { typ: "fis".into(), sub: "x".into(), app_id: "a".into(), iat: 0, exp: 10 });
        assert!(tokens.verify::<FisClaims>(&expired, "fis").is_err());
    }
}
