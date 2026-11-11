//! One uploaded event → one row of the GA4 events table (dm-specs TBL-002…TBL-010), plus the
//! user-table snapshot (TBL-011). Fields are written in the order BigQuery exports them; unknown
//! values are `null`, repeated fields are `[]`.

use std::collections::{BTreeSet, HashMap};

use mongodb::bson::{doc, Bson, Document};

use super::reference::{apple_marketing_name, default_channel_group, ECOMMERCE_EVENTS};
use super::tables;
use crate::config::AnalyticsConfig;
use crate::dm::GeoHeaders;
use crate::pb_dm::analytics_value::Value;
use crate::pb_dm::{AnalyticsItem, AnalyticsParam, AnalyticsUserProperty, AnalyticsValue, ClientContext, UploadEventsRequest};

const LAST_CLICK_LOOKBACK_MICROS: i64 = 90 * 86_400 * 1_000_000;

// ── Small helpers ───────────────────────────────────────────────────────────────────────────────

fn s(value: &str) -> Bson {
    if value.is_empty() { Bson::Null } else { Bson::String(value.to_string()) }
}

fn o(value: Option<String>) -> Bson {
    value.map_or(Bson::Null, Bson::String)
}

fn f(value: Option<f64>) -> Bson {
    value.map_or(Bson::Null, |v| Bson::Double(round6(v)))
}

fn i(value: Option<i64>) -> Bson {
    value.map_or(Bson::Null, Bson::Int64)
}

fn positive(value: i64) -> Bson {
    if value > 0 { Bson::Int64(value) } else { Bson::Null }
}

fn round6(v: f64) -> f64 {
    (v * 1e6).round() / 1e6
}

/// `{string_value, int_value, float_value, double_value}` with exactly one set (TBL-003).
pub fn value_document(value: Option<&AnalyticsValue>) -> Document {
    let v = value.and_then(|v| v.value.as_ref());
    doc! {
        "string_value": match v { Some(Value::StringValue(s)) => Bson::String(s.clone()), _ => Bson::Null },
        "int_value": match v { Some(Value::IntValue(n)) => Bson::Int64(*n), _ => Bson::Null },
        "float_value": match v { Some(Value::FloatValue(n)) => Bson::Double(f64::from(*n)), _ => Bson::Null },
        "double_value": match v { Some(Value::DoubleValue(n)) => Bson::Double(*n), _ => Bson::Null },
    }
}

fn params_array(params: &[AnalyticsParam]) -> Vec<Document> {
    params.iter().map(|p| doc! { "key": &p.key, "value": value_document(p.value.as_ref()) }).collect()
}

/// Parameter lookups by key.
struct Params<'a>(HashMap<&'a str, &'a Value>);

impl<'a> Params<'a> {
    fn new(params: &'a [AnalyticsParam]) -> Self {
        Self(params.iter().filter_map(|p| p.value.as_ref().and_then(|v| v.value.as_ref()).map(|v| (p.key.as_str(), v))).collect())
    }

    fn num(&self, key: &str) -> Option<f64> {
        match self.0.get(key)? {
            Value::IntValue(n) => Some(*n as f64),
            Value::DoubleValue(n) => Some(*n),
            Value::FloatValue(n) => Some(f64::from(*n)),
            Value::StringValue(s) => s.trim().parse().ok().filter(|v: &f64| v.is_finite()),
        }
    }

    fn int(&self, key: &str) -> Option<i64> {
        match self.0.get(key)? {
            Value::IntValue(n) => Some(*n),
            Value::DoubleValue(n) if n.fract() == 0.0 => Some(*n as i64),
            Value::StringValue(s) => s.trim().parse().ok(),
            _ => None,
        }
    }

    /// Text form: strings as they are, numbers formatted (ids logged as numbers).
    fn text(&self, key: &str) -> Option<String> {
        let out = match self.0.get(key)? {
            Value::StringValue(s) => s.clone(),
            Value::IntValue(n) => n.to_string(),
            Value::DoubleValue(n) => n.to_string(),
            Value::FloatValue(n) => n.to_string(),
        };
        Some(out).filter(|s| !s.is_empty())
    }
}

fn number(document: &Document, key: &str) -> f64 {
    match document.get(key) {
        Some(Bson::Int32(v)) => f64::from(*v),
        Some(Bson::Int64(v)) => *v as f64,
        Some(Bson::Double(v)) => *v,
        _ => 0.0,
    }
}

fn int(document: &Document, key: &str) -> Option<i64> {
    match document.get(key) {
        Some(Bson::Int32(v)) => Some(i64::from(*v)),
        Some(Bson::Int64(v)) => Some(*v),
        Some(Bson::Double(v)) => Some(*v as i64),
        _ => None,
    }
}

// ── Campaigns / traffic sources (TBL-007) ───────────────────────────────────────────────────────

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Campaign {
    pub campaign_id: Option<String>,
    pub campaign_name: Option<String>,
    pub source: Option<String>,
    pub medium: Option<String>,
    pub term: Option<String>,
    pub content: Option<String>,
    pub source_platform: Option<String>,
    pub creative_format: Option<String>,
    pub marketing_tactic: Option<String>,
    pub gclid: Option<String>,
    pub dclid: Option<String>,
    pub srsltid: Option<String>,
}

const CAMPAIGN_KEYS: [&str; 12] = [
    "campaign_id", "campaign_name", "source", "medium", "term", "content", "source_platform", "creative_format",
    "marketing_tactic", "gclid", "dclid", "srsltid",
];

impl Campaign {
    fn from_params(p: &Params) -> Option<Self> {
        let mut c = Campaign {
            campaign_id: p.text("campaign_id"),
            campaign_name: p.text("campaign").or_else(|| p.text("campaign_name")),
            source: p.text("source"),
            medium: p.text("medium"),
            term: p.text("term"),
            content: p.text("content"),
            source_platform: p.text("source_platform"),
            creative_format: p.text("creative_format"),
            marketing_tactic: p.text("marketing_tactic"),
            gclid: p.text("gclid"),
            dclid: p.text("dclid"),
            srsltid: p.text("srsltid"),
        };
        if c == Campaign::default() {
            return None;
        }
        // Google Ads auto-tagging: a click id without utm tags is google / cpc.
        if c.source.is_none() && (c.gclid.is_some() || c.dclid.is_some()) {
            c.source = Some("google".into());
            c.medium.get_or_insert_with(|| "cpc".into());
        }
        Some(c)
    }

    fn is_direct(&self) -> bool {
        matches!(self.source.as_deref(), None | Some("(direct)")) && self.gclid.is_none() && self.dclid.is_none()
    }

    fn values(&self) -> [&Option<String>; 12] {
        [
            &self.campaign_id, &self.campaign_name, &self.source, &self.medium, &self.term, &self.content, &self.source_platform,
            &self.creative_format, &self.marketing_tactic, &self.gclid, &self.dclid, &self.srsltid,
        ]
    }

    pub fn to_document(&self) -> Document {
        let mut d = Document::new();
        for (key, value) in CAMPAIGN_KEYS.iter().zip(self.values()) {
            d.insert(*key, o(value.clone()));
        }
        d
    }

    pub fn from_document(d: &Document) -> Self {
        let get = |k: &str| d.get_str(k).ok().map(str::to_string);
        Campaign {
            campaign_id: get("campaign_id"),
            campaign_name: get("campaign_name"),
            source: get("source"),
            medium: get("medium"),
            term: get("term"),
            content: get("content"),
            source_platform: get("source_platform"),
            creative_format: get("creative_format"),
            marketing_tactic: get("marketing_tactic"),
            gclid: get("gclid"),
            dclid: get("dclid"),
            srsltid: get("srsltid"),
        }
    }

    fn collected(&self) -> Document {
        doc! {
            "manual_campaign_id": o(self.campaign_id.clone()),
            "manual_campaign_name": o(self.campaign_name.clone()),
            "manual_source": o(self.source.clone()),
            "manual_medium": o(self.medium.clone()),
            "manual_term": o(self.term.clone()),
            "manual_content": o(self.content.clone()),
            "manual_source_platform": o(self.source_platform.clone()),
            "manual_creative_format": o(self.creative_format.clone()),
            "manual_marketing_tactic": o(self.marketing_tactic.clone()),
            "gclid": o(self.gclid.clone()),
            "dclid": o(self.dclid.clone()),
            "srsltid": o(self.srsltid.clone()),
        }
    }
}

fn traffic_source(first: Option<&Campaign>) -> Document {
    match first {
        Some(c) => doc! {
            "name": c.campaign_name.clone().unwrap_or_else(|| "(not set)".into()),
            "medium": c.medium.clone().unwrap_or_else(|| "(not set)".into()),
            "source": c.source.clone().unwrap_or_else(|| "(not set)".into()),
        },
        None => doc! { "name": "(direct)", "medium": "(none)", "source": "(direct)" },
    }
}

fn last_click(campaign: Option<&Campaign>) -> Document {
    let direct = Campaign { source: Some("(direct)".into()), medium: Some("(none)".into()), campaign_name: Some("(direct)".into()), ..Default::default() };
    let c = campaign.unwrap_or(&direct);
    let source = c.source.clone().unwrap_or_else(|| "(not set)".into());
    let medium = c.medium.clone().unwrap_or_else(|| "(not set)".into());
    let name = c.campaign_name.clone().unwrap_or_else(|| "(not set)".into());
    let group = default_channel_group(&source, &medium, &name, c.gclid.is_some() || c.dclid.is_some());
    doc! {
        "manual_campaign": {
            "campaign_id": o(c.campaign_id.clone()),
            "campaign_name": &name,
            "source": &source,
            "medium": &medium,
            "term": o(c.term.clone()),
            "content": o(c.content.clone()),
            "source_platform": o(c.source_platform.clone()),
            "creative_format": o(c.creative_format.clone()),
            "marketing_tactic": o(c.marketing_tactic.clone()),
        },
        "google_ads_campaign": Bson::Null,
        "cross_channel_campaign": {
            "campaign_id": o(c.campaign_id.clone()),
            "campaign_name": &name,
            "source": &source,
            "medium": &medium,
            "source_platform": o(c.source_platform.clone()),
            "default_channel_group": group,
            "primary_channel_group": group,
        },
        "sa360_campaign": Bson::Null,
        "cm360_campaign": Bson::Null,
        "dv360_campaign": Bson::Null,
    }
}

// ── The user's lifetime profile (analytics_users) ──────────────────────────────────────────────

#[derive(Clone, Debug, Default)]
pub struct Profile {
    pub first_touch_micros: Option<i64>,
    pub ltv_revenue: f64,
    pub ltv_revenue_usd: f64,
    pub purchases: i64,
    pub sessions: i64,
    pub engaged_sessions: i64,
    pub engagement_ms: i64,
    pub first_purchase_date: Option<String>,
    pub first_source: Option<Campaign>,
    pub last_click: Option<Campaign>,
    pub last_click_micros: i64,
    pub last_engaged_session: Option<i64>,
    pub last_screen: Option<String>,
    pub last_active_micros: i64,
}

impl Profile {
    pub fn from_document(d: &Document) -> Self {
        Profile {
            first_touch_micros: int(d, "first_touch_micros"),
            ltv_revenue: number(d, "ltv_revenue"),
            ltv_revenue_usd: number(d, "ltv_revenue_usd"),
            purchases: number(d, "purchases") as i64,
            sessions: number(d, "sessions") as i64,
            engaged_sessions: number(d, "engaged_sessions") as i64,
            engagement_ms: number(d, "engagement_ms") as i64,
            first_purchase_date: d.get_str("first_purchase_date").ok().map(str::to_string),
            first_source: d.get_document("first_source").ok().map(Campaign::from_document),
            last_click: d.get_document("last_click").ok().map(Campaign::from_document),
            last_click_micros: int(d, "last_click_micros").unwrap_or(0),
            last_engaged_session: int(d, "last_engaged_session"),
            last_screen: d.get_str("last_screen").ok().map(str::to_string),
            last_active_micros: int(d, "last_active_micros").unwrap_or(0),
        }
    }
}

// ── Records shared by every row of an upload ───────────────────────────────────────────────────

/// `IOS` / `ANDROID` / `WEB` from the context's platform.
pub fn platform(context: &ClientContext) -> String {
    match context.platform.to_ascii_lowercase().as_str() {
        "ios" | "ipados" | "macos" | "tvos" | "watchos" | "visionos" => "IOS".into(),
        "android" => "ANDROID".into(),
        "web" | "js" | "browser" => "WEB".into(),
        "" => "IOS".into(),
        other => other.to_ascii_uppercase(),
    }
}

/// The GA4 device record. Falls back to the request context for SDKs that send no record.
pub fn device_record(request: &UploadEventsRequest, context: &ClientContext) -> Document {
    let default = Default::default();
    let d = request.device.as_ref().unwrap_or(&default);
    let platform = platform(context);
    let os = if !d.operating_system.is_empty() {
        d.operating_system.clone()
    } else {
        match platform.as_str() {
            "IOS" => "iOS".into(),
            "ANDROID" => "Android".into(),
            _ => String::new(),
        }
    };
    let version = if d.operating_system_version.is_empty() { context.os_version.clone() } else { d.operating_system_version.clone() };
    // GA4 writes "iOS 18.1" / "Android 14".
    let os_version = if version.is_empty() || os.is_empty() || version.starts_with(&os) { version } else { format!("{os} {version}") };
    let hardware = if d.mobile_os_hardware_model.is_empty() && platform != "WEB" { context.device_model.clone() } else { d.mobile_os_hardware_model.clone() };
    let brand = if d.mobile_brand_name.is_empty() && platform == "IOS" { "Apple".to_string() } else { d.mobile_brand_name.clone() };
    let marketing = if d.mobile_marketing_name.is_empty() && brand == "Apple" {
        apple_marketing_name(&hardware).map(str::to_string).unwrap_or_default()
    } else {
        d.mobile_marketing_name.clone()
    };
    let model_name = if d.mobile_model_name.is_empty() && brand == "Apple" {
        if hardware.starts_with("iPad") { "iPad".into() } else if hardware.starts_with("iPhone") { "iPhone".into() } else { String::new() }
    } else {
        d.mobile_model_name.clone()
    };
    let category = if !d.category.is_empty() {
        d.category.clone()
    } else if hardware.starts_with("iPad") {
        "tablet".into()
    } else if platform == "IOS" || platform == "ANDROID" {
        "mobile".into()
    } else {
        String::new()
    };
    let language = if d.language.is_empty() { context.locale.replace('_', "-").to_ascii_lowercase() } else { d.language.to_ascii_lowercase() };
    let limited = if !d.is_limited_ad_tracking.is_empty() {
        d.is_limited_ad_tracking.clone()
    } else if request.device.is_some() && platform != "WEB" {
        if d.advertising_id.is_empty() { "Yes".into() } else { "No".into() }
    } else {
        String::new()
    };
    let web = platform == "WEB";
    doc! {
        "category": s(&category),
        "mobile_brand_name": s(&brand),
        "mobile_model_name": s(&model_name),
        "mobile_marketing_name": s(&marketing),
        "mobile_os_hardware_model": s(&hardware),
        "operating_system": s(&os),
        "operating_system_version": s(&os_version),
        "vendor_id": s(&d.vendor_id),
        "advertising_id": s(&d.advertising_id),
        "language": s(&language),
        "is_limited_ad_tracking": s(&limited),
        "time_zone_offset_seconds": if request.device.is_some() { Bson::Int64(d.time_zone_offset_seconds) } else { Bson::Null },
        "browser": s(&d.browser),
        "browser_version": s(&d.browser_version),
        "web_info": if web {
            Bson::Document(doc! { "browser": s(&d.browser), "browser_version": s(&d.browser_version), "hostname": s(&d.hostname) })
        } else {
            Bson::Null
        },
    }
}

pub fn privacy_record(request: &UploadEventsRequest) -> Bson {
    let default = Default::default();
    let p = request.privacy_info.as_ref().unwrap_or(&default);
    Bson::Document(doc! {
        "analytics_storage": s(&p.analytics_storage),
        "ads_storage": s(&p.ads_storage),
        "uses_transient_token": s(&p.uses_transient_token),
    })
}

/// `null` for web (GA4 has no app_info for web streams).
pub fn app_info_record(request: &UploadEventsRequest, context: &ClientContext) -> Option<Document> {
    if platform(context) == "WEB" {
        return None;
    }
    let default = Default::default();
    let a = request.app_info.as_ref().unwrap_or(&default);
    Some(doc! {
        "id": s(if a.id.is_empty() { &context.bundle_id } else { &a.id }),
        "version": s(if a.version.is_empty() { &context.app_version } else { &a.version }),
        "install_store": s(&a.install_store),
        "DM_app_id": s(&context.app_id),
        "install_source": s(&a.install_source),
    })
}

#[derive(Clone, Debug, Default)]
pub struct Geo {
    pub continent: Option<String>,
    pub sub_continent: Option<String>,
    pub country: Option<String>,
    pub region: Option<String>,
    pub city: Option<String>,
    pub metro: Option<String>,
}

/// GA4 geo from CDN headers (only called with DM_TRUST_GEO_HEADERS=true).
pub fn geo(headers: &GeoHeaders) -> Geo {
    let mut geo = Geo { region: headers.region.clone(), city: headers.city.clone(), metro: headers.metro.clone(), ..Default::default() };
    if let Some(code) = &headers.country {
        if let Some((country, sub, continent)) = super::countries::country(code) {
            geo.country = Some(country.into());
            geo.sub_continent = Some(sub.into());
            geo.continent = Some(continent.into());
        }
    }
    if geo.continent.is_none() {
        geo.continent = headers.continent.as_deref().and_then(super::countries::continent_from_code).map(str::to_string);
    }
    geo
}

pub fn geo_document(geo: &Geo) -> Document {
    doc! {
        "continent": o(geo.continent.clone()),
        "sub_continent": o(geo.sub_continent.clone()),
        "country": o(geo.country.clone()),
        "region": o(geo.region.clone()),
        "city": o(geo.city.clone()),
        "metro": o(geo.metro.clone()),
    }
}

fn user_properties_array(properties: &[AnalyticsUserProperty]) -> Vec<Document> {
    properties
        .iter()
        .map(|p| {
            let mut value = value_document(p.value.as_ref());
            value.insert("set_timestamp_micros", positive(p.set_timestamp_micros));
            doc! { "key": &p.key, "value": value }
        })
        .collect()
}

/// `{key: value}` (profile and console user page).
pub fn user_properties_map(properties: &[AnalyticsUserProperty]) -> Document {
    properties
        .iter()
        .map(|p| {
            let v = match p.value.as_ref().and_then(|v| v.value.as_ref()) {
                Some(Value::StringValue(s)) => Bson::String(s.clone()),
                Some(Value::IntValue(n)) => Bson::Int64(*n),
                Some(Value::DoubleValue(n)) => Bson::Double(*n),
                Some(Value::FloatValue(n)) => Bson::Double(f64::from(*n)),
                None => Bson::Null,
            };
            (p.key.clone(), v)
        })
        .collect()
}

// ── Ecommerce, items, publisher (TBL-008, TBL-009) ──────────────────────────────────────────────

const ITEM_TEXT: [&str; 22] = [
    "item_id", "item_name", "item_brand", "item_variant", "item_category", "item_category2", "item_category3", "item_category4",
    "item_category5", "coupon", "affiliation", "location_id", "item_list_id", "item_list_name", "item_list_index", "index", "promotion_id",
    "promotion_name", "creative_name", "creative_slot", "price", "quantity",
];

struct ItemRow {
    document: Document,
    quantity: i64,
    key: Option<String>,
}

fn item_row(item: &AnalyticsItem, event: &Params, name: &str, currency: &str, cfg: &AnalyticsConfig) -> ItemRow {
    let p = Params::new(&item.params);
    // Item list / promotion fields fall back to the event's own parameters.
    let inherited = |key: &str| p.text(key).or_else(|| event.text(key));
    let price = p.num("price");
    let quantity = p.int("quantity").or_else(|| p.num("quantity").map(|q| q.round() as i64)).unwrap_or(1);
    let line = price.map(|price| price * quantity as f64);
    let purchase = matches!(name, "purchase" | "in_app_purchase");
    let revenue = if purchase { line } else { None };
    let refund = if name == "refund" { line } else { None };
    let usd = |v: Option<f64>| v.and_then(|v| cfg.to_usd(v, currency));
    let custom: Vec<Document> = item
        .params
        .iter()
        .filter(|param| !ITEM_TEXT.contains(&param.key.as_str()))
        .map(|param| doc! { "key": &param.key, "value": value_document(param.value.as_ref()) })
        .collect();
    let document = doc! {
        "item_id": o(p.text("item_id")),
        "item_name": o(p.text("item_name")),
        "item_brand": o(p.text("item_brand")),
        "item_variant": o(p.text("item_variant")),
        "item_category": o(p.text("item_category")),
        "item_category2": o(p.text("item_category2")),
        "item_category3": o(p.text("item_category3")),
        "item_category4": o(p.text("item_category4")),
        "item_category5": o(p.text("item_category5")),
        "price_in_usd": f(usd(price)),
        "price": f(price),
        "quantity": quantity,
        "item_revenue_in_usd": f(usd(revenue)),
        "item_revenue": f(revenue),
        "item_refund_in_usd": f(usd(refund)),
        "item_refund": f(refund),
        "coupon": o(p.text("coupon")),
        "affiliation": o(p.text("affiliation")),
        "location_id": o(p.text("location_id")),
        "item_list_id": o(inherited("item_list_id")),
        "item_list_name": o(inherited("item_list_name")),
        // Firebase's item parameter `index` is the position in the list.
        "item_list_index": o(p.text("item_list_index").or_else(|| p.text("index")).or_else(|| event.text("item_list_index"))),
        "promotion_id": o(inherited("promotion_id")),
        "promotion_name": o(inherited("promotion_name")),
        "creative_name": o(inherited("creative_name")),
        "creative_slot": o(inherited("creative_slot")),
        "item_params": custom,
    };
    ItemRow { document, quantity, key: p.text("item_id").or_else(|| p.text("item_name")) }
}

fn ecommerce(name: &str, p: &Params, items: &[ItemRow], currency: &str, cfg: &AnalyticsConfig) -> Bson {
    if !ECOMMERCE_EVENTS.contains(&name) && items.is_empty() {
        return Bson::Null;
    }
    let value = p.num("value");
    let usd = |v: Option<f64>| v.and_then(|v| cfg.to_usd(v, currency));
    let purchase = if matches!(name, "purchase" | "in_app_purchase") { value } else { None };
    let refund = if name == "refund" { value } else { None };
    let shipping = p.num("shipping");
    let tax = p.num("tax");
    let unique: BTreeSet<&str> = items.iter().filter_map(|i| i.key.as_deref()).collect();
    Bson::Document(doc! {
        "total_item_quantity": if items.is_empty() { Bson::Null } else { Bson::Int64(items.iter().map(|i| i.quantity).sum()) },
        "purchase_revenue_in_usd": f(usd(purchase)),
        "purchase_revenue": f(purchase),
        "refund_value_in_usd": f(usd(refund)),
        "refund_value": f(refund),
        "shipping_value_in_usd": f(usd(shipping)),
        "shipping_value": f(shipping),
        "tax_value_in_usd": f(usd(tax)),
        "tax_value": f(tax),
        "unique_items": if items.is_empty() { Bson::Null } else { Bson::Int64(unique.len() as i64) },
        "transaction_id": o(p.text("transaction_id")),
    })
}

fn publisher(name: &str, p: &Params, currency: &str, cfg: &AnalyticsConfig) -> Bson {
    if name != "ad_impression" {
        return Bson::Null;
    }
    Bson::Document(doc! {
        "ad_revenue_in_usd": f(p.num("value").and_then(|v| cfg.to_usd(v, currency))),
        "ad_format": o(p.text("ad_format")),
        "ad_source_name": o(p.text("ad_source").or_else(|| p.text("ad_source_name"))),
        "ad_unit_id": o(p.text("ad_unit_name").or_else(|| p.text("ad_unit_id"))),
    })
}

fn hostname_of(url: &str) -> Option<String> {
    let rest = url.split_once("://")?.1;
    let host = rest.split(['/', '?', '#']).next()?.rsplit('@').next()?.split(':').next()?;
    Some(host.to_ascii_lowercase()).filter(|h| !h.is_empty())
}

// ── Rows ────────────────────────────────────────────────────────────────────────────────────────

pub struct Built {
    /// (event_date, row)
    pub rows: Vec<(String, Document)>,
    /// The profile after this upload.
    pub profile: Profile,
    /// event_date of every row (one user-table snapshot per date).
    pub dates: BTreeSet<String>,
}

#[allow(clippy::too_many_arguments)]
pub fn build(
    cfg: &AnalyticsConfig,
    context: &ClientContext,
    request: &UploadEventsRequest,
    indices: &[usize],
    geo: &Geo,
    before: &Profile,
    received_micros: i64,
    cookieless: bool,
) -> Built {
    let mut profile = before.clone();
    let platform = platform(context);
    let web = platform == "WEB";
    let stream_id = format!("{}:{}", context.app_id, platform.to_ascii_lowercase());
    let device = device_record(request, context);
    let app_info = app_info_record(request, context).map_or(Bson::Null, Bson::Document);
    let privacy = privacy_record(request);
    let geo_doc = geo_document(geo);
    let user_properties = if cookieless { Vec::new() } else { user_properties_array(&request.user_properties) };
    let user_id = if cookieless { Bson::Null } else { s(&request.user_id) };
    let pseudo = if cookieless {
        Bson::Null
    } else if request.user_pseudo_id.is_empty() {
        s(&context.installation_id)
    } else {
        Bson::String(request.user_pseudo_id.clone())
    };
    let offset = if request.upload_timestamp_micros > 0 { Bson::Int64(received_micros - request.upload_timestamp_micros) } else { Bson::Null };
    let device_host = request.device.as_ref().map(|d| d.hostname.clone()).unwrap_or_default();
    if profile.first_touch_micros.is_none() && request.user_first_touch_timestamp_micros > 0 {
        profile.first_touch_micros = Some(request.user_first_touch_timestamp_micros);
    }

    let mut rows = Vec::with_capacity(request.events.len());
    let mut dates = BTreeSet::new();
    for (event, &index) in request.events.iter().zip(indices) {
        let ts = event.timestamp_micros;
        let date = tables::event_date(ts, cfg.timezone);
        let mut params = event.params.clone();
        if request.debug && !params.iter().any(|p| p.key == "debug_mode") {
            params.push(super::param("debug_mode", Value::IntValue(1)));
        }
        let p = Params::new(&params);
        let name = event.name.as_str();

        // Profile changes that already apply to this row.
        if matches!(name, "first_open" | "first_visit") && profile.first_touch_micros.is_none() {
            profile.first_touch_micros = Some(ts);
        }
        let campaign = Campaign::from_params(&p);
        if let Some(c) = campaign.as_ref().filter(|c| !c.is_direct()) {
            if profile.first_source.is_none() {
                profile.first_source = Some(c.clone());
            }
            if ts >= profile.last_click_micros {
                profile.last_click = Some(c.clone());
                profile.last_click_micros = ts;
            }
        }
        if name == "session_start" {
            profile.sessions += 1;
        }
        let engaged = p.int("session_engaged") == Some(1);
        if engaged {
            if let Some(session) = p.int("ga_session_id") {
                if profile.last_engaged_session != Some(session) {
                    profile.engaged_sessions += 1;
                    profile.last_engaged_session = Some(session);
                }
            }
        }
        let engagement = p.int("engagement_time_msec").unwrap_or(0).max(0);
        profile.engagement_ms += engagement;
        let currency = p.text("currency").map(|c| c.to_ascii_uppercase()).unwrap_or_else(|| cfg.currency.clone());
        let value = p.num("value");
        if matches!(name, "purchase" | "in_app_purchase") {
            profile.purchases += 1;
            if let Some(v) = value {
                profile.ltv_revenue += cfg.to_property_currency(v, &currency).unwrap_or(0.0);
                profile.ltv_revenue_usd += cfg.to_usd(v, &currency).unwrap_or(0.0);
            }
            if profile.first_purchase_date.as_ref().is_none_or(|d| date < *d) {
                profile.first_purchase_date = Some(date.clone());
            }
        }
        if let Some(screen) = p.text("DM_screen") {
            profile.last_screen = Some(screen);
        }
        profile.last_active_micros = profile.last_active_micros.max(ts);

        let items: Vec<ItemRow> = event.items.iter().map(|item| item_row(item, &p, name, &currency, cfg)).collect();
        let ecommerce = ecommerce(name, &p, &items, &currency, cfg);
        let publisher = publisher(name, &p, &currency, cfg);
        let last_click_campaign = profile.last_click.as_ref().filter(|_| profile.last_click_micros >= ts - LAST_CLICK_LOOKBACK_MICROS);
        let hostname = if web {
            Some(device_host.clone()).filter(|h| !h.is_empty()).or_else(|| p.text("page_location").and_then(|u| hostname_of(&u)))
        } else {
            None
        };

        let mut row = Document::new();
        if !request.batch_id.is_empty() {
            row.insert("_id", format!("{}:{index}", request.batch_id));
        }
        row.insert("event_date", &date);
        row.insert("event_timestamp", ts);
        row.insert("event_name", name);
        row.insert("event_params", params_array(&params));
        row.insert("event_previous_timestamp", positive(event.previous_timestamp_micros));
        row.insert("event_value_in_usd", f(value.and_then(|v| cfg.to_usd(v, &currency))));
        row.insert("event_bundle_sequence_id", positive(request.bundle_sequence_id));
        row.insert("event_server_timestamp_offset", offset.clone());
        row.insert("user_id", user_id.clone());
        row.insert("user_pseudo_id", pseudo.clone());
        row.insert("privacy_info", privacy.clone());
        row.insert("user_properties", user_properties.clone());
        row.insert("user_first_touch_timestamp", if cookieless { Bson::Null } else { i(profile.first_touch_micros) });
        row.insert(
            "user_ltv",
            if cookieless { Bson::Null } else { Bson::Document(doc! { "revenue": round6(profile.ltv_revenue), "currency": &cfg.currency }) },
        );
        row.insert("device", device.clone());
        row.insert("geo", geo_doc.clone());
        row.insert("app_info", app_info.clone());
        row.insert("traffic_source", traffic_source(if cookieless { None } else { profile.first_source.as_ref() }));
        row.insert("stream_id", &stream_id);
        row.insert("platform", &platform);
        row.insert("event_dimensions", if web { Bson::Document(doc! { "hostname": o(hostname) }) } else { Bson::Null });
        row.insert("ecommerce", ecommerce);
        row.insert("items", items.into_iter().map(|i| i.document).collect::<Vec<_>>());
        row.insert("collected_traffic_source", campaign.as_ref().map_or(Bson::Null, |c| Bson::Document(c.collected())));
        row.insert("is_active_user", engagement > 0 || engaged || matches!(name, "first_open" | "first_visit"));
        row.insert("batch_event_index", index as i64);
        row.insert("batch_page_id", positive(request.page_id));
        row.insert("batch_ordering_id", positive(request.bundle_sequence_id));
        row.insert("session_traffic_source_last_click", last_click(if cookieless { None } else { last_click_campaign }));
        row.insert("publisher", publisher);
        dates.insert(date.clone());
        rows.push((date, row));
    }
    Built { rows, profile, dates }
}

/// Fields shared by pseudonymous_users_ and users_ rows (TBL-011).
pub fn user_snapshot(
    cfg: &AnalyticsConfig,
    context: &ClientContext,
    request: &UploadEventsRequest,
    profile: &Profile,
    geo: &Geo,
    date: &str,
    today: &str,
) -> Document {
    let device = device_record(request, context);
    let privacy = request.privacy_info.clone().unwrap_or_default();
    let properties: Vec<Document> = request
        .user_properties
        .iter()
        .map(|p| {
            let text = match p.value.as_ref().and_then(|v| v.value.as_ref()) {
                Some(Value::StringValue(s)) => Bson::String(s.clone()),
                Some(Value::IntValue(n)) => Bson::String(n.to_string()),
                Some(Value::DoubleValue(n)) => Bson::String(n.to_string()),
                Some(Value::FloatValue(n)) => Bson::String(n.to_string()),
                None => Bson::Null,
            };
            doc! { "key": &p.key, "value": { "string_value": text, "set_timestamp_micros": positive(p.set_timestamp_micros), "user_property_name": &p.key } }
        })
        .collect();
    let _ = cfg;
    doc! {
        "stream_id": format!("{}:{}", context.app_id, platform(context).to_ascii_lowercase()),
        "user_info": {
            "last_active_timestamp_micros": positive(profile.last_active_micros),
            "user_first_touch_timestamp_micros": i(profile.first_touch_micros),
            "first_purchase_date": o(profile.first_purchase_date.clone()),
        },
        "device": {
            "operating_system": device.get("operating_system").cloned().unwrap_or(Bson::Null),
            "category": device.get("category").cloned().unwrap_or(Bson::Null),
            "mobile_brand_name": device.get("mobile_brand_name").cloned().unwrap_or(Bson::Null),
            "mobile_model_name": device.get("mobile_model_name").cloned().unwrap_or(Bson::Null),
            "unified_screen_name": o(profile.last_screen.clone()),
        },
        "geo": { "city": o(geo.city.clone()), "country": o(geo.country.clone()), "continent": o(geo.continent.clone()), "region": o(geo.region.clone()) },
        "audiences": Vec::<Document>::new(),
        "user_properties": properties,
        "user_ltv": {
            "revenue_in_usd": round6(profile.ltv_revenue_usd),
            "sessions": profile.sessions,
            "engagement_time_millis": profile.engagement_ms,
            "purchases": profile.purchases,
            "engaged_sessions": profile.engaged_sessions,
            "session_duration_micros": profile.engagement_ms * 1000,
        },
        "privacy_info": {
            "is_limited_ad_tracking": device.get("is_limited_ad_tracking").cloned().unwrap_or(Bson::Null),
            "is_ads_personalization_allowed": s(&privacy.ads_storage),
        },
        "occurrence_date": date,
        "last_updated_date": today,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dm::analytics::{now_micros, param, value};
    use crate::pb_dm::{AnalyticsDevice, AnalyticsEvent, AnalyticsPrivacyInfo};

    fn cfg() -> AnalyticsConfig {
        let mut c = AnalyticsConfig {
            timezone: chrono_tz::Asia::Kolkata,
            timezone_problem: None,
            currency: "INR".into(),
            usd_rates: HashMap::from([("USD".into(), 1.0), ("INR".into(), 0.012)]),
            usd_rates_problems: vec![],
            trust_geo_headers: true,
            user_tables: true,
        };
        c.usd_rates.insert("EUR".into(), 1.08);
        c
    }

    fn ctx() -> ClientContext {
        ClientContext { app_id: "dmart-ready".into(), installation_id: "inst-1".into(), platform: "ios".into(), app_version: "2.1".into(), bundle_id: "com.dmart.ready".into(), ..Default::default() }
    }

    fn ev(name: &str, params: Vec<AnalyticsParam>, items: Vec<AnalyticsItem>) -> AnalyticsEvent {
        AnalyticsEvent { name: name.into(), timestamp_micros: now_micros(), params, items, ..Default::default() }
    }

    fn get<'a>(row: &'a Document, path: &str) -> &'a Bson {
        let mut current = row.get(path.split('.').next().unwrap()).unwrap();
        for part in path.split('.').skip(1) {
            current = current.as_document().unwrap().get(part).unwrap();
        }
        current
    }

    const COLUMNS: [&str; 30] = [
        "event_date", "event_timestamp", "event_name", "event_params", "event_previous_timestamp", "event_value_in_usd",
        "event_bundle_sequence_id", "event_server_timestamp_offset", "user_id", "user_pseudo_id", "privacy_info", "user_properties",
        "user_first_touch_timestamp", "user_ltv", "device", "geo", "app_info", "traffic_source", "stream_id", "platform",
        "event_dimensions", "ecommerce", "items", "collected_traffic_source", "is_active_user", "batch_event_index", "batch_page_id",
        "batch_ordering_id", "session_traffic_source_last_click", "publisher",
    ];

    // spec: TBL-002, TBL-003, TBL-010
    #[test]
    fn every_column_and_typed_values() {
        let request = UploadEventsRequest {
            batch_id: "batch-1".into(),
            bundle_sequence_id: 7,
            upload_timestamp_micros: now_micros() - 2_000_000,
            user_pseudo_id: "pseudo-1".into(),
            device: Some(AnalyticsDevice { mobile_os_hardware_model: "iPhone16,2".into(), operating_system: "iOS".into(), operating_system_version: "18.1".into(), ..Default::default() }),
            events: vec![
                ev("add_to_cart", vec![
                    param("price", Value::DoubleValue(10.5)),
                    param("qty", Value::IntValue(2)),
                    param("name", Value::StringValue("x".into())),
                ], vec![]),
                ev("screen_view", vec![], vec![]),
                ev("user_engagement", vec![param("engagement_time_msec", Value::IntValue(1500))], vec![]),
            ],
            ..Default::default()
        };
        let built = build(&cfg(), &ctx(), &request, &[0, 1, 2], &Geo::default(), &Profile::default(), now_micros(), false);
        assert_eq!(built.rows.len(), 3);
        for (_, row) in &built.rows {
            let keys: Vec<&str> = row.keys().map(String::as_str).filter(|k| *k != "_id").collect();
            assert_eq!(keys, COLUMNS.to_vec());
            for record in ["device", "privacy_info", "user_ltv", "traffic_source", "geo"] {
                assert!(row.get_document(record).is_ok(), "{record}");
            }
        }
        let (_, first) = &built.rows[0];
        assert_eq!(first.get_str("_id").unwrap(), "batch-1:0");
        let params = first.get_array("event_params").unwrap();
        let find = |k: &str| params.iter().map(|p| p.as_document().unwrap()).find(|p| p.get_str("key").unwrap() == k).unwrap().get_document("value").unwrap().clone();
        assert_eq!(find("price"), doc! { "string_value": Bson::Null, "int_value": Bson::Null, "float_value": Bson::Null, "double_value": 10.5 });
        assert_eq!(find("qty").get_i64("int_value").unwrap(), 2);
        assert_eq!(find("name").get_str("string_value").unwrap(), "x");
        assert_eq!(first.get_i64("event_bundle_sequence_id").unwrap(), 7);
        assert_eq!(built.rows[2].1.get_i64("batch_event_index").unwrap(), 2);
        assert!(first.get_i64("event_server_timestamp_offset").unwrap() >= 2_000_000);
        assert_eq!(first.get_str("platform").unwrap(), "IOS");
        assert_eq!(first.get_str("stream_id").unwrap(), "dmart-ready:ios");
        assert_eq!(get(first, "device.mobile_marketing_name").as_str(), Some("iPhone 15 Pro Max"));
        assert_eq!(get(first, "device.operating_system_version").as_str(), Some("iOS 18.1"));
        assert_eq!(get(first, "device.category").as_str(), Some("mobile"));
        assert_eq!(get(first, "app_info.DM_app_id").as_str(), Some("dmart-ready"));
        assert_eq!(*get(first, "geo.country"), Bson::Null);
        assert_eq!(first.get_bool("is_active_user").unwrap(), false);
        assert_eq!(built.rows[2].1.get_bool("is_active_user").unwrap(), true);
        assert_eq!(*first.get("ecommerce").unwrap() != Bson::Null, true); // add_to_cart is an ecommerce event
        assert_eq!(*built.rows[1].1.get("ecommerce").unwrap(), Bson::Null);
        assert_eq!(get(first, "traffic_source.source").as_str(), Some("(direct)"));
        assert_eq!(get(first, "session_traffic_source_last_click.cross_channel_campaign.default_channel_group").as_str(), Some("Direct"));
    }

    // spec: TBL-008, TBL-005
    #[test]
    fn purchase_ecommerce_items_and_ltv() {
        let item = AnalyticsItem { params: vec![param("item_id", Value::StringValue("A".into())), param("price", Value::IntValue(100)), param("quantity", Value::IntValue(3)), param("shelf", Value::StringValue("top".into())), param("index", Value::IntValue(4))] };
        let purchase = |v: i64| ev("purchase", vec![param("value", Value::IntValue(v)), param("currency", Value::StringValue("INR".into())), param("tax", Value::IntValue(10)), param("transaction_id", Value::StringValue("T1".into()))], vec![item.clone()]);
        let request = UploadEventsRequest { events: vec![purchase(300), purchase(50)], ..Default::default() };
        let built = build(&cfg(), &ctx(), &request, &[0, 1], &Geo::default(), &Profile::default(), now_micros(), false);
        let row = &built.rows[0].1;
        assert_eq!(get(row, "ecommerce.purchase_revenue").as_f64(), Some(300.0));
        assert_eq!(get(row, "ecommerce.purchase_revenue_in_usd").as_f64(), Some(3.6));
        assert_eq!(get(row, "ecommerce.tax_value").as_f64(), Some(10.0));
        assert_eq!(get(row, "ecommerce.total_item_quantity").as_i64(), Some(3));
        assert_eq!(get(row, "ecommerce.unique_items").as_i64(), Some(1));
        assert_eq!(get(row, "ecommerce.transaction_id").as_str(), Some("T1"));
        assert_eq!(row.get_f64("event_value_in_usd").unwrap(), 3.6);
        let items = row.get_array("items").unwrap();
        let first = items[0].as_document().unwrap();
        assert_eq!(first.get_f64("item_revenue").unwrap(), 300.0);
        assert_eq!(first.get_array("item_params").unwrap().len(), 1);
        assert_eq!(first.get_str("item_list_index").unwrap(), "4");
        assert_eq!(get(row, "user_ltv.revenue").as_f64(), Some(300.0));
        assert_eq!(get(&built.rows[1].1, "user_ltv.revenue").as_f64(), Some(350.0));
        assert_eq!(get(&built.rows[1].1, "user_ltv.currency").as_str(), Some("INR"));
        assert_eq!(built.profile.purchases, 2);
    }

    // spec: TBL-007, TBL-009
    #[test]
    fn traffic_sources_and_publisher() {
        let campaign = ev("campaign_details", vec![
            param("source", Value::StringValue("google".into())),
            param("medium", Value::StringValue("cpc".into())),
            param("campaign", Value::StringValue("diwali".into())),
        ], vec![]);
        let later = ev("screen_view", vec![], vec![]);
        let ad = ev("ad_impression", vec![param("value", Value::DoubleValue(0.5)), param("currency", Value::StringValue("USD".into())), param("ad_format", Value::StringValue("banner".into()))], vec![]);
        let request = UploadEventsRequest { events: vec![campaign, later, ad], ..Default::default() };
        let built = build(&cfg(), &ctx(), &request, &[0, 1, 2], &Geo::default(), &Profile::default(), now_micros(), false);
        let (c, l, a) = (&built.rows[0].1, &built.rows[1].1, &built.rows[2].1);
        assert_eq!(get(c, "collected_traffic_source.manual_source").as_str(), Some("google"));
        assert_eq!(*l.get("collected_traffic_source").unwrap(), Bson::Null);
        assert_eq!(get(l, "traffic_source.name").as_str(), Some("diwali"));
        assert_eq!(get(l, "session_traffic_source_last_click.cross_channel_campaign.default_channel_group").as_str(), Some("Paid Search"));
        assert_eq!(get(a, "publisher.ad_revenue_in_usd").as_f64(), Some(0.5));
        assert_eq!(get(a, "publisher.ad_format").as_str(), Some("banner"));
        assert_eq!(*l.get("publisher").unwrap(), Bson::Null);
    }

    // spec: TBL-005, TBL-006
    #[test]
    fn cookieless_web_and_geo() {
        let request = UploadEventsRequest {
            user_id: "u1".into(),
            user_pseudo_id: "p1".into(),
            privacy_info: Some(AnalyticsPrivacyInfo { analytics_storage: "No".into(), ads_storage: "No".into(), ..Default::default() }),
            user_properties: vec![AnalyticsUserProperty { key: "city".into(), value: value(Value::StringValue("x".into())), set_timestamp_micros: 1 }],
            events: vec![ev("page_view", vec![param("page_location", Value::StringValue("https://shop.dmart.in/cart?x=1".into()))], vec![])],
            ..Default::default()
        };
        let web = ClientContext { platform: "web".into(), ..ctx() };
        let headers = GeoHeaders { country: Some("IN".into()), city: Some("Ahmedabad".into()), ..Default::default() };
        let built = build(&cfg(), &web, &request, &[0], &geo(&headers), &Profile::default(), now_micros(), true);
        let row = &built.rows[0].1;
        assert_eq!(*row.get("user_pseudo_id").unwrap(), Bson::Null);
        assert_eq!(*row.get("user_id").unwrap(), Bson::Null);
        assert!(row.get_array("user_properties").unwrap().is_empty());
        assert_eq!(get(row, "privacy_info.ads_storage").as_str(), Some("No"));
        assert_eq!(*row.get("app_info").unwrap(), Bson::Null);
        assert_eq!(get(row, "event_dimensions.hostname").as_str(), Some("shop.dmart.in"));
        assert_eq!(get(row, "geo.country").as_str(), Some("India"));
        assert_eq!(get(row, "geo.continent").as_str(), Some("Asia"));
        assert_eq!(get(row, "geo.sub_continent").as_str(), Some("Southern Asia"));
        assert_eq!(get(row, "geo.city").as_str(), Some("Ahmedabad"));
        assert_eq!(row.get_str("platform").unwrap(), "WEB");
    }

    // spec: TBL-011
    #[test]
    fn user_snapshot_fields() {
        let profile = Profile { sessions: 2, engagement_ms: 1500, last_screen: Some("Home".into()), last_active_micros: 5, ..Default::default() };
        let request = UploadEventsRequest { user_properties: vec![AnalyticsUserProperty { key: "city".into(), value: value(Value::IntValue(7)), set_timestamp_micros: 9 }], ..Default::default() };
        let d = user_snapshot(&cfg(), &ctx(), &request, &profile, &Geo::default(), "20261109", "20261109");
        assert_eq!(get(&d, "user_ltv.sessions").as_i64(), Some(2));
        assert_eq!(get(&d, "user_ltv.session_duration_micros").as_i64(), Some(1_500_000));
        assert_eq!(get(&d, "device.unified_screen_name").as_str(), Some("Home"));
        let prop = d.get_array("user_properties").unwrap()[0].as_document().unwrap().get_document("value").unwrap().clone();
        assert_eq!(prop.get_str("string_value").unwrap(), "7");
        assert_eq!(d.get_str("occurrence_date").unwrap(), "20261109");
    }
}
