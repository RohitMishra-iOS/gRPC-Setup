//! Fixed lookup tables and rules of the GA4 export (dm-specs TBL-006, TBL-007, TBL-008).

/// Reserved prefixes for event, parameter and user property names (ANA-001).
/// `DM_` holds the SDKs' own parameters (DM_screen, DM_event_origin …, dm-specs ANA-015).
pub const RESERVED_PREFIXES: &[&str] = &["firebase_", "google_", "ga_", "dm_", "DM_", "apple_"];

/// The SDKs' parameters that GA4 / Firebase name `firebase_*` are named `DM_*` here (ANA-015).
/// Uploads from SDK versions that still send the `firebase_*` names are renamed on arrival, and
/// rows stored before the rename are migrated once at start-up (rename.rs).
pub const RENAMED_PARAMS: &[(&str, &str)] = &[
    ("firebase_screen", "DM_screen"),
    ("firebase_screen_class", "DM_screen_class"),
    ("firebase_screen_id", "DM_screen_id"),
    ("firebase_previous_screen", "DM_previous_screen"),
    ("firebase_previous_class", "DM_previous_class"),
    ("firebase_previous_id", "DM_previous_id"),
    ("firebase_event_origin", "DM_event_origin"),
];

/// The current name of a parameter (`firebase_screen` → `DM_screen`), or None if unchanged.
pub fn renamed_param(key: &str) -> Option<&'static str> {
    RENAMED_PARAMS.iter().find(|(old, _)| *old == key).map(|(_, new)| *new)
}

/// Parameters the SDKs add themselves (ANA-011, TBL-010): exempt from the reserved-prefix rule and
/// from the 25-parameter limit.
pub const AUTOMATIC_PARAMS: &[&str] = &[
    "ga_session_id",
    "ga_session_number",
    "engagement_time_msec",
    "session_engaged",
    "DM_screen",
    "DM_screen_class",
    "DM_screen_id",
    "DM_previous_screen",
    "DM_previous_class",
    "DM_previous_id",
    "DM_event_origin",
    "entrances",
    "debug_mode",
];

/// Events the SDKs log on their own (DM_event_origin = auto for old uploads, TBL-004).
pub const AUTOMATIC_EVENTS: &[&str] = &[
    "first_open", "first_visit", "session_start", "screen_view", "page_view", "user_engagement", "app_update",
    "os_update", "app_exception", "app_remove", "notification_receive", "notification_open", "notification_foreground",
    "notification_dismiss",
];

/// GA4 ecommerce events: these rows get an `ecommerce` record (TBL-008).
pub const ECOMMERCE_EVENTS: &[&str] = &[
    "purchase", "refund", "in_app_purchase", "add_to_cart", "remove_from_cart", "view_item", "view_item_list",
    "select_item", "begin_checkout", "add_payment_info", "add_shipping_info", "view_cart", "add_to_wishlist",
    "view_promotion", "select_promotion",
];

pub fn is_reserved(name: &str) -> bool {
    RESERVED_PREFIXES.iter().any(|p| name.starts_with(p))
}

/// Marketing name of an Apple hardware model (`iPhone16,2` → `iPhone 15 Pro Max`).
pub fn apple_marketing_name(model: &str) -> Option<&'static str> {
    Some(match model {
        "i386" | "x86_64" | "arm64" => "Simulator",
        "iPhone10,1" | "iPhone10,4" => "iPhone 8",
        "iPhone10,2" | "iPhone10,5" => "iPhone 8 Plus",
        "iPhone10,3" | "iPhone10,6" => "iPhone X",
        "iPhone11,2" => "iPhone XS",
        "iPhone11,4" | "iPhone11,6" => "iPhone XS Max",
        "iPhone11,8" => "iPhone XR",
        "iPhone12,1" => "iPhone 11",
        "iPhone12,3" => "iPhone 11 Pro",
        "iPhone12,5" => "iPhone 11 Pro Max",
        "iPhone12,8" => "iPhone SE (2nd generation)",
        "iPhone13,1" => "iPhone 12 mini",
        "iPhone13,2" => "iPhone 12",
        "iPhone13,3" => "iPhone 12 Pro",
        "iPhone13,4" => "iPhone 12 Pro Max",
        "iPhone14,4" => "iPhone 13 mini",
        "iPhone14,5" => "iPhone 13",
        "iPhone14,2" => "iPhone 13 Pro",
        "iPhone14,3" => "iPhone 13 Pro Max",
        "iPhone14,6" => "iPhone SE (3rd generation)",
        "iPhone14,7" => "iPhone 14",
        "iPhone14,8" => "iPhone 14 Plus",
        "iPhone15,2" => "iPhone 14 Pro",
        "iPhone15,3" => "iPhone 14 Pro Max",
        "iPhone15,4" => "iPhone 15",
        "iPhone15,5" => "iPhone 15 Plus",
        "iPhone16,1" => "iPhone 15 Pro",
        "iPhone16,2" => "iPhone 15 Pro Max",
        "iPhone17,1" => "iPhone 16 Pro",
        "iPhone17,2" => "iPhone 16 Pro Max",
        "iPhone17,3" => "iPhone 16",
        "iPhone17,4" => "iPhone 16 Plus",
        "iPhone17,5" => "iPhone 16e",
        "iPhone18,1" => "iPhone 17 Pro",
        "iPhone18,2" => "iPhone 17 Pro Max",
        "iPhone18,3" => "iPhone 17",
        "iPhone18,4" => "iPhone Air",
        "iPad12,1" | "iPad12,2" => "iPad (9th generation)",
        "iPad13,18" | "iPad13,19" => "iPad (10th generation)",
        "iPad15,7" | "iPad15,8" => "iPad (A16)",
        "iPad14,1" | "iPad14,2" => "iPad mini (6th generation)",
        "iPad16,1" | "iPad16,2" => "iPad mini (A17 Pro)",
        "iPad13,1" | "iPad13,2" => "iPad Air (4th generation)",
        "iPad13,16" | "iPad13,17" => "iPad Air (5th generation)",
        "iPad14,8" | "iPad14,9" => "iPad Air 11-inch (M2)",
        "iPad14,10" | "iPad14,11" => "iPad Air 13-inch (M2)",
        "iPad15,3" | "iPad15,4" => "iPad Air 11-inch (M3)",
        "iPad15,5" | "iPad15,6" => "iPad Air 13-inch (M3)",
        "iPad13,4" | "iPad13,5" | "iPad13,6" | "iPad13,7" => "iPad Pro 11-inch (3rd generation)",
        "iPad13,8" | "iPad13,9" | "iPad13,10" | "iPad13,11" => "iPad Pro 12.9-inch (5th generation)",
        "iPad14,3" | "iPad14,4" => "iPad Pro 11-inch (4th generation)",
        "iPad14,5" | "iPad14,6" => "iPad Pro 12.9-inch (6th generation)",
        "iPad16,3" | "iPad16,4" => "iPad Pro 11-inch (M4)",
        "iPad16,5" | "iPad16,6" => "iPad Pro 13-inch (M4)",
        _ => return None,
    })
}

const SEARCH_ENGINES: &[&str] = &["google", "bing", "yahoo", "baidu", "duckduckgo", "yandex", "ecosia", "naver", "ask", "aol"];
const SOCIAL_SITES: &[&str] = &[
    "facebook", "fb", "instagram", "ig", "twitter", "x", "t.co", "linkedin", "lnkd.in", "pinterest", "reddit", "youtube",
    "tiktok", "snapchat", "whatsapp", "quora", "telegram", "threads", "sharechat", "moj",
];

fn is_one_of(source: &str, list: &[&str]) -> bool {
    let s = source.to_ascii_lowercase();
    list.iter().any(|name| s == *name || s.starts_with(&format!("{name}.")) || s.contains(&format!(".{name}.")) || s.ends_with(&format!(".{name}")))
}

/// GA4's default channel group for a source / medium (simplified rules, TBL-007).
pub fn default_channel_group(source: &str, medium: &str, campaign: &str, has_click_id: bool) -> &'static str {
    let medium = medium.to_ascii_lowercase();
    let campaign = campaign.to_ascii_lowercase();
    let paid = has_click_id || medium.contains("cp") || medium == "ppc" || medium == "retargeting" || medium.starts_with("paid");
    let search = is_one_of(source, SEARCH_ENGINES);
    let social = is_one_of(source, SOCIAL_SITES) || matches!(medium.as_str(), "social" | "social-network" | "social-media" | "sm" | "social network" | "social media");
    if (source == "(direct)" || source.is_empty()) && matches!(medium.as_str(), "(none)" | "(not set)" | "") {
        return "Direct";
    }
    if matches!(medium.as_str(), "display" | "banner" | "expandable" | "interstitial" | "cpm") {
        return "Display";
    }
    if paid && search {
        return "Paid Search";
    }
    if paid && social {
        return "Paid Social";
    }
    if paid {
        return "Paid Other";
    }
    if medium == "organic" || search {
        return "Organic Search";
    }
    if social {
        return "Organic Social";
    }
    if matches!(medium.as_str(), "email" | "e-mail" | "e_mail" | "e mail") || source.eq_ignore_ascii_case("email") {
        return "Email";
    }
    if medium == "affiliate" {
        return "Affiliates";
    }
    if medium == "audio" {
        return "Audio";
    }
    if medium == "sms" || source.eq_ignore_ascii_case("sms") {
        return "SMS";
    }
    if medium.ends_with("push") || medium.contains("mobile") || medium.contains("notification") || campaign.contains("push") {
        return "Mobile Push Notifications";
    }
    if matches!(medium.as_str(), "referral" | "app" | "link") {
        return "Referral";
    }
    "Unassigned"
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: TBL-007
    #[test]
    fn channel_groups() {
        assert_eq!(default_channel_group("(direct)", "(none)", "", false), "Direct");
        assert_eq!(default_channel_group("google", "cpc", "diwali", false), "Paid Search");
        assert_eq!(default_channel_group("google", "organic", "", false), "Organic Search");
        assert_eq!(default_channel_group("instagram", "social", "", false), "Organic Social");
        assert_eq!(default_channel_group("facebook", "paid_social", "", false), "Paid Social");
        assert_eq!(default_channel_group("newsletter", "email", "", false), "Email");
        assert_eq!(default_channel_group("partner", "referral", "", false), "Referral");
        assert_eq!(default_channel_group("dmart", "push", "", false), "Mobile Push Notifications");
        assert_eq!(default_channel_group("x", "y", "", false), "Organic Social"); // "x" is X (Twitter)
        assert_eq!(default_channel_group("other", "unknown", "", false), "Unassigned");
    }

    // spec: TBL-006
    #[test]
    fn apple_models_and_countries() {
        assert_eq!(apple_marketing_name("iPhone16,2"), Some("iPhone 15 Pro Max"));
        assert_eq!(apple_marketing_name("Pixel 8"), None);
        assert_eq!(super::super::countries::country("in"), Some(("India", "Southern Asia", "Asia")));
        assert_eq!(super::super::countries::country("US").map(|c| c.2), Some("Americas"));
        assert_eq!(super::super::countries::continent_from_code("AS"), Some("Asia"));
    }
}
