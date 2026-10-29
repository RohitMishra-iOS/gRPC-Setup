//! Security rules for Storage and the Database — a compact cousin of Firebase Security Rules.
//!
//! Rules are a list (stored in `security_rules`, edited in the console or the admin API):
//!
//!   [
//!     {"match": "users/{uid}/{rest=**}",  "read": "auth != null && auth.uid == uid", "write": "auth.uid == uid"},
//!     {"match": "public/{file=**}",       "read": "true", "write": "auth != null && request.resource.size < 5 * 1024 * 1024"},
//!     {"match": "posts/{postId}",         "read": "true",
//!      "create": "auth != null && request.resource.data.author == auth.uid",
//!      "update": "auth.uid == resource.data.author", "delete": "auth.uid == resource.data.author"}
//!   ]
//!
//! * `match` segments: literal, `{name}` (one segment), `{name=**}` (the rest, zero or more).
//! * Operations: `read` = get + list, `write` = create + update + delete; specific keys win.
//! * A request is allowed if ANY matching rule allows it (like Firebase). No match = denied.
//! * Expressions: literals (true false null 12 1.5 'text' "text" [a, b]), variables
//!   (auth, request, resource, path variables), `.field` access, `[key]` index,
//!   `== != < <= > >= && || ! in + - * /`, and methods `size()`, `matches(regex)`,
//!   `keys()`, `hasAll(list)`, `hasAny(list)`, `hasOnly(list)`, `lower()`, `upper()`.
//!   Missing fields evaluate to null; comparing null with anything but null is false.
//!
//! Context:
//!   auth                     null, or {uid, token: {email, email_verified, sign_in_provider, …custom claims}}
//!   request.time             now, epoch ms
//!   request.method           get | list | create | update | delete
//!   request.resource         the incoming document/object: {data, size, contentType, metadata, name}
//!   resource                 the existing document/object (null when creating)
//!   request.app              {id, appcheck: true|false}

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value as Json};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Rule {
    #[serde(rename = "match")]
    pub pattern: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub get: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Get,
    List,
    Create,
    Update,
    Delete,
}

impl Op {
    pub fn name(self) -> &'static str {
        match self {
            Op::Get => "get",
            Op::List => "list",
            Op::Create => "create",
            Op::Update => "update",
            Op::Delete => "delete",
        }
    }
}

impl Rule {
    fn condition(&self, op: Op) -> Option<&str> {
        let specific = match op {
            Op::Get => &self.get,
            Op::List => &self.list,
            Op::Create => &self.create,
            Op::Update => &self.update,
            Op::Delete => &self.delete,
        };
        let general = match op {
            Op::Get | Op::List => &self.read,
            _ => &self.write,
        };
        specific.as_deref().or(general.as_deref())
    }
}

/// Default rules: signed-in users only.
pub fn default_rules() -> Vec<Rule> {
    vec![Rule {
        pattern: "{path=**}".into(),
        read: Some("auth != null".into()),
        write: Some("auth != null".into()),
        get: None,
        list: None,
        create: None,
        update: None,
        delete: None,
    }]
}

/// Path variables when `pattern` matches `path` (both '/'-separated).
pub fn match_path(pattern: &str, path: &str) -> Option<Map<String, Json>> {
    let pattern: Vec<&str> = pattern.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect();
    let path: Vec<&str> = path.trim_matches('/').split('/').filter(|s| !s.is_empty()).collect();
    let mut vars = Map::new();
    let mut i = 0;
    for (pi, segment) in pattern.iter().enumerate() {
        if let Some(inner) = segment.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
            if let Some(name) = inner.strip_suffix("=**") {
                if pi != pattern.len() - 1 {
                    return None; // ** only at the end
                }
                vars.insert(name.to_string(), Json::String(path[i.min(path.len())..].join("/")));
                return Some(vars);
            }
            let value = path.get(i)?;
            vars.insert(inner.to_string(), Json::String((*value).to_string()));
        } else if path.get(i) != Some(segment) {
            return None;
        }
        i += 1;
    }
    (i == path.len()).then_some(vars)
}

/// Ok(()) if any matching rule allows `op` on `path`; Err(reason) otherwise.
pub fn check(rules: &[Rule], path: &str, op: Op, context: &Json) -> Result<(), String> {
    let mut matched = false;
    let mut errors = Vec::new();
    for rule in rules {
        let Some(vars) = match_path(&rule.pattern, path) else { continue };
        let Some(condition) = rule.condition(op) else { continue };
        matched = true;
        let mut scope = context.as_object().cloned().unwrap_or_default();
        scope.extend(vars);
        match evaluate(condition, &Json::Object(scope)) {
            Ok(Json::Bool(true)) => return Ok(()),
            Ok(_) => {}
            Err(e) => errors.push(format!("rule '{}': {e}", rule.pattern)),
        }
    }
    Err(if !matched {
        format!("no rule allows {} on {path}", op.name())
    } else if errors.is_empty() {
        format!("{} on {path} denied by security rules", op.name())
    } else {
        errors.join("; ")
    })
}

/// Validates every expression in a rule set (used when saving rules).
pub fn validate(rules: &[Rule]) -> Result<(), String> {
    for rule in rules {
        if rule.pattern.trim().is_empty() {
            return Err("every rule needs a match pattern".into());
        }
        for (name, expr) in [("read", &rule.read), ("write", &rule.write), ("get", &rule.get), ("list", &rule.list), ("create", &rule.create), ("update", &rule.update), ("delete", &rule.delete)] {
            if let Some(expr) = expr {
                parse(expr).map_err(|e| format!("{} → {name}: {e}", rule.pattern))?;
            }
        }
    }
    Ok(())
}

// ── Expression language ──────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Num(f64),
    Str(String),
    Ident(String),
    Op(&'static str),
}

fn lex(src: &str) -> Result<Vec<Tok>, String> {
    let chars: Vec<char> = src.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    const OPS: [&str; 22] = ["==", "!=", "<=", ">=", "&&", "||", "<", ">", "!", "(", ")", "[", "]", ",", ".", "+", "-", "*", "/", "%", "?", ":"];
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c.is_ascii_digit() {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_digit() || (chars[i] == '.' && chars.get(i + 1).is_some_and(|d| d.is_ascii_digit()))) {
                i += 1;
            }
            out.push(Tok::Num(chars[start..i].iter().collect::<String>().parse().map_err(|_| "bad number")?));
        } else if c == '\'' || c == '"' {
            let quote = c;
            i += 1;
            let mut s = String::new();
            while i < chars.len() && chars[i] != quote {
                if chars[i] == '\\' && i + 1 < chars.len() {
                    i += 1;
                }
                s.push(chars[i]);
                i += 1;
            }
            if i >= chars.len() {
                return Err("unterminated string".into());
            }
            i += 1;
            out.push(Tok::Str(s));
        } else if c.is_alphabetic() || c == '_' || c == '$' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '$') {
                i += 1;
            }
            out.push(Tok::Ident(chars[start..i].iter().collect()));
        } else {
            let two: String = chars[i..(i + 2).min(chars.len())].iter().collect();
            if let Some(op) = OPS.iter().find(|o| o.len() == 2 && **o == two) {
                out.push(Tok::Op(op));
                i += 2;
            } else if let Some(op) = OPS.iter().find(|o| o.len() == 1 && o.starts_with(c)) {
                out.push(Tok::Op(op));
                i += 1;
            } else {
                return Err(format!("unexpected character '{c}'"));
            }
        }
    }
    Ok(out)
}

#[derive(Debug, Clone)]
enum Expr {
    Lit(Json),
    Var(String),
    List(Vec<Expr>),
    Field(Box<Expr>, String),
    Index(Box<Expr>, Box<Expr>),
    Call(Box<Expr>, String, Vec<Expr>),
    Not(Box<Expr>),
    Neg(Box<Expr>),
    Bin(&'static str, Box<Expr>, Box<Expr>),
    Cond(Box<Expr>, Box<Expr>, Box<Expr>),
}

struct Parser {
    toks: Vec<Tok>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }
    fn eat_op(&mut self, op: &str) -> bool {
        if matches!(self.peek(), Some(Tok::Op(o)) if *o == op) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn eat_ident(&mut self, word: &str) -> bool {
        if matches!(self.peek(), Some(Tok::Ident(w)) if w == word) {
            self.pos += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, op: &str) -> Result<(), String> {
        if self.eat_op(op) { Ok(()) } else { Err(format!("expected '{op}'")) }
    }
    fn ternary(&mut self) -> Result<Expr, String> {
        let cond = self.or()?;
        if self.eat_op("?") {
            let a = self.ternary()?;
            self.expect(":")?;
            let b = self.ternary()?;
            return Ok(Expr::Cond(Box::new(cond), Box::new(a), Box::new(b)));
        }
        Ok(cond)
    }
    fn or(&mut self) -> Result<Expr, String> {
        let mut left = self.and()?;
        while self.eat_op("||") {
            left = Expr::Bin("||", Box::new(left), Box::new(self.and()?));
        }
        Ok(left)
    }
    fn and(&mut self) -> Result<Expr, String> {
        let mut left = self.cmp()?;
        while self.eat_op("&&") {
            left = Expr::Bin("&&", Box::new(left), Box::new(self.cmp()?));
        }
        Ok(left)
    }
    fn cmp(&mut self) -> Result<Expr, String> {
        let left = self.add()?;
        for op in ["==", "!=", "<=", ">=", "<", ">"] {
            if self.eat_op(op) {
                return Ok(Expr::Bin(op, Box::new(left), Box::new(self.add()?)));
            }
        }
        if self.eat_ident("in") {
            return Ok(Expr::Bin("in", Box::new(left), Box::new(self.add()?)));
        }
        Ok(left)
    }
    fn add(&mut self) -> Result<Expr, String> {
        let mut left = self.mul()?;
        loop {
            if self.eat_op("+") {
                left = Expr::Bin("+", Box::new(left), Box::new(self.mul()?));
            } else if self.eat_op("-") {
                left = Expr::Bin("-", Box::new(left), Box::new(self.mul()?));
            } else {
                return Ok(left);
            }
        }
    }
    fn mul(&mut self) -> Result<Expr, String> {
        let mut left = self.unary()?;
        loop {
            if self.eat_op("*") {
                left = Expr::Bin("*", Box::new(left), Box::new(self.unary()?));
            } else if self.eat_op("/") {
                left = Expr::Bin("/", Box::new(left), Box::new(self.unary()?));
            } else if self.eat_op("%") {
                left = Expr::Bin("%", Box::new(left), Box::new(self.unary()?));
            } else {
                return Ok(left);
            }
        }
    }
    fn unary(&mut self) -> Result<Expr, String> {
        if self.eat_op("!") {
            return Ok(Expr::Not(Box::new(self.unary()?)));
        }
        if self.eat_op("-") {
            return Ok(Expr::Neg(Box::new(self.unary()?)));
        }
        self.postfix()
    }
    fn postfix(&mut self) -> Result<Expr, String> {
        let mut e = self.primary()?;
        loop {
            if self.eat_op(".") {
                let Some(Tok::Ident(name)) = self.peek().cloned() else { return Err("expected a name after '.'".into()) };
                self.pos += 1;
                if self.eat_op("(") {
                    let args = self.args(")")?;
                    e = Expr::Call(Box::new(e), name, args);
                } else {
                    e = Expr::Field(Box::new(e), name);
                }
            } else if self.eat_op("[") {
                let index = self.ternary()?;
                self.expect("]")?;
                e = Expr::Index(Box::new(e), Box::new(index));
            } else {
                return Ok(e);
            }
        }
    }
    fn args(&mut self, close: &str) -> Result<Vec<Expr>, String> {
        let mut args = Vec::new();
        if self.eat_op(close) {
            return Ok(args);
        }
        loop {
            args.push(self.ternary()?);
            if self.eat_op(close) {
                return Ok(args);
            }
            self.expect(",")?;
        }
    }
    fn primary(&mut self) -> Result<Expr, String> {
        match self.peek().cloned() {
            Some(Tok::Num(n)) => {
                self.pos += 1;
                Ok(Expr::Lit(json!(n)))
            }
            Some(Tok::Str(s)) => {
                self.pos += 1;
                Ok(Expr::Lit(Json::String(s)))
            }
            Some(Tok::Ident(word)) => {
                self.pos += 1;
                Ok(match word.as_str() {
                    "true" => Expr::Lit(Json::Bool(true)),
                    "false" => Expr::Lit(Json::Bool(false)),
                    "null" => Expr::Lit(Json::Null),
                    _ => Expr::Var(word),
                })
            }
            Some(Tok::Op("(")) => {
                self.pos += 1;
                let e = self.ternary()?;
                self.expect(")")?;
                Ok(e)
            }
            Some(Tok::Op("[")) => {
                self.pos += 1;
                Ok(Expr::List(self.args("]")?))
            }
            Some(t) => Err(format!("unexpected {t:?}")),
            None => Err("unexpected end of expression".into()),
        }
    }
}

fn parse(src: &str) -> Result<Expr, String> {
    let mut parser = Parser { toks: lex(src)?, pos: 0 };
    let expr = parser.ternary()?;
    if parser.pos != parser.toks.len() {
        return Err(format!("unexpected {:?}", parser.toks[parser.pos]));
    }
    Ok(expr)
}

pub fn evaluate(src: &str, scope: &Json) -> Result<Json, String> {
    eval(&parse(src)?, scope)
}

fn truthy(v: &Json) -> bool {
    matches!(v, Json::Bool(true))
}

fn num(v: &Json) -> Option<f64> {
    v.as_f64()
}

fn compare(a: &Json, b: &Json) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Json::Number(_), Json::Number(_)) => num(a)?.partial_cmp(&num(b)?),
        (Json::String(x), Json::String(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

fn equal(a: &Json, b: &Json) -> bool {
    match (a, b) {
        (Json::Number(_), Json::Number(_)) => num(a) == num(b),
        _ => a == b,
    }
}

fn eval(e: &Expr, scope: &Json) -> Result<Json, String> {
    Ok(match e {
        Expr::Lit(v) => v.clone(),
        Expr::Var(name) => scope.get(name).cloned().unwrap_or(Json::Null),
        Expr::List(items) => Json::Array(items.iter().map(|i| eval(i, scope)).collect::<Result<_, _>>()?),
        Expr::Field(target, name) => eval(target, scope)?.get(name).cloned().unwrap_or(Json::Null),
        Expr::Index(target, index) => {
            let target = eval(target, scope)?;
            match eval(index, scope)? {
                Json::String(k) => target.get(&k).cloned().unwrap_or(Json::Null),
                Json::Number(n) => n.as_u64().and_then(|i| target.get(i as usize).cloned()).unwrap_or(Json::Null),
                _ => Json::Null,
            }
        }
        Expr::Not(inner) => Json::Bool(!truthy(&eval(inner, scope)?)),
        Expr::Neg(inner) => json!(-num(&eval(inner, scope)?).ok_or("'-' needs a number")?),
        Expr::Cond(c, a, b) => {
            if truthy(&eval(c, scope)?) { eval(a, scope)? } else { eval(b, scope)? }
        }
        Expr::Bin("&&", a, b) => Json::Bool(truthy(&eval(a, scope)?) && truthy(&eval(b, scope)?)),
        Expr::Bin("||", a, b) => Json::Bool(truthy(&eval(a, scope)?) || truthy(&eval(b, scope)?)),
        Expr::Bin(op, a, b) => {
            let (a, b) = (eval(a, scope)?, eval(b, scope)?);
            match *op {
                "==" => Json::Bool(equal(&a, &b)),
                "!=" => Json::Bool(!equal(&a, &b)),
                "<" => Json::Bool(compare(&a, &b) == Some(std::cmp::Ordering::Less)),
                "<=" => Json::Bool(matches!(compare(&a, &b), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal))),
                ">" => Json::Bool(compare(&a, &b) == Some(std::cmp::Ordering::Greater)),
                ">=" => Json::Bool(matches!(compare(&a, &b), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))),
                "in" => Json::Bool(match &b {
                    Json::Array(items) => items.iter().any(|i| equal(i, &a)),
                    Json::Object(map) => a.as_str().is_some_and(|k| map.contains_key(k)),
                    _ => false,
                }),
                "+" => match (&a, &b) {
                    (Json::String(x), Json::String(y)) => Json::String(format!("{x}{y}")),
                    _ => json!(num(&a).ok_or("'+' needs numbers or strings")? + num(&b).ok_or("'+' needs numbers or strings")?),
                },
                "-" | "*" | "/" | "%" => {
                    let (x, y) = (num(&a).ok_or(format!("'{op}' needs numbers"))?, num(&b).ok_or(format!("'{op}' needs numbers"))?);
                    json!(match *op { "-" => x - y, "*" => x * y, "/" => x / y, _ => x % y })
                }
                _ => return Err(format!("unknown operator {op}")),
            }
        }
        Expr::Call(target, method, args) => {
            let target = eval(target, scope)?;
            let args: Vec<Json> = args.iter().map(|a| eval(a, scope)).collect::<Result<_, _>>()?;
            let list_arg = |i: usize| args.get(i).and_then(Json::as_array).cloned().unwrap_or_default();
            let keys = || -> Vec<Json> {
                match &target {
                    Json::Object(m) => m.keys().map(|k| Json::String(k.clone())).collect(),
                    Json::Array(items) => items.clone(),
                    _ => vec![],
                }
            };
            match method.as_str() {
                "size" => json!(match &target {
                    Json::String(s) => s.chars().count(),
                    Json::Array(a) => a.len(),
                    Json::Object(m) => m.len(),
                    _ => 0,
                }),
                "matches" => {
                    let pattern = args.first().and_then(Json::as_str).ok_or("matches() needs a regex string")?;
                    let re = regex_lite::Regex::new(&format!("^(?:{pattern})$")).map_err(|e| format!("bad regex: {e}"))?;
                    Json::Bool(target.as_str().is_some_and(|s| re.is_match(s)))
                }
                "keys" => Json::Array(keys()),
                "hasAll" => {
                    let k = keys();
                    Json::Bool(list_arg(0).iter().all(|x| k.contains(x)))
                }
                "hasAny" => {
                    let k = keys();
                    Json::Bool(list_arg(0).iter().any(|x| k.contains(x)))
                }
                "hasOnly" => {
                    let allowed = list_arg(0);
                    Json::Bool(keys().iter().all(|x| allowed.contains(x)))
                }
                "lower" => Json::String(target.as_str().unwrap_or_default().to_lowercase()),
                "upper" => Json::String(target.as_str().unwrap_or_default().to_uppercase()),
                other => return Err(format!("unknown method {other}()")),
            }
        }
    })
}

/// The `auth` value for rules.
pub fn auth_value(user: Option<&super::tokens::IdClaims>) -> Json {
    match user {
        None => Json::Null,
        Some(claims) => {
            let mut token = serde_json::to_value(claims).unwrap_or_default();
            if let Some(map) = token.as_object_mut() {
                let provider = claims.provider().to_string();
                map.insert("sign_in_provider".into(), Json::String(provider));
            }
            json!({ "uid": claims.sub, "token": token })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(uid: Option<&str>) -> Json {
        json!({
            "auth": uid.map(|u| json!({ "uid": u, "token": { "email": "a@b.com", "admin": true } })),
            "request": { "time": 1000, "method": "create", "resource": { "data": { "author": "u1", "title": "Hi" }, "size": 2048, "contentType": "image/png" } },
            "resource": { "data": { "author": "u1" } },
        })
    }

    #[test]
    fn paths() {
        assert_eq!(match_path("users/{uid}", "users/abc").unwrap()["uid"], "abc");
        assert!(match_path("users/{uid}", "users/abc/x").is_none());
        assert_eq!(match_path("users/{uid}/{rest=**}", "users/abc/a/b").unwrap()["rest"], "a/b");
        assert_eq!(match_path("{all=**}", "x/y").unwrap()["all"], "x/y");
        assert!(match_path("posts/{id}", "users/1").is_none());
    }

    #[test]
    fn expressions() {
        let c = ctx(Some("u1"));
        let t = |e: &str| evaluate(e, &c).unwrap() == json!(true);
        assert!(t("auth != null && auth.uid == 'u1'"));
        assert!(t("request.resource.data.author == auth.uid"));
        assert!(t("request.resource.size < 5 * 1024 * 1024"));
        assert!(t("request.resource.contentType.matches('image/.*')"));
        assert!(!t("request.resource.contentType.matches('video/.*')"));
        assert!(t("request.resource.data.keys().hasOnly(['author', 'title'])"));
        assert!(t("'title' in request.resource.data"));
        assert!(t("request.resource.data.title.size() <= 100"));
        assert!(t("auth.token.admin == true"));
        assert!(t("resource.data.missing == null"));
        assert!(!t("resource.data.missing > 3"));
        assert!(t("!(auth == null)"));
        assert!(t("auth.uid in ['u0', 'u1']"));
        assert!(evaluate("auth.uid ==", &c).is_err());
    }

    #[test]
    fn rule_sets() {
        let rules: Vec<Rule> = serde_json::from_value(json!([
            { "match": "users/{uid}/{rest=**}", "read": "auth.uid == uid", "write": "auth.uid == uid" },
            { "match": "posts/{id}", "read": "true", "create": "auth != null && request.resource.data.author == auth.uid" }
        ]))
        .unwrap();
        validate(&rules).unwrap();
        assert!(check(&rules, "users/u1/photo.png", Op::Get, &ctx(Some("u1"))).is_ok());
        assert!(check(&rules, "users/u2/photo.png", Op::Get, &ctx(Some("u1"))).is_err());
        assert!(check(&rules, "posts/1", Op::Get, &ctx(None)).is_ok());
        assert!(check(&rules, "posts/1", Op::Create, &ctx(Some("u1"))).is_ok());
        assert!(check(&rules, "posts/1", Op::Create, &ctx(None)).is_err());
        assert!(check(&rules, "posts/1", Op::Delete, &ctx(Some("u1"))).is_err(), "no delete rule");
        assert!(check(&rules, "other/1", Op::Get, &ctx(Some("u1"))).is_err(), "no match");
        let bad: Vec<Rule> = serde_json::from_value(json!([{ "match": "x", "read": "auth.uid ==" }])).unwrap();
        assert!(validate(&bad).is_err());
    }
}
