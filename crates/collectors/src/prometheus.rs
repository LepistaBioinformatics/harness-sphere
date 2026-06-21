//! Prometheus scrape collector (Optional) — pulls a Prometheus text exposition over HTTP
//! and converts it to canonical signals.
//!
//! Built for OpenClaw's authenticated `GET /api/diagnostics/prometheus` endpoint, but works
//! against any `/metrics`-style endpoint. No HTTP crate: a minimal HTTP/1.1 `GET` over a raw
//! `TcpStream` with `Connection: close` (the OpenClaw gateway is keep-alive, so an explicit
//! close is required for read-to-EOF to terminate). The whole connect+read is bounded by a
//! timeout; a missing/erroring endpoint degrades the source (Optional) rather than failing.
//!
//! Mapping (curated): the GenAI metrics — the §3.e differentiator — follow the OpenTelemetry
//! GenAI semantic conventions (`gen_ai.client.token.usage`, `gen_ai.client.operation.duration`).
//! Everything else is renamed off the raw `openclaw_` prefix into a single consistent
//! `harnesssphere.openclaw.*` namespace (labels → attributes, declared `# TYPE` → `MetricKind`).
//! Histogram families are reassembled into a single `Signal::Histogram` per series.

use async_trait::async_trait;
use harnesssphere_domain::{
    AttrValue, Attributes, CollectError, Criticality, HistogramPoint, Layer, Metric, MetricKind,
    ProbeResult, Signal, SignalSink, SignalSource, SourceDescriptor,
};
use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Bound on a single scrape (connect + request + read-to-EOF).
const SCRAPE_TIMEOUT: Duration = Duration::from_secs(5);

pub struct PrometheusCollector {
    descriptor: SourceDescriptor,
    /// Parsed scrape target, or the reason it is unusable (Optional → degrades, never fatal).
    url: Result<ScrapeUrl, String>,
    /// Bearer token for the `Authorization` header. Resolved from a file/env by the caller —
    /// never read from inline config.
    token: Option<String>,
    /// `harness.name` attribute stamped on every emitted signal (e.g. "openclaw").
    harness_name: String,
}

impl PrometheusCollector {
    /// `url` is an `http://host:port/path` exposition endpoint; `token`, when present, is sent
    /// as `Authorization: Bearer <token>`.
    pub fn new(
        url: impl Into<String>,
        token: Option<String>,
        harness_name: impl Into<String>,
        interval: Duration,
    ) -> Self {
        PrometheusCollector {
            descriptor: SourceDescriptor {
                name: "prometheus",
                layer: Layer::Gateway,
                criticality: Criticality::Optional,
                default_interval: interval,
            },
            url: parse_http_url(&url.into()),
            token,
            harness_name: harness_name.into(),
        }
    }
}

#[async_trait]
impl SignalSource for PrometheusCollector {
    fn descriptor(&self) -> &SourceDescriptor {
        &self.descriptor
    }

    async fn probe(&mut self) -> ProbeResult {
        match &self.url {
            Ok(_) => ProbeResult::Ready,
            Err(e) => ProbeResult::Unavailable(e.clone()),
        }
    }

    async fn collect(&mut self, sink: &dyn SignalSink) -> Result<(), CollectError> {
        let url = self
            .url
            .as_ref()
            .map_err(|e| CollectError::Unavailable(e.clone()))?;
        let body = http_get(url, self.token.as_deref(), SCRAPE_TIMEOUT)
            .await
            .map_err(CollectError::Unavailable)?;
        let parsed = parse_exposition(&body);
        for sig in to_signals(&parsed, &url.authority, &self.harness_name) {
            sink.emit(sig);
        }
        Ok(())
    }
}

// --------------------------------------------------------------------------------------------
// HTTP
// --------------------------------------------------------------------------------------------

struct ScrapeUrl {
    host: String,
    port: u16,
    /// Request target (path + optional query), e.g. `/api/diagnostics/prometheus`.
    path: String,
    /// `host:port`, used for the `Host` header and error messages.
    authority: String,
}

fn parse_http_url(raw: &str) -> Result<ScrapeUrl, String> {
    let rest = raw.strip_prefix("http://").ok_or_else(|| {
        if raw.starts_with("https://") {
            format!("https scrape is not supported (TLS out of scope): {raw}")
        } else {
            format!("prometheus_scrape_url must start with http:// : {raw}")
        }
    })?;
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (
            h.to_owned(),
            p.parse::<u16>()
                .map_err(|_| format!("invalid port in scrape url: {authority}"))?,
        ),
        None => (authority.to_owned(), 80),
    };
    if host.is_empty() {
        return Err(format!("empty host in scrape url: {raw}"));
    }
    Ok(ScrapeUrl {
        host,
        port,
        path: path.to_owned(),
        authority: authority.to_owned(),
    })
}

/// Minimal HTTP/1.1 `GET`. Sends `Connection: close` and reads to EOF — no chunked/keep-alive
/// handling needed. The entire exchange is bounded by `budget`.
async fn http_get(url: &ScrapeUrl, token: Option<&str>, budget: Duration) -> Result<String, String> {
    let exchange = async {
        let mut stream = TcpStream::connect((url.host.as_str(), url.port))
            .await
            .map_err(|e| format!("connect {}: {e}", url.authority))?;

        let mut req = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nUser-Agent: harnesssphere\r\n\
             Accept: text/plain\r\nConnection: close\r\n",
            url.path, url.authority
        );
        if let Some(t) = token {
            req.push_str("Authorization: Bearer ");
            req.push_str(t);
            req.push_str("\r\n");
        }
        req.push_str("\r\n");

        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| format!("write request: {e}"))?;
        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .await
            .map_err(|e| format!("read response: {e}"))?;
        Ok::<Vec<u8>, String>(raw)
    };

    let raw = timeout(budget, exchange)
        .await
        .map_err(|_| format!("scrape timed out after {budget:?}"))??;

    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| "malformed HTTP response (no header terminator)".to_owned())?;
    let headers = String::from_utf8_lossy(&raw[..split]);
    let status_line = headers.lines().next().unwrap_or("");
    let code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse::<u16>().ok())
        .ok_or_else(|| format!("malformed status line: {status_line}"))?;
    if !(200..300).contains(&code) {
        return Err(format!("scrape returned HTTP {code}"));
    }
    Ok(String::from_utf8_lossy(&raw[split + 4..]).into_owned())
}

// --------------------------------------------------------------------------------------------
// Prometheus text exposition parser
// --------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FamilyType {
    Counter,
    Gauge,
    Histogram,
    Summary,
    Untyped,
}

#[derive(Debug, Clone)]
struct Sample {
    name: String,
    labels: Vec<(String, String)>,
    value: f64,
}

#[derive(Debug, Default)]
struct Parsed {
    types: HashMap<String, FamilyType>,
    samples: Vec<Sample>,
}

fn parse_exposition(text: &str) -> Parsed {
    let mut out = Parsed::default();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix('#') {
            // Only `# TYPE <name> <type>` carries data we need; `# HELP` and other comments
            // are ignored.
            if let Some(t) = rest.trim_start().strip_prefix("TYPE ") {
                let mut it = t.split_whitespace();
                if let (Some(name), Some(ty)) = (it.next(), it.next()) {
                    let ft = match ty {
                        "counter" => FamilyType::Counter,
                        "gauge" => FamilyType::Gauge,
                        "histogram" => FamilyType::Histogram,
                        "summary" => FamilyType::Summary,
                        _ => FamilyType::Untyped,
                    };
                    out.types.insert(name.to_owned(), ft);
                }
            }
            continue;
        }
        if let Some(s) = parse_sample(line) {
            out.samples.push(s);
        }
    }
    out
}

fn parse_sample(line: &str) -> Option<Sample> {
    let split = line.find(['{', ' ', '\t'])?;
    let name = line[..split].trim();
    if name.is_empty() {
        return None;
    }
    let after = &line[split..];
    let (labels, rest) = if after.starts_with('{') {
        let end = find_label_end(after)?;
        (parse_labels(&after[1..end]), &after[end + 1..])
    } else {
        (Vec::new(), after)
    };
    // `value [timestamp]` — the optional trailing timestamp is ignored.
    let value = parse_value(rest.split_whitespace().next()?)?;
    Some(Sample {
        name: name.to_owned(),
        labels,
        value,
    })
}

/// Index of the `}` closing the label set in `s` (which starts with `{`), respecting quoted
/// values. The braces/quotes/backslash are ASCII, so byte indices are char boundaries.
fn find_label_end(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    let mut i = 1;
    let (mut in_quote, mut escaped) = (false, false);
    while i < b.len() {
        let c = b[i];
        if in_quote {
            if escaped {
                escaped = false;
            } else if c == b'\\' {
                escaped = true;
            } else if c == b'"' {
                in_quote = false;
            }
        } else if c == b'"' {
            in_quote = true;
        } else if c == b'}' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Parses `k1="v1",k2="v2"` (the content between the braces), handling `\\`, `\"`, `\n`
/// escapes and commas inside quoted values.
fn parse_labels(s: &str) -> Vec<(String, String)> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        while i < b.len() && matches!(b[i], b',' | b' ' | b'\t') {
            i += 1;
        }
        if i >= b.len() {
            break;
        }
        let key_start = i;
        while i < b.len() && b[i] != b'=' {
            i += 1;
        }
        if i >= b.len() {
            break;
        }
        let key = s[key_start..i].trim().to_owned();
        i += 1; // '='
        if i >= b.len() || b[i] != b'"' {
            break;
        }
        i += 1; // opening quote
        let mut val: Vec<u8> = Vec::new();
        while i < b.len() {
            let c = b[i];
            if c == b'\\' && i + 1 < b.len() {
                match b[i + 1] {
                    b'n' => val.push(b'\n'),
                    b'"' => val.push(b'"'),
                    b'\\' => val.push(b'\\'),
                    other => {
                        val.push(b'\\');
                        val.push(other);
                    }
                }
                i += 2;
                continue;
            }
            if c == b'"' {
                i += 1;
                break;
            }
            val.push(c);
            i += 1;
        }
        if !key.is_empty() {
            out.push((key, String::from_utf8_lossy(&val).into_owned()));
        }
    }
    out
}

fn parse_value(tok: &str) -> Option<f64> {
    match tok {
        "+Inf" | "Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        "NaN" | "Nan" => Some(f64::NAN),
        _ => tok.parse::<f64>().ok(),
    }
}

// --------------------------------------------------------------------------------------------
// Aggregation + curated mapping
// --------------------------------------------------------------------------------------------

enum Role {
    Bucket,
    Sum,
    Count,
}

/// If `name` is a component (`_bucket`/`_sum`/`_count`) of a declared histogram family, returns
/// `(base, role)`.
fn hist_component(name: &str, types: &HashMap<String, FamilyType>) -> Option<(String, Role)> {
    for (suffix, role) in [
        ("_bucket", Role::Bucket),
        ("_sum", Role::Sum),
        ("_count", Role::Count),
    ] {
        if let Some(base) = name.strip_suffix(suffix)
            && types.get(base) == Some(&FamilyType::Histogram)
        {
            return Some((base.to_owned(), role));
        }
    }
    None
}

struct HistAcc {
    /// Series labels with `le` removed.
    labels: Vec<(String, String)>,
    /// `(le, cumulative_count)` pairs from the `_bucket` lines.
    buckets: Vec<(f64, f64)>,
    sum: Option<f64>,
    count: Option<f64>,
}

/// Stable key identifying a histogram series (labels minus `le`).
fn series_key(labels: &[(String, String)]) -> String {
    let mut kept: Vec<&(String, String)> = labels.iter().filter(|(k, _)| k != "le").collect();
    kept.sort_by(|a, b| a.0.cmp(&b.0));
    kept.iter()
        .map(|(k, v)| format!("{k}\u{1}{v}"))
        .collect::<Vec<_>>()
        .join("\u{2}")
}

fn to_signals(parsed: &Parsed, server: &str, harness: &str) -> Vec<Signal> {
    let mut out = Vec::new();
    let mut hist: HashMap<(String, String), HistAcc> = HashMap::new();

    for s in &parsed.samples {
        if let Some((base, role)) = hist_component(&s.name, &parsed.types) {
            let acc = hist
                .entry((base, series_key(&s.labels)))
                .or_insert_with(|| HistAcc {
                    labels: s
                        .labels
                        .iter()
                        .filter(|(k, _)| k != "le")
                        .cloned()
                        .collect(),
                    buckets: Vec::new(),
                    sum: None,
                    count: None,
                });
            match role {
                Role::Bucket => {
                    if let Some(le) = s
                        .labels
                        .iter()
                        .find(|(k, _)| k == "le")
                        .and_then(|(_, v)| parse_value(v))
                    {
                        acc.buckets.push((le, s.value));
                    }
                }
                Role::Sum => acc.sum = Some(s.value),
                Role::Count => acc.count = Some(s.value),
            }
            continue;
        }

        // Scalar (counter/gauge/untyped, or a summary's component lines).
        let kind = match parsed.types.get(&s.name) {
            Some(FamilyType::Counter) => MetricKind::Counter,
            _ => MetricKind::Gauge,
        };
        let mapped = map_name(&s.name);
        let mut labels = s.labels.clone();
        rewrite_genai_labels(&s.name, &mut labels);
        out.push(Signal::Metric(Metric {
            name: mapped.name,
            kind,
            value: s.value,
            unit: mapped.unit.map(str::to_owned),
            attributes: build_attrs(labels, server, harness),
            timestamp: SystemTime::now(),
        }));
    }

    for ((base, _), acc) in hist {
        let mapped = map_name(&base);
        let mut labels = acc.labels.clone();
        rewrite_genai_labels(&base, &mut labels);
        let (bounds, counts, count, sum) = reassemble(&acc);
        out.push(Signal::Histogram(HistogramPoint {
            name: mapped.name,
            unit: mapped.unit.map(str::to_owned),
            count,
            sum,
            bucket_counts: counts,
            explicit_bounds: bounds,
            min: None,
            max: None,
            start_time: SystemTime::now(),
            timestamp: SystemTime::now(),
            attributes: build_attrs(labels, server, harness),
        }));
    }

    out
}

/// Cumulative Prometheus buckets → OTel explicit-bucket form: finite bounds, per-bucket deltas
/// (the `+Inf` bucket becomes the final count), plus total count/sum.
fn reassemble(acc: &HistAcc) -> (Vec<f64>, Vec<u64>, u64, f64) {
    let mut buckets = acc.buckets.clone();
    buckets.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let mut bounds = Vec::new();
    let mut counts = Vec::new();
    let mut prev = 0.0_f64;
    for (le, cum) in &buckets {
        let delta = (cum - prev).max(0.0).round() as u64;
        if le.is_finite() {
            bounds.push(*le);
        }
        counts.push(delta);
        prev = *cum;
    }
    // OTel requires bucket_counts.len() == explicit_bounds.len() + 1 (the implicit +Inf bucket).
    if counts.len() == bounds.len() {
        counts.push(0);
    }
    let count = acc
        .count
        .map(|c| c.round() as u64)
        .unwrap_or_else(|| buckets.last().map(|(_, c)| c.round() as u64).unwrap_or(0));
    (bounds, counts, count, acc.sum.unwrap_or(0.0))
}

struct Mapped {
    name: String,
    unit: Option<&'static str>,
}

/// Curated name/unit mapping. The GenAI pair follows the OTel GenAI semantic conventions; the
/// rest is namespaced under `harnesssphere.openclaw.*` with the Prometheus type/unit suffixes
/// folded into the `MetricKind`/unit.
fn map_name(base: &str) -> Mapped {
    match base {
        "openclaw_gen_ai_client_token_usage" => Mapped {
            name: "gen_ai.client.token.usage".to_owned(),
            unit: Some("{token}"),
        },
        "openclaw_model_call_duration_seconds" => Mapped {
            name: "gen_ai.client.operation.duration".to_owned(),
            unit: Some("s"),
        },
        _ => {
            let stripped = base.strip_prefix("openclaw_").unwrap_or(base);
            let core = stripped.strip_suffix("_total").unwrap_or(stripped);
            let (core, unit) = if let Some(c) = core.strip_suffix("_seconds") {
                (c, Some("s"))
            } else if let Some(c) = core.strip_suffix("_bytes") {
                (c, Some("By"))
            } else if let Some(c) = core.strip_suffix("_ratio") {
                (c, Some("1"))
            } else {
                (core, None)
            };
            Mapped {
                name: format!("harnesssphere.openclaw.{core}"),
                unit,
            }
        }
    }
}

/// Rewrites the labels of the curated GenAI metrics to semconv attribute keys; preserves any
/// other label under `openclaw.*` so no data is dropped.
fn rewrite_genai_labels(base: &str, labels: &mut [(String, String)]) {
    if base != "openclaw_gen_ai_client_token_usage" && base != "openclaw_model_call_duration_seconds"
    {
        return;
    }
    for (k, _) in labels.iter_mut() {
        *k = match k.as_str() {
            "model" => "gen_ai.request.model".to_owned(),
            "provider" => "gen_ai.provider.name".to_owned(),
            "token_type" => "gen_ai.token.type".to_owned(),
            "error_category" => "error.type".to_owned(),
            other => format!("openclaw.{other}"),
        };
    }
}

fn build_attrs(labels: Vec<(String, String)>, server: &str, harness: &str) -> Attributes {
    let mut attrs: Attributes = labels
        .into_iter()
        .map(|(k, v)| (k, AttrValue::Str(v)))
        .collect();
    attrs.push(("server.address".to_owned(), AttrValue::Str(server.to_owned())));
    attrs.push(("harness.name".to_owned(), AttrValue::Str(harness.to_owned())));
    attrs
}

// --------------------------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tokio::net::TcpListener;

    fn metric<'a>(sigs: &'a [Signal], name: &str) -> Option<&'a Metric> {
        sigs.iter().find_map(|s| match s {
            Signal::Metric(m) if m.name == name => Some(m),
            _ => None,
        })
    }

    fn histogram<'a>(sigs: &'a [Signal], name: &str) -> Option<&'a HistogramPoint> {
        sigs.iter().find_map(|s| match s {
            Signal::Histogram(h) if h.name == name => Some(h),
            _ => None,
        })
    }

    fn attr<'a>(attrs: &'a Attributes, key: &str) -> Option<&'a AttrValue> {
        attrs.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    #[test]
    fn parses_real_openclaw_telemetry_line() {
        // The exact bytes the live endpoint returns with no AI traffic yet.
        let raw = include_str!("../tests/fixtures/openclaw_prometheus.txt");
        let sigs = to_signals(&parse_exposition(raw), "127.0.0.1:18789", "openclaw");
        let m = metric(&sigs, "harnesssphere.openclaw.telemetry_exporter")
            .expect("telemetry_exporter metric");
        assert_eq!(m.kind, MetricKind::Counter);
        assert_eq!(m.value, 1.0);
        assert_eq!(
            attr(&m.attributes, "exporter"),
            Some(&AttrValue::Str("diagnostics-prometheus".to_owned()))
        );
        assert_eq!(
            attr(&m.attributes, "server.address"),
            Some(&AttrValue::Str("127.0.0.1:18789".to_owned()))
        );
        assert_eq!(
            attr(&m.attributes, "harness.name"),
            Some(&AttrValue::Str("openclaw".to_owned()))
        );
    }

    #[test]
    fn parses_escapes_inf_nan_and_timestamp() {
        let text = "# TYPE g gauge\n\
             g{path=\"/a,b\",msg=\"he said \\\"hi\\\"\"} 1.5 1700000000000\n\
             g{path=\"inf\"} +Inf\n\
             g{path=\"nan\"} NaN\n";
        let p = parse_exposition(text);
        assert_eq!(p.samples.len(), 3);
        // comma + escaped quote inside the quoted value
        assert_eq!(p.samples[0].labels[0], ("path".to_owned(), "/a,b".to_owned()));
        assert_eq!(
            p.samples[0].labels[1],
            ("msg".to_owned(), "he said \"hi\"".to_owned())
        );
        assert_eq!(p.samples[0].value, 1.5); // trailing timestamp ignored
        assert!(p.samples[1].value.is_infinite() && p.samples[1].value > 0.0);
        assert!(p.samples[2].value.is_nan());
    }

    #[test]
    fn reassembles_histogram_to_explicit_buckets() {
        let text = "# TYPE openclaw_run_duration_seconds histogram\n\
             openclaw_run_duration_seconds_bucket{outcome=\"ok\",le=\"0.1\"} 2\n\
             openclaw_run_duration_seconds_bucket{outcome=\"ok\",le=\"0.5\"} 5\n\
             openclaw_run_duration_seconds_bucket{outcome=\"ok\",le=\"+Inf\"} 6\n\
             openclaw_run_duration_seconds_sum{outcome=\"ok\"} 1.23\n\
             openclaw_run_duration_seconds_count{outcome=\"ok\"} 6\n";
        let sigs = to_signals(&parse_exposition(text), "h:1", "openclaw");
        let h = histogram(&sigs, "harnesssphere.openclaw.run_duration").expect("histogram");
        assert_eq!(h.unit.as_deref(), Some("s"));
        assert_eq!(h.explicit_bounds, vec![0.1, 0.5]);
        assert_eq!(h.bucket_counts, vec![2, 3, 1]); // cumulative 2,5,6 → deltas
        assert_eq!(h.count, 6);
        assert_eq!(h.sum, 1.23);
        assert_eq!(
            attr(&h.attributes, "outcome"),
            Some(&AttrValue::Str("ok".to_owned()))
        );
    }

    #[test]
    fn maps_genai_token_usage_to_semconv() {
        let text = "# TYPE openclaw_gen_ai_client_token_usage histogram\n\
             openclaw_gen_ai_client_token_usage_bucket{model=\"claude\",provider=\"anthropic\",token_type=\"input\",le=\"+Inf\"} 3\n\
             openclaw_gen_ai_client_token_usage_sum{model=\"claude\",provider=\"anthropic\",token_type=\"input\"} 120\n\
             openclaw_gen_ai_client_token_usage_count{model=\"claude\",provider=\"anthropic\",token_type=\"input\"} 3\n";
        let sigs = to_signals(&parse_exposition(text), "h:1", "openclaw");
        let h = histogram(&sigs, "gen_ai.client.token.usage").expect("genai histogram");
        assert_eq!(h.unit.as_deref(), Some("{token}"));
        assert_eq!(
            attr(&h.attributes, "gen_ai.request.model"),
            Some(&AttrValue::Str("claude".to_owned()))
        );
        assert_eq!(
            attr(&h.attributes, "gen_ai.provider.name"),
            Some(&AttrValue::Str("anthropic".to_owned()))
        );
        assert_eq!(
            attr(&h.attributes, "gen_ai.token.type"),
            Some(&AttrValue::Str("input".to_owned()))
        );
    }

    #[test]
    fn long_tail_counter_namespaced_and_typed() {
        let text = "# TYPE openclaw_model_tokens_total counter\n\
             openclaw_model_tokens_total{token_type=\"output\"} 42\n";
        let sigs = to_signals(&parse_exposition(text), "h:1", "openclaw");
        let m = metric(&sigs, "harnesssphere.openclaw.model_tokens").expect("counter");
        assert_eq!(m.kind, MetricKind::Counter);
        assert_eq!(m.value, 42.0);
    }

    #[test]
    fn rejects_non_http_urls() {
        assert!(parse_http_url("https://x/m").is_err());
        assert!(parse_http_url("localhost:9090/m").is_err());
        let u = parse_http_url("http://127.0.0.1:18789/api/diagnostics/prometheus").unwrap();
        assert_eq!(u.host, "127.0.0.1");
        assert_eq!(u.port, 18789);
        assert_eq!(u.path, "/api/diagnostics/prometheus");
    }

    #[tokio::test]
    async fn http_get_reads_body_and_sends_auth() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = std::sync::Arc::new(Mutex::new(String::new()));
        let seen2 = seen.clone();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 1024];
            let n = sock.read(&mut buf).await.unwrap();
            *seen2.lock().unwrap() = String::from_utf8_lossy(&buf[..n]).into_owned();
            let body = "# TYPE x gauge\nx 7\n";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
        });

        let url = parse_http_url(&format!("http://{addr}/metrics")).unwrap();
        let body = http_get(&url, Some("secret-tok"), Duration::from_secs(2))
            .await
            .unwrap();
        assert!(body.contains("x 7"));
        let req = seen.lock().unwrap().clone();
        assert!(req.contains("GET /metrics HTTP/1.1"));
        assert!(req.contains("Connection: close"));
        assert!(req.contains("Authorization: Bearer secret-tok"));
    }

    /// Canned-response server: each connection gets `resp` and is closed.
    async fn serve_once(resp: &'static str) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await;
            sock.write_all(resp.as_bytes()).await.unwrap();
        });
        addr
    }

    #[tokio::test]
    async fn http_get_errors_on_non_2xx() {
        let addr = serve_once("HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\n\r\nboom").await;
        let url = parse_http_url(&format!("http://{addr}/metrics")).unwrap();
        let err = http_get(&url, None, Duration::from_secs(2)).await.unwrap_err();
        assert!(err.contains("500"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn http_get_errors_on_malformed_response() {
        let addr = serve_once("garbage-without-header-terminator").await;
        let url = parse_http_url(&format!("http://{addr}/metrics")).unwrap();
        let err = http_get(&url, None, Duration::from_secs(2)).await.unwrap_err();
        assert!(err.contains("malformed"), "unexpected error: {err}");
    }
}
