//! Extraction utilities for metrics, timestamps, dimensions, and source positions.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;

use bytes::Bytes;
use logsluice_core::position::SourcePosition;
use logsluice_core::simd::SimdKernelTable;

use crate::config::{NormalizerConfig, SourcePositionConfig, WhenCondition};
use crate::event::{FieldPath, LogEvent, Value, value_to_bytes, value_to_string_lossy};

/// Result of extracting a numeric metric value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricValue {
    /// Exact integer value.
    Int(i64),
    /// Truncated/coerced float value.
    Coerced(i64),
    /// Invalid type (cannot be interpreted as integer or float).
    Invalid,
}

/// Extracts a metric value from a JSON [`Value`].
#[must_use]
pub fn metric_value(value: &Value) -> MetricValue {
    match value {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                MetricValue::Int(i)
            } else if let Some(u) = n.as_u64() {
                MetricValue::Int(i64::try_from(u).unwrap_or(i64::MAX))
            } else if let Some(f) = n.as_f64() {
                MetricValue::Coerced(f.trunc() as i64)
            } else {
                MetricValue::Invalid
            }
        }
        _ => MetricValue::Invalid,
    }
}

/// Error returned when timestamp parsing fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampError;

impl std::fmt::Display for TimestampError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid or unparseable timestamp")
    }
}

impl std::error::Error for TimestampError {}

/// Parses an event timestamp into epoch milliseconds.
///
/// # Errors
/// Returns [`TimestampError`] if the value is neither a valid RFC3339 string nor a non-negative epoch number.
pub fn extract_timestamp_ms(value: &Value) -> Result<i64, TimestampError> {
    match value {
        Value::Number(n) => {
            let val = n
                .as_i64()
                .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok()))
                .ok_or(TimestampError)?;
            parse_numeric_epoch_ms(val)
        }
        Value::String(s) => parse_timestamp_str(s),
        _ => Err(TimestampError),
    }
}

fn parse_numeric_epoch_ms(val: i64) -> Result<i64, TimestampError> {
    if val < 0 {
        return Err(TimestampError);
    }
    if val < 100_000_000_000 {
        // Seconds
        Ok(val.saturating_mul(1000))
    } else if val < 100_000_000_000_000 {
        // Milliseconds
        Ok(val)
    } else if val < 100_000_000_000_000_000 {
        // Microseconds
        Ok(val / 1000)
    } else {
        // Nanoseconds
        Ok(val / 1_000_000)
    }
}

fn parse_timestamp_str(s: &str) -> Result<i64, TimestampError> {
    let s = s.trim();
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Ok(dt.timestamp_millis());
    }
    if let Ok(i) = s.parse::<i64>() {
        return parse_numeric_epoch_ms(i);
    }
    Err(TimestampError)
}

/// Extracts an OTLP `timeUnixNano` timestamp into epoch milliseconds.
#[must_use]
pub fn extract_otlp_time_nano_ms(value: &Value) -> Option<i64> {
    match value {
        Value::Number(n) => {
            let val = n
                .as_i64()
                .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok()))?;
            if val <= 0 {
                None
            } else {
                Some(val / 1_000_000)
            }
        }
        Value::String(s) => {
            let val = s.trim().parse::<i64>().ok()?;
            if val <= 0 {
                None
            } else {
                Some(val / 1_000_000)
            }
        }
        _ => None,
    }
}

/// Pre-parsed target paths for source position extraction.
#[derive(Clone, Debug)]
pub struct SourcePositionPaths {
    /// Field path for the source topic name.
    pub topic: FieldPath,
    /// Field path for the source partition number.
    pub partition: FieldPath,
    /// Field path for the source partition offset.
    pub offset: FieldPath,
}

impl SourcePositionPaths {
    /// Creates pre-parsed target paths from source position configuration.
    #[must_use]
    pub fn new(cfg: &SourcePositionConfig) -> Self {
        Self {
            topic: FieldPath::parse_or_literal(cfg.topic_field.as_str()),
            partition: FieldPath::parse_or_literal(cfg.partition_field.as_str()),
            offset: FieldPath::parse_or_literal(cfg.offset_field.as_str()),
        }
    }

    /// Extracts source position from a log event if all fields are present and valid.
    #[must_use]
    pub fn extract(&self, event: &LogEvent) -> Option<SourcePosition> {
        let topic = event
            .get(&self.topic)
            .map(|v| value_to_string_lossy(v).into_owned())?;
        let partition = event
            .get(&self.partition)
            .and_then(Value::as_i64)
            .and_then(|i| i32::try_from(i).ok())?;
        let offset = event.get(&self.offset).and_then(Value::as_i64)?;

        Some(SourcePosition {
            topic: Arc::from(topic),
            partition,
            offset,
        })
    }
}

/// Normalizer runtime executor.
#[derive(Clone, Debug)]
pub struct NormalizerExecutor {
    /// Underlying normalizer configuration.
    pub config: NormalizerConfig,
    /// Pre-parsed conditional field path, if configured.
    pub when_path: Option<FieldPath>,
}

impl NormalizerExecutor {
    /// Creates a normalizer executor with optional precompiled condition path.
    #[must_use]
    pub fn new(config: NormalizerConfig) -> Self {
        let when_path = match &config {
            NormalizerConfig::IpCidr {
                when: Some(WhenCondition { field, .. }),
                ..
            } => Some(FieldPath::parse_or_literal(field.as_str())),
            _ => None,
        };
        Self { config, when_path }
    }

    /// Applies normalization to the given dimension value.
    #[must_use]
    pub fn apply(
        &self,
        event: &LogEvent,
        value: &Value,
        kernels: &SimdKernelTable,
    ) -> Option<Bytes> {
        let when_path = self.when_path.as_ref();
        self.apply_with_resolver(value, kernels, |_field| {
            when_path
                .and_then(|path| event.get(path))
                .map(|v| value_to_string_lossy(v).into_owned())
        })
    }

    /// Applies normalization to the given value using a custom field resolver for conditions.
    #[must_use]
    pub fn apply_with_resolver<F>(
        &self,
        value: &Value,
        kernels: &SimdKernelTable,
        mut when_lookup: F,
    ) -> Option<Bytes>
    where
        F: FnMut(&str) -> Option<String>,
    {
        match &self.config {
            NormalizerConfig::IpCidr {
                ipv4_prefix,
                ipv6_prefix,
                when,
            } => {
                if let Some(cond) = when {
                    let actual = when_lookup(&cond.field);
                    if actual.as_deref() != Some(cond.equals.as_str()) {
                        return Some(Bytes::from(value_to_string_lossy(value).into_owned()));
                    }
                }

                let s = value_to_string_lossy(value);
                let trimmed = s.trim();

                if let Ok(ip) = trimmed.parse::<IpAddr>() {
                    match ip {
                        IpAddr::V4(ipv4) => {
                            let mapped = ipv4.to_ipv6_mapped().octets();
                            let masked = (kernels.mask_ip_cidr)(mapped, 96 + *ipv4_prefix);
                            let masked_ipv4 =
                                Ipv4Addr::new(masked[12], masked[13], masked[14], masked[15]);
                            Some(Bytes::from(masked_ipv4.to_string()))
                        }
                        IpAddr::V6(ipv6) => {
                            let masked = (kernels.mask_ip_cidr)(ipv6.octets(), *ipv6_prefix);
                            let masked_ipv6 = Ipv6Addr::from(masked);
                            Some(Bytes::from(masked_ipv6.to_string()))
                        }
                    }
                } else {
                    Some(Bytes::from(s.into_owned()))
                }
            }
            NormalizerConfig::LogFingerprint => {
                let input = value_to_bytes(value);
                let mut out = Vec::new();
                (kernels.fingerprint_log_bytes)(&input, &mut out);
                Some(Bytes::from(out))
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use logsluice_core::simd::SimdMode;

    #[test]
    fn timestamp_and_ipv6_normalizer_edge_cases() {
        let expected_ms = 1_700_000_000_000_i64;

        // Value::Number across all 4 epoch ranges: seconds, milliseconds, microseconds, nanoseconds
        assert_eq!(
            extract_timestamp_ms(&Value::from(1_700_000_000_i64)),
            Ok(expected_ms)
        );
        assert_eq!(
            extract_timestamp_ms(&Value::from(1_700_000_000_000_i64)),
            Ok(expected_ms)
        );
        assert_eq!(
            extract_timestamp_ms(&Value::from(1_700_000_000_000_000_i64)),
            Ok(expected_ms)
        );
        assert_eq!(
            extract_timestamp_ms(&Value::from(1_700_000_000_000_000_000_i64)),
            Ok(expected_ms)
        );

        // Numeric strings across all 4 epoch ranges
        assert_eq!(
            extract_timestamp_ms(&Value::from("1700000000")),
            Ok(expected_ms)
        );
        assert_eq!(
            extract_timestamp_ms(&Value::from("1700000000000")),
            Ok(expected_ms)
        );
        assert_eq!(
            extract_timestamp_ms(&Value::from("1700000000000000")),
            Ok(expected_ms)
        );
        assert_eq!(
            extract_timestamp_ms(&Value::from("1700000000000000000")),
            Ok(expected_ms)
        );
        assert_eq!(
            extract_timestamp_ms(&Value::from(" 2023-11-14T22:13:20Z ")),
            Ok(expected_ms)
        );

        // Negative numbers, floats, overflow u64, non-timestamp strings, booleans, null
        assert_eq!(
            extract_timestamp_ms(&Value::from(-1_i64)),
            Err(TimestampError)
        );
        assert_eq!(
            extract_timestamp_ms(&Value::from("-100")),
            Err(TimestampError)
        );
        assert_eq!(
            extract_timestamp_ms(&Value::from(1.5_f64)),
            Err(TimestampError)
        );
        assert_eq!(
            extract_timestamp_ms(&Value::from(u64::MAX)),
            Err(TimestampError)
        );
        assert_eq!(
            extract_timestamp_ms(&Value::from("not-a-timestamp")),
            Err(TimestampError)
        );
        assert_eq!(
            extract_timestamp_ms(&Value::Bool(true)),
            Err(TimestampError)
        );
        assert_eq!(extract_timestamp_ms(&Value::Null), Err(TimestampError));
        assert_eq!(
            TimestampError.to_string(),
            "invalid or unparseable timestamp"
        );

        // extract_otlp_time_nano_ms edge cases
        assert_eq!(
            extract_otlp_time_nano_ms(&Value::from(1_700_000_000_000_000_000_i64)),
            Some(expected_ms)
        );
        assert_eq!(extract_otlp_time_nano_ms(&Value::from(0_i64)), None);
        assert_eq!(extract_otlp_time_nano_ms(&Value::from(-5_i64)), None);
        assert_eq!(extract_otlp_time_nano_ms(&Value::from(u64::MAX)), None);
        assert_eq!(
            extract_otlp_time_nano_ms(&Value::from("1700000000000000000")),
            Some(expected_ms)
        );
        assert_eq!(extract_otlp_time_nano_ms(&Value::from("0")), None);
        assert_eq!(extract_otlp_time_nano_ms(&Value::from("-5")), None);
        assert_eq!(extract_otlp_time_nano_ms(&Value::from("invalid")), None);
        assert_eq!(extract_otlp_time_nano_ms(&Value::Null), None);

        // metric_value with u64::MAX and invalid types
        assert_eq!(
            metric_value(&Value::from(u64::MAX)),
            MetricValue::Int(i64::MAX)
        );
        assert_eq!(metric_value(&Value::Null), MetricValue::Invalid);

        // SourcePositionPaths missing or out-of-range partition
        let sp_paths = SourcePositionPaths::new(&SourcePositionConfig::default());
        let mut ev = LogEvent::new();
        assert!(sp_paths.extract(&ev).is_none());
        ev.insert_path("topic", "t");
        ev.insert_path("partition", i64::from(i32::MAX) + 1);
        ev.insert_path("offset", 10);
        assert!(sp_paths.extract(&ev).is_none());

        // NormalizerConfig::IpCidr on IPv6, IPv4, clamped prefixes,when condition, and non-IP fallback
        let kernels = SimdKernelTable::detect(SimdMode::Scalar);
        let norm = NormalizerExecutor::new(NormalizerConfig::IpCidr {
            ipv4_prefix: 24,
            ipv6_prefix: 48,
            when: None,
        });
        let ipv6_out = norm
            .apply(&ev, &Value::from("2001:db8:abcd:1234::1"), &kernels)
            .unwrap();
        assert_eq!(ipv6_out.as_ref(), b"2001:db8:abcd::");

        let ipv4_out = norm
            .apply(&ev, &Value::from("10.20.30.40"), &kernels)
            .unwrap();
        assert_eq!(ipv4_out.as_ref(), b"10.20.30.0");

        let fallback_out = norm
            .apply(&ev, &Value::from("not-an-ip"), &kernels)
            .unwrap();
        assert_eq!(fallback_out.as_ref(), b"not-an-ip");

        // Clamped prefixes (ipv4_prefix > 32, ipv6_prefix > 128)
        let clamped_norm = NormalizerExecutor::new(NormalizerConfig::IpCidr {
            ipv4_prefix: 64,
            ipv6_prefix: 200,
            when: Some(WhenCondition {
                field: "event.action".to_string(),
                equals: "deny".to_string(),
            }),
        });
        // Condition not met -> unmasked
        let unmasked = clamped_norm
            .apply(&ev, &Value::from("2001:db8:abcd:1234::1"), &kernels)
            .unwrap();
        assert_eq!(unmasked.as_ref(), b"2001:db8:abcd:1234::1");

        // Condition met -> masked with clamped prefix (full 128 bits preserved)
        ev.insert_path("event.action", "deny");
        let clamped_v6 = clamped_norm
            .apply(&ev, &Value::from("2001:db8:abcd:1234::1"), &kernels)
            .unwrap();
        assert_eq!(clamped_v6.as_ref(), b"2001:db8:abcd:1234::1");
        let clamped_v4 = clamped_norm
            .apply(&ev, &Value::from("10.20.30.40"), &kernels)
            .unwrap();
        assert_eq!(clamped_v4.as_ref(), b"10.20.30.40");

        // LogFingerprint normalizer
        let fp_norm = NormalizerExecutor::new(NormalizerConfig::LogFingerprint);
        let fp_out = fp_norm
            .apply(&ev, &Value::from("conn from 10.0.0.1"), &kernels)
            .unwrap();
        assert_eq!(fp_out.as_ref(), b"conn from <IP>");
    }
}
