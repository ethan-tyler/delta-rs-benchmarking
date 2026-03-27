use chrono::{DateTime, Utc};
use serde::{de, Deserialize, Deserializer, Serialize};

fn deserialize_schema_version_v2<'de, D>(deserializer: D) -> Result<u32, D::Error>
where
    D: Deserializer<'de>,
{
    let value = u32::deserialize(deserializer)?;
    if value == 2 {
        Ok(value)
    } else {
        Err(de::Error::custom(format!(
            "schema_version must be 2 (found {value})"
        )))
    }
}

fn deserialize_case_classification<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    parse_case_classification(&value).map_err(de::Error::custom)
}

fn parse_case_classification(value: &str) -> Result<String, String> {
    match value {
        "supported" | "expected_failure" => Ok(value.to_string()),
        other => Err(format!(
            "classification must be one of: supported, expected_failure (found {other})"
        )),
    }
}

pub fn validate_case_classification(value: &str) -> Result<(), String> {
    parse_case_classification(value).map(|_| ())
}

fn is_terminal() -> bool {
    std::io::IsTerminal::is_terminal(&std::io::stdout())
}

fn colorize(text: &str, code: &str) -> String {
    if is_terminal() {
        format!("\x1b[{code}m{text}\x1b[0m")
    } else {
        text.to_string()
    }
}

fn colorize_status(status: &str) -> String {
    match status {
        "ok" => colorize(status, "32"),
        "failed" => colorize(status, "31"),
        "expected_failure" => colorize(status, "33"),
        _ => status.to_string(),
    }
}

pub fn render_run_summary_table(cases: &[CaseResult]) -> String {
    let headers = [
        "case".to_string(),
        "status".to_string(),
        "mean_ms".to_string(),
        "min_ms".to_string(),
        "max_ms".to_string(),
        "stddev_ms".to_string(),
        "cv_pct".to_string(),
    ];
    // right-align: false for case & status, true for all numeric columns
    let right_align = [false, false, true, true, true, true, true];

    let mut rows = Vec::with_capacity(cases.len());
    for case in cases {
        let status = match (case.success, case.classification.as_str()) {
            (true, "expected_failure") => "expected_failure",
            (true, _) => "ok",
            (false, _) => "failed",
        };
        let stats = case.elapsed_stats.as_ref();
        rows.push(vec![
            case.case.clone(),
            status.to_string(),
            format_stat(stats.map(|s| s.mean_ms)),
            format_stat(stats.map(|s| s.min_ms)),
            format_stat(stats.map(|s| s.max_ms)),
            format_stat(stats.map(|s| s.stddev_ms)),
            format_stat(stats.and_then(|s| s.cv_pct)),
        ]);
    }

    // Compute widths from raw (uncolored) values
    let mut widths: Vec<usize> = headers.iter().map(String::len).collect();
    for row in &rows {
        for (idx, value) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(value.len());
        }
    }

    // Apply color to status column after width calculation
    let colored_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            let mut colored = row.clone();
            colored[1] = colorize_status(&row[1]);
            colored
        })
        .collect();

    let mut output = String::new();
    let border = render_table_border(&widths);
    output.push_str(&border);
    output.push('\n');
    output.push_str(&render_table_row(&headers, &widths, &right_align));
    output.push('\n');
    output.push_str(&border);
    output.push('\n');
    for (colored_row, raw_row) in colored_rows.iter().zip(rows.iter()) {
        output.push_str(&render_table_row_colored(
            colored_row,
            raw_row,
            &widths,
            &right_align,
        ));
        output.push('\n');
    }
    output.push_str(&border);
    output
}

fn format_stat(value: Option<f64>) -> String {
    value
        .map(|v| format!("{v:.3}"))
        .unwrap_or_else(|| "-".to_string())
}

fn render_table_border(widths: &[usize]) -> String {
    let mut border = String::new();
    border.push('+');
    for width in widths {
        border.push_str(&"-".repeat(width + 2));
        border.push('+');
    }
    border
}

fn render_table_row(values: &[String], widths: &[usize], right_align: &[bool]) -> String {
    let mut row = String::new();
    row.push('|');
    for (idx, value) in values.iter().enumerate() {
        row.push(' ');
        if right_align.get(idx).copied().unwrap_or(false) {
            row.push_str(&" ".repeat(widths[idx] - value.len()));
            row.push_str(value);
        } else {
            row.push_str(value);
            row.push_str(&" ".repeat(widths[idx] - value.len()));
        }
        row.push(' ');
        row.push('|');
    }
    row
}

/// Render a table row where some cells may contain ANSI color codes.
/// Uses `raw_values` for width calculation (visible length) and `colored_values` for display.
fn render_table_row_colored(
    colored_values: &[String],
    raw_values: &[String],
    widths: &[usize],
    right_align: &[bool],
) -> String {
    let mut row = String::new();
    row.push('|');
    for (idx, colored) in colored_values.iter().enumerate() {
        let raw_len = raw_values[idx].len();
        row.push(' ');
        if right_align.get(idx).copied().unwrap_or(false) {
            row.push_str(&" ".repeat(widths[idx] - raw_len));
            row.push_str(colored);
        } else {
            row.push_str(colored);
            row.push_str(&" ".repeat(widths[idx] - raw_len));
        }
        row.push(' ');
        row.push('|');
    }
    row
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchContext {
    #[serde(deserialize_with = "deserialize_schema_version_v2")]
    pub schema_version: u32,
    pub label: String,
    pub git_sha: Option<String>,
    pub created_at: DateTime<Utc>,
    pub host: String,
    pub suite: String,
    pub scale: String,
    pub iterations: u32,
    pub warmup: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timing_phase: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hardening_profile_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hardening_profile_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_microcode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kernel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_params: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_steal_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numa_topology: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress_policy_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintenance_window_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SampleMetrics {
    pub rows_processed: Option<u64>,
    pub bytes_processed: Option<u64>,
    pub operations: Option<u64>,
    pub table_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_scanned: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_pruned: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_scanned: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scan_time_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rewrite_time_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak_rss_mb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_time_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_read: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_written: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_touched: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_skipped: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spill_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contention: Option<ContentionMetrics>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanRewriteMetrics {
    pub files_scanned: Option<u64>,
    pub files_pruned: Option<u64>,
    pub bytes_scanned: Option<u64>,
    pub scan_time_ms: Option<u64>,
    pub rewrite_time_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeIOMetrics {
    pub peak_rss_mb: Option<u64>,
    pub cpu_time_ms: Option<u64>,
    pub bytes_read: Option<u64>,
    pub bytes_written: Option<u64>,
    pub files_touched: Option<u64>,
    pub files_skipped: Option<u64>,
    pub spill_bytes: Option<u64>,
    pub result_hash: Option<String>,
    pub schema_hash: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentionMetrics {
    pub worker_count: u64,
    pub race_count: u64,
    pub ops_attempted: u64,
    pub ops_succeeded: u64,
    pub ops_failed: u64,
    pub conflict_append: u64,
    pub conflict_delete_read: u64,
    pub conflict_delete_delete: u64,
    pub conflict_metadata_changed: u64,
    pub conflict_protocol_changed: u64,
    pub conflict_transaction: u64,
    pub version_already_exists: u64,
    pub max_commit_attempts_exceeded: u64,
    pub other_errors: u64,
}

impl SampleMetrics {
    pub fn base(
        rows_processed: Option<u64>,
        bytes_processed: Option<u64>,
        operations: Option<u64>,
        table_version: Option<u64>,
    ) -> Self {
        Self {
            rows_processed,
            bytes_processed,
            operations,
            table_version,
            files_scanned: None,
            files_pruned: None,
            bytes_scanned: None,
            scan_time_ms: None,
            rewrite_time_ms: None,
            peak_rss_mb: None,
            cpu_time_ms: None,
            bytes_read: None,
            bytes_written: None,
            files_touched: None,
            files_skipped: None,
            spill_bytes: None,
            result_hash: None,
            schema_hash: None,
            contention: None,
        }
    }

    pub fn with_scan_rewrite(mut self, metrics: ScanRewriteMetrics) -> Self {
        self.files_scanned = metrics.files_scanned;
        self.files_pruned = metrics.files_pruned;
        self.bytes_scanned = metrics.bytes_scanned;
        self.scan_time_ms = metrics.scan_time_ms;
        self.rewrite_time_ms = metrics.rewrite_time_ms;
        self
    }

    pub fn with_scan_rewrite_metrics(
        self,
        files_scanned: Option<u64>,
        files_pruned: Option<u64>,
        bytes_scanned: Option<u64>,
        scan_time_ms: Option<u64>,
        rewrite_time_ms: Option<u64>,
    ) -> Self {
        self.with_scan_rewrite(ScanRewriteMetrics {
            files_scanned,
            files_pruned,
            bytes_scanned,
            scan_time_ms,
            rewrite_time_ms,
        })
    }

    pub fn with_runtime_io(mut self, metrics: RuntimeIOMetrics) -> Self {
        self.peak_rss_mb = metrics.peak_rss_mb;
        self.cpu_time_ms = metrics.cpu_time_ms;
        self.bytes_read = metrics.bytes_read;
        self.bytes_written = metrics.bytes_written;
        self.files_touched = metrics.files_touched;
        self.files_skipped = metrics.files_skipped;
        self.spill_bytes = metrics.spill_bytes;
        self.result_hash = metrics.result_hash;
        self.schema_hash = metrics.schema_hash;
        self
    }

    pub fn with_contention(mut self, metrics: ContentionMetrics) -> Self {
        self.contention = Some(metrics);
        self
    }

    // Builder ergonomics: this mirrors JSON schema fields to keep callsites explicit.
    #[allow(clippy::too_many_arguments)]
    pub fn with_runtime_io_metrics(
        self,
        peak_rss_mb: Option<u64>,
        cpu_time_ms: Option<u64>,
        bytes_read: Option<u64>,
        bytes_written: Option<u64>,
        files_touched: Option<u64>,
        files_skipped: Option<u64>,
        spill_bytes: Option<u64>,
        result_hash: Option<String>,
        schema_hash: Option<String>,
    ) -> Self {
        self.with_runtime_io(RuntimeIOMetrics {
            peak_rss_mb,
            cpu_time_ms,
            bytes_read,
            bytes_written,
            files_touched,
            files_skipped,
            spill_bytes,
            result_hash,
            schema_hash,
        })
    }
}

impl From<u64> for SampleMetrics {
    fn from(rows: u64) -> Self {
        Self::base(Some(rows), None, None, None)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IterationSample {
    pub elapsed_ms: f64,
    pub rows: Option<u64>,
    pub bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<SampleMetrics>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaseFailure {
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ElapsedStats {
    pub min_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub median_ms: f64,
    pub stddev_ms: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cv_pct: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaseResult {
    pub case: String,
    pub success: bool,
    #[serde(deserialize_with = "deserialize_case_classification")]
    pub classification: String,
    pub samples: Vec<IterationSample>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elapsed_stats: Option<ElapsedStats>,
    pub failure: Option<CaseFailure>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchRunResult {
    #[serde(deserialize_with = "deserialize_schema_version_v2")]
    pub schema_version: u32,
    pub context: BenchContext,
    pub cases: Vec<CaseResult>,
}

#[cfg(test)]
mod tests {
    use super::{render_run_summary_table, CaseFailure, CaseResult, ElapsedStats};

    fn success_case(name: &str, mean_ms: f64, cv_pct: Option<f64>) -> CaseResult {
        CaseResult {
            case: name.to_string(),
            success: true,
            classification: "supported".to_string(),
            samples: Vec::new(),
            elapsed_stats: Some(ElapsedStats {
                min_ms: mean_ms - 1.0,
                max_ms: mean_ms + 1.0,
                mean_ms,
                median_ms: mean_ms,
                stddev_ms: 0.2,
                cv_pct,
            }),
            failure: None,
        }
    }

    #[test]
    fn run_summary_table_includes_header_and_stats() {
        let output = render_run_summary_table(&[success_case("scan_full_narrow", 10.5, Some(2.4))]);

        assert!(output.contains("case"));
        assert!(output.contains("status"));
        assert!(output.contains("mean_ms"));
        assert!(output.contains("scan_full_narrow"));
        assert!(output.contains("ok"));
        assert!(output.contains("10.500"));
        assert!(output.contains("2.400"));
    }

    #[test]
    fn run_summary_table_formats_failures_without_elapsed_stats() {
        let output = render_run_summary_table(&[CaseResult {
            case: "merge_upsert_10pct".to_string(),
            success: false,
            classification: "supported".to_string(),
            samples: Vec::new(),
            elapsed_stats: None,
            failure: Some(CaseFailure {
                message: "boom".to_string(),
            }),
        }]);

        assert!(output.contains("merge_upsert_10pct"));
        assert!(output.contains("failed"));
        assert!(output.contains(" - "));
    }
}
