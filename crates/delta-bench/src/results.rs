use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchContext {
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
        }
    }

    pub fn with_scan_rewrite_metrics(
        mut self,
        files_scanned: Option<u64>,
        files_pruned: Option<u64>,
        bytes_scanned: Option<u64>,
        scan_time_ms: Option<u64>,
        rewrite_time_ms: Option<u64>,
    ) -> Self {
        self.files_scanned = files_scanned;
        self.files_pruned = files_pruned;
        self.bytes_scanned = bytes_scanned;
        self.scan_time_ms = scan_time_ms;
        self.rewrite_time_ms = rewrite_time_ms;
        self
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CaseResult {
    pub case: String,
    pub success: bool,
    pub samples: Vec<IterationSample>,
    pub failure: Option<CaseFailure>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BenchRunResult {
    pub schema_version: u32,
    pub context: BenchContext,
    pub cases: Vec<CaseResult>,
}
