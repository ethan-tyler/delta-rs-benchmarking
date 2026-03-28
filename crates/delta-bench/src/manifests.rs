use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::assertions::CaseAssertion;
use crate::error::{BenchError, BenchResult};

pub const DEFAULT_RUST_MANIFEST_PATH: &str = "bench/manifests/core_rust.yaml";
pub const DEFAULT_PYTHON_MANIFEST_PATH: &str = "bench/manifests/core_python.yaml";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BenchmarkManifest {
    pub id: String,
    pub description: String,
    #[serde(default)]
    pub cases: Vec<ManifestCase>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ManifestCase {
    pub id: String,
    pub target: String,
    #[serde(default = "default_runner")]
    pub runner: String,
    #[serde(default = "default_lane")]
    pub lane: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub supports_decision: Option<bool>,
    #[serde(default)]
    pub required_runs: Option<u32>,
    #[serde(default)]
    pub decision_threshold_pct: Option<f64>,
    #[serde(default)]
    pub decision_metric: Option<String>,
    #[serde(default)]
    pub assertions: Vec<ManifestAssertion>,
}

const fn default_enabled() -> bool {
    true
}

fn default_runner() -> String {
    "rust".to_string()
}

fn default_lane() -> String {
    "macro".to_string()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ManifestAssertion {
    ExactResultHash { value: String },
    SchemaHash { value: String },
    ExpectedErrorContains { value: String },
    VersionMonotonicity,
}

impl ManifestAssertion {
    pub fn to_case_assertion(&self) -> CaseAssertion {
        match self {
            Self::ExactResultHash { value } => CaseAssertion::ExactResultHash(value.clone()),
            Self::SchemaHash { value } => CaseAssertion::SchemaHash(value.clone()),
            Self::ExpectedErrorContains { value } => {
                CaseAssertion::ExpectedErrorContains(value.clone())
            }
            Self::VersionMonotonicity => CaseAssertion::VersionMonotonicity,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DatasetId {
    TinySmoke,
    MediumSelective,
    SmallFiles,
    ManyVersions,
    TpcdsDuckdb,
}

impl DatasetId {
    pub fn parse(value: &str) -> BenchResult<Self> {
        match value {
            "tiny_smoke" => Ok(Self::TinySmoke),
            "medium_selective" => Ok(Self::MediumSelective),
            "small_files" => Ok(Self::SmallFiles),
            "many_versions" => Ok(Self::ManyVersions),
            "tpcds_duckdb" => Ok(Self::TpcdsDuckdb),
            other => Err(BenchError::InvalidArgument(format!(
                "unknown dataset_id '{other}' (expected one of: tiny_smoke, medium_selective, small_files, many_versions, tpcds_duckdb)"
            ))),
        }
    }

    pub const fn scale(self) -> &'static str {
        match self {
            Self::TinySmoke => "sf1",
            Self::MediumSelective => "sf10",
            // P0 maps these scenario IDs to the currently supported scale; suites derive shape.
            Self::SmallFiles => "sf1",
            Self::ManyVersions => "sf1",
            Self::TpcdsDuckdb => "sf1",
        }
    }

    pub const fn fixture_profile(self) -> &'static str {
        match self {
            Self::ManyVersions => "many_versions",
            Self::TpcdsDuckdb => "tpcds_duckdb",
            Self::TinySmoke | Self::MediumSelective | Self::SmallFiles => "standard",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DatasetAssertionPolicy {
    pub relax_tpcds_exact_result_hash: bool,
}

impl DatasetId {
    pub const fn assertion_policy(self) -> DatasetAssertionPolicy {
        match self {
            Self::TpcdsDuckdb => DatasetAssertionPolicy {
                relax_tpcds_exact_result_hash: true,
            },
            Self::TinySmoke | Self::MediumSelective | Self::SmallFiles | Self::ManyVersions => {
                DatasetAssertionPolicy {
                    relax_tpcds_exact_result_hash: false,
                }
            }
        }
    }
}

pub fn load_manifest(path: impl AsRef<Path>) -> BenchResult<BenchmarkManifest> {
    let path = path.as_ref();
    let bytes = std::fs::read(path)?;
    serde_yaml::from_slice::<BenchmarkManifest>(&bytes).map_err(|error| {
        BenchError::InvalidArgument(format!("invalid manifest '{}': {error}", path.display()))
    })
}

pub(crate) fn benchmark_repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Preflight check for `list`/`run` commands to fail fast when required
/// manifests are missing from the benchmark repository.
pub fn ensure_required_manifests_exist() -> BenchResult<()> {
    ensure_required_manifests_exist_under_root(&benchmark_repo_root())
}

pub(crate) fn ensure_required_manifests_exist_under_root(root: &Path) -> BenchResult<()> {
    let required = [DEFAULT_RUST_MANIFEST_PATH, DEFAULT_PYTHON_MANIFEST_PATH];
    let mut missing = Vec::new();
    for relative in required {
        let path = root.join(relative);
        if !path.is_file() {
            missing.push((relative, path));
        }
    }
    if missing.is_empty() {
        return Ok(());
    }

    let details = missing
        .iter()
        .map(|(relative, path)| format!("- {relative} (expected at {})", path.display()))
        .collect::<Vec<_>>()
        .join("\n");

    Err(BenchError::InvalidArgument(format!(
        "manifest preflight failed for delta-bench `list`/`run` commands:\n{details}\n\
         ensure manifest files are present under `bench/manifests`."
    )))
}

#[cfg(test)]
mod tests {
    use super::ensure_required_manifests_exist_under_root;

    #[test]
    fn required_manifest_preflight_reports_missing_files_with_actionable_message() {
        let temp = tempfile::tempdir().expect("tempdir");
        let err = ensure_required_manifests_exist_under_root(temp.path())
            .expect_err("missing manifests should fail preflight");
        let message = err.to_string();
        assert!(
            message.contains("core_rust.yaml"),
            "missing rust manifest should be called out: {message}"
        );
        assert!(
            message.contains("core_python.yaml"),
            "missing python manifest should be called out: {message}"
        );
        assert!(
            message.contains("bench/manifests"),
            "error should explain where files belong: {message}"
        );
    }
}
