use serde::{Deserialize, Serialize};

fn default_fixture_profile() -> String {
    "standard".to_string()
}

fn default_table_inventory() -> Vec<String> {
    Vec::new()
}

fn default_fixture_recipe_hash() -> String {
    String::new()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NarrowSaleRow {
    pub id: i64,
    pub ts_ms: i64,
    pub region: String,
    pub value_i64: i64,
    pub flag: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FixtureRecipe {
    pub schema_version: u32,
    pub generator_version: u32,
    pub seed: u64,
    pub scale: String,
    pub rows: usize,
    pub profile: String,
    pub table_inventory: Vec<String>,
    pub many_versions_append_commits: usize,
    pub read_partition_chunk_size: usize,
    pub merge_partition_chunk_size: usize,
    pub delete_update_partition_chunk_size: usize,
    pub optimize_small_files_chunk_size: usize,
    pub optimize_seed_rows: usize,
    pub merge_seed_rows: usize,
    pub vacuum_seed_rows: usize,
    pub tpcds_duckdb_chunk_rows: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_component_hash: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FixtureManifest {
    pub schema_version: u32,
    #[serde(default)]
    pub generator_version: u32,
    pub seed: u64,
    pub scale: String,
    pub rows: usize,
    #[serde(default = "default_fixture_profile")]
    pub profile: String,
    #[serde(default)]
    pub dataset_fingerprint: String,
    #[serde(default = "default_table_inventory")]
    pub table_inventory: Vec<String>,
    #[serde(default = "default_fixture_recipe_hash")]
    pub fixture_recipe_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fixture_recipe: Option<FixtureRecipe>,
}
