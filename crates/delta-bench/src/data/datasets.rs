use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NarrowSaleRow {
    pub id: i64,
    pub ts_ms: i64,
    pub region: String,
    pub value_i64: i64,
    pub flag: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FixtureManifest {
    pub schema_version: u32,
    pub seed: u64,
    pub scale: String,
    pub rows: usize,
}
