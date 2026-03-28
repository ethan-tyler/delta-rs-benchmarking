use deltalake_core::datafusion::prelude::SessionContext;
use deltalake_core::DeltaTable;

use crate::cli::BenchmarkLane;
use crate::error::BenchResult;
use crate::fingerprint::{hash_record_batch_schema, hash_record_batches_unordered};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SemanticValidation {
    pub digest: String,
    pub schema_hash: String,
    pub summary: String,
}

pub fn lane_requires_semantic_validation(lane: BenchmarkLane) -> bool {
    lane == BenchmarkLane::Correctness
}

pub async fn validate_table_state(table: &DeltaTable) -> BenchResult<SemanticValidation> {
    let ctx = SessionContext::new();
    ctx.register_table("bench", table.table_provider().await?)?;
    let df = ctx.sql("SELECT * FROM bench").await?;
    let batches = df.collect().await?;
    let row_count = batches
        .iter()
        .map(|batch| batch.num_rows() as u64)
        .sum::<u64>();
    let digest = hash_record_batches_unordered(&batches)?;
    let schema_hash = hash_record_batch_schema(&batches)?;
    let summary = match table.version().map(|version| version as u64) {
        Some(version) => format!("rows={row_count};table_version={version}"),
        None => format!("rows={row_count};table_version=unknown"),
    };
    Ok(SemanticValidation {
        digest,
        schema_hash,
        summary,
    })
}
