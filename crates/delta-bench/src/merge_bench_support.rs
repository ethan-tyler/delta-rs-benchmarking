use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use deltalake_core::arrow::compute::concat_batches;
use deltalake_core::datafusion;
use deltalake_core::datafusion::catalog::Session;
use deltalake_core::datafusion::common::tree_node::{Transformed, TreeNode};
use deltalake_core::datafusion::common::{Column, ScalarValue, TableReference};
use deltalake_core::datafusion::datasource::provider_as_source;
use deltalake_core::datafusion::functions_aggregate::expr_fn::{max, min};
use deltalake_core::datafusion::logical_expr::expr::{InList, Placeholder};
use deltalake_core::datafusion::logical_expr::{
    Aggregate, Between, BinaryExpr, Expr, LogicalPlan, LogicalPlanBuilder, Operator,
};
use deltalake_core::datafusion::physical_plan::ExecutionPlan;
use deltalake_core::datafusion::prelude::{lit, SessionContext};
use deltalake_core::kernel::EagerSnapshot;
use deltalake_core::DeltaTableError;
use either::{Left, Right};
use futures::TryStreamExt as _;
use itertools::Itertools;

use crate::data::datasets::NarrowSaleRow;
use crate::data::fixtures::{load_rows, merge_partitioned_target_table_url, rows_to_batch};
use crate::error::{BenchError, BenchResult};
use crate::storage::StorageConfig;

const SOURCE_ALIAS: &str = "source";
const TARGET_ALIAS: &str = "target";
const LOCALIZED_REGION: &str = "us";
const SOURCE_ROW_LIMIT: usize = 32;

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MergeFilterGeneralizeVariant {
    PartitionEqSourceTarget,
    NonPartitionEqMinmax,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MergeFilterEarlyVariant {
    LocalizedPartitionExpansion,
    MixedPartitionAndStats,
    StreamingSource,
}

#[doc(hidden)]
#[derive(Clone)]
pub struct MergeGeneralizeContext {
    predicate: Expr,
    partition_columns: Vec<String>,
    source_name: TableReference,
    target_name: TableReference,
    streaming_source: bool,
}

#[doc(hidden)]
#[derive(Clone)]
pub struct MergeEarlyFilterContext {
    snapshot: EagerSnapshot,
    session: Arc<dyn Session + Send + Sync>,
    source: LogicalPlan,
    join_predicate: Expr,
    source_name: TableReference,
    target_name: TableReference,
    streaming_source: bool,
}

#[doc(hidden)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct GeneralizeFilterOutcome {
    pub filter: Expr,
    pub placeholder_aliases: Vec<String>,
    pub aggregate_placeholders: usize,
}

#[doc(hidden)]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EarlyFilterOutcome {
    pub filter: Expr,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MergeFilterBenchOutcome {
    pub rendered_len: usize,
    pub placeholder_count: usize,
}

// Keep this block aligned with deltalake-core/src/operations/merge/filter.rs.
// The merge_bench_support sync contract test compares these items against the pinned dependency source.
#[derive(Debug)]
enum ReferenceTableCheck {
    HasReference(String),
    NoReference,
    Unknown,
}

impl ReferenceTableCheck {
    fn has_reference(&self) -> bool {
        matches!(self, ReferenceTableCheck::HasReference(_))
    }
}

#[derive(Debug)]
struct PredicatePlaceholder {
    pub expr: Expr,
    pub alias: String,
    pub is_aggregate: bool,
}

#[doc(hidden)]
pub fn benchmark_generalize_context(
    variant: MergeFilterGeneralizeVariant,
) -> MergeGeneralizeContext {
    let source_name = TableReference::parse_str(SOURCE_ALIAS);
    let target_name = TableReference::parse_str(TARGET_ALIAS);

    let predicate = match variant {
        MergeFilterGeneralizeVariant::PartitionEqSourceTarget => {
            qualified_col(&source_name, "region").eq(qualified_col(&target_name, "region"))
        }
        MergeFilterGeneralizeVariant::NonPartitionEqMinmax => {
            qualified_col(&source_name, "id").eq(qualified_col(&target_name, "id"))
        }
    };

    MergeGeneralizeContext {
        predicate,
        partition_columns: vec!["region".to_string()],
        source_name,
        target_name,
        streaming_source: false,
    }
}

#[doc(hidden)]
pub async fn benchmark_early_filter_context(
    fixtures_dir: &Path,
    scale: &str,
    variant: MergeFilterEarlyVariant,
    storage: &StorageConfig,
) -> BenchResult<MergeEarlyFilterContext> {
    let table = storage
        .open_table(merge_partitioned_target_table_url(
            fixtures_dir,
            scale,
            storage,
        )?)
        .await?;
    let snapshot = table.snapshot()?.snapshot().clone();

    let source_name = TableReference::parse_str(SOURCE_ALIAS);
    let target_name = TableReference::parse_str(TARGET_ALIAS);

    let all_rows = load_rows(fixtures_dir, scale)?;
    let source_rows = match variant {
        MergeFilterEarlyVariant::LocalizedPartitionExpansion => {
            partition_expansion_source_rows(&all_rows)
        }
        MergeFilterEarlyVariant::MixedPartitionAndStats
        | MergeFilterEarlyVariant::StreamingSource => localized_source_rows(&all_rows),
    };
    let source_batch = rows_to_batch(&source_rows)?;
    let session_context = SessionContext::new();
    let source_df = session_context.read_batch(source_batch)?;
    let session: Arc<dyn Session + Send + Sync> = Arc::new(session_context.state());
    let source = LogicalPlanBuilder::scan(
        source_name.clone(),
        provider_as_source(source_df.into_view()),
        None,
    )?
    .build()?;

    let (join_predicate, streaming_source) = match variant {
        MergeFilterEarlyVariant::LocalizedPartitionExpansion => (
            qualified_col(&source_name, "region").eq(qualified_col(&target_name, "region")),
            false,
        ),
        MergeFilterEarlyVariant::MixedPartitionAndStats => (
            qualified_col(&source_name, "id")
                .eq(qualified_col(&target_name, "id"))
                .and(
                    qualified_col(&source_name, "region").eq(qualified_col(&target_name, "region")),
                ),
            false,
        ),
        MergeFilterEarlyVariant::StreamingSource => (
            qualified_col(&source_name, "id")
                .eq(qualified_col(&target_name, "id"))
                .and(qualified_col(&target_name, "region").eq(lit(LOCALIZED_REGION))),
            true,
        ),
    };

    Ok(MergeEarlyFilterContext {
        snapshot,
        session,
        source,
        join_predicate,
        source_name,
        target_name,
        streaming_source,
    })
}

#[doc(hidden)]
pub fn benchmark_generalize_filter(
    ctx: &MergeGeneralizeContext,
) -> BenchResult<GeneralizeFilterOutcome> {
    let mut placeholders = Vec::new();
    let filter = generalize_filter(
        ctx.predicate.clone(),
        &ctx.partition_columns,
        &ctx.source_name,
        &ctx.target_name,
        &mut placeholders,
        ctx.streaming_source,
    )
    .ok_or_else(|| {
        BenchError::InvalidArgument("merge generalize filter resolved to None".to_string())
    })?;

    Ok(GeneralizeFilterOutcome {
        filter,
        placeholder_aliases: placeholders.iter().map(|p| p.alias.clone()).collect(),
        aggregate_placeholders: placeholders.iter().filter(|p| p.is_aggregate).count(),
    })
}

#[doc(hidden)]
pub fn timed_generalize_filter(
    ctx: &MergeGeneralizeContext,
) -> BenchResult<MergeFilterBenchOutcome> {
    let outcome = benchmark_generalize_filter(ctx)?;
    Ok(MergeFilterBenchOutcome {
        rendered_len: outcome.filter.to_string().len(),
        placeholder_count: outcome.placeholder_aliases.len(),
    })
}

#[doc(hidden)]
pub async fn benchmark_try_construct_early_filter(
    ctx: &MergeEarlyFilterContext,
) -> BenchResult<EarlyFilterOutcome> {
    let filter = try_construct_early_filter(
        ctx.join_predicate.clone(),
        &ctx.snapshot,
        ctx.session.as_ref(),
        &ctx.source,
        &ctx.source_name,
        &ctx.target_name,
        ctx.streaming_source,
    )
    .await?
    .ok_or_else(|| {
        BenchError::InvalidArgument("merge early filter resolved to None".to_string())
    })?;

    Ok(EarlyFilterOutcome { filter })
}

#[doc(hidden)]
pub async fn timed_try_construct_early_filter(
    ctx: &MergeEarlyFilterContext,
) -> BenchResult<MergeFilterBenchOutcome> {
    let outcome = benchmark_try_construct_early_filter(ctx).await?;
    Ok(MergeFilterBenchOutcome {
        rendered_len: outcome.filter.to_string().len(),
        placeholder_count: 0,
    })
}

fn localized_source_rows(rows: &[NarrowSaleRow]) -> Vec<NarrowSaleRow> {
    rows.iter()
        .filter(|row| row.region == LOCALIZED_REGION)
        .take(SOURCE_ROW_LIMIT)
        .cloned()
        .collect()
}

fn partition_expansion_source_rows(rows: &[NarrowSaleRow]) -> Vec<NarrowSaleRow> {
    let mut selected = Vec::new();
    let mut seen_regions = HashSet::new();

    if let Some(row) = rows.iter().find(|row| row.region == LOCALIZED_REGION) {
        selected.push(row.clone());
        seen_regions.insert(row.region.clone());
    }

    for row in rows {
        if seen_regions.insert(row.region.clone()) {
            selected.push(row.clone());
        }
    }

    selected
}

fn qualified_col(table: &TableReference, name: &str) -> Expr {
    Expr::Column(Column::new(Some(table.clone()), name))
}

fn references_table(expr: &Expr, table: &TableReference) -> ReferenceTableCheck {
    match expr {
        Expr::Alias(alias) => references_table(&alias.expr, table),
        Expr::Column(col) => col
            .relation
            .as_ref()
            .map(|rel| {
                if rel == table {
                    ReferenceTableCheck::HasReference(col.name.to_owned())
                } else {
                    ReferenceTableCheck::NoReference
                }
            })
            .unwrap_or(ReferenceTableCheck::NoReference),
        Expr::Negative(neg) => references_table(neg, table),
        Expr::Cast(cast) => references_table(&cast.expr, table),
        Expr::TryCast(try_cast) => references_table(&try_cast.expr, table),
        Expr::ScalarFunction(func) => {
            if func.args.len() == 1 {
                references_table(&func.args[0], table)
            } else {
                ReferenceTableCheck::Unknown
            }
        }
        Expr::IsNull(inner) => references_table(inner, table),
        Expr::Literal(_, _) => ReferenceTableCheck::NoReference,
        _ => ReferenceTableCheck::Unknown,
    }
}

fn construct_placeholder(
    binary: BinaryExpr,
    source_left: bool,
    is_partition_column: bool,
    column_name: String,
    placeholders: &mut Vec<PredicatePlaceholder>,
) -> Option<Expr> {
    if is_partition_column {
        let placeholder_name = format!("{column_name}_{}", placeholders.len());
        let placeholder = Expr::Placeholder(Placeholder {
            id: placeholder_name.clone(),
            field: None,
        });

        let (left, right, source_expr): (Box<Expr>, Box<Expr>, Expr) = if source_left {
            (placeholder.into(), binary.clone().right, *binary.left)
        } else {
            (binary.clone().left, placeholder.into(), *binary.right)
        };

        let replaced = Expr::BinaryExpr(BinaryExpr {
            left,
            op: binary.op,
            right,
        });

        placeholders.push(PredicatePlaceholder {
            expr: source_expr,
            alias: placeholder_name,
            is_aggregate: false,
        });

        Some(replaced)
    } else {
        match binary.op {
            Operator::Eq => {
                let name_min = format!("{column_name}_{}_min", placeholders.len());
                let placeholder_min = Expr::Placeholder(Placeholder {
                    id: name_min.clone(),
                    field: None,
                });
                let name_max = format!("{column_name}_{}_max", placeholders.len());
                let placeholder_max = Expr::Placeholder(Placeholder {
                    id: name_max.clone(),
                    field: None,
                });
                let (source_expr, target_expr) = if source_left {
                    (*binary.left, *binary.right)
                } else {
                    (*binary.right, *binary.left)
                };

                let replaced = Expr::Between(Between {
                    expr: target_expr.into(),
                    negated: false,
                    low: placeholder_min.into(),
                    high: placeholder_max.into(),
                });

                placeholders.push(PredicatePlaceholder {
                    expr: min(source_expr.clone()),
                    alias: name_min,
                    is_aggregate: true,
                });
                placeholders.push(PredicatePlaceholder {
                    expr: max(source_expr),
                    alias: name_max,
                    is_aggregate: true,
                });

                Some(replaced)
            }
            _ => None,
        }
    }
}

#[rustfmt::skip]
fn generalize_filter(
    predicate: Expr,
    partition_columns: &Vec<String>,
    source_name: &TableReference,
    target_name: &TableReference,
    placeholders: &mut Vec<PredicatePlaceholder>,
    streaming_source: bool,
) -> Option<Expr> {
    match predicate {
        Expr::BinaryExpr(binary) => {
            if !streaming_source {
                if references_table(&binary.right, source_name).has_reference() {
                    if let ReferenceTableCheck::HasReference(left_target) =
                        references_table(&binary.left, target_name)
                    {
                        return construct_placeholder(
                            binary,
                            false,
                            partition_columns.contains(&left_target),
                            left_target,
                            placeholders,
                        );
                    }
                    return None;
                }
                if references_table(&binary.left, source_name).has_reference() {
                    if let ReferenceTableCheck::HasReference(right_target) =
                        references_table(&binary.right, target_name)
                    {
                        return construct_placeholder(
                            binary,
                            true,
                            partition_columns.contains(&right_target),
                            right_target,
                            placeholders,
                        );
                    }
                    return None;
                }
            }

            let left = generalize_filter(
                *binary.left,
                partition_columns,
                source_name,
                target_name,
                placeholders,
                streaming_source,
            );
            let right = generalize_filter(
                *binary.right,
                partition_columns,
                source_name,
                target_name,
                placeholders,
                streaming_source,
            );

            match (left, right) {
                (None, None) => None,
                (None, Some(one_side)) | (Some(one_side), None) => {
                    match binary.op {
                        Operator::And => Some(one_side),
                        Operator::Or => None,
                        _ => None,
                    }
                }
                (Some(l), Some(r)) => Expr::BinaryExpr(BinaryExpr {
                    left: l.into(),
                    op: binary.op,
                    right: r.into(),
                })
                .into(),
            }
        }
        Expr::InList(in_list) => {
            let compare_expr = generalize_filter(
                *in_list.expr,
                partition_columns,
                source_name,
                target_name,
                placeholders,
                streaming_source,
            )?;

            let mut list_expr = Vec::new();
            for item in in_list.list.into_iter() {
                match item {
                    Expr::Literal(_, _) => list_expr.push(item),
                    _ => {
                        if let Some(item) = generalize_filter(
                            item.clone(),
                            partition_columns,
                            source_name,
                            target_name,
                            placeholders,
                            streaming_source,
                        ) {
                            list_expr.push(item)
                        }
                    }
                }
            }

            if !list_expr.is_empty() {
                Expr::InList(InList {
                    expr: compare_expr.into(),
                    list: list_expr,
                    negated: in_list.negated,
                })
                .into()
            } else {
                None
            }
        }
        other => match references_table(&other, source_name) {
            ReferenceTableCheck::HasReference(col) => {
                if !streaming_source {
                    let placeholder_name = format!("{col}_{}", placeholders.len());
                    let placeholder = Expr::Placeholder(Placeholder {
                        id: placeholder_name.clone(),
                        field: None,
                    });

                    placeholders.push(PredicatePlaceholder {
                        expr: other,
                        alias: placeholder_name,
                        is_aggregate: true,
                    });
                    Some(placeholder)
                } else {
                    None
                }
            }
            ReferenceTableCheck::NoReference => Some(other),
            ReferenceTableCheck::Unknown => None,
        },
    }
}

async fn try_construct_early_filter(
    join_predicate: Expr,
    table_snapshot: &EagerSnapshot,
    session_state: &dyn Session,
    source: &LogicalPlan,
    source_name: &TableReference,
    target_name: &TableReference,
    streaming_source: bool,
) -> BenchResult<Option<Expr>> {
    let table_metadata = table_snapshot.metadata();
    let partition_columns = table_metadata.partition_columns();

    let mut placeholders = Vec::default();

    match generalize_filter(
        join_predicate,
        &partition_columns.to_vec(),
        source_name,
        target_name,
        &mut placeholders,
        streaming_source,
    ) {
        None => Ok(None),
        Some(filter) => {
            if placeholders.is_empty() || streaming_source {
                Ok(Some(filter))
            } else {
                let (agg_columns, group_columns) = placeholders.into_iter().partition_map(|p| {
                    if p.is_aggregate {
                        Left(p.expr.alias(p.alias))
                    } else {
                        Right(p.expr.alias(p.alias))
                    }
                });
                let distinct_partitions = LogicalPlan::Aggregate(Aggregate::try_new(
                    source.clone().into(),
                    group_columns,
                    agg_columns,
                )?);
                let execution_plan = session_state
                    .create_physical_plan(&distinct_partitions)
                    .await?;
                let items = execute_plan_to_batch(session_state, execution_plan).await?;
                let placeholder_names = items
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().to_owned())
                    .collect_vec();
                let expr = (0..items.num_rows())
                    .map(|i| {
                        let replacements = placeholder_names
                            .iter()
                            .map(|placeholder| {
                                let col = items.column_by_name(placeholder).unwrap();
                                let value = ScalarValue::try_from_array(col, i)?;
                                Ok((placeholder.clone(), value))
                            })
                            .try_collect::<_, _, DeltaTableError>()?;
                        Ok(replace_placeholders(filter.clone(), &replacements))
                    })
                    .collect::<BenchResult<Vec<_>>>()?
                    .into_iter()
                    .reduce(Expr::or);
                Ok(expr)
            }
        }
    }
}

fn replace_placeholders(expr: Expr, placeholders: &HashMap<String, ScalarValue>) -> Expr {
    expr.transform(&|expr| match expr {
        Expr::Placeholder(Placeholder { id, .. }) => {
            let value = placeholders[&id].clone();
            Ok(Transformed::yes(lit(value)))
        }
        _ => Ok(Transformed::no(expr)),
    })
    .unwrap()
    .data
}

async fn execute_plan_to_batch(
    state: &dyn Session,
    plan: Arc<dyn ExecutionPlan>,
) -> BenchResult<deltalake_core::arrow::record_batch::RecordBatch> {
    let data = futures::future::try_join_all(
        (0..plan.properties().output_partitioning().partition_count()).map(|p| {
            let plan_copy = plan.clone();
            let task_context = state.task_ctx().clone();
            async move {
                let batch_stream = plan_copy.execute(p, task_context)?;
                let schema = batch_stream.schema();
                let batches = batch_stream.try_collect::<Vec<_>>().await?;
                datafusion::error::Result::<_>::Ok(concat_batches(&schema, batches.iter())?)
            }
        }),
    )
    .await?;

    Ok(concat_batches(&plan.schema(), data.iter())?)
}
