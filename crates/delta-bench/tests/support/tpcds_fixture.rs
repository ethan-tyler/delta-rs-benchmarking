use std::path::Path;
use std::sync::Arc;

use deltalake_core::arrow::array::{Float64Array, Int64Array};
use deltalake_core::arrow::datatypes::{DataType, Field, Schema};
use deltalake_core::arrow::record_batch::RecordBatch;
use deltalake_core::protocol::SaveMode;
use deltalake_core::DeltaTable;
use url::Url;

/// Write a minimal TPC-DS store_sales fixture table for testing.
pub async fn write_store_sales_fixture(fixtures_dir: &Path, scale: &str) {
    let table_dir = fixtures_dir.join(scale).join("tpcds").join("store_sales");
    std::fs::create_dir_all(&table_dir).expect("create fixture table dir");

    let table_url = Url::from_directory_path(&table_dir).expect("table url");
    let schema = Arc::new(Schema::new(vec![
        Field::new("ss_customer_sk", DataType::Int64, false),
        Field::new("ss_ext_sales_price", DataType::Float64, false),
        Field::new("ss_item_sk", DataType::Int64, false),
        Field::new("ss_quantity", DataType::Int64, false),
        Field::new("ss_sold_date_sk", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 3])),
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 15.0])),
            Arc::new(Int64Array::from(vec![100, 101, 100, 102])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int64Array::from(vec![2450815, 2450816, 2450817, 2450818])),
        ],
    )
    .expect("record batch");

    let _ = DeltaTable::try_from_url(table_url)
        .await
        .expect("open table")
        .write(vec![batch])
        .with_save_mode(SaveMode::Overwrite)
        .await
        .expect("write fixture");
}
