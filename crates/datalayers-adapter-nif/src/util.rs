use std::process::exit;
use std::sync::Arc;

use arrow_array::{
    Float32Array, Int32Array, Int64Array, Int8Array, RecordBatch, TimestampMillisecondArray,
};
use arrow_cast::pretty::pretty_format_batches;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::TimeZone;
use regex::Regex;

/// Applies a message filter on the input error to only retain the `message` field.
/// This function is meant to be used to filter error messages from the Datalayers server.
pub fn filter_message(err: &str) -> String {
    let mut err = err
        .replace(['\n', '\r'], " ")
        .replace("\\\"", "[ESCAPED_QUOTE]");
    let regex = Regex::new(r#"message: "(.*?)(?: at src/dbserver/src.*?)?""#).unwrap();
    if let Some(capture) = regex.captures(&err) {
        err = capture[1]
            .replace("[ESCAPED_QUOTE]", "\\\"")
            .replace('\\', "");
    }
    err
}

pub fn print_affected_rows(batches: &[RecordBatch]) {
    let affected_rows = batches
        .first()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    println!("Affected rows: {}", affected_rows);
}

pub fn print_batches(batches: &[RecordBatch]) {
    let formatted = pretty_format_batches(batches)
        .inspect_err(|e| {
            println!("Failed to print batches: {}", e);
            exit(1)
        })
        .unwrap();
    println!("{}", formatted);
}

pub fn make_insert_binding() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, Some("Asia/Shanghai".into())),
            false,
        ),
        Field::new("sid", DataType::Int32, true),
        Field::new("value", DataType::Float32, true),
        Field::new("flag", DataType::Int8, true),
    ]));

    // Sets the timezone to UTC+8.
    let loc = chrono::FixedOffset::east_opt(8 * 60 * 60).unwrap();
    let ts_data = [
        loc.with_ymd_and_hms(2024, 9, 2, 10, 0, 0),
        loc.with_ymd_and_hms(2024, 9, 2, 10, 5, 0),
        loc.with_ymd_and_hms(2024, 9, 2, 10, 10, 0),
        loc.with_ymd_and_hms(2024, 9, 2, 10, 15, 0),
        loc.with_ymd_and_hms(2024, 9, 2, 10, 20, 0),
    ]
    .map(|x| x.unwrap().timestamp_millis());
    let sid_data = [1, 2, 3, 4, 5].map(Some);
    let value_data = [12.5, 15.3, 9.8, 22.1, 30.0].map(Some);
    let flag_data = [0, 1, 0, 1, 0].map(Some);

    let ts_array =
        Arc::new(TimestampMillisecondArray::from(ts_data.to_vec()).with_timezone("Asia/Shanghai"))
            as _;
    let sid_array = Arc::new(Int32Array::from(sid_data.to_vec())) as _;
    let value_array = Arc::new(Float32Array::from(value_data.to_vec())) as _;
    let flag_array = Arc::new(Int8Array::from(flag_data.to_vec())) as _;

    RecordBatch::try_new(
        schema,
        [ts_array, sid_array, value_array, flag_array].into(),
    )
    .inspect_err(|e| {
        println!("Failed to build a record batch: {}", e);
        exit(1)
    })
    .unwrap()
}

pub fn make_query_binding(sid: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("sid", DataType::Int32, true)]));
    let sid_array = Arc::new(Int32Array::from(vec![Some(sid)])) as _;
    RecordBatch::try_new(schema, [sid_array].into())
        .inspect_err(|e| {
            println!("Failed to build a record batch: {}", e);
            exit(1)
        })
        .unwrap()
}
