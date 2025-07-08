use arrow_array::builder::{
    Float32Builder, Int32Builder, Int8Builder, StringBuilder, TimestampMillisecondBuilder,
};
use arrow_array::ArrayRef;
use arrow_array::{
    Float32Array, Int8Array, Int32Array, RecordBatch, TimestampMillisecondArray,
};
use arrow_cast::display::array_value_to_string;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use arrow_flight::{
    sql::client::PreparedStatement,
};
use rustler::{NifResult, Term, types::list::ListIterator};
use std::sync::Arc;
use tonic::transport::Channel;
use chrono::TimeZone;

pub fn record_batch_to_term(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut result: Vec<Vec<String>> = Vec::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row: Vec<String> = Vec::new();
            for col in batch.columns() {
                // TODO: better type mapping
                row.push(array_value_to_string(col, row_index).unwrap());
            }
            result.push(row);
        }
    }
    result
}

pub fn params_to_record_batch(
    prepared_stmt: &PreparedStatement<Channel>,
    term: Term,
) -> NifResult<RecordBatch> {
    let schema = prepared_stmt.parameter_schema().map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?.clone();
    let list_iterator: ListIterator = term.decode()?;
    let rows: Vec<Vec<Term>> = list_iterator
        .map(|row_term| row_term.decode::<Vec<Term>>())
        .collect::<NifResult<Vec<Vec<Term>>>>()?;

    if rows.is_empty() {
        return Ok(RecordBatch::try_new(Arc::new(schema), vec![]).unwrap());
    }

    let num_rows = rows.len();
    let num_columns = rows[0].len();

    if num_columns != schema.fields().len() {
        return Err(rustler::Error::BadArg); // Number of columns does not match schema
    }

    let mut arrow_columns: Vec<ArrayRef> = Vec::with_capacity(num_columns);


    println!("Schema: {:?}\n", schema);
    println!("==============================\n");
    println!("Converting {} rows with {} columns to RecordBatch\n", num_rows, num_columns);
    println!("==============================\n");
    println!("Rows: {:?}", rows);
    for (i, field) in schema.fields().iter().enumerate() {
        let mut column_data: Vec<Term> = Vec::with_capacity(num_rows);
        for row in &rows {
            if row.len() != num_columns {
                return Err(rustler::Error::BadArg); // Rows with different lengths
            }
            column_data.push(row[i]);
        }

        let array = match field.data_type() {
            DataType::Int8 => {
                let mut builder = Int8Builder::new();
                for term in column_data {
                    builder.append_value(term.decode::<i8>()?);
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Int32 => {
                let mut builder = Int32Builder::new();
                for term in column_data {
                    builder.append_value(term.decode::<i32>()?);
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Float32 => {
                let mut builder = Float32Builder::new();
                for term in column_data {
                    builder.append_value(term.decode::<f32>()?);
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::new();
                for term in column_data {
                    builder.append_value(term.decode::<String>()?);
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let mut builder = TimestampMillisecondBuilder::new();
                for term in column_data {
                    builder.append_value(term.decode::<i64>()?);
                }
                Arc::new(builder.finish()) as ArrayRef
            }
            _ => return Err(rustler::Error::BadArg), // Unsupported data type
        };
        arrow_columns.push(array);
    }

    RecordBatch::try_new(Arc::new(schema), arrow_columns)
        .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))
}


#[allow(dead_code)]
pub fn example_make_insert_binding() -> RecordBatch {
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
    let ts_data: Vec<_> = vec![
        loc.with_ymd_and_hms(2024, 9, 2, 10, 0, 0),
        loc.with_ymd_and_hms(2024, 9, 2, 10, 5, 0),
        loc.with_ymd_and_hms(2024, 9, 2, 10, 10, 0),
        loc.with_ymd_and_hms(2024, 9, 2, 10, 15, 0),
        loc.with_ymd_and_hms(2024, 9, 2, 10, 20, 0),
    ].into_iter().map(|x| x.unwrap().timestamp_millis()).collect();
    let sid_data: Vec<_> = vec![1, 2, 3, 4, 5].into_iter().map(Some).collect();
    let value_data: Vec<_> = vec![12.5, 15.3, 9.8, 22.1, 30.0].into_iter().map(Some).collect();
    let flag_data: Vec<_> = vec![0, 1, 0, 1, 0].into_iter().map(Some).collect();

    let ts_array =
        Arc::new(TimestampMillisecondArray::from(ts_data).with_timezone("Asia/Shanghai"))
            as _;
    let sid_array = Arc::new(Int32Array::from(sid_data)) as _;
    let value_array = Arc::new(Float32Array::from(value_data)) as _;
    let flag_array = Arc::new(Int8Array::from(flag_data)) as _;

    RecordBatch::try_new(
        schema,
        vec![ts_array, sid_array, value_array, flag_array],
    ).map_err(|e| rustler::Error::Term(Box::new(e.to_string())))
    .unwrap()
}
