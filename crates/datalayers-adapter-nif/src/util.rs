use crate::types::{append_value_to_builder, get_array_builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::display::array_value_to_string;
use arrow_flight::sql::client::PreparedStatement;
use rustler::{Error, NifResult, Term, types::list::ListIterator};
use std::sync::Arc;
use tonic::transport::Channel;

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
) -> Result<RecordBatch, Error> {
    let schema = prepared_stmt
        .parameter_schema()
        .map_err(|e| Error::Term(Box::new(e.to_string())))?
        .clone();

    let list_iterator: ListIterator = term.decode()?;
    let matrix: Vec<Vec<Term>> = list_iterator
        .map(|row_term| row_term.decode::<Vec<Term>>())
        .collect::<NifResult<Vec<Vec<Term>>>>()?;

    let mut array_columns: Vec<ArrayRef> = Vec::with_capacity(matrix[0].len());

    for (i, field) in schema.fields().iter().enumerate() {
        let data_type = field.data_type();
        let mut builder = get_array_builder(data_type);

        for row in &matrix {
            append_value_to_builder(&mut builder, row[i], data_type);
        }
        array_columns.push(builder.finish());
    }

    RecordBatch::try_new(Arc::new(schema), array_columns)
        .map_err(|e| Error::Term(Box::new(e.to_string())))
}
