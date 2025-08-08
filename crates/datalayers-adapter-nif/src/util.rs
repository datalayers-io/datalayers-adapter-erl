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
                row.push(array_value_to_string(col, row_index).unwrap_or_default());
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
    let schema = Arc::new(
        prepared_stmt
            .parameter_schema()
            .map_err(|e| Error::Term(Box::new(e.to_string())))?
            .clone(),
    );

    let list_iterator: ListIterator = term.decode()?;
    let matrix: Vec<Vec<Term>> = list_iterator
        .map(|row_term| row_term.decode::<Vec<Term>>())
        .collect::<NifResult<Vec<Vec<Term>>>>()?;

    let expected_cols = schema.fields().len();

    if matrix.is_empty() {
        if expected_cols == 0 {
            // No params expected, no params given. This is valid.
            return RecordBatch::try_new(schema, vec![])
                .map_err(|e| Error::Term(Box::new(e.to_string())));
        } else {
            // Params expected, but none given.
            return Err(Error::BadArg);
        }
    }

    // Check that all rows have the correct number of columns.
    for row in &matrix {
        if row.len() != expected_cols {
            return Err(Error::BadArg);
        }
    }

    let mut array_columns: Vec<ArrayRef> = Vec::with_capacity(expected_cols);

    for (i, field) in schema.fields().iter().enumerate() {
        let data_type = field.data_type();
        let mut builder = get_array_builder(data_type)?;

        for row in &matrix {
            // This access is now safe because of the check above.
            append_value_to_builder(&mut builder, row[i], data_type)?;
        }
        array_columns.push(builder.finish());
    }

    RecordBatch::try_new(schema, array_columns).map_err(|e| Error::Term(Box::new(e.to_string())))
}
