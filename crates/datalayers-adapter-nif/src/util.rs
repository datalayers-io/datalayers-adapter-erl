use arrow_array::RecordBatch;
use arrow_cast::display::array_value_to_string;

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
