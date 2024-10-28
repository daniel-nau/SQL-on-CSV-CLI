use crate::aggregates::{Aggregate};
use crate::csv_reader;
use std::error::Error;
use std::collections::HashMap;

pub fn execute_query(aggregates: &mut crate::aggregates::Aggregates, file_name: &str) -> Result<(), Box<dyn Error>> {
    let mut rdr = csv_reader::read_csv(file_name)?;

    // Get headers to map column names to indices
    let headers = rdr.headers()?.clone();
    let header_map: HashMap<_, _> = headers.iter().enumerate().map(|(i, h)| (h.to_string(), i)).collect();

    for result in rdr.records() {
        let record = result?;

        for (col_name, agg) in &mut aggregates.functions {
            // Get the index from the header map
            if let Some(&index) = header_map.get(col_name) {
                // Now we can safely get the value at that index
                if let Some(value) = record.get(index) {
                    if let Ok(value) = value.parse::<f64>() {
                        agg.apply(value);
                    }
                }
            } else {
                eprintln!("Column name '{}' does not exist in the CSV file.", col_name);
            }
        }
    }

    Ok(())
}
