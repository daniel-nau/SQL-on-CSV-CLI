// src/csv_reader.rs
use std::error::Error;
use std::fs::File;
use csv::{ReaderBuilder, StringRecord};

pub fn read_csv(file_path: &str) -> Result<(Vec<String>, Vec<StringRecord>), Box<dyn Error>> {
    // Open the CSV file
    let file = File::open(file_path)?;

    // Create a CSV reader from the file
    let mut rdr = ReaderBuilder::new()
        .has_headers(true)  // Assumes the first line is headers
        .from_reader(file);

    // Get the headers (first row) to know column names
    let headers = rdr.headers()?.iter().map(|s| s.to_string()).collect();

    // Collect all records into a Vec<StringRecord>
    let records: Vec<StringRecord> = rdr.records().collect::<Result<_, _>>()?;

    Ok((headers, records))
}