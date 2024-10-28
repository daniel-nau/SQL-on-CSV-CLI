use std::error::Error;
use std::fs::File;
use csv::ReaderBuilder;

pub fn read_csv(file_path: &str) -> Result<(Vec<String>, csv::Reader<File>), Box<dyn Error>> {
    // Open the CSV file
    let file = File::open(file_path)?;

    // Create a CSV reader with flexible options
    let mut rdr = ReaderBuilder::new()
        .has_headers(true) // Assuming the CSV has headers
        // .flexible(true)    // Allow for flexible column formats
        .from_reader(file);

    // Get the headers (first row) to know column names
    let headers = rdr.headers()?.iter().map(|s| s.to_string()).collect::<Vec<String>>();

    Ok((headers, rdr))
}
