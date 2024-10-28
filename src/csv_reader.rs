use std::error::Error;
use std::fs::File;
use csv::ReaderBuilder;

pub fn read_csv(file_name: &str) -> Result<csv::Reader<File>, Box<dyn Error>> {
    let file = File::open(file_name)?;
    let rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
    Ok(rdr)
}
