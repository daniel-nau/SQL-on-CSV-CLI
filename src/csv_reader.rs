use std::error::Error;
use std::fs::File;
// use csv::{ReaderBuilder, Reader, StringRecord};
use csv::ReaderBuilder;
use memmap2::Mmap;
// use std::io::{self, Cursor};
use std::io::{self};

// Helper function to map a file safely
pub fn map_file(file_path: &str) -> io::Result<Mmap> {
    let file = File::open(file_path)?;

    // Safety: ensure that the file is valid and we can safely map it
    let metadata = file.metadata()?;
    if metadata.len() == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "File is empty"));
    }

    // Memory map the file (unsafe, but validated)
    unsafe { Mmap::map(&file).map_err(|e| io::Error::new(io::ErrorKind::Other, e)) }
}

// struct CsvReaderWithMmap {
//     mmap: Mmap,
//     reader: Reader<Cursor<&'static [u8]>>,
// }

// impl CsvReaderWithMmap {
//     // Method to iterate over records in the CSV file
//     pub fn records(&mut self) -> csv::Result<impl Iterator<Item = csv::Result<StringRecord>>> {
//         Ok(self.reader.records())
//     }
// }

// pub fn read_csv(file_path: &str) -> Result<(Vec<String>, CsvReaderWithMmap), Box<dyn Error>> {
pub fn read_csv(file_path: &str) -> Result<(Vec<String>, csv::Reader<File>), Box<dyn Error>> {
    // XXX V1 .0968 seconds
    // Open the CSV file
    let file = File::open(file_path)?;

    // Create a CSV reader with flexible options
    let mut rdr = ReaderBuilder::new()
        .has_headers(true) // Assuming the CSV has headers
        // .flexible(true)    // Allow for flexible column formats
        .from_reader(file);

    // Get the headers (first row) to know column names
    let headers = rdr
        .headers()?
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    Ok((headers, rdr))

    // XXX V2 .0944 seconds
    // // Open the CSV file
    // let file = File::open(file_path)?;

    // // Memory-map the file
    // let mmap = unsafe { Mmap::map(&file)? };

    // // Create a buffered reader from the memory-mapped data
    // // let buf_reader = BufReader::new(io::Cursor::new(&mmap));
    // let buf_reader = BufReader::new(io::Cursor::new(mmap.to_vec()));  // Copy the data into a Vec<u8> to own it

    // // Create the CSV reader from the buffered reader
    // let mut rdr = ReaderBuilder::new()
    //     .has_headers(true)
    //     .from_reader(buf_reader);

    // // Get the headers (first row) to know column names
    // let headers = rdr.headers()?.iter().map(|s| s.to_string()).collect::<Vec<String>>();

    // Ok((headers, rdr))

    // XXX V3
    // // Open the CSV file
    // let file = File::open(file_path)?;

    // // Memory-map the file
    // let mmap = unsafe { Mmap::map(&file)? };
    // let cursor = Cursor::new(mmap.as_ref());

    // // Create the CSV reader directly from the memory-mapped file (no need for BufReader)
    // let mut rdr = ReaderBuilder::new()
    //     .has_headers(true) // Assuming the CSV has headers
    //     .from_reader(cursor); // Directly use the memory-mapped slice

    // // Get the headers (first row) to know column names
    // let headers = rdr.headers()?.iter().map(|s| s.to_string()).collect::<Vec<String>>();

    // Ok((headers, CsvReaderWithMmap { mmap, reader: rdr }))
}
