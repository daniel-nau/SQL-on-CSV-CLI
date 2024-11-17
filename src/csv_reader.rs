use memmap2::Mmap;
use std::error::Error;
use std::fs::File;
use std::io;
use std::str::from_utf8;

/// Memory-maps the given file.
pub fn map_file(file_path: &str) -> io::Result<Mmap> {
    // Open the CSV file
    let file = File::open(file_path)?;

    // Get the metadata of the file to check its size
    let metadata = file.metadata()?;
    if metadata.len() == 0 {
        // Return an error if the file is empty
        return Err(io::Error::new(io::ErrorKind::InvalidData, "File is empty"));
    }

    // Memory map the file (unsafe, but validated)
    unsafe { Mmap::map(&file).map_err(|e| io::Error::new(io::ErrorKind::Other, e)) }
}

/// A struct that owns the memory-mapped file and provides an iterator for lines.
pub struct CsvReader {
    mmap: Mmap,
}

impl CsvReader {
    /// Creates a new CsvReader for the given file path.
    pub fn new(file_path: &str) -> Result<Self, Box<dyn Error>> {
        // Memory map the file
        let mmap = map_file(file_path)?;
        Ok(CsvReader { mmap })
    }

    /// Returns an iterator over the lines of the CSV file.
    pub fn lines(&self) -> LineIterator {
        LineIterator::new(&self.mmap)
    }
}

/// An iterator to return lines from the memory-mapped file.
pub struct LineIterator<'a> {
    mmap: &'a Mmap, // Reference to the memory-mapped file
    start: usize,   // Start position for the next line
    end: usize,     // End position of the file
}

impl<'a> LineIterator<'a> {
    /// Creates a new LineIterator for the given memory-mapped file.
    fn new(mmap: &'a Mmap) -> Self {
        LineIterator {
            mmap,
            start: 0,
            end: mmap.len(),
        }
    }
}

impl<'a> Iterator for LineIterator<'a> {
    type Item = io::Result<String>;

    /// Returns the next line from the memory-mapped file.
    fn next(&mut self) -> Option<Self::Item> {
        // Check if the current position is beyond the end of the file
        if self.start >= self.end {
            return None; // No more lines to read
        }

        let start = self.start;
        // Find the next newline character (`\n`)
        if let Some(pos) = self.mmap[start..].iter().position(|&b| b == b'\n') {
            // Extract the line from the current position to the newline character
            let line = &self.mmap[start..start + pos];
            // Update the start position to the character after the newline
            self.start += pos + 1;
            // Convert the line to a UTF-8 string and return it
            match from_utf8(line) {
                Ok(s) => Some(Ok(s.to_string())),
                Err(e) => Some(Err(io::Error::new(io::ErrorKind::InvalidData, e))),
            }
        } else {
            // No more newlines, return the last line (if any)
            let line = &self.mmap[start..];
            // Set start to end to indicate we're done
            self.start = self.end;
            // Convert the line to a UTF-8 string and return it
            match from_utf8(line) {
                Ok(s) => Some(Ok(s.to_string())),
                Err(e) => Some(Err(io::Error::new(io::ErrorKind::InvalidData, e))),
            }
        }
    }
}
