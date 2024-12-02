use memmap2::Mmap;
use std::error::Error;
use std::fs::File;
use std::io;

/// Memory-maps the given file.
// #[inline(never)]
pub fn map_file(file_path: &str) -> io::Result<Mmap> {
    let file = File::open(file_path)?;
    let metadata = file.metadata()?;
    if metadata.len() == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "File is empty"));
    }
    unsafe { Mmap::map(&file).map_err(|e| io::Error::new(io::ErrorKind::Other, e)) }
}

/// A struct that owns the memory-mapped file and provides an iterator for lines.
pub struct CsvReader {
    mmap: Mmap,
}

impl CsvReader {
    pub fn new(file_path: &str) -> Result<Self, Box<dyn Error>> {
        let mmap = map_file(file_path)?;
        Ok(CsvReader { mmap })
    }

    pub fn lines(&self) -> LineIterator {
        LineIterator::new(&self.mmap)
    }
}

/// An iterator to return lines from the memory-mapped file.
pub struct LineIterator<'a> {
    mmap: &'a Mmap,
    start: usize,
    end: usize,
}

impl<'a> LineIterator<'a> {
    fn new(mmap: &'a Mmap) -> Self {
        LineIterator {
            mmap,
            start: 0,
            end: mmap.len(),
        }
    }
}

impl<'a> Iterator for LineIterator<'a> {
    type Item = io::Result<&'a [u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start >= self.end {
            return None;
        }

        let mmap_slice = &self.mmap[self.start..self.end];
        let mut pos = 0;

        while pos < mmap_slice.len() {
            if mmap_slice[pos] == b'\n' {
                let line = &mmap_slice[..pos];
                self.start += pos + 1;
                return Some(Ok(line));
            }
            pos += 1;
        }

        let line = mmap_slice;
        self.start = self.end;
        Some(Ok(line))
    }
}
