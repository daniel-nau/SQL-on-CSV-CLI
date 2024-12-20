/*
    TODO:
    - Make a final build
        - cargo fmt and cargo clippy
        - Remove #[inline(never)] for final build
        - Optimize the build (strip, lto, etc.) in Cargo.toml
        - cargo build --release
        - Strip the release binary
    - Do testing with smaller file and compare with DuckDB output
    - Do final benchmarking on Isengard
        - More runs
        - Compare final version with DuckDB and V1
    - Put on GitHub and create a README

    Future work:
    - Better error handling
    - Put all of the file processing in a separate file to refactor
    - Pre-compute WHERE clause for aggregates to prevent redundant splitting in check_condition()
    - Improve aggregate functions
    - Add better string support
    - Add more SQL features
*/

use memchr::memchr_iter;
use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::io::{self, Write};

// Modules for handling specific functionalities
mod aggregates;
mod condition_checker;
mod csv_reader;
mod sql_parser;

/// Main entry point for the program.
// #[inline(never)]
fn main() -> Result<(), Box<dyn Error>> {
    // Parse command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} --query \"<SQL Query>\"", args[0]);
        return Err("Invalid number of arguments".into());
    }

    let query_flag: &str = &args[1];
    let sql_query: &str = &args[2];

    if query_flag != "--query" {
        eprintln!("First argument must be --query");
        return Err("First argument must be --query".into());
    }

    // Parse the SQL query
    match sql_parser::parse_query(sql_query) {
        Ok(mut command) => {
            match (
                command.columns.len(),
                command.columns.first(),
                &command.condition,
            ) {
                // Handle "SELECT COUNT(*) FROM <file> WHERE <condition>"
                (1, Some(col), Some(condition)) if col == "COUNT(*)" => {
                    let count = count_with_condition(&command.data_file, condition)?;
                    println!("COUNT(*)");
                    println!("{}", count);
                }
                // Handle "SELECT COUNT(*) FROM <file>"
                (1, Some(col), _none) if col == "COUNT(*)" => {
                    let count = count_star(&command.data_file)?;
                    println!("COUNT(*)");
                    println!("{}", count);
                }
                // Handle "SELECT * FROM <file> WHERE <condition>"
                (1, Some(col), Some(_)) if col == "*" => {
                    handle_select_star_with_condition(&command)?;
                }
                // Handle "SELECT * FROM <file>"
                (1, Some(col), _none) if col == "*" => {
                    return select_star(&command.data_file);
                }
                // Handle other queries
                _ => handle_complex_query(&mut command)?,
            }
        }
        Err(err) => {
            eprintln!("Error parsing query: {}", err);
        }
    }
    Ok(())
}

fn extract_field<'a>(
    record: &'a [u8],
    headers: &[String],
    required_header: &str,
) -> Option<&'a str> {
    if let Some(index) = headers.iter().position(|h| h == required_header) {
        for (current_index, field) in record.split(|&b| b == b',').enumerate() {
            if current_index == index {
                return Some(std::str::from_utf8(field).unwrap());
            }
        }
    }
    None
}

fn extract_fields<'a>(
    record: &'a [u8],
    headers: &[String],
    required_headers: &[String],
) -> Vec<&'a str> {
    let mut fields = Vec::new();
    for required_header in required_headers {
        if let Some(index) = headers.iter().position(|h| h == required_header) {
            for (current_index, field) in record.split(|&b| b == b',').enumerate() {
                if current_index == index {
                    fields.push(std::str::from_utf8(field).unwrap());
                    break;
                }
            }
        }
    }
    fields
}

fn extract_required_headers(headers: &[String], condition: &str) -> Vec<String> {
    let mut required_headers_set = HashSet::new();
    let parts: Vec<&str> = condition.split_whitespace().collect();

    for part in parts {
        if part != "AND"
            && part != "OR"
            && !part
                .chars()
                .all(|c| c.is_numeric() || c == '.' || c == '<' || c == '>' || c == '=' || c == '!')
        {
            required_headers_set.insert(part.to_string());
        }
    }

    headers
        .iter()
        .filter(|header| required_headers_set.contains(*header))
        .cloned()
        .collect()
}

// #[inline(never)]
fn get_headers<'a>(
    line_iter: &mut impl Iterator<Item = io::Result<&'a [u8]>>,
) -> Result<Vec<String>, Box<dyn Error>> {
    if let Some(Ok(header_line)) = line_iter.next() {
        // Split the header line into individual column names and collect into a Vec<String>
        Ok(header_line
            .split(|&b| b == b',')
            .map(|s| String::from_utf8_lossy(s).trim().to_string())
            .collect::<Vec<String>>())
    } else {
        // Return an error if the headers cannot be read
        Err("Failed to read headers".into())
    }
}

/// Counts the number of rows in the CSV file (excluding the header row).
fn count_star(file_path: &str) -> Result<usize, Box<dyn Error>> {
    let mmap = csv_reader::map_file(file_path)?; // Memory-map the file
    // let line_count = mmap.iter().filter(|&&b| b == b'\n').count(); // Count newline characters
    let line_count = memchr_iter(b'\n', &mmap).count(); // Count newline characters using memchr

    // Check if the last byte is a newline character
    let last_byte_is_newline = mmap.last() == Some(&b'\n');

    // If the last byte is not a newline, increment the line count by one
    let total_lines = if last_byte_is_newline {
        line_count
    } else {
        line_count + 1
    };

    Ok(total_lines - 1) // Exclude the header
}

/// Counts rows in the CSV file that satisfy a given condition.
fn count_with_condition(file_path: &str, condition: &str) -> Result<usize, Box<dyn Error>> {
    let csv_reader = csv_reader::CsvReader::new(file_path)?;
    let mut count = 0;

    let mut line_iter = csv_reader.lines();
    let headers = get_headers(&mut line_iter)?;

    let parsed_command = sql_parser::ParsedCommand {
        columns: vec![],
        data_file: file_path.to_string(),
        condition: Some(condition.to_string()),
    };

    // Check if there is only one condition
    let single_condition = !condition.contains("AND") && !condition.contains("OR");

    if single_condition {
        // Determine the required field from the condition
        let parts: Vec<&str> = condition.split_whitespace().collect(); // Split the condition/WHERE clause
        let required_header = parts[0]; // Get the first part of the condition, the column name
        let required_headers = vec![required_header.to_string()];

        for result in line_iter {
            let record = result?;
            if let Some(value) = extract_field(record, &headers, required_header) {
                let required_field = vec![value];
                if condition_checker::evaluate_condition(
                    condition,
                    &required_headers,
                    &required_field,
                ) {
                    count += 1;
                }
            }
        }
    } else {
        let required_headers = extract_required_headers(&headers, condition);
        // println!("{:?}", required_headers);
        // Process and count records matching the compound condition
        for result in line_iter {
            let record = result?;
            let fields = extract_fields(record, &headers, &required_headers);

            if condition_checker::check_condition(&parsed_command, &required_headers, &fields) {
                count += 1;
            }
        }
    }

    Ok(count)
}

/// Outputs the entire CSV file content to `stdout`.
fn select_star(file_path: &str) -> Result<(), Box<dyn Error>> {
    let mmap = csv_reader::map_file(file_path)?; // Memory-map the file
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    handle.write_all(&mmap)?; // Write directly to `stdout`
    handle.flush()?; // Ensure all data is written
    Ok(())
}

/// Handles queries like "SELECT * FROM <file> WHERE <condition>".
fn handle_select_star_with_condition(
    command: &sql_parser::ParsedCommand,
) -> Result<(), Box<dyn Error>> {
    // Create a CsvReader for the given file path
    let csv_reader = csv_reader::CsvReader::new(&command.data_file)?;
    let mut line_iter = csv_reader.lines();

    let headers = get_headers(&mut line_iter)?;
    println!("{}", headers.join(","));

    // Check if there is only one condition
    let single_condition = command
        .condition
        .as_deref()
        .map_or(false, |cond| !cond.contains("AND") && !cond.contains("OR"));

    let required_headers = extract_required_headers(&headers, command.condition.as_ref().unwrap());

    if single_condition {
        // Process each record (line) in the CSV file
        for result in line_iter {
            // Get the next line from the iterator
            let record = result?;
            let fields = extract_fields(record, &headers, &required_headers);

            // Check if the record matches the condition specified in the command
            if condition_checker::evaluate_condition(
                command.condition.as_ref().unwrap(),
                &required_headers,
                &fields,
            ) {
                // Convert the byte slice to a string and print the entire record
                let record_str = std::str::from_utf8(record).unwrap();
                println!("{}", record_str);
            }
        }
    } else {
        // Process each record (line) in the CSV file
        for result in line_iter {
            // Get the next line from the iterator
            let record = result?;
            let fields = extract_fields(record, &headers, &required_headers);

            // Check if the record matches the condition specified in the command
            if condition_checker::check_condition(command, &required_headers, &fields) {
                // Convert the byte slice to a string and print the entire record
                let record_str = std::str::from_utf8(record).unwrap();
                println!("{}", record_str);
            }
        }
    }

    Ok(())
}

/// Handles more complex queries with aggregate functions or column selections.
// #[inline(never)]
fn handle_complex_query(command: &mut sql_parser::ParsedCommand) -> Result<(), Box<dyn Error>> {
    let mut csv_reader = csv_reader::CsvReader::new(&command.data_file)?;
    let is_aggregate_query = command
        .columns
        .iter()
        .any(|col| sql_parser::is_aggregate_function(col.as_str()));

    if is_aggregate_query {
        handle_aggregate_query(command, &mut csv_reader)?;
    } else {
        handle_column_selection_query(command, &mut csv_reader)?;
    }

    Ok(())
}

/// Handles queries with aggregate functions (e.g., SUM, AVG, MIN).
// #[inline(never)]
fn handle_aggregate_query(
    command: &mut sql_parser::ParsedCommand,
    csv_reader: &mut csv_reader::CsvReader,
) -> Result<(), Box<dyn Error>> {
    let mut aggregates = aggregates::Aggregates::new();

    let mut line_iter = csv_reader.lines();
    let headers = get_headers(&mut line_iter)?;

    // Special case: Change "COUNT(*)" to "COUNT(<first_column>)"
    if command.columns.contains(&"COUNT(*)".to_string()) {
        if let Some(first_column) = headers.first() {
            command.columns = command
                .columns
                .iter()
                .map(|col| {
                    if col == "COUNT(*)" {
                        format!("COUNT({})", first_column)
                    } else {
                        col.clone()
                    }
                })
                .collect();
        }
    }

    // Register aggregate functions
    for column in &command.columns {
        if column.starts_with("SUM(") {
            aggregates.add_function(column.clone(), Box::new(aggregates::Sum::new()));
        } else if column.starts_with("AVG(") {
            aggregates.add_function(column.clone(), Box::new(aggregates::Avg::new()));
        } else if column.starts_with("MIN(") {
            aggregates.add_function(column.clone(), Box::new(aggregates::Min::new()));
        } else if column.starts_with("MAX(") {
            aggregates.add_function(column.clone(), Box::new(aggregates::Max::new()));
        } else if column.starts_with("COUNT(") {
            aggregates.add_function(column.clone(), Box::new(aggregates::Count::new()));
        }
    }
    // println!("{:?}", aggregates);

    // Create a map from column names to their indices for quick lookup
    let column_indices: std::collections::HashMap<_, _> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| (h.clone(), i))
        .collect();

    // Apply aggregates to matching records
    if let Some(_condition) = &command.condition {
        let required_headers =
            extract_required_headers(&headers, command.condition.as_ref().unwrap());
        // Check if there is only one condition
        let single_condition = command
            .condition
            .as_deref()
            .map_or(false, |cond| !cond.contains("AND") && !cond.contains("OR"));

        if single_condition {
            // Process each record (line) in the CSV file
            for result in line_iter {
                let record = result?;
                let fields = extract_fields(record, &headers, &required_headers);

                // Check if the record matches the single condition
                if condition_checker::evaluate_condition(
                    command.condition.as_ref().unwrap(),
                    &required_headers,
                    &fields,
                ) {
                    let record: Vec<&str> = record
                        .split(|&b| b == b',')
                        .map(|s| std::str::from_utf8(s).unwrap())
                        .collect();
                    for (func, agg) in aggregates.functions.iter_mut() {
                        if let Some(column_name) = func.split(&['(', ')'][..]).nth(1) {
                            if let Some(&index) = column_indices.get(column_name) {
                                if let Ok(value) = record[index].parse::<f64>() {
                                    agg.apply(value);
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // Process each record (line) in the CSV file
            for result in line_iter {
                let record = result?;
                let fields = extract_fields(record, &headers, &required_headers);

                // Check if the record matches the compound condition
                if condition_checker::check_condition(command, &required_headers, &fields) {
                    let record: Vec<&str> = record
                        .split(|&b| b == b',')
                        .map(|s| std::str::from_utf8(s).unwrap())
                        .collect();
                    for (func, agg) in aggregates.functions.iter_mut() {
                        if let Some(column_name) = func.split(&['(', ')'][..]).nth(1) {
                            if let Some(&index) = column_indices.get(column_name) {
                                if let Ok(value) = record[index].parse::<f64>() {
                                    agg.apply(value);
                                }
                            }
                        }
                    }
                }
            }
        }
    } else {
        // No condition
        for result in line_iter {
            let record = result?;
            let record: Vec<&str> = record
                .split(|&b| b == b',')
                .map(|s| std::str::from_utf8(s).unwrap())
                .collect();
            for (func, agg) in aggregates.functions.iter_mut() {
                if let Some(column_name) = func.split(&['(', ')'][..]).nth(1) {
                    if let Some(&index) = column_indices.get(column_name) {
                        if let Ok(value) = record[index].parse::<f64>() {
                            agg.apply(value);
                        }
                    }
                }
            }
        }
    }

    // Output aggregate results
    let results = aggregates.results(&command.columns);
    let mut labels = Vec::new();
    let mut values = Vec::new();
    for column in &command.columns {
        let label = if column.starts_with("COUNT(") && column.contains(&headers[0]) {
            "COUNT(*)".to_string()
        } else {
            column.clone()
        };
        let value = results
            .get(column)
            .map_or("NaN".to_string(), |v| v.to_string());
        // println!("{}: {}", label, value);
        labels.push(label);
        values.push(value);
    }

    println!("{}", labels.join(","));
    println!("{}", values.join(","));

    Ok(())
}

/// Handles column selection queries (e.g., "SELECT col1, col2").
// #[inline(never)]
fn handle_column_selection_query(
    command: &sql_parser::ParsedCommand,
    csv_reader: &mut csv_reader::CsvReader,
) -> Result<(), Box<dyn Error>> {
    let mut line_iter = csv_reader.lines();
    let headers = get_headers(&mut line_iter)?;

    // Prepare the buffered writer for faster output
    let stdout = std::io::stdout();
    let mut writer = std::io::BufWriter::new(stdout.lock());

    // Print the selected columns as the header
    writeln!(writer, "{}", command.columns.join(","))?;

    let selected_headers: Vec<String> =
        extract_required_headers(&headers, &(command.columns.join(" ")));

    // Map column names to their indexes
    let column_indexes: Vec<_> = command
        .columns
        .iter()
        .filter_map(|col| selected_headers.iter().position(|h| h.trim() == col))
        .collect();

    // Preallocate a buffer to avoid reallocations, based on column_indexes size
    let mut selected_fields_buffer = Vec::with_capacity(column_indexes.len());

    // Process records based on whether there is a condition or not
    if let Some(_condition) = &command.condition {
        // Get the headers from the WHERE clause
        let checked_headers =
            extract_required_headers(&headers, command.condition.as_ref().unwrap());

        // Check if there is only one condition
        let single_condition = command
            .condition
            .as_deref()
            .map_or(false, |cond| !cond.contains("AND") && !cond.contains("OR"));

        // println!("{:?}", selected_headers);

        if single_condition {
            // There is a condition
            for result in line_iter {
                let record = result?;
                let checked_fields = extract_fields(record, &headers, &checked_headers);

                if condition_checker::evaluate_condition(
                    command.condition.as_ref().unwrap(),
                    &checked_headers,
                    &checked_fields,
                ) {
                    let selected_fields = extract_fields(record, &headers, &selected_headers);

                    // Select the fields based on the column indexes
                    for &index in &column_indexes {
                        selected_fields_buffer.push(selected_fields[index].as_bytes());
                    }

                    // Write the selected fields directly to the writer
                    for (i, field) in selected_fields_buffer.iter().enumerate() {
                        if i > 0 {
                            writer.write_all(b",")?;
                        }
                        writer.write_all(field)?;
                    }
                    writer.write_all(b"\n")?;

                    // Reset the buffer for the next line by truncating it
                    selected_fields_buffer.truncate(0); // More efficient than clear() for reusing capacity
                }
            }

            writer.flush()?; // Ensure all output is written to stdout
        } else {
            // There is a condition
            for result in line_iter {
                let record = result?;
                let checked_fields = extract_fields(record, &headers, &checked_headers);

                if condition_checker::check_condition(command, &checked_headers, &checked_fields) {
                    let selected_fields = extract_fields(record, &headers, &selected_headers);

                    // Select the fields based on the column indexes
                    for &index in &column_indexes {
                        selected_fields_buffer.push(selected_fields[index].as_bytes());
                    }

                    // Write the selected fields directly to the writer
                    for (i, field) in selected_fields_buffer.iter().enumerate() {
                        if i > 0 {
                            writer.write_all(b",")?;
                        }
                        writer.write_all(field)?;
                    }
                    writer.write_all(b"\n")?;

                    // Reset the buffer for the next line by truncating it
                    selected_fields_buffer.truncate(0); // More efficient than clear() for reusing capacity
                }
            }
        }

        writer.flush()?; // Ensure all output is written to stdout
    } else {
        // No condition
        for result in line_iter {
            let record = result?;
            let selected_fields = extract_fields(record, &headers, &selected_headers);

            // Select the fields based on the column indexes
            for &index in &column_indexes {
                selected_fields_buffer.push(selected_fields[index].as_bytes());
            }

            // Write the selected fields directly to the writer
            for (i, field) in selected_fields_buffer.iter().enumerate() {
                if i > 0 {
                    writer.write_all(b",")?;
                }
                writer.write_all(field)?;
            }
            writer.write_all(b"\n")?;

            // Reset the buffer for the next line by truncating it
            selected_fields_buffer.truncate(0); // More efficient than clear() for reusing capacity
        }

        writer.flush()?; // Ensure all output is written to stdout
    }

    Ok(())
}
