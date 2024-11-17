/*
    TODO:
    - #1. Testing to make sure the outputs are correct
    - #2. Do benchmarking (increase # of runs for mine and DuckDB)
    - #3. Generate flamegraphs for profiling and keep tracks of what I did to optimize for my report (different versions/executable names?)
    - #4. Do improvements and optimizations (UPDATE CARGO.TOML VERSION AND DO cargo pkgid TO SEE VERSIONS)
    - WORK ON READER! (SEE IF I CAN USE BufReader)
    - Double check outputs (COUNT(*) and general format)
    - https://users.rust-lang.org/t/how-can-i-input-and-output-contents-fastest-in-output-stream-in-a-oj-system/61054/2
    - IN REPORT AND SLIDES, SHOW THAT TIME IS "REAL" TIME
    - Do smaller files to make sure the output is the same
    - Add support for SELECT * with conditions
    - Look into making ReaderBuilder more efficient
    - Use float32 instead of float64?
    - And spaces in strings of column names? (csv and query support)
    - Map aggregate function to column name (or vice versa) and then map to column index
    - Do more testing and double check to see which parts of the code are slow compared to csvsql
        - Max/Min/Avg/Sum kinda slow
    - See if SELECT without WHERE still uses check_condition?
    - Jump to field we are comparing to with the WHERE clause (map column names to index?)
    - Add support for strings
    - Print out data at the end or as it's processed? Speed vs. memory?
    - Ensure robust error handling
    - Add types?
    - Refactor code into smaller, more modular functions and clean up code
    - Optimize and explore alternatives for better performance ()
        - Consider avoiding Vecs where possible
        - Use references instead of cloning strings
        - Look into other stuff
        - Look into reader buffer size
        - rustfmt and clippy: https://www.reddit.com/r/rust/comments/w25npu/how_does_rust_optimize_this_code_to_increase_the/
            - cargo fmt and cargo clippy
        - Research other optimizations: https://users.rust-lang.org/t/can-anyone-share-tips-for-optimize-coding-in-rust/45406/2
    - Document the code and provide examples
    - Prepare for release and strip the binary ([profile.release] optimizations (opt-level))
    - Run thorough testing and benchmarking (add automated tests?)
        - Find alternative CSV files to test with
*/

use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{self, Write};

// Modules for handling specific functionalities
mod aggregates;
mod condition_checker;
mod csv_reader;
mod sql_parser;

/// Main entry point for the program.
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
                command.columns.get(0),
                &command.condition,
            ) {
                // Handle "SELECT COUNT(*) FROM <file> WHERE <condition>"
                (1, Some(col), Some(condition)) if col == "COUNT(*)" => {
                    let count = count_with_condition(&command.data_file, condition)?;
                    println!("COUNT(*): {}", count);
                }
                // Handle "SELECT COUNT(*) FROM <file>"
                (1, Some(col), _none) if col == "COUNT(*)" => {
                    let count = count_star(&command.data_file)?;
                    println!("COUNT(*): {}", count);
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

/// Counts the number of rows in the CSV file (excluding the header row).
fn count_star(file_path: &str) -> Result<usize, Box<dyn Error>> {
    let mmap = csv_reader::map_file(&file_path)?; // Memory-map the file
    let line_count = mmap.iter().filter(|&&b| b == b'\n').count(); // Count newline characters
    Ok(line_count - 1) // Exclude the header
}

/// Counts rows in the CSV file that satisfy a given condition.
fn count_with_condition(file_path: &str, condition: &str) -> Result<usize, Box<dyn Error>> {
    let (headers, mut rdr) = csv_reader::read_csv(file_path)?; // Read the CSV file and headers
    let mut count = 0;

    // Process and count records matching the condition
    for result in rdr.records() {
        let record = result?;
        let parsed_command = sql_parser::ParsedCommand {
            columns: vec![],
            data_file: file_path.to_string(),
            condition: Some(condition.to_string()),
        };
        if condition_checker::check_condition(&parsed_command, &headers, &record) {
            count += 1;
        }
    }
    Ok(count)
}

/// Outputs the entire CSV file content to `stdout`.
fn select_star(file_path: &str) -> Result<(), Box<dyn Error>> {
    let mmap = csv_reader::map_file(&file_path)?; // Memory-map the file
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
    let (headers, mut rdr) = csv_reader::read_csv(&command.data_file)?;
    println!("{}", headers.join(",")); // Print headers

    // Process records matching the condition
    for result in rdr.records() {
        let record = result?;
        if condition_checker::check_condition(command, &headers, &record) {
            let row: Vec<&str> = record.iter().collect();
            println!("{}", row.join(","));
        }
    }

    Ok(())
}

/// Handles more complex queries with aggregate functions or column selections.
fn handle_complex_query(command: &mut sql_parser::ParsedCommand) -> Result<(), Box<dyn Error>> {
    let (headers, mut rdr) = csv_reader::read_csv(&command.data_file)?;
    let is_aggregate_query = command
        .columns
        .iter()
        .any(|col| sql_parser::is_aggregate_function(col.as_str()));

    // Special case: Change "COUNT(*)" to "COUNT(<first_column>)"
    if command.columns.contains(&"COUNT(*)".to_string()) {
        let first_column = headers.get(0).unwrap_or(&String::new()).clone();
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

    if is_aggregate_query {
        handle_aggregate_query(command, &headers, &mut rdr)?;
    } else {
        handle_column_selection_query(command, &headers, &mut rdr)?;
    }

    Ok(())
}

/// Handles queries with aggregate functions (e.g., SUM, AVG, MIN).
fn handle_aggregate_query(
    command: &sql_parser::ParsedCommand,
    headers: &[String],
    rdr: &mut csv::Reader<File>,
) -> Result<(), Box<dyn Error>> {
    let mut aggregates = aggregates::Aggregates::new();

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

    // Apply aggregates to matching records
    for result in rdr.records() {
        let record = result?;
        if condition_checker::check_condition(command, headers, &record) {
            for (i, field) in record.iter().enumerate() {
                if let Ok(value) = field.parse::<f64>() {
                    for func in &command.columns {
                        if func.contains(&headers[i]) {
                            if let Some(agg) = aggregates.functions.get_mut(func) {
                                agg.apply(value);
                            }
                        }
                    }
                }
            }
        }
    }

    // Output aggregate results
    let results = aggregates.results(&command.columns);
    for column in &command.columns {
        let label = if column.starts_with("COUNT(") && column.contains(&headers[0]) {
            "COUNT(*)".to_string()
        } else {
            column.clone()
        };
        let value = results
            .get(column)
            .map_or("NaN".to_string(), |v| v.to_string());
        println!("{}: {}", label, value);
    }

    Ok(())
}

/// Handles column selection queries (e.g., "SELECT col1, col2").
fn handle_column_selection_query(
    command: &sql_parser::ParsedCommand,
    headers: &[String],
    rdr: &mut csv::Reader<File>,
) -> Result<(), Box<dyn Error>> {
    let column_indexes: Vec<_> = command
        .columns
        .iter()
        .filter_map(|col| headers.iter().position(|h| h == col))
        .collect();

    println!("{}", command.columns.join(",")); // Print selected columns' headers

    // Process records, optionally filtering based on the condition
    for result in rdr.records() {
        let record = result?;
        if command.condition.is_none()
            || condition_checker::check_condition(command, headers, &record)
        {
            let selected_fields: Vec<&str> = column_indexes
                .iter()
                .map(|&index| record.get(index).unwrap_or(""))
                .collect();
            println!("{}", selected_fields.join(","));
        }
    }

    Ok(())
}
