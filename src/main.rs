/*
    TODO:
    - #1. Testing to make sure the outputs are correct
    - #2. Do benchmarking (increase # of runs for mine and DuckDB)
    - #3. Generate flamegraphs for profiling and keep tracks of what I did to optimize for my report (different versions/executable names?)
    - #4. Do improvements and optimizations (UPDATE CARGO.TOML VERSION AND DO cargo pkgid TO SEE VERSIONS)
    - Double check outputs (COUNT(*) and general format)
    - Remove spaces after commas in output
    - IN REPORT AND SLIDES, SHOW THAT TIME IS "REAL" TIME
    - Do smaller files to make sure the output is the same
    - Add support for SELECT * with conditions
    - Do something different than wc -l and cat for COUNT(*) and SELECT * respectively
    - Look into making ReaderBuilder more efficient
    - Use float32 instead of float64?
    - Make it to be able to have a path with a ../ or ./ at the beginning and _ in file name like original Chicago crime data name
        - And spaces in strings of column names? (csv and query support) 
    - Map aggregate function to column name (or vice versa) and then map to column index
    - Do more testing and double check to see which parts of the code are slow compared to csvsql
        - Max/Min/Avg/Sum kinda slow
    - Use BufReader for COUNT(*) and SELECT *?
    - See if SELECT without WHERE still uses check_condition?
    - Jump to field we are comparing to with the WHERE clause (map column names to index?)
    - Add support for strings
    - Print out like sql does or just print out like CSV?
    - Print out data at the end or as it's processed? Speed vs. memory?
    - Ensure robust error handling
    - Add spaces after commas in the SELECT * case OR remove spaces from my output for consistency
    - Add types?
    - Refactor code into smaller, more modular functions and clean up code
    - Optimize and explore alternatives for better performance ()
        - Consider avoiding Vecs where possible
        - Use references instead of cloning strings
        - Look into other stuff
        - Look into reader buffer size
        - rustfmt and clippy: https://www.reddit.com/r/rust/comments/w25npu/how_does_rust_optimize_this_code_to_increase_the/
        - Research other optimizations: https://users.rust-lang.org/t/can-anyone-share-tips-for-optimize-coding-in-rust/45406/2
    - Document the code and provide examples
    - Prepare for release and strip the binary ([profile.release] optimizations (opt-level))
    - Run thorough testing and benchmarking (add automated tests?)
        - Find alternative CSV files to test with (chicagoCrimeData kills csvkit csvsql)
*/

use std::env;
use regex::Regex;
use std::error::Error;
use std::process::Command;

mod csv_reader;  // Import the csv_reader module for reading CSV files
mod aggregates;  // Import the aggregates module for aggregate functions

// Struct to represent the parsed components of the SQL query
#[derive(Debug)]
struct ParsedCommand {
    columns: Vec<String>,      // Selected columns or aggregate functions
    data_file: String,         // Name of the CSV file to read
    condition: Option<String>, // Optional condition for filtering rows
}

// Parses the SQL query string and extracts the columns, file, and condition
fn parse_query(query: &str) -> Result<ParsedCommand, String> {
    // Regular expression to match SELECT queries with an optional WHERE clause
    let re = Regex::new(
        r"(?i)SELECT\s+(?P<columns>.+?)\s+FROM\s+(?P<data_file>[\w/]+\.csv)(?:\s+WHERE\s+(?P<condition>.+))?"
    ).unwrap();

    if let Some(caps) = re.captures(query) {
        // Split the selected columns by comma and trim whitespace
        let columns = caps["columns"].split(',')
            .map(|col| col.trim().to_string())
            .collect();

        // Extract the data file name and optional condition
        let data_file = caps["data_file"].to_string();
        let condition = caps.name("condition").map(|m| m.as_str().to_string());

        Ok(ParsedCommand { columns, data_file, condition })
    } else {
        Err("Invalid SQL Query format".to_string())
    }
}

// Helper function to check if a column specifies an aggregate function
fn is_aggregate_function(column: &str) -> bool {
    column == "COUNT(*)"
    || column.starts_with("SUM(") 
    || column.starts_with("AVG(") 
    || column.starts_with("MIN(") 
    || column.starts_with("MAX(") 
    || column.starts_with("COUNT(")
}

// Special function to handle "SELECT COUNT(*) FROM <file>" using wc -l for efficiency
fn count_star(file_path: &str) -> Result<usize, Box<dyn Error>> {
    // Read the CSV file and get headers
    let (_, mut rdr) = csv_reader::read_csv(file_path)?;

    let mut count = 0;
    // Process records and count those that meet the condition
    for result in rdr.records() {
        result?;
        count += 1;
    }

    Ok(count)
}

// Function to count rows based on a condition ("SELECT COUNT() WHERE <condition>")
fn count_with_condition(file_path: &str, condition: &str) -> Result<usize, Box<dyn Error>> {
    // Read the CSV file and get headers
    let (headers, mut rdr) = csv_reader::read_csv(file_path)?;

    let mut count = 0;

    // Process records and count those that meet the condition
    for result in rdr.records() {
        let record = result?;
        if check_condition(&ParsedCommand { columns: vec![], data_file: file_path.to_string(), condition: Some(condition.to_string()) }, &headers, &record) {
            count += 1;
        }
    }

    Ok(count)
}

// Special function to print all rows of a file using `cat` ("SELECT * FROM <file>")
fn select_star(file_path: &str) -> Result<(), Box<dyn Error>> {
    let output = Command::new("cat")
        .arg(file_path)
        .output()?;

    // Print the file contents to stdout
    print!("{}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}

// Modified helper function to evaluate compound WHERE clause with AND/OR
fn check_condition(command: &ParsedCommand, headers: &[String], record: &csv::StringRecord) -> bool {
    if let Some(cond) = &command.condition {
        // Split conditions on OR, then split each OR clause on AND
        let or_clauses: Vec<&str> = cond.split("OR").map(|s| s.trim()).collect();

        for or_clause in or_clauses {
            let and_clauses: Vec<&str> = or_clause.split("AND").map(|s| s.trim()).collect();

            let mut and_result = true;
            for and_clause in and_clauses {
                if !evaluate_condition(and_clause, headers, record) {
                    and_result = false;
                    break;
                }
            }

            if and_result {
                return true;
            }
        }
        false
    } else {
        true
    }
}

// Helper function to evaluate a single condition
fn evaluate_condition(condition: &str, headers: &[String], record: &csv::StringRecord) -> bool {
    let parts: Vec<&str> = condition.split_whitespace().collect();
    if parts.len() == 3 {
        let column_name = parts[0];
        let operator = parts[1];
        let value: f64 = parts[2].parse().unwrap_or(f64::NAN);

        if let Some(column_index) = headers.iter().position(|h| h == column_name) {
            let field_value: f64 = record.get(column_index).unwrap_or("").parse().unwrap_or(f64::NAN);
            return match operator {
                "<" => field_value < value,
                ">" => field_value > value,
                "<=" => field_value <= value,
                ">=" => field_value >= value,
                "==" => field_value == value,
                "!=" => field_value != value,
                _ => false,
            };
        }
    }
    false
}

fn main() -> Result<(), Box<dyn Error>> {
    // Collect command-line arguments
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

    // Parse the query
    match parse_query(sql_query) {
        Ok(mut command) => {
            // Special case for "SELECT COUNT(*) FROM <file>"
            if command.columns.len() == 1 && command.columns[0] == "COUNT(*)" {
                // Handle count with or without a condition
                if command.condition.is_none() {
                    // Use the optimized line counting function
                    match count_star(&command.data_file) {
                        Ok(count) => {
                            println!("COUNT(*): {}", count);
                        }
                        Err(e) => {
                            eprintln!("Error counting lines: {}", e);
                            return Err(e);
                        }
                    }
                } else {
                    // Count with a condition
                    let total_count = count_with_condition(&command.data_file, command.condition.as_ref().unwrap())?;
                    println!("COUNT(*): {}", total_count);
                }
            } else if command.columns.len() == 1 && command.columns[0] == "*" && command.condition.is_none() {
                // Special case for "SELECT * FROM <file>"
                return select_star(&command.data_file);
            } else if command.columns.len() == 1 && command.columns[0] == "*" && command.condition.is_some() {
                // Case for "SELECT * FROM <file> WHERE <condition>"
                let (headers, mut rdr) = csv_reader::read_csv(&command.data_file)?;

                // Print the headers
                println!("{}", headers.join(","));

                // Process and filter records
                for result in rdr.records() {
                    let record = result?;
                    if check_condition(&command, &headers, &record) {
                        let row: Vec<&str> = record.iter().collect();
                        println!("{}", row.join(","));
                    }
                }
            } else {
                // Read the CSV file and get headers
                let (headers, mut rdr) = csv_reader::read_csv(&command.data_file)?;

                // Check if any column is an aggregate function
                let is_aggregate_query: bool = command.columns.iter().any(|col| is_aggregate_function(col));

                // Special case for COUNT(*) with other aggregate functions. Change COUNT(*) to COUNT(header[0])
                if command.columns.contains(&"COUNT(*)".to_string()) {
                    let first_column = headers.get(0).unwrap_or(&"".to_string()).clone();
                    command.columns = command.columns.iter().map(|col| {
                        if col == "COUNT(*)" {
                            format!("COUNT({})", first_column)
                        } else {
                            col.clone()
                        }
                    }).collect();
                }

                if is_aggregate_query {
                    // Initialize an Aggregates instance to store function results
                    let mut aggregates: aggregates::Aggregates = aggregates::Aggregates::new();

                    // Register each aggregate function
                    for column in &command.columns {
                        if column.starts_with("SUM(") {
                            // println!("Column: {}", column);
                            // println!("{:?}", Box::new(aggregates::Sum::new()));
                            aggregates.add_function(column.clone(), Box::new(aggregates::Sum::new()));
                            // println!("{:?}", aggregates.functions);
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

                    // Process each record, applying aggregates if it meets the condition
                    for result in rdr.records() {
                        let record = result?;
                        if check_condition(&command, &headers, &record) {
                            for (i, field) in record.iter().enumerate() {
                                // println!("i {}, field: {}", i, field);
                                if let Ok(value) = field.parse::<f64>() {
                                    for func in &command.columns {
                                        if func.contains(&headers[i]) {
                                            // println!("func: {}", func);
                                            // println!("{:?}", &aggregates.functions);
                                            // println!("{:?}", aggregates.functions.get_mut(func));
                                            // println!("{:?}", &headers[i]);
                                            if let Some(agg) = aggregates.functions.get_mut(func) {
                                                agg.apply(value);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Output the results of each aggregate function
                    let results = aggregates.results(&command.columns);
                    for column in &command.columns {
                        // Special case for COUNT(*) with other aggregate queries (use COUNT(*) label since we changed COUNT(*) to COUNT(headers[0]))
                        let label = if column.starts_with("COUNT(") && column.contains(&headers[0]) {
                            "COUNT(*)".to_string()
                        } else {
                            column.clone()
                        };
                        if let Some(result) = results.get(column) {
                            println!("{}: {}", label, result);
                        } else {
                            println!("{}: NaN", column);
                        }
                    }
                } else {
                    // Handle basic column selection (non-aggregate queries)
                    let column_indexes: Vec<_> = command.columns.iter()
                        .filter_map(|col| headers.iter().position(|h| h == col))
                        .collect();

                    // Print header row for selected columns
                    println!("{}", command.columns.join(","));

                    if command.condition.is_none() {
                        // Process records without filtering
                        for result in rdr.records() {
                            let record = result?;
                            let selected_fields: Vec<&str> = column_indexes.iter()
                                .map(|&index| record.get(index).unwrap_or(""))
                                .collect();
                            println!("{}", selected_fields.join(","));
                        }
                    } else {
                        // Process records, filtering and printing selected columns if they meet the condition
                        for result in rdr.records() {
                            let record = result?;
                            if check_condition(&command, &headers, &record) {
                                let selected_fields: Vec<&str> = column_indexes.iter()
                                    .map(|&index| record.get(index).unwrap_or(""))
                                    .collect();
                                println!("{}", selected_fields.join(","));
                            }
                        }
                    }
                }
            }
        }
        Err(err) => {
            eprintln!("Error parsing query: {}", err);
        }
    }

    Ok(())
}