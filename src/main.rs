/*
    TODO:
    -Add support for multiple conditions in the WHERE clause (logical operators (AND, OR))
    -Optimization/check to see if there are better ways to do certain things (see about jumping through different columns)
    -Search for things to avoid (vecs?)
    -Add COUNT(*) support (wc -l)
    -Select * support
    -Make release and strip the binary
    -Testing/benchmarking
    -Make sure to handle errors/edge cases properly (like COUNT(*) with a condition)
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
    column.starts_with("SUM(") 
    || column.starts_with("AVG(") 
    || column.starts_with("MIN(") 
    || column.starts_with("MAX(") 
    || column.starts_with("COUNT(")
}

// Special function to handle "SELECT COUNT(*) FROM <file>" using wc -l for efficiency
fn count_lines_excluding_header(file_path: &str) -> Result<usize, Box<dyn Error>> {
    // Use `wc -l` to get the line count
    let output = Command::new("wc")
        .arg("-l")
        .arg(file_path)
        .output()?;
    
    // Parse the output to get the line count as a number
    let count_str = String::from_utf8_lossy(&output.stdout);
    let line_count: usize = count_str.split_whitespace().next().unwrap().parse()?;
    
    // Subtract one to exclude the header row
    Ok(line_count - 1)
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
        Ok(command) => {
            // Special case for "SELECT COUNT(*) FROM <file>"
            if command.columns.len() == 1 && command.columns[0] == "COUNT(*)" && command.condition.is_none() {
                // Use the optimized line counting function
                match count_lines_excluding_header(&command.data_file) {
                    Ok(count) => {
                        println!("COUNT(*): {} (excluding header)", count);
                        return Ok(());
                    }
                    Err(e) => {
                        eprintln!("Error counting lines: {}", e);
                        return Err(e);
                    }
                }
            }

            // Read the CSV file and get headers
            let (headers, mut rdr) = csv_reader::read_csv(&command.data_file)?;

            // Check if any column is an aggregate function
            let is_aggregate_query: bool = command.columns.iter().any(|col| is_aggregate_function(col));

            if is_aggregate_query {
                // Initialize an Aggregates instance to store function results
                let mut aggregates: aggregates::Aggregates = aggregates::Aggregates::new();

                // Register each aggregate function
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

                // Process each record, applying aggregates if it meets the condition
                for result in rdr.records() {
                    let record = result?;
                    let meets_condition = check_condition(&command, &headers, &record);

                    if meets_condition {
                        // Apply each aggregate function for matching records
                        for (i, field) in record.iter().enumerate() {
                            if let Ok(value) = field.parse::<f64>() {
                                for func in &command.columns {
                                    if func.contains(&headers[i]) {
                                        if let Some(agg) = aggregates.functions.get_mut(func) {
                                            agg.apply(value); // Apply the value to the aggregate function
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
                    if let Some(result) = results.get(column) {
                        println!("{}: {}", column, result);
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
                println!("{}", command.columns.join(", "));

                // Process records, filtering and printing selected columns if they meet the condition
                for result in rdr.records() {
                    let record = result?;
                    let meets_condition = check_condition(&command, &headers, &record);

                    if meets_condition {
                        // Collect values of selected columns for output
                        let selected_fields: Vec<&str> = column_indexes.iter()
                            .map(|&index| record.get(index).unwrap_or(""))
                            .collect();
                        println!("{}", selected_fields.join(", "));
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

// Helper function to evaluate the WHERE condition on each row
fn check_condition(command: &ParsedCommand, headers: &[String], record: &csv::StringRecord) -> bool {
    if let Some(cond) = &command.condition {
        // Parse the condition into column name, operator, and value
        let parts: Vec<&str> = cond.split_whitespace().collect();
        if parts.len() == 3 {
            let column_name = parts[0];
            let operator = parts[1];
            let value: f64 = parts[2].parse().unwrap_or(f64::NAN);

            // Find the index of the column in the CSV headers
            if let Some(column_index) = headers.iter().position(|h| h == column_name) {
                let field_value: f64 = record.get(column_index).unwrap_or("").parse().unwrap_or(f64::NAN);
                // Check the condition based on the operator
                return match operator {
                    "<" => field_value < value,
                    ">" => field_value > value,
                    "<=" => field_value <= value,
                    ">=" => field_value >= value,
                    "==" => field_value == value,
                    "!=" => field_value != value,
                    _ => false, // Unrecognized operator defaults to false
                };
            }
        }
    }
    true // If no condition is provided, return true by default
}