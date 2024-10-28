use std::env;
use regex::Regex;
use std::error::Error;
use csv::StringRecord;

mod csv_reader;  // Import the csv_reader module
mod aggregates;  // Import the aggregates module

#[derive(Debug)]
struct ParsedCommand {
    operation: String,
    columns: Vec<String>,
    data_file: String,
    condition: Option<String>,
}

fn parse_query(query: &str) -> Result<ParsedCommand, String> {
    let re = Regex::new(r"(?i)SELECT\s+(?P<columns>.+?)\s+FROM\s+(?P<data_file>[\w/]+\.csv)(?:\s+WHERE\s+(?P<condition>.+))?").unwrap();

    if let Some(caps) = re.captures(query) {
        let columns = caps["columns"].split(',')
            .map(|col| col.trim().to_string())
            .collect();
        
        let data_file = caps["data_file"].to_string();
        let condition = caps.name("condition").map(|m| m.as_str().to_string());

        Ok(ParsedCommand {
            operation: "SELECT".to_string(),
            columns,
            data_file,
            condition,
        })
    } else {
        Err("Invalid SQL Query format".to_string())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: {} --query \"<SQL Query>\"", args[0]);
        return Err("Invalid number of arguments".into());
    }

    let query_flag: &str = &args[1];
    let sql_query: &str = &args[2];

    if query_flag != "--query" {
        eprintln!("First argument must be --query");
        return Err("First argument must be --query".into());
    }

    match parse_query(sql_query) {
        Ok(command) => {
            println!("Parsed Command: {:?}", command);
            let (headers, mut rdr) = csv_reader::read_csv(&command.data_file)?;

            // Initialize Aggregates
            let mut aggregates: aggregates::Aggregates = aggregates::Aggregates::new();

            // Identify which columns to apply aggregates on
            for column in &command.columns {
                if column.starts_with("SUM(") {
                    let col_name = &column[4..column.len() - 1]; // Remove "SUM(" and ")"
                    aggregates.add_function(column.clone(), Box::new(aggregates::Sum::new())); // Use full column name
                } else if column.starts_with("AVG(") {
                    let col_name = &column[4..column.len() - 1]; // Remove "AVG(" and ")"
                    aggregates.add_function(column.clone(), Box::new(aggregates::Avg::new())); // Use full column name
                } else if column.starts_with("MIN(") {
                    let col_name = &column[4..column.len() - 1]; // Remove "MIN(" and ")"
                    aggregates.add_function(column.clone(), Box::new(aggregates::Min::new())); // Use full column name
                } else if column.starts_with("MAX(") {
                    let col_name = &column[4..column.len() - 1]; // Remove "MAX(" and ")"
                    aggregates.add_function(column.clone(), Box::new(aggregates::Max::new())); // Use full column name
                } else if column.starts_with("COUNT(") {
                    let col_name = &column[6..column.len() - 1]; // Remove "COUNT(" and ")"
                    aggregates.add_function(column.clone(), Box::new(aggregates::Count::new())); // Use full column name
                }
            }

            // Print headers for debugging
            println!("CSV Headers: {:?}", headers);

            // Process records
            for result in rdr.records() {
                match result {
                    Ok(record) => {
                        // Print the record for debugging
                        println!("Record: {:?}", record);
                        
                        // Check if the record meets the condition
                        let mut meets_condition = true;

                        if let Some(cond) = &command.condition {
                            // Split the condition and check the column and value
                            let parts: Vec<&str> = cond.split_whitespace().collect();
                            if parts.len() == 3 {
                                let column_name = parts[0];
                                let operator = parts[1];
                                let value: f64 = parts[2].parse().unwrap_or(f64::NAN); // Parse the right side of the condition

                                println!("Checking condition for column '{}': {} {}", column_name, operator, value);
                                
                                if let Some(column_index) = headers.iter().position(|h| h == column_name) {
                                    let field_value: f64 = record.get(column_index).unwrap_or("").parse().unwrap_or(f64::NAN);

                                    // Debug print for comparison
                                    println!("Comparing field value {} with condition value {}", field_value, value);

                                    // Check the condition based on the operator
                                    meets_condition = match operator {
                                        "<" => field_value < value,
                                        ">" => field_value > value,
                                        "<=" => field_value <= value,
                                        ">=" => field_value >= value,
                                        "==" => field_value == value,
                                        "!=" => field_value != value,
                                        _ => true, // If the operator is not recognized, don't filter
                                    };
                                } else {
                                    println!("Warning: Column '{}' not found in headers.", column_name);
                                    meets_condition = false; // Force condition to false if column not found
                                }
                            } else {
                                println!("Warning: Invalid condition format. Expected format: 'column operator value'.");
                                meets_condition = false; // Force condition to false for invalid format
                            }
                        }

                        // If the record meets the condition, apply aggregates
                        if meets_condition {
                            for (i, field) in record.iter().enumerate() {
                                if let Ok(value) = field.parse::<f64>() {
                                    println!("Parsed value: {} from column {}", value, headers[i]);
                                    
                                    // Use the full function name for lookup
                                    for func in &command.columns {
                                        if func.contains(&headers[i]) {
                                            // Debug before applying
                                            println!("Applying {} to aggregate for {}", value, func);
                                            if let Some(agg) = aggregates.functions.get_mut(func) {
                                                agg.apply(value);
                                                // Debug after applying
                                                println!("Aggregate state after applying: {:?}", agg);
                                            } else {
                                                println!("Warning: No aggregate function found for '{}'.", func);
                                            }
                                        }
                                    }
                                } else {
                                    println!("Warning: Failed to parse value '{}', skipping.", field);
                                }
                            }
                        } else {
                            println!("Record does not meet condition, skipping: {:?}", record);
                        }

                    }
                    Err(e) => {
                        eprintln!("Error reading record: {}", e);
                    }
                }
            }

            // Output results in a simplified format
            let results = aggregates.results(&command.columns);
            println!("Results: {:?}", results);

            // Print each aggregate function with its result
            for column in &command.columns {
                // let adjusted_col_name = column.trim_end_matches(')').trim(); // Remove the closing parenthesis
                // let function_name = adjusted_col_name.to_string();
                // println!("Function Name: {}", function_name);

                // Get the result, defaulting to NaN if not found
                if let Some(result) = results.get(column) {
                    if result.is_nan() {
                        println!("{}: NaN", column); // Handle case where result is missing
                    } else {
                        println!("{}: {}", column, result);
                    }
                } else {
                    println!("{}: NaN", column); // Handle case where result is missing
                }
            }
        }
        Err(err) => {
            eprintln!("Error parsing query: {}", err);
        }
    }

    Ok(())
}
