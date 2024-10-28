use std::env;
use regex::Regex;
use std::error::Error;

mod csv_reader;  // Import the csv_reader module
mod aggregates;  // Import the aggregates module

#[derive(Debug)]
struct ParsedCommand {
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
            columns,
            data_file,
            condition,
        })
    } else {
        Err("Invalid SQL Query format".to_string())
    }
}

fn is_aggregate_function(column: &str) -> bool {
    column.starts_with("SUM(") || column.starts_with("AVG(") ||
    column.starts_with("MIN(") || column.starts_with("MAX(") ||
    column.starts_with("COUNT(")
}

fn main() -> Result<(), Box<dyn Error>> {
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

    match parse_query(sql_query) {
        Ok(command) => {
            let (headers, mut rdr) = csv_reader::read_csv(&command.data_file)?;

            // Separate aggregate functions from basic column selections
            let is_aggregate_query = command.columns.iter().any(|col| is_aggregate_function(col));

            if is_aggregate_query {
                // Initialize Aggregates
                let mut aggregates: aggregates::Aggregates = aggregates::Aggregates::new();

                // Add aggregate functions
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

                // Process records and apply aggregates
                for result in rdr.records() {
                    let record = result?;
                    let meets_condition = check_condition(&command, &headers, &record);

                    if meets_condition {
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
                    if let Some(result) = results.get(column) {
                        println!("{}: {}", column, result);
                    } else {
                        println!("{}: NaN", column);
                    }
                }
            } else {
                // Basic column selection
                let column_indexes: Vec<_> = command.columns.iter()
                .filter_map(|col| headers.iter().position(|h| h == col))
                .collect();

                // Print header row
                println!("{}", command.columns.join(", "));

                for result in rdr.records() {
                    let record = result?;
                    let meets_condition = check_condition(&command, &headers, &record);

                    if meets_condition {
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

fn check_condition(command: &ParsedCommand, headers: &[String], record: &csv::StringRecord) -> bool {
    if let Some(cond) = &command.condition {
        let parts: Vec<&str> = cond.split_whitespace().collect();
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
    }
    true // No condition to apply, so return true
}
