// src/main.rs
use std::env;
use regex::Regex;
use std::error::Error;
use std::fs;

mod csv_reader;  // Import the csv_reader module

#[derive(Debug)]
struct ParsedCommand {
    operation: String,
    columns: Vec<String>,
    data_file: String,
    condition: Option<String>,
}

fn parse_query(query: &str) -> Result<ParsedCommand, String> {
    // Regex to capture the SQL-like query
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

    let query_flag = &args[1];
    let sql_query = &args[2];

    if query_flag != "--query" {
        eprintln!("First argument must be --query");
        return Err("First argument must be --query".into());
    }

    match parse_query(sql_query) {
        Ok(command) => {
            println!("Parsed Command: {:?}", command);
            println!("Data File: {}", command.data_file);
            
            // Check if the file exists
            if !fs::metadata(&command.data_file).is_ok() {
                eprintln!("Error: File not found: {}", command.data_file);
                return Err(format!("File not found: {}", command.data_file).into());
            }

            // Read the CSV file and process it
            let (headers, records) = csv_reader::read_csv(&command.data_file)?;
            
            // Example: Print out headers and records
            println!("Headers: {:?}", headers);
            for record in records {
                for (i, field) in record.iter().enumerate() {
                    println!("Column {} ({}): {}", i, headers[i], field);
                }
            }
        }
        Err(err) => {
            eprintln!("Error parsing query: {}", err);
        }
    }
    
    Ok(())
}
