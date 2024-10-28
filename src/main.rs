mod aggregates;
mod csv_reader;
mod parser;
mod query_executor;

use std::error::Error;
use std::env;

fn main() -> Result<(), Box<dyn Error>> {
    // Collect command-line arguments
    let args: Vec<String> = env::args().collect();

    // Print out the arguments received for debugging
    println!("Arguments: {:?}", args);

    // Ensure correct usage
    if args.len() != 4 || args[1] != "--query" {
        eprintln!("Usage: csvsql --query \"<SQL_QUERY>\" <file.csv>");
        std::process::exit(1);
    }

    // Extract the full SQL query and file name
    let query = &args[2];
    let file_name = &args[3];

    // Print out the parsed query and file name for debugging
    println!("Query: {}", query);
    println!("File: {}", file_name);

    // Parse the query
    let (columns, _file) = parser::parse_query(query);
    println!("Parsed columns: {:?}", columns);

    // Create aggregate structs
    let mut aggregates = aggregates::Aggregates::new();
    for column in columns {
        let col_name = match column.as_str() {
            _ if column.starts_with("MIN(") => column.trim_start_matches("MIN(").trim_end_matches(")").to_string(),
            _ if column.starts_with("MAX(") => column.trim_start_matches("MAX(").trim_end_matches(")").to_string(),
            _ if column.starts_with("SUM(") => column.trim_start_matches("SUM(").trim_end_matches(")").to_string(),
            _ if column.starts_with("AVG(") => column.trim_start_matches("AVG(").trim_end_matches(")").to_string(),
            _ if column.starts_with("COUNT(") => column.trim_start_matches("COUNT(").trim_end_matches(")").to_string(),
            _ => continue,
        };
        
        // Add the appropriate aggregate function
        match column {
            c if c.starts_with("MIN(") => aggregates.add_function(col_name.clone(), Box::new(aggregates::Min::new())),
            c if c.starts_with("MAX(") => aggregates.add_function(col_name.clone(), Box::new(aggregates::Max::new())),
            c if c.starts_with("SUM(") => aggregates.add_function(col_name.clone(), Box::new(aggregates::Sum::new())),
            c if c.starts_with("AVG(") => aggregates.add_function(col_name.clone(), Box::new(aggregates::Avg::new())),
            c if c.starts_with("COUNT(") => aggregates.add_function(col_name.clone(), Box::new(aggregates::Count::new())),
            _ => {}
        }
    }

    // Debug: Print the aggregates set up
    println!("Aggregates set up: {:?}", aggregates.functions.keys().collect::<Vec<_>>());

    // Execute the query
    query_executor::execute_query(&mut aggregates, file_name)?;

    // Retrieve and print results
    let results = aggregates.results();
    for (col, result) in results {
        println!("{}: {}", col, result);
    }

    Ok(())
}
