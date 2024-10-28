use std::env;
use regex::Regex;

#[derive(Debug)]
struct ParsedCommand {
    operation: String,
    columns: Vec<String>,
    data_file: String,
    condition: Option<String>,
}

fn parse_query(query: &str) -> Result<ParsedCommand, String> {
    let re = Regex::new(r"(?i)SELECT\s+(?P<columns>.+?)\s+FROM\s+(?P<data_file>[\w/]+)(?:\s+WHERE\s+(?P<condition>.+))?").unwrap();

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

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        eprintln!("Usage: {} --query \"<SQL Query>\" <data_file>", args[0]);
        return;
    }

    let query_flag = &args[1];
    let sql_query = &args[2];
    let data_file = &args[3];

    if query_flag != "--query" {
        eprintln!("First argument must be --query");
        return;
    }

    match parse_query(sql_query) {
        Ok(command) => {
            println!("Parsed Command: {:?}", command);
            println!("Data File: {}", command.data_file);

            // Placeholder for further processing of the command
            // For example, implementing CSV reading and executing the query.
            if command.condition.is_some() {
                println!("Condition: {}", command.condition.as_ref().unwrap());
            } else {
                println!("No conditions specified.");
            }
            // Implement logic for handling the parsed command
        }
        Err(err) => {
            eprintln!("Error parsing query: {}", err);
        }
    }
}
