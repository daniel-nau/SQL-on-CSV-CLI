use std::env;

fn main() {
    // Collect command line arguments
    let args: Vec<String> = env::args().collect();

    // Check if the number of arguments is correct
    if args.len() != 4 {
        eprintln!("Usage: {} --query \"<SQL Query>\" <data_file>", args[0]);
        return;
    }

    // Extracting the SQL query and data file from arguments
    let query_flag = &args[1];
    let sql_query = &args[2];
    let data_file = &args[3];

    // Validate the query flag
    if query_flag != "--query" {
        eprintln!("First argument must be --query");
        return;
    }

    // Print the parsed values (for demonstration)
    println!("SQL Query: {}", sql_query);
    println!("Data File: {}", data_file);

    // Here you can add code to process the SQL query and data file
}
