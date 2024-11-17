use regex::Regex;

// Struct to represent the parsed components of the SQL query
#[derive(Debug)]
pub struct ParsedCommand {
    pub columns: Vec<String>,      // Selected columns or aggregate functions
    pub data_file: String,         // Name of the CSV file to read
    pub condition: Option<String>, // Optional condition for filtering rows
}

// Parses the SQL query string and extracts the columns, file, and condition
pub fn parse_query(query: &str) -> Result<ParsedCommand, String> {
    // Regular expression to match SELECT queries with an optional WHERE clause
    let re = Regex::new(
        r"(?i)SELECT\s+(?P<columns>.+?)\s+FROM\s+(?P<data_file>(?:[.\./]+)?[\w/._-]+\.csv)(?:\s+WHERE\s+(?P<condition>.+?))?\s*$"
    ).unwrap();

    if let Some(caps) = re.captures(query) {
        // Split the selected columns by comma and trim whitespace
        let columns = caps["columns"]
            .split(',')
            .map(|col| col.trim().to_string())
            .collect();

        // Extract the data file name and optional condition
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

// Helper function to check if a column specifies an aggregate function
pub fn is_aggregate_function(column: &str) -> bool {
    column == "COUNT(*)"
        || column.starts_with("SUM(")
        || column.starts_with("AVG(")
        || column.starts_with("MIN(")
        || column.starts_with("MAX(")
        || column.starts_with("COUNT(")
}
