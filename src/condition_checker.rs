use crate::sql_parser;
use regex::Regex;

// Modified helper function to evaluate compound WHERE clause with AND/OR
pub fn check_condition(
    command: &sql_parser::ParsedCommand,
    headers: &[String],
    record: &[&str],
) -> bool {
    if let Some(cond) = &command.condition {
        // Split conditions on OR, then split each OR clause on AND
        cond.split("OR").any(|or_clause| {
            or_clause.split("AND").all(|and_clause| {
                let trimmed_clause = and_clause.trim();
                // Evaluate the condition here (assuming a function `evaluate_condition`)
                evaluate_condition(trimmed_clause, headers, record)
            })
        })
    } else {
        true
    }
}

// Helper function to evaluate a single condition
pub fn evaluate_condition(condition: &str, headers: &[String], record: &[&str]) -> bool {
    let re = Regex::new(r"(\w+)\s*(=|!=|>|<|>=|<=)\s*'([^']*)'|(\w+)\s*(=|!=|>|<|>=|<=)\s*([\d.]+)").unwrap();
    if let Some(caps) = re.captures(condition) {
        let column_name = caps.get(1).or(caps.get(4)).unwrap().as_str();
        let operator = caps.get(2).or(caps.get(5)).unwrap().as_str();
        let value = caps.get(3).map_or_else(|| caps.get(6).unwrap().as_str(), |m| m.as_str());

        if let Some(column_index) = headers.iter().position(|h| h == column_name) {
            let record_value = record[column_index];

            if caps.get(3).is_some() {
                // Handle string comparison
                match operator {
                    "=" => return record_value == value,
                    "!=" => return record_value != value,
                    _ => return false, // Unsupported operator for strings
                }
            } else {
                // Handle numeric comparison
                let numeric_value = value.parse::<f64>().unwrap();
                let record_numeric_value = record_value.parse::<f64>().unwrap();
                match operator {
                    "=" => return record_numeric_value == numeric_value,
                    "!=" => return record_numeric_value != numeric_value,
                    ">" => return record_numeric_value > numeric_value,
                    "<" => return record_numeric_value < numeric_value,
                    ">=" => return record_numeric_value >= numeric_value,
                    "<=" => return record_numeric_value <= numeric_value,
                    _ => return false, // Unsupported operator for numbers
                }
            }
        }
    }
    false
}
