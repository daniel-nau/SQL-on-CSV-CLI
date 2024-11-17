use crate::sql_parser;

// Modified helper function to evaluate compound WHERE clause with AND/OR
pub fn check_condition(
    command: &sql_parser::ParsedCommand,
    headers: &[String],
    record: &[&str],
) -> bool {
    if let Some(cond) = &command.condition {
        // Split conditions on OR, then split each OR clause on AND
        let or_clauses: Vec<&str> = cond.split("OR").map(|s| s.trim()).collect();

        for or_clause in or_clauses {
            let and_clauses: Vec<&str> = or_clause.split("AND").map(|s| s.trim()).collect();

            // Evaluate all AND conditions within the OR clause
            if and_clauses
                .iter()
                .all(|&and_clause| evaluate_condition(and_clause, headers, record))
            {
                return true; // If all AND conditions are true, the OR clause is true
            }
        }
        return false;
    }
    true
}

// Helper function to evaluate a single condition
pub fn evaluate_condition(condition: &str, headers: &[String], record: &[&str]) -> bool {
    let parts: Vec<&str> = condition.split_whitespace().collect();
    if parts.len() == 3 {
        let column_name = parts[0];
        let operator = parts[1];
        let value: f64 = parts[2].parse().unwrap_or(f64::NAN);

        if let Some(column_index) = headers.iter().position(|h| h == column_name) {
            let field_value: f64 = record
                .get(column_index)
                .unwrap_or(&"")
                .trim()
                .parse()
                .unwrap_or(f64::NAN);

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
