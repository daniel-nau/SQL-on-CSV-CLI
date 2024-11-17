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

            let mut and_result = true;
            for and_clause in and_clauses {
                if !evaluate_condition(and_clause, headers, record) {
                    and_result = false;
                    break;
                }
            }

            if and_result {
                return true;
            }
        }
        false
    } else {
        true
    }
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
