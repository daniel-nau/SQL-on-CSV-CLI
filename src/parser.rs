pub fn parse_query(query: &str) -> (Vec<String>, String) {
    let query = query.replace("SELECT", "").replace("FROM", "");
    let parts: Vec<&str> = query.trim().split_whitespace().collect();

    let columns: Vec<String> = parts[..parts.len() - 1].iter().map(|s| s.to_string()).collect();
    let file = parts.last().unwrap().to_string();

    (columns, file)
}
