#!/bin/bash

# Data file
data_file="../data/small_wide.csv"
table_name="small_wide"

# Queries to be tested
queries=(
    "SELECT COUNT(*) FROM $table_name"
    "SELECT * FROM $table_name"
    "SELECT col_200, col_500, col_800 FROM $table_name"
    "SELECT SUM(col_200), AVG(col_200), MAX(col_200) FROM $table_name"

    "SELECT COUNT(*) FROM $table_name WHERE col_200 < 0.5"
    "SELECT COUNT(*) FROM $table_name WHERE col_200 < 0.5 AND col_500 > 0.2"
    "SELECT COUNT(*) FROM $table_name WHERE col_200 < 0.5 OR col_500 < 0.3"

    "SELECT * FROM $table_name WHERE col_200 < 0.5"
    "SELECT * FROM $table_name WHERE col_200 < 0.5 AND col_500 > 0.2"
    "SELECT * FROM $table_name WHERE col_200 < 0.5 OR col_500 < 0.3"

    "SELECT col_200, col_500, col_800 FROM $table_name WHERE col_200 < 0.5"
    "SELECT col_200, col_500, col_800 FROM $table_name WHERE col_200 < 0.5 AND col_500 > 0.2"
    "SELECT col_200, col_500, col_800 FROM $table_name WHERE col_200 < 0.5 OR col_500 < 0.3"

    "SELECT SUM(col_200), AVG(col_200), MAX(col_200) FROM $table_name WHERE col_200 < 0.5"
    "SELECT SUM(col_200), AVG(col_200), MAX(col_200) FROM $table_name WHERE col_200 < 0.5 AND col_500 > 0.2"
    "SELECT SUM(col_200), AVG(col_200), MAX(col_200) FROM $table_name WHERE col_200 < 0.5 OR col_500 < 0.3"
)

# Run each query once and save the results
for index in "${!queries[@]}"; do
    query="${queries[$index]}"
    command="csvsql --query \"$query\" --tables $table_name $data_file"
    
    output_file="csvsql_query_result_$index.txt"
    
    run_time=$( { time -p bash -c "$command" > $output_file; } 2>&1 | grep real | awk '{print $2}' )
    if [ $? -ne 0 ]; then
        echo "Error running query: $query"
        exit 1
    fi
    
    # echo "Query $index: $run_time seconds" >> query_result_$index.txt
done

echo "Script completed successfully!"