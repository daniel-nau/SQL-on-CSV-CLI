#!/bin/bash

# Data file
data_file="../data/small_wide.csv"

# Queries to be tested
queries=(
    "SELECT COUNT(*) FROM $data_file"
    "SELECT * FROM $data_file"
    "SELECT col_200, col_500, col_800 FROM $data_file"
    "SELECT SUM(col_200), AVG(col_200), MAX(col_200) FROM $data_file"

    "SELECT COUNT(*) FROM $data_file WHERE col_200 < 0.5"
    "SELECT COUNT(*) FROM $data_file WHERE col_200 < 0.5 AND col_500 > 0.2"
    "SELECT COUNT(*) FROM $data_file WHERE col_200 < 0.5 OR col_500 < 0.3"

    "SELECT * FROM $data_file WHERE col_200 < 0.5"
    "SELECT * FROM $data_file WHERE col_200 < 0.5 AND col_500 > 0.2"
    "SELECT * FROM $data_file WHERE col_200 < 0.5 OR col_500 < 0.3"

    "SELECT col_200, col_500, col_800 FROM $data_file WHERE col_200 < 0.5"
    "SELECT col_200, col_500, col_800 FROM $data_file WHERE col_200 < 0.5 AND col_500 > 0.2"
    "SELECT col_200, col_500, col_800 FROM $data_file WHERE col_200 < 0.5 OR col_500 < 0.3"

    "SELECT SUM(col_200), AVG(col_200), MAX(col_200) FROM $data_file WHERE col_200 < 0.5"
    "SELECT SUM(col_200), AVG(col_200), MAX(col_200) FROM $data_file WHERE col_200 < 0.5 AND col_500 > 0.2"
    "SELECT SUM(col_200), AVG(col_200), MAX(col_200) FROM $data_file WHERE col_200 < 0.5 OR col_500 < 0.3"
)

# Run each query once and save the results
for index in "${!queries[@]}"; do
    query="${queries[$index]}"
    command="../target/release/csvsql_v2_benchmark --query \"$query\""
    
    output_file="mycsvsql_query_result_$index.txt"
    
    run_time=$( { time -p bash -c "$command" > $output_file; } 2>&1 | grep real | awk '{print $2}' )
    if [ $? -ne 0 ]; then
        echo "Error running query: $query"
        exit 1
    fi
    
    # echo "Query $index: $run_time seconds" >> query_result_$index.txt
done

echo "Script completed successfully!"