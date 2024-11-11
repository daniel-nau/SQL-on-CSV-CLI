#!/bin/bash

# Number of runs
num_runs=10

# Data file
data_file="../data/small_tall.csv"
table_name="small_tall"

# Queries to be tested
queries=(
    "SELECT COUNT(*) FROM $table_name"
    "SELECT * FROM $table_name"
    "SELECT col_2, col_5, col_8 FROM $table_name"
    "SELECT SUM(col_2), AVG(col_2), MAX(col_2) FROM $table_name"

    "SELECT COUNT(*) FROM $table_name WHERE col_2 < 0.5"
    "SELECT COUNT(*) FROM $table_name WHERE col_2 < 0.5 AND col_5 > 0.2"
    "SELECT COUNT(*) FROM $table_name WHERE col_2 < 0.5 OR col_5 < 0.3"

    "SELECT * FROM $table_name WHERE col_2 < 0.5"
    "SELECT * FROM $table_name WHERE col_2 < 0.5 AND col_5 > 0.2"
    "SELECT * FROM $table_name WHERE col_2 < 0.5 OR col_5 < 0.3"

    "SELECT col_2, col_5, col_8 FROM $table_name WHERE col_2 < 0.5"
    "SELECT col_2, col_5, col_8 FROM $table_name WHERE col_2 < 0.5 AND col_5 > 0.2"
    "SELECT col_2, col_5, col_8 FROM $table_name WHERE col_2 < 0.5 OR col_5 < 0.3"

    "SELECT SUM(col_2), AVG(col_2), MAX(col_2) FROM $table_name WHERE col_2 < 0.5"
    "SELECT SUM(col_2), AVG(col_2), MAX(col_2) FROM $table_name WHERE col_2 < 0.5 AND col_5 > 0.2"
    "SELECT SUM(col_2), AVG(col_2), MAX(col_2) FROM $table_name WHERE col_2 < 0.5 OR col_5 < 0.3"
)

# Output CSV header
echo "Query,Total Time,Number of Runs,Average Time" > csvsql_small_tall_benchmarks.csv

# Run each query multiple times and calculate the average time
for query in "${queries[@]}"; do
    # echo "In loop!"
    total_time=0
    command="csvsql --query \"$query\" --tables $table_name $data_file"
    
    for i in $(seq 1 $num_runs); do
        # if [ $i -eq 1 ]; then
        #     output_file="csvsql_small_tall_output.csv"
        #     echo "$query" >> $output_file
        # else
        #     output_file="/dev/null"
        # fi
        output_file="/dev/null"
        
        run_time=$( { time -p bash -c "$command" >> $output_file; } 2>&1 | grep real | awk '{print $2}' )
        if [ $? -ne 0 ]; then
            echo "Error running query: $query"
            exit 1
        fi
        total_time=$(echo "$total_time + $run_time" | bc)
        # echo "Run $i: $run_time seconds" # TODO: Comment out for big runs
    done
    
    avg_time=$(echo "scale=9; $total_time / $num_runs" | bc)
    echo "\"$query\",$total_time,$num_runs,$avg_time" >> csvsql_small_tall_benchmarks.csv
done

echo "Script completed successfully!"