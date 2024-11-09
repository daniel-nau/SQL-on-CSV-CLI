#!/bin/bash

# Number of runs
num_runs=5

# Queries for "tall" tables
tall_queries=(
    "SELECT COUNT(*) FROM \$table_name"
    "SELECT * FROM \$table_name"
    "SELECT col_2, col_5, col_8 FROM \$table_name"
    "SELECT SUM(col_2), AVG(col_2), MAX(col_2) FROM \$table_name"
    "SELECT COUNT(*) FROM \$table_name WHERE col_2 < 0.5"
    "SELECT COUNT(*) FROM \$table_name WHERE col_2 < 0.5 AND col_5 > 0.2"
    "SELECT COUNT(*) FROM \$table_name WHERE col_2 < 0.5 OR col_5 < 0.3"
    "SELECT * FROM \$table_name WHERE col_2 < 0.5"
    "SELECT * FROM \$table_name WHERE col_2 < 0.5 AND col_5 > 0.2"
    "SELECT * FROM \$table_name WHERE col_2 < 0.5 OR col_5 < 0.3"
    "SELECT col_2, col_5, col_8 FROM \$table_name WHERE col_2 < 0.5"
    "SELECT col_2, col_5, col_8 FROM \$table_name WHERE col_2 < 0.5 AND col_5 > 0.2"
    "SELECT col_2, col_5, col_8 FROM \$table_name WHERE col_2 < 0.5 OR col_5 < 0.3"
    "SELECT SUM(col_2), AVG(col_2), MAX(col_2) FROM \$table_name WHERE col_2 < 0.5"
    "SELECT SUM(col_2), AVG(col_2), MAX(col_2) FROM \$table_name WHERE col_2 < 0.5 AND col_5 > 0.2"
    "SELECT SUM(col_2), AVG(col_2), MAX(col_2) FROM \$table_name WHERE col_2 < 0.5 OR col_5 < 0.3"
)

# Queries for "wide" tables
wide_queries=(
    "SELECT COUNT(*) FROM \$table_name"
    "SELECT * FROM \$table_name"
    "SELECT col_200, col_500, col_800 FROM \$table_name"
    "SELECT SUM(col_200), AVG(col_200), MAX(col_200) FROM \$table_name"
    
    "SELECT COUNT(*) FROM \$table_name WHERE col_200 < 0.5"
    "SELECT COUNT(*) FROM \$table_name WHERE col_200 < 0.5 AND col_500 > 0.2"
    "SELECT COUNT(*) FROM \$table_name WHERE col_200 < 0.5 OR col_500 < 0.3"

    "SELECT * FROM \$table_name WHERE col_200 < 0.5"
    "SELECT * FROM \$table_name WHERE col_200 < 0.5 AND col_500 > 0.2"
    "SELECT * FROM \$table_name WHERE col_200 < 0.5 OR col_500 < 0.3"

    "SELECT col_200, col_500, col_800 FROM \$table_name WHERE col_200 < 0.5"
    "SELECT col_200, col_500, col_800 FROM \$table_name WHERE col_200 < 0.5 AND col_500 > 0.2"
    "SELECT col_200, col_500, col_800 FROM \$table_name WHERE col_200 < 0.5 OR col_500 < 0.3"

    "SELECT SUM(col_200), AVG(col_200), MAX(col_200) FROM \$table_name WHERE col_200 < 0.5"
    "SELECT SUM(col_200), AVG(col_200), MAX(col_200) FROM \$table_name WHERE col_200 < 0.5 AND col_500 > 0.2"
    "SELECT SUM(col_200), AVG(col_200), MAX(col_200) FROM \$table_name WHERE col_200 < 0.5 OR col_500 < 0.3"
)

# Iterate through each CSV file in the ../data/ directory
for data_file in ../data/*.csv; do
    table_name="test"
    base_name=$(basename "$data_file" .csv)
    output_file="duckdb_${base_name}_output.csv"
    echo "Query,Total Time,Number of Runs,Average Time" > $output_file

    # Skip files starting with "large"
    if [[ "$base_name" == large* ]]; then
        echo "Skipping large file: $base_name"
        continue
    fi

    # Determine which set of queries to use
    if [[ "$base_name" == *"wide"* ]]; then
        queries=("${wide_queries[@]}")
    elif [[ "$base_name" == *"tall"* ]]; then
        queries=("${tall_queries[@]}")
    else
        echo "Unknown table type for $base_name"
        continue
    fi

    # Run each query multiple times and calculate the average time
    for query in "${queries[@]}"; do
        total_time=0
        command="duckdb database.db <<EOF
DROP TABLE IF EXISTS $table_name;
CREATE TABLE $table_name AS SELECT * FROM read_csv_auto('$data_file');
${query//\$table_name/$table_name};
EOF
        "

        for i in $(seq 1 $num_runs); do
            run_time=$( { time -p bash -c "$command" >> /dev/null; } 2>&1 | grep real | awk '{print $2}' )
            if [ $? -ne 0 ]; then
                echo "Error running query: $query"
                exit 1
            fi
            total_time=$(echo "$total_time + $run_time" | bc)
        done

        avg_time=$(echo "scale=9; $total_time / $num_runs" | bc)
        echo "\"$query\",$total_time,$num_runs,$avg_time" >> $output_file
    done

    echo "Output for $data_file completed successfully!"
done

echo "All outputs completed successfully!"