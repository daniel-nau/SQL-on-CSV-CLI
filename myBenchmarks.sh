#!/bin/bash

# Number of runs
num_runs=10

# Command to be timed
# command="./target/release/csvsql --query \"SELECT MIN(Low) FROM data/HistoricalData_1730160199611.csv WHERE Open < 200\""
# command="./target/release/csvsql --query \"SELECT AVG(District) FROM data/chicagoCrimeData.csv WHERE Year == 2015\""
command="./target/release/csvsql --query \"SELECT AVG(low) FROM data/all_stocks_5yr.csv\""

# Initialize total time
total_time=0

# Run the command multiple times
for i in $(seq 1 $num_runs); do
    # Measure the time and extract the real time in seconds with higher precision
    run_time=$( { /usr/bin/time -f "%e" bash -c "$command" > /dev/null; } 2>&1 )
    total_time=$(echo "$total_time + $run_time" | bc -l)
    # echo "Run $i: $run_time seconds" # TODO: Comment out for big runs
done

# Calculate the average time with higher precision
average_time=$(echo "scale=9; $total_time / $num_runs" | bc -l) # TODO: Add more precision?

echo "Average execution time over $num_runs runs: $average_time seconds"