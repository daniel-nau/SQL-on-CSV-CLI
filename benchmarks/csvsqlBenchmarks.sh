#!/bin/bash

# Number of runs
num_runs=100

# Command to be timed
command="csvsql --query \"SELECT MIN(Low) FROM HistoricalData_1730160199611 WHERE Open < 200\" ../data/HistoricalData_1730160199611.csv"

# Initialize total time
total_time=0

# Run the command multiple times
for i in $(seq 1 $num_runs); do
    # Measure the time and extract the real time in seconds
    run_time=$( { time -p bash -c "$command" > /dev/null; } 2>&1 | grep real | awk '{print $2}' )
    total_time=$(echo "$total_time + $run_time" | bc)
    # echo "Run $i: $run_time seconds" # TODO: Comment out for big runs
done

# Calculate the average time
average_time=$(echo "scale=3; $total_time / $num_runs" | bc)

echo "Average execution time over $num_runs runs: $average_time seconds"