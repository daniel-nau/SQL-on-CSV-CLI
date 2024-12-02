#!/bin/bash

# Loop through all files matching the naming scheme
for file in csvsql_query_result_*.txt; do
    # Extract the index from the filename
    index=$(echo $file | grep -o '[0-9]\+')
    
    # Define the corresponding mycsvsql file
    mycsvsql_file="mycsvsql_query_result_$index.txt"
    
    # Check if the corresponding mycsvsql file exists
    if [ -f "$mycsvsql_file" ]; then
        echo "Diff for files: $file and $mycsvsql_file"
        diff "$file" "$mycsvsql_file"
    else
        echo "File $mycsvsql_file does not exist."
    fi
    echo
done