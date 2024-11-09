#!/bin/bash

# Loop through all .sh files in the current directory
for script in *.sh; do
    # Check if the file is executable
    if [[ -x "$script" ]]; then
        echo "Running $script..."
        ./"$script"
        if [ $? -ne 0 ]; then
            echo "Error running $script"
            exit 1
        fi
    else
        echo "Skipping $script, not executable"
    fi
done

echo "All scripts executed."