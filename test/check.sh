#!/bin/bash

# Get list of installed packages
installed_packages=$(sudo apt list --installed 2>/dev/null | grep -v '^Listing...' | awk '{print $1}')

# Calculate one month ago timestamp
one_month_ago=$(date -d "1 month ago" +%s)

# Loop through each package
for package in $installed_packages; do
    # Get list of files associated with the package
    package_files=$(dpkg -L $package 2>/dev/null)
    
    # Check last access time for each file
    for file in $package_files; do
        if [ -e "$file" ]; then
            last_access=$(stat -c %X "$file")
            
            # Compare last access time with one month ago
            if [ "$last_access" -lt "$one_month_ago" ]; then
                echo "Package '$package' hasn't been used for one month or more."
                break
            fi
        fi
    done
done

