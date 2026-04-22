#!/bin/bash

BASE_URL="https://hearts360.medisoft.rw/data_sharing_export.php"
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
TARGET_DIR="test_data"

mkdir -p "$TARGET_DIR"

echo "Fetching facility list..."

# 1. Capture the page source
PAGE_SOURCE=$(curl -s -L -A "$UA" "$BASE_URL")

# 2. Extract IDs and Names
facility_list=$(echo "$PAGE_SOURCE" | \
    tr -d '\n\r' | \
    sed 's/<option/\n<option/g' | \
    grep 'value="[0-9]' | \
    sed -n 's/.*value="\([0-9]*\)".*>\(.*\)<\/option>.*/\1_\2/p' | \
    sed 's/ /_/g' | \
    sed 's/__*/_/g')

if [ -z "$facility_list" ]; then
    echo "Error: Could not extract facilities."
    exit 1
fi

# 3. Iterate and Download
for entry in $facility_list; do
    raw_id=$(echo "$entry" | cut -d'_' -f1)
    
    # Extract name and clean: 
    # 1. Remove non-alphanumeric/underscore 
    # 2. Remove trailing underscores 
    # 3. Collapse multiple underscores to one
    name=$(echo "$entry" | cut -d'_' -f2- | sed 's/[^a-zA-Z0-9_]//g' | sed 's/_*$//' | sed 's/__*/_/g')

    padded_id=$(printf "%03d" "$raw_id")

    echo "--- Facility: $name (ID: $padded_id) ---"

    for type in patients bp glucose; do
        # Combined filename
        FILENAME="${padded_id}_${name}_${type}.csv"
        
        # Final pass to ensure no double underscores in the final string
        FILENAME=$(echo "$FILENAME" | sed 's/__*/_/g')
        OUTPUT_PATH="${TARGET_DIR}/${FILENAME}"
        
        echo "Downloading $type..."
        curl -L -s -A "$UA" \
             "${BASE_URL}?facility_id=${raw_id}&export=${type}" \
             -o "$OUTPUT_PATH"

        if [ -s "$OUTPUT_PATH" ]; then
            echo "   [OK] Saved to $OUTPUT_PATH"
        else
            echo "   [Empty] No data for $type"
            rm -f "$OUTPUT_PATH"
        fi
    done
    sleep 1
done

echo "Finished. Files in '$TARGET_DIR' have single underscores only."