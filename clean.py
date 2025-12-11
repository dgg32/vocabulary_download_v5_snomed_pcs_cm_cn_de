#!/usr/bin/env python3
"""
Clean OMOP CSV files to fix quote issues for Neo4j import
This script handles tab-delimited CSV files and fixes malformed quotes
"""

import csv
import os
import sys
from pathlib import Path

def clean_csv_file(input_path, output_path):
    """
    Clean a CSV file by reading it without quote processing and writing it cleanly
    """
    print(f"Processing: {input_path}")
    
    line_count = 0
    error_count = 0
    
    try:
        with open(input_path, 'r', encoding='utf-8', errors='replace') as infile, \
             open(output_path, 'w', encoding='utf-8', newline='') as outfile:
            
            # Read without any quote processing
            reader = csv.reader(infile, delimiter='\t', quoting=csv.QUOTE_NONE)
            writer = csv.writer(outfile, delimiter='\t', quoting=csv.QUOTE_NONE, escapechar='\\')
            
            for i, row in enumerate(reader):
                try:
                    # Clean each field - remove problematic quotes and extra whitespace
                    cleaned_row = []
                    for field in row:
                        # Remove leading/trailing quotes if they exist
                        field = field.strip()
                        if field.startswith('"') and field.endswith('"'):
                            field = field[1:-1]
                        # Replace any remaining quotes with escaped quotes
                        field = field.replace('"', '\\"')
                        cleaned_row.append(field)
                    
                    writer.writerow(cleaned_row)
                    line_count += 1
                    
                    if line_count % 100000 == 0:
                        print(f"  Processed {line_count:,} lines...")
                        
                except Exception as e:
                    error_count += 1
                    print(f"  Warning: Error on line {i+1}: {str(e)}")
                    continue
        
        print(f"✓ Completed: {line_count:,} lines processed, {error_count} errors")
        return True
        
    except Exception as e:
        print(f"✗ Failed to process {input_path}: {str(e)}")
        return False

def main():
    # Get the directory path from command line or use default
    if len(sys.argv) > 1:
        input_dir = Path(sys.argv[1])
    else:
        input_dir = Path.cwd()
    
    print(f"Input directory: {input_dir}")
    print("=" * 60)
    
    # Files to clean
    files_to_clean = ['CONCEPT.csv', 'CONCEPT_SYNONYM.csv', 'CONCEPT_RELATIONSHIP.csv']
    
    success_count = 0
    for filename in files_to_clean:
        input_path = input_dir / filename
        output_path = input_dir / f"{filename.replace('.csv', '_cleaned.csv')}"
        
        if not input_path.exists():
            print(f"⊘ Skipping {filename} (not found)")
            print()
            continue
        
        if clean_csv_file(input_path, output_path):
            success_count += 1
        print()
    
    print("=" * 60)
    print(f"Summary: {success_count}/{len(files_to_clean)} files cleaned successfully")
    print()
    print("Cleaned files created:")
    for filename in files_to_clean:
        cleaned_path = input_dir / f"{filename.replace('.csv', '_cleaned.csv')}"
        if cleaned_path.exists():
            size_mb = cleaned_path.stat().st_size / (1024 * 1024)
            print(f"  - {cleaned_path.name} ({size_mb:.2f} MB)")
    print()
    print("Next steps:")
    print("1. Copy the *_cleaned.csv files to your Neo4j import directory")
    print("2. Update your import script to use the cleaned file names")
    print("   Example: 'file:///CONCEPT_cleaned.csv'")

if __name__ == "__main__":
    main()