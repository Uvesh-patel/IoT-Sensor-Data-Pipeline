#!/usr/bin/env python3
"""
Extract and analyze unique entity types from NGSI-LD JSON files.

This script recursively scans the specified directory for JSON files,
extracts the entity type from each file, and reports on the unique types found.
It will automatically locate the output/json folder from anywhere in the project.
"""

import os
import json
import sys
from collections import defaultdict


def find_project_root():
    """
    Find the project root directory that contains 'output/json'
    by walking up from the current directory until the folder is found.
    
    Returns:
        str: Path to output/json directory or None if not found
    """
    # Start from the current working directory
    current_dir = os.path.abspath(os.getcwd())
    max_levels = 5  # Don't go up more than 5 levels
    
    for _ in range(max_levels):
        # Check if this directory contains the output/json folder
        json_dir = os.path.join(current_dir, 'output', 'json')
        if os.path.isdir(json_dir):
            return json_dir
            
        # Check if this directory contains an 'output' folder
        output_dir = os.path.join(current_dir, 'output')
        if os.path.isdir(output_dir):
            # Check if output has a json subfolder
            json_dir = os.path.join(output_dir, 'json')
            if os.path.isdir(json_dir):
                return json_dir
        
        # Go one directory up
        parent = os.path.dirname(current_dir)
        if parent == current_dir:  # Reached root
            break
        current_dir = parent
    
    return None


def scan_json_files(directory):
    """
    Recursively scan JSON files in the given directory and extract entity types.
    
    Args:
        directory: Path to the directory containing JSON files
        
    Returns:
        tuple: (total_files, type_counts, type_examples)
            - total_files: Total number of JSON files scanned
            - type_counts: Dictionary mapping entity types to their counts
            - type_examples: Dictionary mapping entity types to example entity IDs
    """
    if not os.path.exists(directory):
        print(f"Error: Directory '{directory}' not found.")
        sys.exit(1)
    
    total_files = 0
    type_counts = defaultdict(int)
    type_examples = defaultdict(list)
    
    # Recursively scan directory
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith('.json'):
                file_path = os.path.join(root, filename)
                total_files += 1
                
                # Print progress every 10,000 files
                if total_files % 10000 == 0:
                    print(f"Scanned {total_files} files...")
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        
                        if 'type' in data:
                            entity_type = data['type']
                            type_counts[entity_type] += 1
                            
                            # Store up to 5 examples per type
                            if len(type_examples[entity_type]) < 5 and 'id' in data:
                                type_examples[entity_type].append(data['id'])
                        else:
                            print(f"Warning: No 'type' field found in {file_path}")
                            
                except json.JSONDecodeError:
                    print(f"Warning: Invalid JSON in {file_path}")
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
    
    return total_files, type_counts, type_examples


def main():
    """
    Main function to extract and analyze entity types from NGSI-LD JSON files.
    """
    # Check if directory is provided as an argument
    if len(sys.argv) > 1:
        json_directory = sys.argv[1]
    else:
        # Auto-locate the output/json directory
        json_directory = find_project_root()
        if not json_directory:
            print("Error: Could not locate the 'output/json' directory.")
            print("Please run this script from within the project directory,")
            print("or provide the path to the JSON directory as an argument:")
            print("python extract_entity_types.py <json_directory>")
            sys.exit(1)
    
    print(f"Scanning directory: {json_directory}")
    print("=" * 80)
    
    # Scan JSON files
    total_files, type_counts, type_examples = scan_json_files(json_directory)
    
    # Sort types by count (most common first)
    sorted_types = sorted(type_counts.items(), key=lambda x: x[1], reverse=True)
    
    # Print results
    print("\nEntity Type Summary:")
    print("=" * 80)
    print(f"Total files scanned: {total_files}")
    print(f"Unique entity types found: {len(type_counts)}")
    
    print("\nEntity Types (sorted by frequency):")
    print("=" * 80)
    for entity_type, count in sorted_types:
        # Extract short name from type URI if applicable
        short_type = entity_type.split('/')[-1] if '/' in entity_type else entity_type
        full_or_short = entity_type
        
        print(f"\nType: {short_type}")
        print(f"Full Type: {full_or_short}")
        print(f"Count: {count} ({count/total_files*100:.2f}% of total)")
        print("Example IDs:")
        for example_id in type_examples[entity_type]:
            print(f"  - {example_id}")
    
    # Compare with Orion-LD counts
    print("\nRecommendation:")
    print("=" * 80)
    print("To check if these entities are present in Orion-LD, update your verification script")
    print("to include ALL of these entity types when querying Orion-LD, as the current script")
    print("might be missing some types.")
    print("\nIf the counts in JSON files don't match what's in Orion-LD, you may need to:")
    print("1. Check upload logs for errors with specific types")
    print("2. Verify your upload script handles all entity types correctly")
    print("3. Re-upload missing entities by filtering by type")


if __name__ == "__main__":
    main()
