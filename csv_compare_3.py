#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Compares two CSV files representing filesystem snapshots (before/after)
to identify added and removed files based on their 'Path' and 'Name'.

Assumes the CSV files have headers, including at least 'Path' and 'Name'.
Performs a case-insensitive comparison of paths and names by default.
"""

import csv
import os
import sys
import argparse
import logging
from collections import namedtuple

# --- Constants ---
# Columns used to uniquely identify a file entry
KEY_COLUMNS = ['Path', 'Name']
# Default output directory name
DEFAULT_OUTPUT_DIR = 'comparison_results'
# Progress update frequency
PROGRESS_INTERVAL = 50000

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# For storing file key information efficiently
FileKey = namedtuple('FileKey', ['path', 'name'])


# --- Helper Functions ---

def create_file_key(row, row_num, filename):
    """
    Creates a unique, normalized key tuple for a file entry from a CSV row.

    Args:
        row (dict): A dictionary representing a row from the CSV file.
        row_num (int): The current row number (for logging).
        filename (str): The name of the file being processed (for logging).

    Returns:
        FileKey: A named tuple (path, name) representing the normalized key,
                 or None if key columns are missing or invalid.
    """
    try:
        # Strip whitespace and potential trailing characters like '\r'
        # Convert to lowercase for case-insensitive comparison
        path = row[KEY_COLUMNS[0]].strip().lower()
        name = row[KEY_COLUMNS[1]].strip().lower()
        return FileKey(path=path, name=name)
    except KeyError as e:
        logging.warning(f"Missing key column '{e}' in {filename}, row {row_num}. Skipping row: {row}")
        return None
    except AttributeError:
        # Handle cases where a value might be None or not a string
        logging.warning(f"Invalid data type for key column in {filename}, row {row_num}. Skipping row: {row}")
        return None


def validate_csv_headers(headers, required_columns, filename):
    """Checks if required columns are present in the CSV headers."""
    missing = [col for col in required_columns if col not in headers]
    if missing:
        logging.error(f"Error: Missing required columns in {filename}: {', '.join(missing)}")
        return False
    return True


# --- Core Logic ---

def find_added_removed_files(before_filepath, after_filepath, output_dir):
    """
    Compares before and after CSV files to find added and removed entries.

    Args:
        before_filepath (str): Path to the 'before' CSV file.
        after_filepath (str): Path to the 'after' CSV file.
        output_dir (str): Directory to save the results ('added_files.csv', 'removed_files.csv').

    Returns:
        tuple: A tuple containing (added_count, removed_count), or (None, None) on failure.
    """
    logging.info(f"Starting comparison: '{before_filepath}' vs '{after_filepath}'")
    logging.info(f"Output will be saved to: '{output_dir}'")

    # --- Step 1: Load keys from the 'before' file ---
    logging.info(f"Processing 'before' file: {before_filepath}")
    before_keys = set()
    before_headers = None
    try:
        with open(before_filepath, mode='r', encoding='utf-8-sig', newline='') as infile:
            reader = csv.DictReader(infile)
            if not reader.fieldnames:
                logging.error(f"Error: Could not read headers from {before_filepath}. Is it empty or invalid?")
                return None, None
            before_headers = reader.fieldnames
            if not validate_csv_headers(before_headers, KEY_COLUMNS, before_filepath):
                return None, None

            row_count = 0
            for i, row in enumerate(reader):
                row_num = i + 2 # Account for header row and 0-based index
                key = create_file_key(row, row_num, before_filepath)
                if key:
                    if key in before_keys:
                        logging.warning(f"Duplicate key {key} found in {before_filepath}, row {row_num}. Keeping first instance.")
                    else:
                        before_keys.add(key)
                row_count += 1
                if row_count % PROGRESS_INTERVAL == 0:
                    logging.info(f"  Processed {row_count} rows from {before_filepath}...")
            logging.info(f"Finished processing {row_count} rows from {before_filepath}. Found {len(before_keys)} unique keys.")

    except FileNotFoundError:
        logging.error(f"Error: 'Before' file not found: {before_filepath}")
        return None, None
    except Exception as e:
        logging.error(f"Error reading {before_filepath}: {e}", exc_info=True)
        return None, None

    # --- Step 2: Process 'after' file to find added files and track existing keys ---
    logging.info(f"Processing 'after' file: {after_filepath}")
    added_count = 0
    after_keys_found = set() # Track keys present in the 'after' file
    after_headers = None

    # Prepare output directory and file for added files
    try:
        os.makedirs(output_dir, exist_ok=True)
        added_filepath = os.path.join(output_dir, 'added_files.csv')

        with open(after_filepath, mode='r', encoding='utf-8-sig', newline='') as after_file, \
             open(added_filepath, 'w', newline='', encoding='utf-8') as added_outfile:

            reader = csv.DictReader(after_file)
            if not reader.fieldnames:
                logging.error(f"Error: Could not read headers from {after_filepath}. Is it empty or invalid?")
                # Clean up potentially created empty added file
                try:
                    added_outfile.close() # Ensure it's closed before removing
                    os.remove(added_filepath)
                except OSError:
                    pass # Ignore error if file couldn't be removed
                return None, None

            after_headers = reader.fieldnames
            if not validate_csv_headers(after_headers, KEY_COLUMNS, after_filepath):
                 # Clean up potentially created empty added file
                try:
                    added_outfile.close()
                    os.remove(added_filepath)
                except OSError:
                    pass
                return None, None

            # Use headers from the 'after' file for the 'added' output
            added_writer = csv.DictWriter(added_outfile, fieldnames=after_headers)
            added_writer.writeheader()

            row_count = 0
            for i, row in enumerate(reader):
                row_num = i + 2 # Account for header row and 0-based index
                key = create_file_key(row, row_num, after_filepath)
                if key:
                    after_keys_found.add(key) # Track this key
                    if key not in before_keys:
                        # This file is in 'after' but not 'before' -> Added
                        added_writer.writerow(row)
                        added_count += 1
                row_count += 1
                if row_count % PROGRESS_INTERVAL == 0:
                    logging.info(f"  Processed {row_count} rows from {after_filepath}...")
            logging.info(f"Finished processing {row_count} rows from {after_filepath}.")
            logging.info(f"Found {added_count} added files.")

    except FileNotFoundError:
        logging.error(f"Error: 'After' file not found: {after_filepath}")
        return None, None
    except Exception as e:
        logging.error(f"Error processing {after_filepath} or writing to {added_filepath}: {e}", exc_info=True)
        return None, None

    # --- Step 3: Re-process 'before' file to find removed files ---
    # A file is removed if its key was in 'before_keys' but not in 'after_keys_found'
    logging.info(f"Determining removed files by re-checking {before_filepath}...")
    removed_count = 0
    removed_filepath = os.path.join(output_dir, 'removed_files.csv')

    try:
        with open(before_filepath, mode='r', encoding='utf-8-sig', newline='') as infile, \
             open(removed_filepath, 'w', newline='', encoding='utf-8') as removed_outfile:

            reader = csv.DictReader(infile)
            # We already validated headers, but good practice to have them for the writer
            if not before_headers: # Should have been set in Step 1
                 logging.error("Internal Error: Before headers not available for writing removed file.")
                 return added_count, None # Return partial success if added file was written

            # Use headers from the 'before' file for the 'removed' output
            removed_writer = csv.DictWriter(removed_outfile, fieldnames=before_headers)
            removed_writer.writeheader()

            row_count = 0
            for i, row in enumerate(reader):
                row_num = i + 2 # Account for header row and 0-based index
                key = create_file_key(row, row_num, before_filepath)
                if key:
                    # Check if this key from 'before' was NOT found when processing 'after'
                    if key not in after_keys_found:
                        removed_writer.writerow(row)
                        removed_count += 1
                row_count += 1
                if row_count % PROGRESS_INTERVAL == 0:
                    logging.info(f"  Re-scanned {row_count} rows from {before_filepath} for removals...")

            logging.info(f"Finished re-scan of {before_filepath}.")
            logging.info(f"Found {removed_count} removed files.")

    except Exception as e:
        # Note: added_files.csv might already exist even if this step fails
        logging.error(f"Error re-reading {before_filepath} or writing {removed_filepath}: {e}", exc_info=True)
        return added_count, None # Return partial success

    return added_count, removed_count


# --- Command Line Argument Parsing ---

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Compare two CSV filesystem snapshots ('before' and 'after') to find added and removed files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Example Usage:
  python {os.path.basename(__file__)} before_snapshot.csv after_snapshot.csv -o results

Details:
  - Compares files based on the '{KEY_COLUMNS[0]}' and '{KEY_COLUMNS[1]}' columns.
  - Comparison is case-insensitive.
  - Expects CSV files with headers.
  - Outputs two files: 'added_files.csv' and 'removed_files.csv' into the specified output directory.
"""
    )
    parser.add_argument(
        "before_file",
        help="Path to the CSV file representing the state BEFORE the change."
    )
    parser.add_argument(
        "after_file",
        help="Path to the CSV file representing the state AFTER the change."
    )
    parser.add_argument(
        "-o", "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to store the output CSV files ('added_files.csv', 'removed_files.csv'). "
             f"Defaults to '{DEFAULT_OUTPUT_DIR}'."
    )
    return parser.parse_args()


# --- Main Execution ---

def main():
    """Main function to parse arguments and run the comparison."""
    args = parse_arguments()

    # Basic validation of input file paths
    if not os.path.isfile(args.before_file):
        logging.error(f"Input 'before' file not found or is not a file: {args.before_file}")
        sys.exit(1)
    if not os.path.isfile(args.after_file):
        logging.error(f"Input 'after' file not found or is not a file: {args.after_file}")
        sys.exit(1)

    added_count, removed_count = find_added_removed_files(
        args.before_file,
        args.after_file,
        args.output_dir
    )

    if added_count is None or removed_count is None:
        logging.error("Comparison process failed. Please check the logs for details.")
        sys.exit(1)
    else:
        logging.info("-" * 30)
        logging.info("Comparison Complete.")
        logging.info(f"  Total files added: {added_count}")
        logging.info(f"  Total files removed: {removed_count}")
        logging.info(f"  Results saved in directory: '{args.output_dir}'")
        logging.info("-" * 30)
        sys.exit(0)


if __name__ == "__main__":
    main()