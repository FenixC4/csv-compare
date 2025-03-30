import argparse
import csv
import logging
import sys
from contextlib import ExitStack
from typing import Dict, Any, Tuple, TextIO, Optional, List, Iterator

# --- Configuration ---

# Configure logging for clear output
# Use INFO level for standard operation, DEBUG for verbose details
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Define the columns used to uniquely identify and sort rows
# Making this a constant improves readability and maintainability.
KEY_COLUMNS: Tuple[str, str] = ("Path", "Name")
# Define standard encoding
DEFAULT_ENCODING: str = 'utf-8'

# --- Custom Exceptions ---

class CsvProcessingError(Exception):
    """Base exception for errors during CSV processing."""
    pass

class FileOpenError(CsvProcessingError):
    """Error opening a file."""
    pass

class MissingColumnError(CsvProcessingError):
    """Required column is missing from a CSV file."""
    pass

class CsvReadError(CsvProcessingError):
    """Error reading data from a CSV file."""
    pass

# --- Core Logic ---

def get_row_key(row: Dict[str, Any], key_columns: Tuple[str, ...]) -> Tuple:
    """
    Extracts the composite key from a CSV row dictionary.

    Args:
        row: A dictionary representing a row from a CSV file.
        key_columns: A tuple of column names that form the unique key.

    Returns:
        A tuple containing the values from the key columns in the specified order.

    Raises:
        KeyError: If any of the key_columns are not present in the row.
                  (Should be caught by header validation ideally, but good practice).
    """
    try:
        return tuple(row[col] for col in key_columns)
    except KeyError as e:
        # This indicates a programming error or unexpected data format change
        # if header validation passed.
        logging.error(f"Key column '{e}' not found in row: {row}")
        raise  # Re-raise as it's likely a critical issue

def validate_csv_headers(reader: csv.DictReader, expected_columns: Tuple[str, ...], filename: str):
    """
    Validates if the required key columns exist in the CSV file's header.

    Args:
        reader: The csv.DictReader instance.
        expected_columns: The tuple of column names required.
        filename: The name of the file being validated (for error messages).

    Raises:
        MissingColumnError: If any expected column is not found in the header.
    """
    if reader.fieldnames is None:
         # Should not happen with DictReader unless file is empty AND no header
         raise MissingColumnError(f"Could not read header from '{filename}'. File might be empty or malformed.")

    missing_cols = [col for col in expected_columns if col not in reader.fieldnames]
    if missing_cols:
        raise MissingColumnError(
            f"File '{filename}' is missing required columns: {', '.join(missing_cols)}"
        )
    logging.debug(f"Header validation passed for '{filename}'. Found columns: {reader.fieldnames}")

def compare_snapshots(
    snapshot1_path: str,
    snapshot2_path: str,
    inserts_path: str,
    deletes_path: str,
    key_columns: Tuple[str, ...] = KEY_COLUMNS,
    encoding: str = DEFAULT_ENCODING
) -> None:
    """
    Compares two sorted CSV snapshots and writes differences to output files.

    Assumes input CSVs are sorted by the specified key_columns.
    Uses a merge-join like algorithm for efficient comparison.

    Args:
        snapshot1_path: Path to the first (older) sorted CSV snapshot.
        snapshot2_path: Path to the second (newer) sorted CSV snapshot.
        inserts_path: Path to write rows present in snapshot2 but not snapshot1.
        deletes_path: Path to write rows present in snapshot1 but not snapshot2.
        key_columns: Tuple of column names forming the unique sort key.
        encoding: The character encoding to use for all files.

    Raises:
        FileOpenError: If any file cannot be opened.
        MissingColumnError: If key columns are missing from input headers.
        CsvReadError: If there's an error reading CSV data (e.g., malformed row).
        IOError: For other file writing issues.
    """
    logging.info(f"Starting comparison between '{snapshot1_path}' and '{snapshot2_path}'")
    logging.info(f"Using key columns: {key_columns}")
    logging.info(f"Outputting inserts to '{inserts_path}', deletes to '{deletes_path}'")

    # ExitStack ensures all opened files are closed, even if errors occur.
    with ExitStack() as stack:
        try:
            # Open input files for reading
            file1: TextIO = stack.enter_context(
                open(snapshot1_path, 'r', newline='', encoding=encoding)
            )
            file2: TextIO = stack.enter_context(
                open(snapshot2_path, 'r', newline='', encoding=encoding)
            )

            reader1: csv.DictReader = csv.DictReader(file1)
            reader2: csv.DictReader = csv.DictReader(file2)

            # Validate headers *after* opening readers
            validate_csv_headers(reader1, key_columns, snapshot1_path)
            validate_csv_headers(reader2, key_columns, snapshot2_path)

            # Determine fieldnames for output files (use newer snapshot for inserts, older for deletes)
            # Ensure consistency if headers differ beyond key columns (optional, depends on requirements)
            # Here, we assume the structure relevant for inserts comes from snapshot2, deletes from snapshot1.
            insert_fieldnames: Optional[List[str]] = reader2.fieldnames
            delete_fieldnames: Optional[List[str]] = reader1.fieldnames
            if not insert_fieldnames or not delete_fieldnames:
                 # This check is slightly redundant due to validate_csv_headers, but belts and suspenders
                 raise CsvProcessingError("Could not determine fieldnames for output files.")


            # Open output files for writing
            file_inserts: TextIO = stack.enter_context(
                open(inserts_path, 'w', newline='', encoding=encoding)
            )
            file_deletes: TextIO = stack.enter_context(
                open(deletes_path, 'w', newline='', encoding=encoding)
            )

            writer_inserts = csv.DictWriter(file_inserts, fieldnames=insert_fieldnames)
            writer_deletes = csv.DictWriter(file_deletes, fieldnames=delete_fieldnames)

            # Write headers to output files
            writer_inserts.writeheader()
            writer_deletes.writeheader()

        except (FileNotFoundError, IOError, OSError) as e:
            # Catch file-related errors during opening/setup
            logging.error(f"Failed to open or setup file: {e}")
            raise FileOpenError(f"Error opening/setting up file: {e}") from e
        except MissingColumnError as e:
             # Propagate header validation errors
             logging.error(f"Header validation failed: {e}")
             raise # Re-raise the specific error

        # --- Comparison Logic ---
        try:
            # Get the first row from each reader (returns None if file is empty)
            row1: Optional[Dict[str, Any]] = next(reader1, None)
            row2: Optional[Dict[str, Any]] = next(reader2, None)

            insert_count = 0
            delete_count = 0

            # Process rows while both files have data
            while row1 is not None and row2 is not None:
                key1: Tuple = get_row_key(row1, key_columns)
                key2: Tuple = get_row_key(row2, key_columns)

                if key1 == key2:
                    # Rows match: advance both readers
                    row1 = next(reader1, None)
                    row2 = next(reader2, None)
                elif key1 < key2:
                    # Row only in snapshot1: it's a deletion
                    writer_deletes.writerow(row1)
                    delete_count += 1
                    row1 = next(reader1, None)
                else: # key1 > key2
                    # Row only in snapshot2: it's an insertion
                    writer_inserts.writerow(row2)
                    insert_count += 1
                    row2 = next(reader2, None)

            # Process remaining rows in snapshot1 (deletions)
            while row1 is not None:
                writer_deletes.writerow(row1)
                delete_count += 1
                row1 = next(reader1, None)

            # Process remaining rows in snapshot2 (insertions)
            while row2 is not None:
                writer_inserts.writerow(row2)
                insert_count += 1
                row2 = next(reader2, None)

            logging.info(f"Comparison finished. Found {insert_count} insertions and {delete_count} deletions.")

        except (csv.Error, KeyError) as e:
            # Catch errors during CSV reading or key access
            # Provide context about which file and potentially which row number
            current_line1 = reader1.line_num if reader1 else 'N/A'
            current_line2 = reader2.line_num if reader2 else 'N/A'
            logging.error(
                f"Error processing CSV data near line {current_line1} of '{snapshot1_path}' "
                f"or line {current_line2} of '{snapshot2_path}': {e}"
            )
            raise CsvReadError(f"Error reading CSV data: {e}") from e
        except IOError as e:
            # Catch errors during writing to output files
            logging.error(f"Error writing to output file: {e}")
            raise # Re-raise as IOError

    # No explicit file closing needed - ExitStack handles it automatically.
    logging.info(f"Successfully wrote inserts to '{inserts_path}' and deletes to '{deletes_path}'.")


# --- Command Line Interface ---

def main():
    """
    Parses command-line arguments and orchestrates the CSV comparison process.
    """
    parser = argparse.ArgumentParser(
        description="Compare two sorted CSV snapshots and output differences.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter # Show default values in help
    )

    parser.add_argument(
        "snapshot1",
        help="Path to the first (older) sorted CSV snapshot file."
    )
    parser.add_argument(
        "snapshot2",
        help="Path to the second (newer) sorted CSV snapshot file."
    )
    parser.add_argument(
        "-i", "--inserts",
        default="inserts.csv",
        help="Path for the output CSV file containing inserted rows."
    )
    parser.add_argument(
        "-d", "--deletes",
        default="deletes.csv",
        help="Path for the output CSV file containing deleted rows."
    )
    parser.add_argument(
        "--key-columns",
        nargs='+', # Allows specifying multiple columns like --key-columns Path Name
        default=list(KEY_COLUMNS), # Use default defined above
        help="Space-separated list of column names forming the unique sort key."
    )
    parser.add_argument(
        "--encoding",
        default=DEFAULT_ENCODING,
        help="Character encoding for all input and output CSV files."
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_const",
        dest="loglevel",
        const=logging.DEBUG,
        default=logging.INFO,
        help="Enable verbose (DEBUG level) logging."
    )

    args = parser.parse_args()

    # Update logging level if verbose flag is set
    logging.getLogger().setLevel(args.loglevel)

    try:
        # Convert key_columns back to tuple for immutability/hashing if needed elsewhere
        key_cols_tuple = tuple(args.key_columns)

        compare_snapshots(
            snapshot1_path=args.snapshot1,
            snapshot2_path=args.snapshot2,
            inserts_path=args.inserts,
            deletes_path=args.deletes,
            key_columns=key_cols_tuple,
            encoding=args.encoding
        )
        logging.info("Script finished successfully.")
        sys.exit(0) # Explicitly exit with success code

    except CsvProcessingError as e:
        # Catch specific application errors raised during comparison
        logging.error(f"A CSV processing error occurred: {e}")
        sys.exit(1) # Exit with error code 1 for application errors
    except Exception as e:
        # Catch any other unexpected errors
        logging.critical(f"An unexpected error occurred: {e}", exc_info=True) # Log traceback
        sys.exit(2) # Exit with a different error code for unexpected errors


if __name__ == '__main__':
    main()
