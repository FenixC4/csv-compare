import argparse
import csv
import logging
import sys
import os # Added
import shutil # Added
import subprocess # Added
import tempfile # Added
from contextlib import ExitStack
from typing import Dict, Any, Tuple, TextIO, Optional, List, Iterator

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
KEY_COLUMNS: Tuple[str, str] = ("Path", "Name")
DEFAULT_ENCODING: str = 'utf-8'
CSVSORT_COMMAND: str = "csvsort" # Command for csvkit's sort utility

# --- Custom Exceptions ---
class CsvProcessingError(Exception): pass
class FileOpenError(CsvProcessingError): pass
class MissingColumnError(CsvProcessingError): pass
class CsvReadError(CsvProcessingError): pass
class ExternalToolError(CsvProcessingError): # New Exception
    """Error executing an external tool like csvsort."""
    pass

# --- Core Logic ---

def get_row_key(row: Dict[str, Any], key_columns: Tuple[str, ...]) -> Tuple:
    """Extracts the composite key from a CSV row dictionary."""
    try:
        # Ensure consistent comparison by converting key parts to strings
        return tuple(str(row[col]) for col in key_columns)
    except KeyError as e:
        logging.error(f"Key column '{e}' not found in row: {row}")
        raise

def validate_csv_headers(reader: csv.DictReader, expected_columns: Tuple[str, ...], filename: str):
    """Validates if the required key columns exist in the CSV file's header."""
    if reader.fieldnames is None:
         raise MissingColumnError(f"Could not read header from '{filename}'. File might be empty or malformed.")
    missing_cols = [col for col in expected_columns if col not in reader.fieldnames]
    if missing_cols:
        raise MissingColumnError(
            f"File '{filename}' is missing required columns: {', '.join(missing_cols)}"
        )
    logging.debug(f"Header validation passed for '{filename}'. Found columns: {reader.fieldnames}")

# --- New Function for External Sorting ---
def sort_file_externally(
    input_path: str,
    output_path: str,
    key_columns: Tuple[str, ...],
    encoding: str
) -> None:
    """
    Sorts a CSV file using an external 'csvsort' command.

    Args:
        input_path: Path to the input CSV file.
        output_path: Path where the sorted CSV file will be written.
        key_columns: Tuple of column names to sort by.
        encoding: The character encoding of the input/output files.

    Raises:
        ExternalToolError: If 'csvsort' is not found or fails.
        FileNotFoundError: If input_path does not exist.
        IOError: If output file cannot be written.
    """
    # 1. Check if csvsort command exists
    if not shutil.which(CSVSORT_COMMAND):
        raise ExternalToolError(
            f"'{CSVSORT_COMMAND}' command not found in PATH. "
            f"Please install csvkit (`pip install csvkit`) and ensure it's accessible."
        )

    # 2. Check if input file exists (basic check)
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file for sorting not found: {input_path}")

    # 3. Construct the command
    # csvsort uses comma-separated column names for the -c flag
    key_cols_str = ",".join(key_columns)
    command = [
        CSVSORT_COMMAND,
        "-c", key_cols_str,
        "--encoding", encoding,
        input_path
    ]

    logging.info(f"Running external sort for '{input_path}'...")
    logging.debug(f"Executing command: {' '.join(command)} > {output_path}")

    try:
        # Open the output file *before* running the subprocess
        with open(output_path, 'wb') as outfile: # Write in binary mode for subprocess stdout
             # Execute the command, redirecting stdout to our output file
             process = subprocess.run(
                 command,
                 stdout=outfile, # Redirect stdout here
                 stderr=subprocess.PIPE, # Capture stderr
                 check=False, # Don't raise CalledProcessError automatically
                 encoding=None # Work with bytes for stderr
             )

        # Check results after process completes
        if process.returncode != 0:
            stderr_output = process.stderr.decode(encoding, errors='replace') if process.stderr else "No stderr output."
            raise ExternalToolError(
                f"'{CSVSORT_COMMAND}' failed for '{input_path}' with exit code {process.returncode}.\n"
                f"Stderr:\n{stderr_output}"
            )
        logging.info(f"Successfully sorted '{input_path}' to '{output_path}'.")

    except IOError as e:
        logging.error(f"Failed to write sorted output to '{output_path}': {e}")
        raise # Re-raise as IOError
    except Exception as e: # Catch other potential subprocess errors
        logging.error(f"An unexpected error occurred while running {CSVSORT_COMMAND}: {e}")
        # Clean up potentially partially written output file on error
        if os.path.exists(output_path):
            try:
                os.remove(output_path)
            except OSError:
                logging.warning(f"Could not remove partial output file '{output_path}' after error.")
        raise ExternalToolError(f"Failed during external sort execution: {e}") from e


# --- Comparison Logic (Mostly Unchanged) ---
def compare_snapshots(
    snapshot1_path: str, # Path to the file *to be read* (original or temp sorted)
    snapshot2_path: str, # Path to the file *to be read* (original or temp sorted)
    inserts_path: str,
    deletes_path: str,
    key_columns: Tuple[str, ...] = KEY_COLUMNS,
    encoding: str = DEFAULT_ENCODING
    # Removed sort_inputs parameter, sorting is handled *before* calling this
) -> None:
    """
    Compares two sorted CSV snapshots and writes differences to output files.

    ASSUMES input CSVs (snapshot1_path, snapshot2_path) ARE ALREADY SORTED
    by the specified key_columns.

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
    # This function now *always* assumes inputs are sorted.
    # The sorting happens externally if requested by the user in main().
    logging.info(f"Starting comparison between '{snapshot1_path}' and '{snapshot2_path}'")
    logging.info(f"Using key columns: {key_columns}")
    logging.info(f"Outputting inserts to '{inserts_path}', deletes to '{deletes_path}'")

    with ExitStack() as stack:
        try:
            # Open input files (which are assumed sorted now)
            file1: TextIO = stack.enter_context(
                open(snapshot1_path, 'r', newline='', encoding=encoding)
            )
            file2: TextIO = stack.enter_context(
                open(snapshot2_path, 'r', newline='', encoding=encoding)
            )

            reader1: csv.DictReader = csv.DictReader(file1)
            reader2: csv.DictReader = csv.DictReader(file2)

            validate_csv_headers(reader1, key_columns, snapshot1_path)
            validate_csv_headers(reader2, key_columns, snapshot2_path)

            insert_fieldnames: Optional[List[str]] = reader2.fieldnames
            delete_fieldnames: Optional[List[str]] = reader1.fieldnames
            if not insert_fieldnames or not delete_fieldnames:
                 raise CsvProcessingError("Could not determine fieldnames for output files.")

            # Open output files
            file_inserts: TextIO = stack.enter_context(
                open(inserts_path, 'w', newline='', encoding=encoding)
            )
            file_deletes: TextIO = stack.enter_context(
                open(deletes_path, 'w', newline='', encoding=encoding)
            )

            writer_inserts = csv.DictWriter(file_inserts, fieldnames=insert_fieldnames)
            writer_deletes = csv.DictWriter(file_deletes, fieldnames=delete_fieldnames)

            writer_inserts.writeheader()
            writer_deletes.writeheader()

        except (FileNotFoundError, IOError, OSError) as e:
            logging.error(f"Failed to open or setup file: {e}")
            raise FileOpenError(f"Error opening/setting up file: {e}") from e
        except MissingColumnError as e:
             logging.error(f"Header validation failed: {e}")
             raise

        # --- Comparison Logic (Unchanged from original) ---
        try:
            row1: Optional[Dict[str, Any]] = next(reader1, None)
            row2: Optional[Dict[str, Any]] = next(reader2, None)
            insert_count = 0
            delete_count = 0

            while row1 is not None and row2 is not None:
                key1: Tuple = get_row_key(row1, key_columns)
                key2: Tuple = get_row_key(row2, key_columns)

                if key1 == key2:
                    row1 = next(reader1, None)
                    row2 = next(reader2, None)
                elif key1 < key2:
                    writer_deletes.writerow(row1)
                    delete_count += 1
                    row1 = next(reader1, None)
                else: # key1 > key2
                    writer_inserts.writerow(row2)
                    insert_count += 1
                    row2 = next(reader2, None)

            while row1 is not None:
                writer_deletes.writerow(row1)
                delete_count += 1
                row1 = next(reader1, None)

            while row2 is not None:
                writer_inserts.writerow(row2)
                insert_count += 1
                row2 = next(reader2, None)

            logging.info(f"Comparison finished. Found {insert_count} insertions and {delete_count} deletions.")

        except (csv.Error, KeyError) as e:
            current_line1 = reader1.line_num if reader1 else 'N/A'
            current_line2 = reader2.line_num if reader2 else 'N/A'
            logging.error(
                f"Error processing CSV data near line {current_line1} of '{snapshot1_path}' "
                f"or line {current_line2} of '{snapshot2_path}': {e}"
            )
            raise CsvReadError(f"Error reading CSV data: {e}") from e
        except IOError as e:
            logging.error(f"Error writing to output file: {e}")
            raise

    logging.info(f"Successfully wrote inserts to '{inserts_path}' and deletes to '{deletes_path}'.")


# --- Command Line Interface ---
def main():
    """Parses command-line arguments and orchestrates the CSV comparison process."""
    parser = argparse.ArgumentParser(
        description=(
            "Compare two CSV snapshots and output differences (insertions/deletions). "
            "Requires input files to be SORTED by key columns, OR use --presort-files "
            "to sort them externally using 'csvsort' (requires csvkit installed)."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    # Input/Output Arguments (unchanged)
    parser.add_argument("snapshot1", help="Path to the first (older) CSV snapshot file.")
    parser.add_argument("snapshot2", help="Path to the second (newer) CSV snapshot file.")
    parser.add_argument("-i", "--inserts", default="inserts.csv", help="Path for the output CSV file containing inserted rows.")
    parser.add_argument("-d", "--deletes", default="deletes.csv", help="Path for the output CSV file containing deleted rows.")
    # Configuration Arguments (unchanged)
    parser.add_argument("--key-columns", nargs='+', default=list(KEY_COLUMNS), help="Space-separated list of column names forming the unique sort key.")
    parser.add_argument("--encoding", default=DEFAULT_ENCODING, help="Character encoding for all input and output CSV files.")
    # New Sorting Argument
    parser.add_argument(
        "--presort-files",
        action="store_true",
        help=(
            "Pre-sort the input snapshot files using the external 'csvsort' command "
            "before comparison. Requires 'csvkit' to be installed and in the system PATH. "
            "Temporary sorted files will be created and deleted."
        )
    )
    # Logging Argument (unchanged)
    parser.add_argument("-v", "--verbose", action="store_const", dest="loglevel", const=logging.DEBUG, default=logging.INFO, help="Enable verbose (DEBUG level) logging.")

    args = parser.parse_args()
    logging.getLogger().setLevel(args.loglevel)

    key_cols_tuple = tuple(args.key_columns)
    snapshot1_to_compare = args.snapshot1
    snapshot2_to_compare = args.snapshot2
    temp_dir = None # Keep track of temporary directory if created

    try:
        if args.presort_files:
            logging.info("Pre-sorting files externally using csvsort...")
            # Create a temporary directory to hold sorted files
            temp_dir = tempfile.mkdtemp(prefix="csv_compare_sort_")
            logging.debug(f"Created temporary directory: {temp_dir}")

            # Define paths for temporary sorted files
            temp_snapshot1 = os.path.join(temp_dir, "sorted_snapshot1.csv")
            temp_snapshot2 = os.path.join(temp_dir, "sorted_snapshot2.csv")

            # Sort the files
            sort_file_externally(args.snapshot1, temp_snapshot1, key_cols_tuple, args.encoding)
            sort_file_externally(args.snapshot2, temp_snapshot2, key_cols_tuple, args.encoding)

            # Update the paths to be used for comparison
            snapshot1_to_compare = temp_snapshot1
            snapshot2_to_compare = temp_snapshot2
            logging.info("External sorting complete.")
        else:
             logging.info("Assuming input files are already sorted. Use --presort-files otherwise.")

        # Run the comparison using the appropriate (original or temporary sorted) files
        compare_snapshots(
            snapshot1_path=snapshot1_to_compare,
            snapshot2_path=snapshot2_to_compare,
            inserts_path=args.inserts,
            deletes_path=args.deletes,
            key_columns=key_cols_tuple,
            encoding=args.encoding
        )
        logging.info("Script finished successfully.")
        sys.exit(0)

    except CsvProcessingError as e:
        logging.error(f"A CSV processing error occurred: {e}")
        sys.exit(1)
    except ExternalToolError as e:
        logging.error(f"External tool error: {e}")
        sys.exit(4) # Different exit code for external tool failures
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(2)
    finally:
        # --- Cleanup ---
        if temp_dir:
            try:
                logging.debug(f"Removing temporary directory: {temp_dir}")
                shutil.rmtree(temp_dir)
            except OSError as e:
                logging.warning(f"Could not remove temporary directory '{temp_dir}': {e}")


if __name__ == '__main__':
    main()
