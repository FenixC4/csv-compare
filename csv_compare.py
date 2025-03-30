import csv
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def open_csv_reader(file_path):
    try:
        f = open(file_path, newline='', encoding='utf-8')
        reader = csv.DictReader(f)
        return f, reader
    except Exception as e:
        logging.error(f"Error opening {file_path}: {e}")
        sys.exit(1)

def open_csv_writer(file_path, fieldnames):
    try:
        f = open(file_path, mode='w', newline='', encoding='utf-8')
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        return f, writer
    except Exception as e:
        logging.error(f"Error opening writer for {file_path}: {e}")
        sys.exit(1)

def get_key(row):
    """Return a tuple (Path, Name) for comparison."""
    return (row["Path"], row["Name"])

def compare_sorted_csv(snapshot1, snapshot2, inserts_out, deletes_out):
    # Open input CSVs
    f1, reader1 = open_csv_reader(snapshot1)
    f2, reader2 = open_csv_reader(snapshot2)
    
    # Check that the "Path" and "Name" columns exist
    for field in ["Path", "Name"]:
        if field not in reader1.fieldnames:
            logging.error(f"File {snapshot1} does not contain a '{field}' column.")
            sys.exit(1)
        if field not in reader2.fieldnames:
            logging.error(f"File {snapshot2} does not contain a '{field}' column.")
            sys.exit(1)
    
    # Open output CSVs using the same header as the input files
    f_inserts, writer_inserts = open_csv_writer(inserts_out, fieldnames=reader2.fieldnames)
    f_deletes, writer_deletes = open_csv_writer(deletes_out, fieldnames=reader1.fieldnames)
    
    # Get first row from each file
    try:
        row1 = next(reader1, None)
        row2 = next(reader2, None)
    except Exception as e:
        logging.error(f"Error reading from CSV: {e}")
        sys.exit(1)
    
    # Merge-like comparison using (Path, Name) as key
    while row1 is not None and row2 is not None:
        key1 = get_key(row1)
        key2 = get_key(row2)
        
        if key1 == key2:
            # The record exists in both snapshots, advance both
            row1 = next(reader1, None)
            row2 = next(reader2, None)
        elif key1 < key2:
            # Present in snapshot1 only => deletion
            writer_deletes.writerow(row1)
            row1 = next(reader1, None)
        else:
            # Present in snapshot2 only => insertion
            writer_inserts.writerow(row2)
            row2 = next(reader2, None)
    
    # Handle remaining rows in snapshot1 (deletions)
    while row1 is not None:
        writer_deletes.writerow(row1)
        row1 = next(reader1, None)
    
    # Handle remaining rows in snapshot2 (insertions)
    while row2 is not None:
        writer_inserts.writerow(row2)
        row2 = next(reader2, None)
    
    # Close all files
    f1.close()
    f2.close()
    f_inserts.close()
    f_deletes.close()
    
    logging.info(f"Comparison complete. Inserts written to '{inserts_out}', deletes written to '{deletes_out}'.")

def main():
    snapshot1 = 'snapshot1.csv'   # Sorted CSV snapshot from the first run
    snapshot2 = 'snapshot2.csv'   # Sorted CSV snapshot from the second run
    inserts_csv = 'inserts.csv'
    deletes_csv = 'deletes.csv'
    
    compare_sorted_csv(snapshot1, snapshot2, inserts_csv, deletes_csv)

if __name__ == '__main__':
    main()
