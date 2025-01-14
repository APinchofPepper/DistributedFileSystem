import sqlite3
from tabulate import tabulate  # For pretty printing the metadata table

def fetch_all_metadata():
    try:
        # Connect to the metadata database
        conn = sqlite3.connect('metadata.db')
        cursor = conn.cursor()

        # Fetch all rows from the metadata table
        cursor.execute('SELECT * FROM metadata')
        rows = cursor.fetchall()

        # Fetch column names for better display
        column_names = [description[0] for description in cursor.description]

        conn.close()

        # Check if metadata is empty
        if not rows:
            print("No metadata found in the database.")
            return

        # Display metadata in a table format
        print(tabulate(rows, headers=column_names, tablefmt="fancy_grid"))

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    print("Displaying all metadata from metadata.db:")
    fetch_all_metadata()
