import sqlite3
from datetime import datetime

def connect():
    """
    Establishes a connection to the 'nba-data.db' database.

    Returns:
    conn (sqlite3.Connection): a Connection object representing the database connection
    """
    try:
        conn = sqlite3.connect("/Users/andrewguo/Library/Mobile Documents/com~apple~CloudDocs/basketball-datascience/data/nba-data.db")
    except sqlite3.Error as e:
        print(f"Error connecting to database: {e}")
        raise e

    return conn

def get_current_time():
    """
    Returns the current date and time in the format 'MM/DD/YYYY, HH:MM:SS'.

    Returns:
    (str): a string representing the current date and time
    """
    current_time = datetime.now()
    return current_time.strftime("%m/%d/%Y, %H:%M:%S")
