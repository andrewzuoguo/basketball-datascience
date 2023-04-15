import sqlite3
from datetime import datetime

def connect():
    try:
        conn = sqlite3.connect("nba.db")
    except Exception as error:
        print(error)

    return conn

def now():
    now = datetime.now()
    return now.strftime("%m/%d/%Y, %H:%M:%S")
