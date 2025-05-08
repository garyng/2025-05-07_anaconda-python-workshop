import sqlite3


def connect_db(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)
