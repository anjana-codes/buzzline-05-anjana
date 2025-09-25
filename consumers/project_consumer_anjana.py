import json
import sqlite3
import pathlib
import time

DATA_FILE = pathlib.Path('data/project_live.json')
DB_FILE = pathlib.Path('data/anjana_keyword.sqlite')


def init_db(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message TEXT,
            author TEXT,
            timestamp TEXT,
            category TEXT,
            sentiment REAL,
            keyword_mentioned TEXT,
            message_length INTEGER
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS keyword_counts (
            keyword TEXT PRIMARY KEY,
            count INTEGER
        )
    ''')

    conn.commit()
    conn.close()


def store_message(message, db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    cursor.execute('''
        INSERT INTO messages (message, author, timestamp, category, sentiment, keyword_mentioned, message_length)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        message.get("message"),
        message.get("author"),
        message.get("timestamp"),
        message.get("category"),
        float(message.get("sentiment", 0.0)),
        message.get("keyword_mentioned"),
        int(message.get("message_length", 0))
    ))

    keyword = message.get("keyword_mentioned")
    if keyword:
        cursor.execute('''
            INSERT INTO keyword_counts (keyword, count)
            VALUES (?, 1)
            ON CONFLICT(keyword) DO UPDATE SET count = count + 1
        ''', (keyword,))

    conn.commit()
    conn.close()


def read_one_message(data_file):
    with open(data_file, 'r') as f:
        for line in f:
            if line.strip():
                return json.loads(line.strip())
    return None


def main():
    init_db(DB_FILE)
    print(f"Starting continuous loop. Reading from {DATA_FILE} and storing in {DB_FILE}")
    try:
        while True:
            message = read_one_message(DATA_FILE)
            if message:
                store_message(message, DB_FILE)
                print("Stored message and updated keyword counts.")
            else:
                print("No new message found.")
            time.sleep(2)  # Delay to prevent busy-looping
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting...")


if __name__ == "__main__":
    main()
