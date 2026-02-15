import sqlite3

conn = sqlite3.connect("bank.db", check_same_thread=False)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS wallets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    currency TEXT,
    balance REAL DEFAULT 0,
    UNIQUE(user_id, currency)
)
""")

conn.commit()