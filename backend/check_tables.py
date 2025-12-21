import sqlite3

conn = sqlite3.connect('ravvyn.db')
cursor = conn.cursor()

# Get all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()
print("Tables in database:", tables)

# Get schema for each table
for table in tables:
    table_name = table[0]
    print(f"\nSchema for {table_name}:")
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    for col in columns:
        print(f"  {col[1]} ({col[2]})")
    
    # Show sample data
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
    sample_data = cursor.fetchall()
    print(f"Sample data from {table_name}:")
    for row in sample_data:
        print(f"  {row}")

conn.close()