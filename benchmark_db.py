import time
import random
import os
import sqlite3
import mysql.connector
from pymongo import MongoClient
from tinydb import TinyDB, Query

# --- Configuration ---
MYSQL_CONFIG = {
    'user': 'bench', 
    'password': 'password',  # Enter your MySQL password here
    'host': 'localhost',
    'database': 'shopping_db'
}

# --- Data Generator ---
def get_dataset():
    """Generates the exact same dataset for all databases"""
    random.seed(42)
    users = []
    orders = []

    # Generate 5,000 Users
    for i in range(1, 5001):
        users.append({
            'user_id': i,
            'name': f"User {i}",
            'email': f"user{i}@example.com",
            'created_at': "2023-01-01 10:00:00"
        })

    # Generate 20,000 Orders
    statuses = ["PENDING", "PAID", "CANCELLED"]
    for i in range(1, 20001):
        orders.append({
            'order_id': i,
            'user_id': random.randint(1, 5000), # Random user FK
            'amount': round(random.uniform(1, 500), 2),
            'status': random.choice(statuses),
            'created_at': "2023-01-02 11:00:00"
        })
        
    return users, orders

# --- Benchmark Timer Decorator ---
class Benchmark:
    def __init__(self, db_name):
        self.db_name = db_name
        self.results = {}

    def measure(self, operation_name, func, *args):
        start = time.perf_counter()
        func(*args)
        end = time.perf_counter()
        duration_ms = (end - start) * 1000
        self.results[operation_name] = duration_ms
        print(f"[{self.db_name}] {operation_name}: {duration_ms:.2f} ms")

    def print_summary(self):
        print(f"\n--- {self.db_name} Results ---")
        for key, val in self.results.items():
            print(f"{key}: {val:.2f} ms")

# ==========================================
# 1. SQLite Implementation
# ==========================================
def run_sqlite(users, orders):
    db_file = "benchmark.db"
    if os.path.exists(db_file):
        os.remove(db_file)
    
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    bench = Benchmark("SQLite")

    # Setup Tables
    c.execute('''CREATE TABLE users (user_id INTEGER PRIMARY KEY, name TEXT, email TEXT, created_at TEXT)''')
    c.execute('''CREATE TABLE orders (order_id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL, status TEXT, created_at TEXT, FOREIGN KEY(user_id) REFERENCES users(user_id))''')
    conn.commit()

    # CREATE
    def insert_data():
        for u in users:
            c.execute("INSERT INTO users VALUES (?, ?, ?, ?)", (u['user_id'], u['name'], u['email'], u['created_at']))
        for o in orders:
            c.execute("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", (o['order_id'], o['user_id'], o['amount'], o['status'], o['created_at']))
        conn.commit()
    bench.measure("Create", insert_data)

    # READ 1 (Point Query 1000x)
    def read_point():
        for _ in range(1000):
            c.execute("SELECT * FROM users WHERE user_id = 2500")
            c.fetchone()
    bench.measure("Read-1 Point", read_point)

    # READ 2 (Range Query)
    def read_range():
        c.execute("SELECT * FROM orders WHERE amount > 300 AND status = 'PAID'")
        c.fetchall()
    bench.measure("Read-2 Range", read_range)

    # READ 3 (User Orders 100x)
    def read_user_orders():
        for _ in range(100):
            c.execute("SELECT * FROM orders WHERE user_id = 1234")
            c.fetchall()
    bench.measure("Read-3 UserOrders", read_user_orders)

    # UPDATE
    def update_op():
        c.execute("UPDATE orders SET status = 'PAID' WHERE status = 'PENDING'")
        conn.commit()
    bench.measure("Update", update_op)

    # DELETE
    def delete_op():
        c.execute("DELETE FROM orders WHERE status = 'CANCELLED'")
        conn.commit()
    bench.measure("Delete", delete_op)

    conn.close()
    return bench.results

# ==========================================
# 2. MySQL Implementation
# ==========================================
def run_mysql(users, orders):
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
    except Exception as e:
        print(f"Skipping MySQL: {e}")
        return {}

    c = conn.cursor()
    bench = Benchmark("MySQL")

    # Setup Tables (Drop if exists)
    c.execute("DROP TABLE IF EXISTS orders")
    c.execute("DROP TABLE IF EXISTS users")
    c.execute("CREATE TABLE users (user_id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255), created_at DATETIME)")
    c.execute("CREATE TABLE orders (order_id INT PRIMARY KEY, user_id INT, amount DECIMAL(10,2), status VARCHAR(50), created_at DATETIME, FOREIGN KEY (user_id) REFERENCES users(user_id))")
    conn.commit()

    # CREATE (One by one)
    def insert_data():
        # Using prepared statements
        for u in users:
            c.execute("INSERT INTO users (user_id, name, email, created_at) VALUES (%s, %s, %s, %s)", (u['user_id'], u['name'], u['email'], u['created_at']))
        for o in orders:
            c.execute("INSERT INTO orders (order_id, user_id, amount, status, created_at) VALUES (%s, %s, %s, %s, %s)", (o['order_id'], o['user_id'], o['amount'], o['status'], o['created_at']))
        conn.commit()
    bench.measure("Create", insert_data)

    # READ 1
    def read_point():
        for _ in range(1000):
            c.execute("SELECT * FROM users WHERE user_id = 2500")
            c.fetchall()
    bench.measure("Read-1 Point", read_point)

    # READ 2
    def read_range():
        c.execute("SELECT * FROM orders WHERE amount > 300 AND status = 'PAID'")
        c.fetchall()
    bench.measure("Read-2 Range", read_range)

    # READ 3
    def read_user_orders():
        for _ in range(100):
            c.execute("SELECT * FROM orders WHERE user_id = 1234")
            c.fetchall()
    bench.measure("Read-3 UserOrders", read_user_orders)

    # UPDATE
    def update_op():
        c.execute("UPDATE orders SET status = 'PAID' WHERE status = 'PENDING'")
        conn.commit()
    bench.measure("Update", update_op)

    # DELETE
    def delete_op():
        c.execute("DELETE FROM orders WHERE status = 'CANCELLED'")
        conn.commit()
    bench.measure("Delete", delete_op)

    conn.close()
    return bench.results

# ==========================================
# 3. MongoDB Implementation
# ==========================================
def run_mongodb(users, orders):
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["shopping_db"]
    except Exception as e:
        print(f"Skipping MongoDB: {e}")
        return {}

    bench = Benchmark("MongoDB")

    # Clean start
    db.users.drop()
    db.orders.drop()

    # CREATE
    def insert_data():
        for u in users:
    	    db.users.insert_one(u.copy())  # <--- Add .copy()
        for o in orders:
    	    db.orders.insert_one(o.copy()) # <--- Add .copy()    bench.measure("Create", insert_data)
    bench.measure("Create", insert_data)

    # READ 1
    def read_point():
        for _ in range(1000):
            db.users.find_one({"user_id": 2500})
    bench.measure("Read-1 Point", read_point)

    # READ 2
    def read_range():
        list(db.orders.find({"amount": {"$gt": 300}, "status": "PAID"}))
    bench.measure("Read-2 Range", read_range)

    # READ 3
    def read_user_orders():
        for _ in range(100):
            list(db.orders.find({"user_id": 1234}))
    bench.measure("Read-3 UserOrders", read_user_orders)

    # UPDATE
    def update_op():
        db.orders.update_many({"status": "PENDING"}, {"$set": {"status": "PAID"}})
    bench.measure("Update", update_op)

    # DELETE
    def delete_op():
        db.orders.delete_many({"status": "CANCELLED"})
    bench.measure("Delete", delete_op)

    return bench.results

# ==========================================
# 4. TinyDB Implementation
# ==========================================
def run_tinydb(users, orders):
    db_file = "tiny_benchmark.json"
    if os.path.exists(db_file):
        os.remove(db_file)
    
    db = TinyDB(db_file)
    users_table = db.table('users')
    orders_table = db.table('orders')
    
    bench = Benchmark("TinyDB")

    # CREATE
    # Warning: TinyDB is file-based and slow for 25k individual inserts
    def insert_data():
        for u in users:
            users_table.insert(u)
        for o in orders:
            orders_table.insert(o)
    bench.measure("Create", insert_data)

    User = Query()
    Order = Query()

    # READ 1
    def read_point():
        for _ in range(1000):
            users_table.get(User.user_id == 2500)
    bench.measure("Read-1 Point", read_point)

    # READ 2
    def read_range():
        orders_table.search((Order.amount > 300) & (Order.status == 'PAID'))
    bench.measure("Read-2 Range", read_range)

    # READ 3
    def read_user_orders():
        for _ in range(100):
            orders_table.search(Order.user_id == 1234)
    bench.measure("Read-3 UserOrders", read_user_orders)

    # UPDATE
    def update_op():
        orders_table.update({'status': 'PAID'}, Order.status == 'PENDING')
    bench.measure("Update", update_op)

    # DELETE
    def delete_op():
        orders_table.remove(Order.status == 'CANCELLED')
    bench.measure("Delete", delete_op)

    return bench.results

# ==========================================
# Main Execution
# ==========================================
if __name__ == "__main__":
    print("Generating Dataset...")
    users_data, orders_data = get_dataset()
    print(f"Generated {len(users_data)} users and {len(orders_data)} orders.")
    print("-" * 30)

    # Run Benchmarks
    print("\nStarting MySQL Benchmark...")
    run_mysql(users_data, orders_data)
    
    print("\nStarting SQLite Benchmark...")
    run_sqlite(users_data, orders_data)

    print("\nStarting MongoDB Benchmark...")
    run_mongodb(users_data, orders_data)

    print("\nStarting TinyDB Benchmark (This may take a while)...")
    run_tinydb(users_data, orders_data)

    print("\nDone.")
