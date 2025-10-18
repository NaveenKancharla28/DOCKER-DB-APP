import os
from dataclasses import dataclass
from typing import List
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

engine: Engine = create_engine(DATABASE_URL, echo=False, future=True)

@dataclass
class User:
    id: int
    email: str
    created_at: str

def bootstrap_db():
    """Bootstrap the database by creating necessary tables."""
    users_sql = """CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY, email TEXT UNIQUE NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"""
    
    example_table_sql = """CREATE TABLE IF NOT EXISTS example_table (
    id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL, age INTEGER NOT NULL);"""
    
    insert_users_sql = """
    INSERT INTO users (email) VALUES
      ('alice@example.com'),
      ('bob@example.com')
    ON CONFLICT (email) DO NOTHING;
    """

    with engine.begin() as conn:
        conn.execute(text(users_sql))
        conn.execute(text(example_table_sql))
        conn.execute(text(insert_users_sql))

def add_user(email: str) -> None:
    """Add a new user to the database."""
    insert_sql = "INSERT INTO users (email) VALUES (:email) ON CONFLICT (email) DO NOTHING;"
    with engine.begin() as conn:
        conn.execute(text(insert_sql), {"email": email})




def add_row(name: str, age: int) -> None:
    """Add a new row to the example_table."""
    insert_sql = "INSERT INTO example_table (name, age) VALUES (:name, :age) ON CONFLICT (name) DO NOTHING;"
    values = {"name": name, "age": age}
    with engine.begin() as conn:
        conn.execute(text(insert_sql), values)





def fetch_users(limit: int = 10) -> List[User]:
    """Fetch some users with a parameterized query (prevents SQL injection)."""
    rows: List[User] = []
    query = text("SELECT id, email, created_at FROM users ORDER BY id LIMIT :lim")
    with engine.begin() as conn:
        result = conn.execute(query, {"lim": limit})
        for r in result:
            rows.append(User(id=r.id, email=r.email, created_at=str(r.created_at)))
    return rows

if __name__ == "__main__":
    bootstrap_db()
    add_user("carolinescottharrison@myownpersonaldomain.com")
    add_user("hfsiunaiu@gmail.com")
    
    print("Fetched users:")
    users = fetch_users(limit=10)
    for u in users:
        print(f"- ({u.id}) {u.email} @ {u.created_at}")
    
    print("\nAdding rows to example_table...")
    for name, age in [("Alice", 30), ("Bob", 25), ("Charlie", 35)]:
        add_row(name, age)
    print("Added rows to example_table.")
