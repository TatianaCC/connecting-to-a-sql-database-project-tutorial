import os
from sqlalchemy import create_engine # Conect to database
import pandas as pd
from dotenv import load_dotenv # oad environmental variables from .env file

load_dotenv()

user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
name = os.getenv('DB_NAME')

address = f"postgresql://{user}:{password}@{host}/{name}"
engine = create_engine(address).execution_options(autocommit=True) # Commit automatically
db = engine.connect()

db.execute('DROP TABLE IF EXISTS publishers, authors, books, book_authors, epub CASCADE')

# Load tables to create from file
with open('../src/sql/create.sql') as file:
    tables = file.read()

# Load data to insert from file
with open('../src/sql/insert.sql') as file:
    data = file.read()

# Create tables
db.execute(tables)

#Insert data
db.execute(data)

# Get titles published by each publisher
query_titlesbypublishers = """
    SELECT publishers.name, array_agg(books.title) AS book_titles
    FROM books
    JOIN publishers ON books.publisher_id = publishers.publisher_id
    GROUP BY publishers.name;
"""

titlesbypublishers = db.execute(query_titlesbypublishers)

for row in titlesbypublishers:
    publisher_name = row[0]
    book_titles = row[1]
    print(f"Publishers: {publisher_name}")
    print("Titles:")
    for title in book_titles:
        print(f"- {title}")
    print()