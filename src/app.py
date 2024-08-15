import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv

# panda options
# pd.set_option('display.max_columns', 200)

# load the .env file variables
file_path = r"C:\Users\angel\connecting-to-a-sql-database-project-tutorial"
load_dotenv(dotenv_path=rf"{file_path}\.env.example")

# 1) Connect to the database here using the SQLAlchemy's create_engine function 
engine = create_engine(f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

with engine.connect() as conn:
  
  # Table's Variables
  publishers_table = 'publishers'
  author_table = 'authors'
  books_table = 'books'
  books_author_table = 'book_authors'
  
  # Execute commands to eliminate all my tables
  conn.execute(
    text(f'DROP TABLE IF EXISTS {publishers_table} CASCADE;')
  )
  
  conn.execute(
    text(f'DROP TABLE IF EXISTS {author_table} CASCADE;')
  )
  
  conn.execute(
    text(f'DROP TABLE IF EXISTS {books_table} CASCADE;')
  )
  
  conn.execute(
    text(f'DROP TABLE IF EXISTS {books_author_table} CASCADE;')
  )
  
  # --__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__
  # 2) Execute the SQL sentences to create your tables using the SQLAlchemy's execute function
  # --__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__
  
  # publisher table
  conn.execute(
    text(f'CREATE TABLE IF NOT EXISTS {publishers_table} (publisher_id INT NOT NULL, name VARCHAR(255) NOT NULL,PRIMARY KEY(publisher_id));')
  )
  
  # authors table
  conn.execute(
    text(f'CREATE TABLE IF NOT EXISTS {author_table} (author_id INT NOT NULL, first_name VARCHAR(100) NOT NULL, middle_name VARCHAR(50) NULL, last_name VARCHAR(100) NULL, PRIMARY KEY(author_id));')
  )
  
  # books table
  conn.execute(
    text(f'CREATE TABLE IF NOT EXISTS {books_table} (book_id INT NOT NULL, title VARCHAR(255) NOT NULL, total_pages INT NULL, rating DECIMAL(4, 2) NULL, isbn VARCHAR(13) NULL, published_date DATE, publisher_id INT NULL, PRIMARY KEY(book_id), CONSTRAINT fk_publisher FOREIGN KEY(publisher_id) REFERENCES publishers(publisher_id));')
  )
  
  # books_authors table
  conn.execute(
    text(f'CREATE TABLE IF NOT EXISTS {books_author_table} (book_id INT NOT NULL, author_id INT NOT NULL, PRIMARY KEY(book_id, author_id), CONSTRAINT fk_book FOREIGN KEY(book_id) REFERENCES books(book_id) ON DELETE CASCADE, CONSTRAINT fk_author FOREIGN KEY(author_id) REFERENCES authors(author_id) ON DELETE CASCADE);')
  )

  # --__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__
  # 3) Execute the SQL sentences to insert your data using the SQLAlchemy's execute function
  # --__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__
  
  # Insert data into publisher's table
  conn.execute(
    text(f'INSERT INTO {publishers_table}(publisher_id, name) VALUES(:publisher_id, :name)'), [
      {'publisher_id' : 1 , 'name' : "'O Reilly Media'"},
      {'publisher_id' : 2 , 'name' : 'A Book Apart'},
      {'publisher_id' : 3 , 'name' : 'A K PETERS'},
      {'publisher_id' : 4 , 'name' : 'Academic Press'},
      {'publisher_id' : 5 , 'name' : 'Addison Wesley'},
      {'publisher_id' : 6 , 'name' : 'Albert&Sweigart'},
      {'publisher_id' : 7 , 'name' : 'Alfred A. Knopf'},
    ]
  )
  
  # Insert data into author's table 
  conn.execute(
    text(f'INSERT INTO {author_table}(author_id, first_name, middle_name, last_name) VALUES(:author_id, :first_name, :middle_name, :last_name)'), [
      {'author_id' : 1, 'first_name' : 'Merritt', 'middle_name' : None , 'last_name' : 'Eric'},
      {'author_id' : 2, 'first_name' : 'Linda', 'middle_name' : None , 'last_name' : 'Mui'},
      {'author_id' : 3, 'first_name' : 'Alecos', 'middle_name' : None , 'last_name' : 'Papadatos'},
      {'author_id' : 4, 'first_name' : 'Anthony', 'middle_name' : None , 'last_name' : 'Molinaro'},
      {'author_id' : 5, 'first_name' : 'David', 'middle_name' : None , 'last_name' : 'Cronin'},
      {'author_id' : 6, 'first_name' : 'Richard', 'middle_name' : None , 'last_name' : 'Blum'},
      {'author_id' : 7, 'first_name' : 'Yuval', 'middle_name' : 'Noah' , 'last_name' : 'Harari'},
      {'author_id' : 8, 'first_name' : 'Paul', 'middle_name' : None , 'last_name' : 'Albitz'},
    ]
  )
  
  # Insert data into books's table 
  conn.execute(
    text(f'INSERT INTO {books_table} (book_id, title, total_pages, rating, isbn, published_date, publisher_id) VALUES(:book_id, :title, :total_pages, :rating, :isbn, :published_date, :publisher_id)'), [
      {'book_id' : 1, 'title' : 'Lean Software Development: An Agile Toolkit', 'total_pages' : 240, 'rating' : 4.17, 'isbn' : '9780320000000', 'published_date' : '2003-05-18', 'publisher_id' : 5},
      {'book_id' : 2, 'title' : 'Facing the Intelligence Explosion', 'total_pages' : 91, 'rating' : 3.87, 'isbn' : None, 'published_date' : '2013-02-01', 'publisher_id' : 7},
      {'book_id' : 3, 'title' : 'Scala in Action', 'total_pages' : 419, 'rating' : 3.74, 'isbn' : '9781940000000', 'published_date' : '2013-04-10', 'publisher_id' : 1},
      {'book_id' : 4, 'title' : 'Patterns of Software: Tales from the Software Community', 'total_pages' : 256, 'rating' : 3.84, 'isbn' : '9780200000000', 'published_date' : '1996-08-15', 'publisher_id' : 1},
      {'book_id' : 5, 'title' : 'Anatomy Of LISP', 'total_pages' : 446, 'rating' : 4.43, 'isbn' : '9780070000000', 'published_date' : '1978-01-01', 'publisher_id' : 3},
      {'book_id' : 6, 'title' : 'Computing machinery and intelligence', 'total_pages' : 24, 'rating' : 4.17, 'isbn' : None, 'published_date' : '2009-03-22', 'publisher_id' : 4},
      {'book_id' : 7, 'title' : 'XML: Visual QuickStart Guide', 'total_pages' : 269, 'rating' : 3.66, 'isbn' : '9780320000000', 'published_date' : '2009-01-01', 'publisher_id' : 5},
      {'book_id' : 8, 'title' : 'SQL Cookbook', 'total_pages' : 595, 'rating' : 3.95, 'isbn' : '9780600000000', 'published_date' : '2005-12-01', 'publisher_id' : 7},
      {'book_id' : 9, 'title' : 'The Apollo Guidance Computer: Architecture And Operation (Springer Praxis Books / Space Exploration)', 'total_pages' : 439, 'rating' : 4.29, 'isbn' : '9781440000000', 'published_date' : '2010-07-01', 'publisher_id' : 6},
      {'book_id' : 10, 'title' : 'Minds and Computers: An Introduction to the Philosophy of Artificial Intelligence', 'total_pages' : 222, 'rating' : 3.54, 'isbn' : '9780750000000', 'published_date' : '2007-02-13', 'publisher_id' : 7},
    ]
  )
  
  # Insert data into books_author's table
  conn.execute(
    text(f'INSERT INTO {books_author_table} (book_id, author_id) VALUES (:book_id, :author_id)'), [
      {'book_id' : 1, 'author_id' : 1},
      {'book_id' : 2, 'author_id' : 8},
      {'book_id' : 3, 'author_id' : 7},
      {'book_id' : 4, 'author_id' : 6},
      {'book_id' : 5, 'author_id' : 5},
      {'book_id' : 6, 'author_id' : 4},
      {'book_id' : 7, 'author_id' : 3},
      {'book_id' : 8, 'author_id' : 2},
      {'book_id' : 9, 'author_id' : 4},
      {'book_id' : 10, 'author_id' : 1},
    ]
  )
  
  conn.commit()
  
# --__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__ 
# 4) Use pandas to print one of the tables as dataframes using read_sql function
# --__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__--__ 

df = pd.read_sql(sql=f'SELECT * FROM {books_table}', con=engine)
print(df)

