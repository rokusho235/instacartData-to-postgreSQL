import pandas as pd;
import psycopg2 as pg2;
from sqlalchemy import create_engine

# creating dataframe objects

dept_df = pd.read_csv("folder/departments.csv");
aisles_df = pd.read_csv("folder/aisles.csv");
orderProds_df = pd.read_csv("folder/order_products.csv").sample(1000);
orders_df = pd.read_csv("folder/orders.csv").sample(1000);
prods_df = pd.read_csv("folder/products.csv");

# connecting to the postgres DB

try:
    connection = pg2.connect(dbname="instacart", user="user", password="###", port="5432")
except:
    print("connection was unsuccessful")

# creating cursor to run SQL on the DB

curse = connection.cursor()

# sqlalchemy is cool cuz it allows for bulk insert--otherwise we'd have to write a loop to insert the data
# creating a sqlalchemy engine which requires another connection string

engine = create_engine("postgresql+psycopg2://postgres:###@localhost/instacart")

# using cursor to run DDL SQL--this does not commit the transaction so we will need to do that next

curse.execute("""
    CREATE TABLE IF NOT EXISTS aisles (
        aisle_id INTEGER PRIMARY KEY,
        aisle VARCHAR
    )
""")

curse.execute("""
    CREATE TABLE IF NOT EXISTS departments (
        department_id INTEGER PRIMARY KEY,
        department VARCHAR
    )
""")

curse.execute("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        product_name VARCHAR,
        aisle_id INTEGER,
        department_id INTEGER,
        FOREIGN KEY (aisle_id) REFERENCES aisles (aisle_id),
        FOREIGN KEY (department_id) REFERENCES departments (department_id)
    )
""")

curse.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        order_number INTEGER,
        order_dow INTEGER,
        order_hour_of_day INTEGER,
        days_since_prior_order INTEGER
    )
""")

curse.execute("""
    CREATE TABLE IF NOT EXISTS order_products (
        order_id INTEGER,
        product_id INTEGER,
        add_to_cart_order INTEGER,
        reordered INTEGER,
        PRIMARY KEY (order_id, product_id),
        FOREIGN KEY (order_id) REFERENCES orders (order_id),
        FOREIGN KEY (product_id) REFERENCES products (product_id)
    )
""")

# committing previous SQL statements

connection.commit()

# dropping the ML extra column

orders_df.drop('eval_set', inplace=True, axis=1)

# inserting the data into the SQL tables from the dataframes using sqlalchemy engine

aisles_df.to_sql("aisles", con=engine, if_exists="append", index=False)
dept_df.to_sql("departments", con=engine, if_exists="append", index=False)
orderProds_df.to_sql("order_products", con=engine, if_exists="append", index=False)
orders_df.to_sql("orders", con=engine, if_exists="append", index=False)
prods_df.to_sql("products", con=engine, if_exists="append", index=False)

