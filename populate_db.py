import sys
import csv

# Remove CSV field size limit (must be BEFORE any csv usage)
csv.field_size_limit(sys.maxsize)

import os
import psycopg2
from psycopg2 import extras
from pathlib import Path
import time

from utils import get_db_url


STAGING_CREATE_SQL = """
-- Drop existing tables if they exist (in correct order due to foreign keys)
DROP TABLE IF EXISTS OrderDetail CASCADE;
DROP TABLE IF EXISTS Product CASCADE;
DROP TABLE IF EXISTS ProductCategory CASCADE;
DROP TABLE IF EXISTS Customer CASCADE;
DROP TABLE IF EXISTS Country CASCADE;
DROP TABLE IF EXISTS Region CASCADE;

DROP TABLE IF EXISTS stage_orderdetails CASCADE;
DROP TABLE IF EXISTS stage_products CASCADE;
DROP TABLE IF EXISTS stage_productcategories CASCADE;
DROP TABLE IF EXISTS stage_customers CASCADE;
DROP TABLE IF EXISTS stage_countries CASCADE;
DROP TABLE IF EXISTS stage_regions CASCADE;

-- Staging tables
CREATE TABLE stage_regions (
    RegionID   INTEGER,
    Region     TEXT
);

CREATE TABLE stage_countries (
    CountryID  INTEGER,
    Country    TEXT,
    Region     TEXT
);

CREATE TABLE stage_customers (
    CustomerID  INTEGER,
    FirstName   TEXT,
    LastName    TEXT,
    Address     TEXT,
    City        TEXT,
    Country     TEXT
);

CREATE TABLE stage_productcategories (
    ProductCategoryID          INTEGER,
    ProductCategory            TEXT,
    ProductCategoryDescription TEXT
);

CREATE TABLE stage_products (
    ProductID         INTEGER,
    ProductName       TEXT,
    ProductUnitPrice  REAL,
    ProductCategory   TEXT
);

CREATE TABLE stage_orderdetails (
    OrderID         INTEGER,
    CustomerName    TEXT,
    ProductName     TEXT,
    OrderDate       INTEGER,
    QuantityOrdered INTEGER
);

-- Lookup tables
CREATE TABLE Region (
    RegionID  INTEGER NOT NULL PRIMARY KEY,
    Region    TEXT NOT NULL
);

CREATE TABLE Country (
    CountryID  INTEGER NOT NULL PRIMARY KEY,
    Country    TEXT NOT NULL,
    RegionID   INTEGER NOT NULL,
    FOREIGN KEY (RegionID) REFERENCES Region(RegionID)
);

-- Core tables
CREATE TABLE Customer (
    CustomerID  INTEGER NOT NULL PRIMARY KEY,
    FirstName   TEXT NOT NULL,
    LastName    TEXT NOT NULL,
    Address     TEXT NOT NULL,
    City        TEXT NOT NULL,
    CountryID   INTEGER NOT NULL,
    FOREIGN KEY (CountryID) REFERENCES Country(CountryID)
);

CREATE TABLE ProductCategory (
    ProductCategoryID          INTEGER NOT NULL PRIMARY KEY,
    ProductCategory            TEXT NOT NULL,
    ProductCategoryDescription TEXT NOT NULL
);

CREATE TABLE Product (
    ProductID          INTEGER NOT NULL PRIMARY KEY,
    ProductName        TEXT NOT NULL,
    ProductUnitPrice   REAL NOT NULL,
    ProductCategoryID  INTEGER NOT NULL,
    FOREIGN KEY (ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)
);

-- Fact table
CREATE TABLE OrderDetail (
    OrderID         INTEGER NOT NULL PRIMARY KEY,
    CustomerID      INTEGER NOT NULL,
    ProductID       INTEGER NOT NULL,
    OrderDate       INTEGER NOT NULL,
    QuantityOrdered INTEGER NOT NULL,
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
    FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
);
"""


FILES = {
    "data": {
        "filename": "data.csv"
    }
}

EXPECTED_COLUMNS = {
    "data": [
        "Name",
        "Address",
        "City",
        "Country",
        "Region",
        "ProductName",
        "ProductCategory",
        "ProductCategoryDescription",
        "ProductUnitPrice",
        "QuantityOrderded",
        "OrderDate",
    ]
}


def load_tsv_to_stage(conn, filepath, expected_columns, batch_size=5000):
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {filepath}")

    with path.open("r", encoding="utf-8-sig") as csvfile:
        csv_reader = csv.DictReader(csvfile, delimiter='\t')

        missing = sorted(set(expected_columns) - set(csv_reader.fieldnames))
        if missing:
            raise ValueError(f"{filepath} missing expected columns: {missing}")

        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM stage_regions")
        cursor.execute("DELETE FROM stage_countries")
        cursor.execute("DELETE FROM stage_customers")
        cursor.execute("DELETE FROM stage_productcategories")
        cursor.execute("DELETE FROM stage_products")
        cursor.execute("DELETE FROM stage_orderdetails")
        conn.commit()

        region_set = set()
        country_set = set()
        customer_set = []
        category_set = set()
        product_set = set()
        order_set = []

        order_id = 1
        customer_id = 1

        for row in csv_reader:
            name = row["Name"].strip()
            address = row["Address"].strip()
            city = row["City"].strip()
            country = row["Country"].strip()
            region = row["Region"].strip()
            pname = row["ProductName"].strip()
            pcat = row["ProductCategory"].strip()
            pdesc = row["ProductCategoryDescription"].strip()
            price_raw = row["ProductUnitPrice"].strip()
            price = price_raw.split(";")[0] if price_raw else ""


            # FIXED: Split semicolon-separated OrderDate & QuantityOrderded
            qty_raw = row["QuantityOrderded"].strip()
            date_raw = row["OrderDate"].strip()

            qty = qty_raw.split(";")[0] if qty_raw else ""
            date = date_raw.split(";")[0] if date_raw else ""

            # Dedup regions
            if region:
                region_set.add(region)

            # Dedup countries
            if country and region:
                country_set.add((country, region))

            # Customers
            if name:
                name_parts = name.split()
                if len(name_parts) == 1:
                    first = name_parts[0]
                    last = ""
                else:
                    first = name_parts[0]
                    last = " ".join(name_parts[1:])

                customer_set.append((customer_id, first, last, address, city, country))
                customer_id += 1

            # Product categories
            if pcat and pdesc:
                category_set.add((pcat, pdesc))

            # Products
            if pname and price:
                product_set.add((pname, price, pcat))

            # Orders
            if pname and qty and date:
                order_set.append((order_id, name, pname, int(date), int(qty)))
                order_id += 1

        # Insert staging rows
        cursor.executemany(
            "INSERT INTO stage_regions(RegionID, Region) VALUES (%s, %s)",
            [(i+1, r) for i, r in enumerate(sorted(region_set))]
        )

        cursor.executemany(
            "INSERT INTO stage_countries(CountryID, Country, Region) VALUES (%s,%s,%s)",
            [(i+1, c, r) for i, (c, r) in enumerate(sorted(country_set))]
        )

        cursor.executemany(
            "INSERT INTO stage_customers(CustomerID, FirstName, LastName, Address, City, Country) VALUES (%s,%s,%s,%s,%s,%s)",
            customer_set
        )

        cursor.executemany(
            "INSERT INTO stage_productcategories(ProductCategoryID, ProductCategory, ProductCategoryDescription) VALUES (%s,%s,%s)",
            [(i+1, c, d) for i, (c, d) in enumerate(sorted(category_set))]
        )

        cursor.executemany(
            "INSERT INTO stage_products(ProductID, ProductName, ProductUnitPrice, ProductCategory) VALUES (%s,%s,%s,%s)",
            [(i+1, p, float(pr.split(';')[0]), cat) for i, (p, pr, cat) in enumerate(sorted(product_set))]
        )


        cursor.executemany(
            "INSERT INTO stage_orderdetails(OrderID, CustomerName, ProductName, OrderDate, QuantityOrdered) VALUES (%s,%s,%s,%s,%s)",
            order_set
        )

        conn.commit()
        cursor.close()


def build_dimensions(conn):
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO Region(RegionID, Region)
        SELECT RegionID, Region FROM stage_regions
        ON CONFLICT (RegionID) DO NOTHING;
    """)

    cur.execute("""
        INSERT INTO Country(CountryID, Country, RegionID)
        SELECT 
            s.CountryID,
            s.Country,
            r.RegionID
        FROM stage_countries s
        JOIN Region r ON r.Region = s.Region
        ON CONFLICT (CountryID) DO NOTHING;
    """)

    cur.execute("""
        INSERT INTO ProductCategory(ProductCategoryID, ProductCategory, ProductCategoryDescription)
        SELECT ProductCategoryID, ProductCategory, ProductCategoryDescription
        FROM stage_productcategories
        ON CONFLICT (ProductCategoryID) DO NOTHING;
    """)

    conn.commit()
    cur.close()


def load_entities(conn):
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO Customer(CustomerID, FirstName, LastName, Address, City, CountryID)
        SELECT
            sc.CustomerID,
            sc.FirstName,
            sc.LastName,
            sc.Address,
            sc.City,
            c.CountryID
        FROM stage_customers sc
        JOIN Country c ON c.Country = sc.Country
        ON CONFLICT (CustomerID) DO NOTHING;
    """)

    cur.execute("""
        INSERT INTO Product(ProductID, ProductName, ProductUnitPrice, ProductCategoryID)
        SELECT
            sp.ProductID,
            sp.ProductName,
            sp.ProductUnitPrice,
            pc.ProductCategoryID
        FROM stage_products sp
        JOIN ProductCategory pc ON pc.ProductCategory = sp.ProductCategory
        ON CONFLICT (ProductID) DO NOTHING;
    """)

    conn.commit()
    cur.close()


def build_facts(conn):
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO OrderDetail(OrderID, CustomerID, ProductID, OrderDate, QuantityOrdered)
        SELECT
            so.OrderID,
            c.CustomerID,
            p.ProductID,
            so.OrderDate,
            so.QuantityOrdered
        FROM stage_orderdetails so
        JOIN Customer c ON TRIM(c.FirstName || ' ' || c.LastName) = TRIM(so.CustomerName)
        JOIN Product p ON p.ProductName = so.ProductName
        ON CONFLICT (OrderID) DO NOTHING;
    """)

    conn.commit()
    cur.close()


# Main execution
if __name__ == "__main__":
    
    DATABASE_URL = get_db_url()

    print("Creating tables...")
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute(STAGING_CREATE_SQL)
    conn.commit()
    cursor.close()
    conn.close()
    print("Tables created successfully\n")

    print("Loading staging data...")
    conn = psycopg2.connect(DATABASE_URL)
    load_tsv_to_stage(
        conn,
        FILES["data"]["filename"],
        EXPECTED_COLUMNS["data"]
    )
    conn.close()
    print("Staging data loaded.\n")

    print("Building dimension tables...")
    conn = psycopg2.connect(DATABASE_URL)
    build_dimensions(conn)
    conn.close()

    print("Loading entity tables...")
    conn = psycopg2.connect(DATABASE_URL)
    load_entities(conn)
    conn.close()

    print("Building fact tables...")
    conn = psycopg2.connect(DATABASE_URL)
    build_facts(conn)
    conn.close()

    print("\n✅ Database migration complete!")
