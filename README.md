# MySQL Database Management and COVID Data Import

This repository contains Python code to manage a MySQL database, create tables, insert data, perform SQL operations, and import COVID-19 data from a public API into a MySQL database.

## Prerequisites

- Python 3.x
- MySQL Server
- Required Python libraries: `mysql-connector`, `requests`, `pandas`

The script will perform the following actions:

- Create a table customers with columns name, age, and id.
- Insert data into the customers table.
- Add a new column address to the customers table.
- Create a table orders with columns number, id, and created.
- Insert data into the orders table.
- Create a foreign key relationship between the orders and customers tables.
- Perform an inner join between the customers and orders tables and print the result.
- Import COVID-19 data from a public API into a new table covid.
- Print the data from the covid table.
