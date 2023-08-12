import datetime
import mysql.connector
import requests
import pandas as pd
import json

mydb = mysql.connector.connect(
    host="localhost",
    user="tamar",
    passwd="password" , 
    database="mydatabase"
)

mycursor = mydb.cursor()
# #-------------------------------------CREATE NEW TABLE-------------------------------------------------#

mycursor.execute("CREATE TABLE customers (name VARCHAR(100), age INT, id INT AUTO_INCREMENT PRIMARY KEY)")

# #-------------------------------------INSERT DATA INTO TABLE--------------------------------------------#

mycursor.execute("INSERT INTO customers (name, age) values (%s,%s)",("David", 26))
mycursor.execute("INSERT INTO customers (name, age) values (%s,%s)",("Tamar", 26))
mycursor.execute("INSERT INTO customers (name, age) values (%s,%s)",("Gal", 26))
mycursor.execute("INSERT INTO customers (name, age) values (%s,%s)",("Adam", 26))
mydb.commit()

# #--------------------------------ADD NEW COLUMN--------------------------------------#

mycursor.execute ("ALTER TABLE customer ADD COLUMN address VARCHAR(50) AFTER name")
mycursor.execute("describe customer")
for x in mycursor:
    print(x)

# #--------------------------------CREATING ORDERS TABLE--------------------------------#

mycursor.execute("CREATE TABLE orders (number INT AUTO_INCREMENT PRIMARY KEY,id INT, created DATETIME)")

# #--------------------------------INSERTING DATA INTO ORDERS TABLE----------------------#

mycursor.execute("INSERT INTO orders (id, created) values (%s,%s)",(1,datetime.datetime.now()))
mycursor.execute("INSERT INTO orders (id, created) values (%s,%s)",(2,datetime.datetime.now()))
mycursor.execute("INSERT INTO orders (id, created) values (%s,%s)",(3,datetime.datetime.now()))
mycursor.execute("INSERT INTO orders (id, created) values (%s,%s)",(4,datetime.datetime.now()))
mycursor.execute("INSERT INTO orders (id, created) values (%s,%s)",(1,datetime.datetime.now()))
mycursor.execute("INSERT INTO orders (id, created) values (%s,%s)",(2,datetime.datetime.now()))
mycursor.execute("INSERT INTO orders (id, created) values (%s,%s)",(3,datetime.datetime.now()))
mycursor.execute("INSERT INTO orders (id, created) values (%s,%s)",(4,datetime.datetime.now()))
mydb.commit()

# #--------------------------------PRINTING TABLES--------------------------------#

mycursor.execute("SELECT * FROM customers")
for x in mycursor:
    print(x)
mycursor.execute("SELECT * FROM orders")
for x in mycursor:
    print(x)

# #--------------------------------CREATING FOREIGN KEY--------------------------------#

mycursor.execute("ALTER TABLE orders ADD FOREIGN KEY (id) REFERENCES customers(id)")
mycursor.execute("describe orders")

# #--------------------------------CREATING A JOIN--------------------------------#

mycursor.execute("SELECT \
    customers.name AS customer, \
    orders.number AS order_number \
    FROM customers \
    INNER JOIN orders ON customers.id = orders.id")

for x in mycursor:
    print(x)

# #-------------------------------------PRINT ALL DATA----------------------------------------#

mycursor.execute("SELECT * FROM customers")
for x in mycursor:
    print(x)
mycursor.execute("SELECT * FROM orders")
for x in mycursor:
    print(x)

# #--------------------------------DELETE SPECIFIC DATA-----------------------------------------#

mycursor.execute("DELETE FROM orders WHERE name = 'David'")
mydb.commit()
mycursor.execute("DELETE FROM orders WHERE name = 'Tamar'")
mydb.commit()
mycursor.execute("DELETE FROM customers WHERE name = 'Tamar'")
mydb.commit()
mycursor.execute("DELETE FROM customers WHERE name = 'John'")
mydb.commit()

# #------------------------------------DELETE TABLES------------------------------------#

mycursor.execute("DROP TABLE customers")
mycursor.execute("DROP TABLE orders")

def main():

    #------------------------------IMPORTING COVID DATA FROM GOV.IL------------------------------#

    limit = 100
    url = f"https://data.gov.il/api/3/action/datastore_search?resource_id=d07c0771-01a8-43b2-96cc-c6154e7fa9bd&limit={limit}"
    response = requests.get(url)
    data = response.json()
    records = data['result']['records']
    df = pd.DataFrame(records)
    df.rename(columns={
        "_id": "id",
        "town_code": "town_code",
        "agas_code": "agas_code",
        "town": "town",
        "date": "date",
        "accumulated_cases": "accumulated_cases",
        "new_cases_on_date": "new_cases_on_date",
        "accumulated_recoveries": "accumulated_recoveries",
        "new_recoveries_on_date": "new_recoveries_on_date",
        "accumulated_hospitalized": "accumulated_hospitalized",
        "new_hospitalized_on_date": "new_hospitalized_on_date",
        "accumulated_deaths": "accumulated_deaths",
        "new_deaths_on_date": "new_deaths_on_date",
        "accumulated_diagnostic_tests": "accumulated_diagnostic_tests",
        "new_diagnostic_tests_on_date": "new_diagnostic_tests_on_date",
        "accumulated_vaccination_first_dose": "accumulated_vaccination_first_dose"
    }, inplace=True)
    #create table in database with the same columns as the dataframe
    mycursor.execute("CREATE TABLE covid (id INT AUTO_INCREMENT PRIMARY KEY, town_code VARCHAR(100),\
                      agas_code VARCHAR(100), town VARCHAR(100), date VARCHAR(100), accumulated_cases VARCHAR(100),\
                      new_cases_on_date VARCHAR(100), accumulated_recoveries VARCHAR(100), new_recoveries_on_date VARCHAR(100),\
                      accumulated_hospitalized VARCHAR(100), new_hospitalized_on_date VARCHAR(100), accumulated_deaths VARCHAR(100),\
                      new_deaths_on_date VARCHAR(100), accumulated_diagnostic_tests VARCHAR(100),\
                      new_diagnostic_tests_on_date VARCHAR(100), accumulated_vaccination_first_dose VARCHAR(100))")

    #insert the dataframe into the database
    for row in df.itertuples():
        sql = "INSERT INTO covid (town_code, agas_code, town, date, accumulated_cases, new_cases_on_date, accumulated_recoveries,\
               new_recoveries_on_date, accumulated_hospitalized, new_hospitalized_on_date, accumulated_deaths, new_deaths_on_date,\
               accumulated_diagnostic_tests, new_diagnostic_tests_on_date, accumulated_vaccination_first_dose) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (row.town_code, row.agas_code, row.town, row.date, row.accumulated_cases, row.new_cases_on_date, row.accumulated_recoveries,\
               row.new_recoveries_on_date, row.accumulated_hospitalized, row.new_hospitalized_on_date, row.accumulated_deaths,\
               row.new_deaths_on_date, row.accumulated_diagnostic_tests, row.new_diagnostic_tests_on_date, row.accumulated_vaccination_first_dose)
        mycursor.execute(sql, val)
        mydb.commit()   

    #--------------------------------PRINTING TABLES--------------------------------#

    mycursor.execute("SELECT * FROM covid")
    for x in mycursor:
        print(x)


if __name__ == "__main__":
    main()
