import mysql.connector
import csv
from datetime import datetime, timezone, timedelta
import time
import random
import configparser

config = configparser.ConfigParser()

# Read the properties file
config.read('join.properties')

# Get the values
host = config.get('database', 'host')
database = config.get('database','database')
port = config.get('database', 'port')
user = config.get('database', 'user')
data_dir = config.get('data','dir')
password = config.get('database', 'password')

# Establish a connection to the MySQL database
mydb = mysql.connector.connect(
  host=host,
  user=user,
  password=password,
  database=database
)

# Create a cursor object to execute SQL queries
cursor = mydb.cursor()

# Define the SQL query to insert data into the table
sql = "INSERT INTO order(orderID, itemType, orderPriority, orderDate, shipDate, unitsSold , totalRevenue, totalCost, totalProfit, region , country , eventTimestamp) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, ROUND(UNIX_TIMESTAMP(CURTIME(4)) * 1000))"

# Run the loop n times
while True:
  
  # Read data from the CSV file
  with open(data_dir, 'r') as file:
      reader = csv.reader(file)
      next(reader) # Skip the header row
      for row in reader:
        data = (row[6], row[2], row[4], row[5],row[7],row[8],row[11],row[12],row[13],row[0],row[1])
        cursor.execute(sql, data)
        print(sql)
        mydb.commit()
  # Add a one-second delay before the next iteration
        time.sleep(1)

# Close the database connection
mydb.close()
