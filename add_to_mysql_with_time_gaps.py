import mysql.connector
import csv
from datetime import datetime, timezone, timedelta
import time
import random
import configparser

config = configparser.ConfigParser()

# Read the properties file
config.read('/home/nuvidu/fyp/debezium/add_to_mysql/app.properties')

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
sql = "INSERT INTO networkTraffic (ip, date, timestamp, browser, traffic, eventTimestamp) VALUES (%s, %s, %s, %s, %s, ROUND(UNIX_TIMESTAMP(CURTIME(4)) * 1000))"

n=1

browsers = ['chr', 'fox', 'mie', 'opr', 'saf']

# Run the loop n times
while True:
  # Read data from the CSV file
  with open(data_dir, 'r') as file:
      reader = csv.reader(file)
      next(reader) # Skip the header row
      data = [(row[0], row[1], row[2], row[-1],int(row[-7])) for row in reader]
      print(str(data))
  cursor.executemany(sql, data)
  mydb.commit()
  # Add a one-second delay before the next iteration
  time.sleep(60)

# Close the database connection
mydb.close()
