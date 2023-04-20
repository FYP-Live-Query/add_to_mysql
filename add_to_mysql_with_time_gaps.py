import mysql.connector
import csv
from datetime import datetime, timezone, timedelta
import time

# Establish a connection to the MySQL database
mydb = mysql.connector.connect(
  host="10.8.100.246",
  user="root",
  password="debezium",
  database="inventory"
)

# Create a cursor object to execute SQL queries
cursor = mydb.cursor()

# Define the SQL query to insert data into the table
sql = "INSERT INTO networkTraffic (ip, date, timestamp, browser, traffic, eventTimestamp) VALUES (%s, %s, %s, %s, %s, %s)"

n=1
# Run the loop n times
for i in range(1):
    # Read data from the CSV file
    with open('records.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader) # Skip the header row
        data = [(row[0], row[1], row[2], row[-1],int(row[-7]),int(time.time() * 1000)) for row in reader]
    cursor.executemany(sql, data)
    mydb.commit()
    # Add a one-second delay before the next iteration
    time.sleep(1)

# Close the database connection
mydb.close()
