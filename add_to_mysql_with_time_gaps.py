import mysql.connector
import csv
from datetime import datetime, timezone, timedelta
import time

# Establish a connection to the MySQL database
mydb = mysql.connector.connect(
  host="172.17.0.4",
  user="root",
  password="debezium",
  database="inventory"
)

# Create a cursor object to execute SQL queries
cursor = mydb.cursor()

# Define the SQL query to insert data into the table
sql = "INSERT INTO networkTraffic (ip, date, timestamp, browser, traffic, eventTimestamp) VALUES (%s, %s, %s, %s, %s, %s)"

# Run the loop 100 times
for i in range(100):
    # Read data from the CSV file
    with open('records.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader) # Skip the header row
        data = [(row[0], row[1], row[2], row[-1],int(row[-7]),datetime.utcnow().replace(tzinfo=timezone.utc)) for row in reader]

    # Execute the SQL query for each batch of data in the list
    n = 3 # batch-size
    for j in range(0, len(data), n):
        batch = data[j:j+n]
        cursor.executemany(sql, batch)
        mydb.commit()

    # Add a one-minute delay before the next iteration
    time.sleep(60)

# Close the database connection
mydb.close()
