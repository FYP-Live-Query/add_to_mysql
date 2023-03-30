import mysql.connector
from datetime import datetime

# Establish a connection to the MySQL database
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="debezium",
  database="inventory"
)

# Create a cursor object to execute SQL queries
cursor = mydb.cursor()

# Define a list of tuples containing the data to insert
data = [
  ("108.23.85.jfd", "2017-06-30", "00:00:00", "chr", datetime.now()),
  ("108.23.85.jfd", "2017-06-30", "00:00:00", "chr", datetime.now()),
  ("108.23.85.jfd", "2017-06-30", "00:00:00", "chr", datetime.now())
]

# Define the SQL query to insert data into the table
sql = "INSERT INTO networkTraffic (ip, date, time, browser, eventTimestamp) VALUES (%s, %s, %s, %s, %s)"

# Execute the SQL query for each tuple in the data list
for row in data:
  cursor.execute(sql, row)
  mydb.commit()

# Close the database connection
mydb.close()
