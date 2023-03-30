import mysql.connector
import csv
from datetime import datetime

# Establish a connection to the MySQL database
mydb = mysql.connector.connect(
  host="172.17.0.4",
  user="root",
  password="debezium",
  database="inventory"
)

# Create a cursor object to execute SQL queries
cursor = mydb.cursor()

# Define a list of tuples containing the data to insert
# data = [
#   ("108.23.85.jfd", "2017-06-30", "00:00:00", "chr", datetime.now()),
#   ("108.23.85.jfd", "2017-06-30", "00:00:00", "chr", datetime.now()),
#   ("108.23.85.jfd", "2017-06-30", "00:00:00", "chr", datetime.now())
# ]
# Read data from the CSV file
with open('records2.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader) # Skip the header row
    data = [(row[0], row[1], row[2], row[-1], datetime.now()) for row in reader]


# Define the SQL query to insert data into the table
sql = "INSERT INTO networkTraffic (ip, date, time, browser, eventTimestamp) VALUES (%s, %s, %s, %s, %s)"

# Execute the SQL query for each tuple in the data list
# for row in data:
#   cursor.execute(sql, row)
#   mydb.commit()
# Execute the SQL query for each tuple in the data list
n =3 # batch-size
for i in range(0, len(data), n):
    batch = data[i:i+n]
    cursor.executemany(sql, batch)
    mydb.commit()

# Close the database connection
mydb.close()
