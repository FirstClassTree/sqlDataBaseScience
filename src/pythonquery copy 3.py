import mysql.connector

# Connect to the MySQL database
connection = mysql.connector.connect(
    host="localhost",
    user="talweiss2",
    password="talweiss30336",
    database="talweiss2"
)

cursor = connection.cursor()

# Create a table
create_table_query = """
CREATE TABLE IF NOT EXISTS employees (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    position VARCHAR(255) NOT NULL,
    hire_date DATE NOT NULL
)
"""
cursor.execute(create_table_query)

# Commit the changes
connection.commit()

# Close the cursor and connection
cursor.close()
connection.close()