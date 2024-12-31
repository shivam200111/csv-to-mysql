import mysql.connector
import pandas as pd
import os

# Database connection parameters
host = "localhost"
database = "emaildb"
user = "root"
password = "root"

try:
    # Establish a connection to the database
    conn = mysql.connector.connect(
        host=host,
        database=database,
        user=user,
        password=password
    )
    cursor = conn.cursor()

    # Read data from CSV file
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(current_dir, 'Book1.csv')
    data = pd.read_csv(file_path)

    # Create table
    query = '''
        CREATE TABLE IF NOT EXISTS EmailSend (
            id INT AUTO_INCREMENT PRIMARY KEY,
            Email VARCHAR(255),
            Send VARCHAR(10)
        );
    '''
    cursor.execute(query)

    # Truncate table
    query = "TRUNCATE TABLE EmailSend;"
    cursor.execute(query)

    # Insert data into table
    query = '''
        INSERT INTO EmailSend (Email, Send)
        VALUES (%s, %s);
    '''
    for index, row in data.iterrows():
        try:
            cursor.execute(query, (str(row['Email']), str(row['Send'])))
        except Exception as e:
            print(f"Error at row {index+1}: {e}")

    # Commit changes
    conn.commit()

    # Verify data migration
    query = "SELECT * FROM EmailSend;"
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        print(row)

except mysql.connector.Error as e:
    print(f"Error: {e}")

finally:
    if 'conn' in locals():
        cursor.close()
        conn.close()
