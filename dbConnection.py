import psycopg2


# https://medium.com/datauniverse/optimizing-database-interaction-in-web-applications-connection-pooling-with-psycopg2-and-c56b37d155f8

try:
    conn = psycopg2.connect(host="localhost", dbname="weatherdb", user="postgres", password="admin", port=5432)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS Pincode (
    pincode CHAR(6) PRIMARY KEY,
    city VARCHAR(100) DEFAULT NULL);
    
    CREATE TABLE IF NOT EXISTS Weather_description (
    description_id SERIAL PRIMARY KEY,
    description VARCHAR(25) DEFAULT NULL);
    
    CREATE TABLE IF NOT EXISTS Weather (
    id SERIAL PRIMARY KEY,
    pincode CHAR(6),
    date TIMESTAMP NULL DEFAULT NOW(), 
    temp FLOAT DEFAULT NULL,
    feels_Like FLOAT DEFAULT NULL,
    humidity FLOAT DEFAULT NULL,
    description_id INT DEFAULT NULL,
    FOREIGN KEY (pincode) REFERENCES Pincode(pincode),
    FOREIGN KEY (description_id) REFERENCES Weather_description(description_id));
    """)

    # cur.execute("""INSERT INTO Pincode (pincode, city) VALUES (828121, 'katras'), (712233, 'Utarpara');""")
    # cur.execute("UPDATE Pincode SET pincode = '123456' WHERE city = 'katras';")
    # cur.execute("""DELETE from Pincode WHERE pincode = '712233'""");
    # conn.commit()
    # cur.close()
    # conn.close()

except psycopg2.Error as e:
    print("Error:", e)

# sql_query = """SELECT pincode FROM Pincode WHERE pincode = %s";"""
# values = (str(weather_information['zip']))
# print("SQL Query:", sql_query)
# print("Values:", values)
# cur.execute(sql_query, values)
