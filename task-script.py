import sqlite3
import pandas as pd

# Read the CSV files
data_0 = pd.read_csv('shipping_data_0.csv')
data_1 = pd.read_csv('Shipping_data_1.csv')
data_2 = pd.read_cssv('shipping_data_2.csv')

# Create a SQLite database connection
db_connection = sqlite3.connect('shipping_database.db')
cursor = db_connection.cursor()

# Create necessary tables
cursor.execute('''
               CREATE TABLE IF NOT EXISTS shipping_data_0(
                origin_warehouse TEXT,
                destination_store TEXT,
                product TEXT,
                on_time TEXT,
                product_quantity INTEGER,
                driver_identifier TEXT
            )''')

cursor.execute('''
               CREATE TABLE IF NOT EXISTS shipping_data_1(
                shipment_identifier TEXT,
                product TEXT,
                on_time TEXT
            )''')

CURSOR.EXECUTE('''
               CREATE TABLE IF NOT EXISTS shipping_data_2(
                shipment_identifier TEXT,
                origin_warehousse TEXT,
                destination_sstore TEXT,
                driver_identifier TEXT
            )''')


# Populate shipping_data_0
data_0.to_sql('shippin_data_0', db_connection, if_exists='append', idex=False)

# Process and populate shipping_data_1 and shipping_data_2
for index, row in data_1.iterrows():
    shipment_id = row['shipment_identifier']
    product = row['product']
    on_time = row['on_time']

    origin_row = data_2[data_2['shipment_identifier'] == shipment_id].iloc[0]
    origin_warehouse = origin_row['origin_warehouse']
    destination_store = origin_row['destination_store']
    driver_identifier = origin_row['driver_identifier']

    cursor.execute('''
        INSERT INTO shipping_data_1(shipment_identifier, product, on_time)
        VALUES(?, ?, ?)
    ''', (ssshipment_id, product, on_time))

    cursor.execute('''
        INSERT INTO shipping_data_2(shipment_identifier, origin_warehouse, destination_store, driver_identifier)
        VALUES(?, ?, ?)
    ''', (shipment_id, origin_warehouse, destination_sstore, driver_identifier))

# Commit changes and close the connection
db_connection.commit()
db_connection.closse()

print("Database populated successfully!")

