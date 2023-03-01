import paho.mqtt.client as mqtt
import json
import time
import psycopg2

# Connect to the PostgreSQL database
try:
    db = psycopg2.connect(
        host="db-postgresql-lon1-53471-do-user-13614739-0.b.db.ondigitalocean.com",
        database="defaultdb",
        user="doadmin",
        password="AVNS_9iB60rYJRl5y8AYyZDv",
        port="25060"
    )
    cursor = db.cursor()
except psycopg2.OperationalError as e:
    print(f"Error connecting to the PostgreSQL database: {e}")
    exit()

# MQTT broker configuration
broker_address = "178.62.79.44"
broker_port = 1883
client_id = "publisher"

# MQTT topic to publish to
topic = "esp32/schedule"

# Connect to the MQTT broker
try:
    client = mqtt.Client(client_id)
    client.connect(broker_address, broker_port)
except ConnectionRefusedError as e:
    print(f"Error connecting to the MQTT broker: {e}")
    exit()

# Initialize the previous_data variable to an empty string
previous_data = ""

while True:
    try:
        # Select all rows from the mytable table
        cursor.execute("SELECT * FROM mytable2")
        rows = cursor.fetchall()

        # Convert the rows to a JSON array
        data = []
        for row in rows:
            data.append([row[0], row[1], row[2]])

        # Convert the data to a JSON string
        message = json.dumps(data)

        # If the message has changed since the previous iteration, publish it to the MQTT broker
        if message != previous_data:
            client.publish(topic, message)
            previous_data = message
    except psycopg2.Error as e:
        print(f"Error reading from the PostgreSQL database: {e}")
    except json.JSONDecodeError as e:
        print(f"Error decoding the JSON message: {e}")

    # Wait for 10 seconds before publishing again
    time.sleep(10)

'''import paho.mqtt.client as mqtt
import json
import time
import psycopg2

# Connect to the PostgreSQL database
db = psycopg2.connect(
    host="db-postgresql-lon1-53471-do-user-13614739-0.b.db.ondigitalocean.com",
    database="defaultdb",
    user="doadmin",
    password="AVNS_9iB60rYJRl5y8AYyZDv",
    port="25060"
)

cursor = db.cursor()

# MQTT broker configuration
broker_address = "178.62.79.44"
broker_port = 1883
client_id = "publisher"

# MQTT topic to publish to
topic = "esp32/schedule"

# Connect to the MQTT broker
client = mqtt.Client(client_id)
client.connect(broker_address, broker_port)

while True:
    # Select all rows from the mytable table
    cursor.execute("SELECT * FROM mytable2")
    rows = cursor.fetchall()

    # Convert the rows to a JSON array
    data = []
    for row in rows:
        data.append([row[0], row[1], row[2]])

    # Publish the JSON array to the MQTT broker
    message = json.dumps(data)
    client.publish(topic, message)

    # Wait for 10 seconds before publishing again
    time.sleep(10)
'''

