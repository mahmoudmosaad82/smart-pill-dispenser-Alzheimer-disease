  GNU nano 4.8                                                                                                                                                                                                                                              subscriber.py                                                                                                                                                                                                                                                         
import paho.mqtt.client as mqtt
import json
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
client_id = "subscriber"

# MQTT topics to subscribe to
topics = ["esp32/schedule", "esp32/device"]

# Connect to the MQTT broker
client = mqtt.Client(client_id)
client.connect(broker_address, broker_port)
def on_message(client, userdata, message):
    # Decode the payload as a JSON array
    data = json.loads(message.payload.decode())

    # Make sure the payload contains at least 3 integers
    if len(data) < 3 or len(data) % 3 != 0:
        print("Invalid payload: {}".format(data))
        return

    # Save the room_pos, hour, and minute values for each set of 3 integers
    for i in range(0, len(data), 3):
        room_pos = data[i]
        hour = data[i+1]
        minute = data[i+2]

        # Check if the room_pos already exists in the database
        # cursor.execute("SELECT room_pos FROM mytable2 WHERE room_pos @> %s", (room_pos,
        cursor.execute("SELECT room_pos FROM mytable2 WHERE room_pos @> ARRAY[%s]::integer[]", (room_pos,))
        row = cursor.fetchone()
        if row :
# Update the hour and minute values if the room_pos already exists in the database
#               cursor.execute("UPDATE mytable2 SET hour = %s, minute = %s WHERE room_pos @> ARRAY[%s]", (hour, minute, room_pos))
                cursor.execute("UPDATE mytable2 SET hour = %s, minute = %s WHERE room_pos @> ARRAY[%s]", (hour, minute, room_pos))

        else :
# Insert a new row with the values if the room_pos does not exist in the datab
                cursor.execute("INSERT INTO mytable2 (room_pos, hour, minute) VALUES (ARRAY[%s], %s, %s)", (room_pos, hour, minute))
            # If the room_pos already exists, update the hour and minute values

        # Commit the changes to the database
        db.commit()

    print("Received message: {}".format(data))
for topic in topics:
    client.subscribe(topic)

# Start the MQTT loop
client.on_message = on_message

# start the MQTT loop
'''def on_message(client, userdata, message):
    print(f"Received message: {message.payload.decode()}")
    data = json.loads(message.payload)
    room_pos = data[0]
    hour = data[1]
    minute = data[2]

    # check if room_pos exists in database
    cursor.execute("SELECT room_pos FROM mytable2 WHERE room_pos = %s", (room_pos,))
    row = cursor.fetchone()
    if row is not None:
        # if room_pos exists, update hour and minute
        cursor.execute("UPDATE mytable2 SET hour = %s, minute = %s WHERE room_pos = %s", (hour, minute, room_pos))
        print(f"Updated existing row for room {room_pos} with hour={hour} and minute={minute}")
    else:
        # if room_pos does not exist, insert new row
        cursor.execute("INSERT INTO mytable2 (room_pos, hour, minute) VALUES (%s, %s, %s)", (room_pos, hour, minute))
        print(f"Inserted new row for room {room_pos} with hour={hour} and minute={minute}")
    db.commit()

# Subscribe to the MQTT topics
for topic in topics:
    client.subscribe(topic)

# Start the MQTT loop
client.on_message = on_message

# start the MQTT loop
client.loop_forever()
def on_message(client, userdata, message):
    # Parse the message payload
    data = json.loads(message.payload)

    if message.topic == "esp32/schedule":
        # Split the array into chunks of 3
        chunks = [data[i:i+3] for i in range(0, len(data), 3)]

        # Iterate over the chunks and process each row
        for chunk in chunks:
            # Extract the values from the chunk
            room_pos = chunk[0]
            hour = chunk[1]
            minute = chunk[2]

            # Check if the room_pos value already exists in the table 
            #cursor.execute("SELECT room_pos FROM mytable WHERE %s = ANY(room_pos)", (room_pos,))
            cursor.execute("SELECT room_pos FROM mytable2 WHERE room_pos = %s", (room_pos,))
            #cursor.execute("SELECT room_pos FROM mytable WHERE room_pos @> ARRAY[%s]", (room_pos,))

            existing_row = cursor.fetchone()

            if existing_row:
                # Update the existing row
                cursor.execute("UPDATE mytable2 SET hour = %s, minute = %s WHERE room_pos = %s", (hour, minute, room_pos))
            else:
                # Insert a new row
                cursor.execute("INSERT INTO mytable2 (room_pos, hour, minute) VALUES (%s, %s, %s)", (room_pos, hour, minute))
            # Commit the changes to the database
            db.commit()

    elif message.topic == "esp32/device":
        # Process the device message
        pass

# Subscribe to the MQTT topics
for topic in topics:
    client.subscribe(topic)

# Start the MQTT loop
client.on_message = on_message
client.loop_forever()

import paho.mqtt.client as mqtt
import psycopg2
import json
topic = "device/schedule"


# create a MySQL connection
db = psycopg2.connect(
  user="doadmin",
port ="25060",
  password="AVNS_PkM25Lh52ZY6Rt4qrJw",
  host="db-postgresql-lon1-21641-do-user-13614739-0.b.db.ondigitalocean.com",
  database="defaultdb"
)

# create a cursor object
cursor = db.cursor()

# define callback functions
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe(topic)
def on_message(client, userdata, message):
    # Parse the message payload
    data = json.loads(message.payload)

    # Split the array into chunks of 3
    chunks = [data[i:i+3] for i in range(0, len(data), 3)]

    # Iterate over the chunks and process each row
    for chunk in chunks:
        # Extract the values from the chunk
        room_pos = chunk[0]
        hour = chunk[1]
        minute = chunk[2]

        # Check if the room_pos value already exists in the table
        cursor.execute("SELECT room_pos FROM mytable WHERE room_pos = %s", (room_pos,))
        existing_row = cursor.fetchone()

        if existing_row:
            # Update the existing row
            cursor.execute("UPDATE mytable SET hour = %s, minute = %s WHERE room_pos = %s", (hour, minute, room_pos))
        else:
            # Insert a new row
            cursor.execute("INSERT INTO mytable (room_pos, hour, minute) VALUES (%s, %s, %s)", (room_pos, hour, minute))

    # Commit the changes to the database
    db.commit()

def on_message(client, userdata, message):
    # Parse the message payload
    data = json.loads(message.payload)

    # Extract the values from the data
    room_pos = data[0]
    hour = data[1]
    minute = data[2]

    # Check if the room_pos value already exists in the table
    cursor.execute("SELECT room_pos FROM mytable WHERE room_pos = %s", (room_pos,))
    existing_row = cursor.fetchone()

    if existing_row:
        # Update the existing row
        cursor.execute("UPDATE mytable SET hour = %s, minute = %s WHERE room_pos = %s", (hour, minute, room_pos))
    else:
        # Insert a new row
        cursor.execute("INSERT INTO mytable (room_pos, hour, minute) VALUES (%s, %s, %s)", (room_pos, hour, minute))
    db.commit()

def on_message(client, userdata, message):
    try:
        data = message.payload.decode('utf-8')
        values = list(map(int, data.split(','))) # assuming data is comma-separated integers
        room_pos, hour, minute = values[0], values[1], values[2]
                # insert the values into the database
        cursor.execute("INSERT INTO mytable (room_pos, hour, minute) VALUES (%s, %s, %s)", (room_pos, hour, minute))
        db.commit()
        print("Values inserted into the database")
    except Exception as e:
        print("Error: ", e)

# set up MQTT client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# connect to MQTT broker
client.connect("178.62.79.44", 1883, 60)

# start the MQTT loop
client.loop_forever()


'''
