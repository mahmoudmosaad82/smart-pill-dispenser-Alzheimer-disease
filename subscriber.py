import paho.mqtt.client as mqtt
import json
import psycopg2
import traceback

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
    try:
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
            cursor.execute("SELECT room_pos FROM mytable2 WHERE room_pos @> ARRAY[%s]::INTEGER[]",(room_pos,))
            row = cursor.fetchone()
            if row:
                # Update the hour and minute values if the room_pos already exists in the database
                cursor.execute("UPDATE mytable2 SET hour = %s, minute = %s WHERE room_pos @> ARRAY[%s]", (hour, minute, room_pos))
            else:
                # Insert a new row with the values if the room_pos does not exist in the database
                cursor.execute("INSERT INTO mytable2 (room_pos, hour, minute) VALUES (ARRAY[%s], %s, %s)", (room_pos, hour, minute))

            # Commit the changes to the database
            db.commit()

        print("Received message: {}".format(data))

    except Exception as e:
        print("An error occurred while processing the message:")
        print(traceback.format_exc())
'''def on_message(client, userdata, message):
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
        cursor.execute("SELECT room_pos FROM mytable2 WHERE room_pos @> ARRAY[%s]::INTEGER[]",(room_pos,))
        row = cursor.fetchone()
        if row:
            # Update the hour and minute values if the room_pos already exists in the database
            cursor.execute("UPDATE mytable2 SET hour = %s, minute = %s WHERE room_pos @> ARRAY[%s]", (hour, minute, room_pos))
        else:
            # Insert a new row with the values if the room_pos does not exist in the database
            cursor.execute("INSERT INTO mytable2 (room_pos, hour, minute) VALUES (ARRAY[%s], %s, %s)", (room_pos, hour, minute))
        # Commit the changes to the database
        db.commit()
    print("Received message: {}".format(data))'''


for topic in topics:
    client.subscribe(topic)

# Start the MQTT loop
client.on_message = on_message
client.loop_forever()




