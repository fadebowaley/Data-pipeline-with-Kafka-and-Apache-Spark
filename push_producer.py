import time
import requests
from kafka import KafkaProducer

# Define a function to get sensor data from a URL


def get_sensor_data_stream():
    try:
        url = 'http://0.0.0.0:3030/sensordata'
        r = requests.get(url)
        return r.text
    except:
        return "Error in Connection"


# Create a Kafka producer with bootstrap servers set to localhost:9092
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

while True:
    try:
        # Get sensor data using the get_sensor_data_stream() function
        msg = get_sensor_data_stream()

        # Send the message to the RawSensorData topic
        producer.send("RawSensorData", msg.encode('utf-8')).get(timeout=10)

        # Print a message indicating the message was produced
        print(f'Producing... {msg}')

        # Wait for 1 second before sending the next message
        time.sleep(1)

    except Exception as e:
        # Handle any errors that occur when sending the message
        print(f'Error sending message: {e}')
        # Wait for 5 seconds before trying to send the message again
        time.sleep(5)
