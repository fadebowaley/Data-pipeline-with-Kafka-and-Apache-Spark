import time
import random
from datetime import datetime
from flask import Flask, Response

app = Flask(__name__)

# Route to get sensor data
@app.route('/sensordata')
def get_sensor_data():
    try:
        # Generate sensor data with random values
        beach = 'Montrose_Beach'
        timestamp = "{}".format((datetime.now()).now().isoformat())
        water_temperature = str(round(random.uniform(31.5, 0.0), 2))
        turbidity = str(round(random.uniform(1683.48, 0.0), 2))
        battery_life = str(round(random.uniform(13.3, 4.8), 2))
        measurement_id = str(random.randint(10000, 999999))

        # Create a string with the sensor data
        response = f"{timestamp} {water_temperature} {turbidity} {battery_life} {beach} {measurement_id}"
        
        # Print the sensor data for debugging purposes
        print(response)

        # Return the sensor data as a response with mimetype set to text/plain
        return Response(response, mimetype='text/plain')

    except Exception as e:
        # Handle any errors that occur when generating the sensor data
        print(f'Error generating sensor data: {e}')
        # Return an error response with status code 500
        return Response("Error generating sensor data", status=500, mimetype='text/plain')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='3030')
