import json
from bson import json_util
from dateutil import parser
from pyspark import SparkContext
from kafka import KafkaConsumer, KafkaProducer
from pymongo.errors import PyMongoError

# Mongo DB
from pymongo import MongoClient

# Set up MongoDB client and database
client = MongoClient('localhost', 27017)
db = client['RealTimeDB']
collection = db['RealTimeCollection']


# Check if a timestamp already exists in the MongoDB collection
def timestamp_exists(timestamp):
    try:
        return collection.count_documents({'TimeStamp': timestamp}) > 0
    except PyMongoError:
        print("Error accessing MongoDB collection.")
        return True


# Validate and structure incoming sensor data
def structure_validate_data(msg):
    data_dict = {}

    # Create RDD
    rdd = sc.parallelize(msg.value.decode("utf-8").split())

    data_dict["RawData"] = str(msg.value.decode("utf-8"))

    # Data validation and create json data dict
    try:
        data_dict["TimeStamp"] = parser.isoparse(rdd.collect()[0])
    except (ValueError, IndexError):
        data_dict["TimeStamp"] = "Error"

    try:
        data_dict["WaterTemperature"] = float(rdd.collect()[1])
        if (data_dict["WaterTemperature"] > 99) or (data_dict["WaterTemperature"] < -10):
            data_dict["WaterTemperature"] = "Sensor Malfunctions"
    except (ValueError, IndexError):
        data_dict["WaterTemperature"] = "Error"

    try:
        data_dict["Turbidity"] = float(rdd.collect()[2])
        if data_dict["Turbidity"] > 5000:
            data_dict["Turbidity"] = "Sensor Malfunctions"
    except (ValueError, IndexError):
        data_dict["Turbidity"] = "Error"

    try:
        data_dict["BatteryLife"] = float(rdd.collect()[3])
    except (ValueError, IndexError):
        data_dict["BatteryLife"] = "Error"

    try:
        data_dict["Beach"] = str(rdd.collect()[4])
    except (ValueError, IndexError):
        data_dict["Beach"] = "Error"

    try:
        data_dict["MeasurementID"] = int(
            str(rdd.collect()[5]).replace("Beach", ""))
    except (ValueError, IndexError):
        data_dict["MeasurementID"] = "Error"

    return data_dict


# Set up Spark context
sc = SparkContext.getOrCreate()
sc.setLogLevel("WARN")

# Set up Kafka consumer and producer
consumer = KafkaConsumer('RawSensorData', auto_offset_reset='earliest', bootstrap_servers=['localhost:9092'],
                         consumer_timeout_ms=1000)
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

for msg in consumer:
    if msg.value.decode("utf-8") != "Error in Connection":
        data = structure_validate_data(msg)

        # Check if the timestamp already exists in the MongoDB collection before adding data to the database
        if not timestamp_exists(data['TimeStamp']):
            try:
                # Push data to MongoDB
                collection.insert_one(data)
                # Send clean sensor data to Kafka topic
                producer.send("CleanSensorData", json.dumps(
                    data, default=json_util.default).encode('utf-8'))
            except PyMongoError:
                print("Error adding data to MongoDB collection.")
        else:
            print(
                f"Data with timestamp {data['TimeStamp']} already exists in the MongoDB collection.")
        print(data)
