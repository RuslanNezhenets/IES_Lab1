from paho.mqtt import client as mqtt_client
import json
import time
from schema.aggregated_data_schema import AggregatedDataSchema
from schema.parking_schema import ParkingSchema
from file_datasource import FileDatasource
import config


def connect_mqtt(broker, port):
    """Create MQTT client"""
    print(f"CONNECT TO {broker}:{port}")

    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print(f"Connected to MQTT Broker ({broker}:{port})!")
        else:
            print("Failed to connect {broker}:{port}, return code %d\n", rc)
            exit(rc)  # Stop execution

    client = mqtt_client.Client()
    client.on_connect = on_connect
    client.connect(broker, port)
    client.loop_start()
    return client


def publish(client, topic, parking_topic, datasource, delay, batch_size=1):
    datasource.startReading()
    while True:
        time.sleep(delay)

        aggregated_data_list = datasource.read(batch_size=batch_size)
        for aggregated_data in aggregated_data_list:
            aggregated_msg = AggregatedDataSchema().dumps(aggregated_data)
            result = client.publish(topic, aggregated_msg)
            if result[0] != 0:
                print(f"Failed to send message to topic {topic}")

        parking_data_list = datasource.read_parking(batch_size=batch_size)
        for parking_data in parking_data_list:
            parking_msg = ParkingSchema().dumps(parking_data)
            result = client.publish(parking_topic, parking_msg)
            if result[0] != 0:
                print(f"Failed to send message to topic {parking_topic}")


def run():
    # Prepare mqtt client
    client = connect_mqtt(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT)
    # Prepare datasource
    datasource = FileDatasource("data/accelerometer.csv", "data/gps.csv", "data/parking.csv")
    # Infinity publish data
    publish(client, config.MQTT_TOPIC, config.PARKING_TOPIC, datasource, config.DELAY, config.BATCH_SIZE)


if __name__ == "__main__":
    run()
