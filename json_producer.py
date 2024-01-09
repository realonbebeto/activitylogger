import psutil
from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from datetime import datetime

from settings import BOOTSTRAP_SERVERS, KAFKA_TOPIC, SCHEMA_REGISTRY_URL


class Log(object):
    """
    Log record

    Args:
        timestamp (str): Log's timestamp

        cpu (int): CPU Usage Percentage

        ram (int): RAM Usage Percentage

    """

    def __init__(self, timestamp, cpu, ram):
        self.timestamp = timestamp
        self.cpu = cpu
        self.ram = ram


def log_to_dict(log, ctx):
    """
    Returns a dict representation of a Log instance for serialization.

    Args:
        log (Log): Log instance.

        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.

    Returns:
        dict: Dict populated with log attributes to be serialized.
    """

    return dict(
        timestamp=log.timestamp,
        cpu=log.cpu,
        ram=log.ram,
    )


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for Log record {}: {}".format(msg.key(), err))
        return
    print(
        "Log record {} successfully produced to {} [{}] at offset {}".format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()
        )
    )


def main(topic: str = "resources"):
    schema_str = """
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "Log",
      "description": "CPU/RAM Log Usage",
      "type": "object",
      "properties": {
        "timestamp": {
          "description": "Log's Timestamp",
          "type": "string"
        },
        "cpu": {
          "description": "CPU Uage",
          "type": "number",
          "exclusiveMinimum": 0
        },
        "ram": {
          "description": "RAM Usage",
          "type": "number"
        }
      },
      "required": [ "timestamp", "cpu", "ram" ]
    }
    """
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    string_serializer = StringSerializer("utf_8")
    json_serializer = JSONSerializer(schema_str, schema_registry_client, log_to_dict)

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    print("Producing log records to topic {}. ^C to exit.".format(topic))
    while True:
        # Serve on_delivery callbacks from previous calls to produce()
        producer.poll(0.0)
        try:
            timestamp = str(datetime.utcnow())
            cpu = psutil.cpu_percent(5)
            ram = psutil.virtual_memory()[2]
            logg = Log(timestamp=timestamp, cpu=cpu, ram=ram)

            producer.produce(
                topic=topic,
                key=string_serializer(str(uuid4())),
                value=json_serializer(
                    logg, SerializationContext(topic, MessageField.VALUE)
                ),
                on_delivery=delivery_report,
            )
        except KeyboardInterrupt:
            break
        except ValueError:
            print("Invalid input, discarding record...")
            continue

    print("\nFlushing records...")
    producer.flush()


if __name__ == "__main__":
    main(KAFKA_TOPIC)
