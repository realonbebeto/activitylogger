from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

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


def dict_to_log(obj, ctx):
    """
    Converts object literal(dict) to a Log instance.

    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        obj (dict): Object literal(dict)
    """

    if obj is None:
        return None

    return Log(
        timestamp=obj["timestamp"],
        cpu=obj["cpu"],
        ram=obj["ram"],
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
    json_deserializer = JSONDeserializer(schema_str, from_dict=dict_to_log)

    consumer_conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "consumer.group.id.json-logger.1",
        "auto.offset.reset": "latest",
    }

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            log = json_deserializer(
                msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
            )

            if log is not None:
                print(
                    "Log record {}: timestamp: {}\n"
                    "\tcpu: {}\n"
                    "\tram: {}\n".format(msg.key(), log.timestamp, log.cpu, log.ram)
                )
        except KeyboardInterrupt:
            break

    consumer.close()


if __name__ == "__main__":
    main(KAFKA_TOPIC)
