from confluent_kafka import (
    KafkaException,
    ConsumerGroupTopicPartitions,
    TopicPartition,
    ConsumerGroupState,
    TopicCollection,
    IsolationLevel,
)
from confluent_kafka.admin import (
    AdminClient,
    NewTopic,
    NewPartitions,
    ConfigResource,
    ConfigEntry,
    ConfigSource,
    AclBinding,
    AclBindingFilter,
    ResourceType,
    ResourcePatternType,
    AclOperation,
    AclPermissionType,
    AlterConfigOpType,
    ScramMechanism,
    ScramCredentialInfo,
    UserScramCredentialUpsertion,
    UserScramCredentialDeletion,
    OffsetSpec,
)
import sys
import threading
import logging
from typing import List

from settings import BOOTSTRAP_SERVERS, KAFKA_TOPIC, SCHEMA_REGISTRY_URL

logging.basicConfig()


def parse_nullable_string(s):
    if s == "None":
        return None
    else:
        return s


def create_topics(a, topics: List):
    """Create topics"""

    new_topics = [
        NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics
    ]
    # Call create_topics to asynchronously create topics, a dict
    # of <topic,future> is returned.
    fs = a.create_topics(new_topics)

    # Wait for operation to finish.
    # Timeouts are preferably controlled by passing request_timeout=15.0
    # to the create_topics() call.
    # All futures will finish at the same time.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))


if __name__ == "__main__":
    a = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    create_topics(a, [KAFKA_TOPIC])
