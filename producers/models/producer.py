"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # Configuring the broker properties.
        self.broker_properties = {
            BROKER_URL = "PLAINTEXT://kafka0:9092,PLAINTEXT://kafka0:9093,PLAINTEXT://kafka0:9094",
            schema_registry = "http://schema-registry:8081/"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configuring the AvroProducer
        self.producer = AvroProducer(
            { "bootstrap.servers" : broker_properties.BROKER_URL},
            schema_registry = broker_properties.schema_registry
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        # Code that creates the topic for this producer if it does not already exist on the Kafka Broker.

        AdminClient.create_topic([
            NewTopic(
                topic_name,
                num_partitions,
                num_replicas,
                config={
                    "cleanup.policy" : "compact",
                    "compression.type" : "lz4",
                    "delete.retention.ms": 100,
                    "file.delete.delay.ms": 100
                }
            )
        ])
        logger.info("topic creation kafka integration complete")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #Flushing the messages in producer's queue before closing 
        producer.flush()
        logger.info("producer close complete")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
