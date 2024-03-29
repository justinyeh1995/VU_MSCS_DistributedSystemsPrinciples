from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json
import traceback 

class KafkaAdapter:
    def __init__(self, logger):
        self.logger = logger
        self.host = "10.0.0.2"
        self.port = "9092"
        self.producer = KafkaProducer(bootstrap_servers=[self.host + ':' + self.port],
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                                      api_version=(0, 10, 1))
        self.consumer = KafkaConsumer(bootstrap_servers=[self.host + ':' + self.port],
                                      value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                      api_version=(0, 10, 1))

    def send(self, topic, message):
        try:
            self.logger.debug("KafkaAdapter::Sending message to topic: " + topic)
            self.producer.send(topic, message)
        except KafkaError as e:
            self.logger.error("KafkaAdapter::Error sending message to topic: " + topic)
            traceback.print_exc()
            raise e
        
    def receive(self, topiclist):
        try:
            self.logger.debug("KafkaAdapter::Receiving message from topic: " + str(topiclist))
            self.consumer.subscribe(topiclist)
            return self.consumer.poll(timeout_ms=1000)
        except KafkaError as e:
            self.logger.error("KafkaAdapter::Error receiving message from topic: " + str(topiclist))
            traceback.print_exc()
            raise e
        
    def close(self):
        self.producer.close()
        self.consumer.close()
    