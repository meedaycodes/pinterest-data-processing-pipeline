from kafka import KafkaProducer
from json import dumps

# Configure our producer which will send data to  the Pinteresttopic topic
pin_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="Pinterest producer",
    value_serializer=lambda mlmessage: dumps(mlmessage).encode("ascii")
)
