from kafka import KafkaConsumer
from json import loads , dumps
import boto3


s3 = boto3.resource('s3')
bucket = s3.Bucket('pinterestdata')
#myclient = boto3.client('s3')


# create our consumer to retrieve the message from the topics
def create_consumer_instance():
    data_batch_consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",    
        value_deserializer=lambda message: loads(message),
        auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
    )
    data_batch_consumer.subscribe(topics=["PinterestTopic", ])
    return data_batch_consumer


def upload_to_s3(consumer_instance):
    i = 0
    for element in consumer_instance:
        i += 1
        message = element.value
        message = dumps(message, indent=4)
        bucket.put_object(Body=message, Key= f'consumer{i}.json')


if __name__ == "__main__":
    kafka_consumer = create_consumer_instance()
    #upload_to_s3(kafka_consumer)

