## Milestones within this Project
- The Project is a simulation of Pinterest Data Processing Pipeline using both  Batch and Streaming Processing techniques with BigData tools including FastAPI, Kafka, AWS S3, Pyspark API (Batch Processing and Structured Streaming), Airflow, Docker Postgresql.
### Milestone one
- After setting up the GitHub repository for this project, the next step was to get ready for the data Ingestion phase, i configured the pinterest  API using pytnon FastAPI framework, in the Pinterest/User_Emulation file, there is a python file user_posting_emulation.py that when run, it infinitely sends in user posting to the end point "/pin" in the local host.
- In the Pinterest_App_1/API, the project_pin_Pin_API.py file listens to the messages being sent to this "/pin" endpoint and a request response 200 ok is seen on our console if the API is configured properly
- To complete our Data ingestion phase, we need to send these users post containing specified key: value information to kafka for batch or stream processing
- For the above action we first need to create a kafka producer instance, kafka producer would get these messages and write them into the specified kafkatopic with the help of kafka broker.
- The code below shows how the kafka producer instance was initiated within our program
```
pin_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="Pinterest producer",
    value_serializer=lambda mlmessage: dumps(mlmessage).encode("ascii")
# sends the data to the the kafkatopic: PinterestTopic
pin_producer.send(topic = "PinterestTopic", value = data)
)
```
### Milestone Two (Batch Processing)
- In this second milestone, our job is to orchestrate how to carry out the batch processing.
- First, we need to store these large amount of data in a cloud service in this case AWS S3 for later processing, we initiate this process by using the python Boto framework to connect to the required AWS service. The code below shows how we intatntiate an s3 bucket service for our program
```
aws_client = boto3.resource('s3')
aws_client.create_bucket(
    Bucket='pinterestdata',
    CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-1',
    },
)
```
- The next step is to connect kafka to the s3 bucket to allow our data to be sent directly to the cloud on extraction. We need another kafka client "kafkaConsumer" to subscribe to the kafkaTopic produced in our first milestone. The code below illustrates how to create a kafka Consumer 
```
def create_consumer_instance():
    data_batch_consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",    
        value_deserializer=lambda message: loads(message),
        auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
    )
    data_batch_consumer.subscribe(topics=["PinterestTopic", ])
    return data_batch_consumer
#Note that we are reading from the same topic as milestone one
```
The complete code can be found in Pinterest_App_1/API/batch_consumer.py file
- Now we need to connect our kafkaConsumer to the AWS S3 cloud we created in the beginning of this milestone. This function defined in the Pinterest_App_1/API/batch_consumer.py file allows us to do this connection 
```
def upload_to_s3(consumer_instance):
    i = 0
    for element in consumer_instance:
        i += 1
        message = element.value
        message = dumps(message, indent=4)
        bucket.put_object(Body=message, Key= f'consumer{i}.json')
```
- When we are ready to perform the batch processing, we need to connect our spark program to the s3 bucket so that the spark engine can read these data directle and perform necessary data transformation actions as required 
- In the batch_processing.py file we complete our batch processing by first pulling our data directly from the cloud service by providing required variables and configuration to initiate the spark engine, we then defined seven (7) udf (User defined functions) to carry out the required transformation process on the raw data we pulled
- The result of this process is printed on our screen.