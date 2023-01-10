## Milestones within this Project
- The Project is a simulation of Pinterest Data Processing Pipeline using both  Batch and Streaming Processing techniques with BigData tools including FastAPI, Kafka, AWS S3, Pyspark API (Batch Processing and Structured Streaming), Airflow, Docker Postgresql.
### Milestone one
- After setting up the GitHub repository for this project, the next step was to get ready for the data Ingestion phase, i configured the pinterest  API using pytnon FastAPI framework, in the Pinterest/User_Emulation file, there is a python file user_posting_emulation.py that when run, it infinitely sends in user posting to the end point "/pin" in the local host.
- In the Pinterest_App_1/API, the project_pin_Pin_API.py file listens to the messages being sent to this "/pin" endpoint and a request response 200 ok is seen on our console if the API is configured properly
- To complete our Data ingestion phase, we need to send these users post containing specified key: value information to kafka for batch or stream processing
- For the above action we first need to create a kafka producer instance, kafka producer would get these messages and write them into the specified kafkatopic with the help of kafka broker.
- The code below shows how the kafka producer instance was initiated within our program
"""
pin_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="Pinterest producer",
    value_serializer=lambda mlmessage: dumps(mlmessage).encode("ascii")
pin_producer.send(topic = "PinterestTopic", value = data)
)
"""