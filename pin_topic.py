from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.cluster import ClusterMetadata

# Create a new Kafka client to adminstrate our Kafka broker
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id="Kafka Administrator"
)

# topics must be pass as a list to the create_topics method
topics = []
topics.append(NewTopic(name="PinterestTopic", num_partitions=3, replication_factor=1))


# Topics to create must be passed as a list
#admin_client.create_topics(new_topics=topics)
#print(admin_client.list_topics())

# Lets create some test data to send using our kafka producer