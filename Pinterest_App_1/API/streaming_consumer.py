from kafka import KafkaConsumer
from json import loads , dumps
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark import SparkConf, SparkContext
from pyspark.rdd import RDD
from pyspark.sql.types import IntegerType, BooleanType, StringType, NullType
from pyspark.sql.functions  import from_json
from pyspark.sql.types import *


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 pyspark-shell streaming_consumer.py'
kafka_topic_name = "PinterestTopic"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

# create our consumer to retrieve the message from the topic
def create_consumer_instance():
    data_batch_consumer = KafkaConsumer(
        bootstrap_servers="localhost:9092",    
        value_deserializer=lambda message: loads(message),
        auto_offset_reset="earliest" # This value ensures the messages are read from the beginning 
    )
    data_batch_consumer.subscribe(topics=["PinterestTopic", ])
    return data_batch_consumer

@udf(returnType = IntegerType())
def clean_follower_count(value):
    val = value.isnumeric()
    if val == True:
        value = int(value)
    elif val == False:
        value = list(value)
        save_key = value.pop(-1)
        value = "".join(value)
        if save_key == "k":
            value = int(value) * 1000
        elif save_key == "M":
            value = int(value) * 1000000
    return value

@udf(returnType = BooleanType())
def clean_message_downloaded(value):
    if value == 1:
        value = True
    elif value == 0:
        value = False
    return value

@udf(returnType = StringType())
def clean_tag_list(value):
    value.strip()
    tag_list_string = "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e"
    if value == tag_list_string:
        value = None
    else:
        value = value
    return value

@udf(returnType = StringType())
def clean_title(value):
    value.strip()
    title = "No Title Data Available"
    if value == title:
        value = None
    else:
        value = value
    return value

@udf(returnType = StringType())
def clean_img_src(value):
    value.strip()
    image_src_string = "Image src error."
    if value == image_src_string:
        value = None
    else:
        value = value
    return value

@udf(returnType = StringType())
def clean_save_location(value):
    value.strip()
    string_to_strip = "Local save in "
    value = value.replace(string_to_strip, "")
    return value

@udf(returnType = StringType())
def clean_description(value):
    value.strip()
    if value == "No description available Story format":
        value = None
    else:
        value = value
    return value

def process(stream_df, batch_id):
    stream_df = stream_df.withColumn("downloaded", clean_message_downloaded(col("downloaded"))) \
        .withColumn("follower_count", clean_follower_count(col("follower_count"))) \
        .withColumn("tag_list", clean_tag_list(col("tag_list"))) \
        .withColumn("title", clean_title(col("title"))) \
        .withColumn("image_src", clean_img_src(col("image_src"))) \
        .withColumn("save_location", clean_save_location(col("save_location"))) \
        .withColumn("description", clean_description(col("description")))
    return stream_df

def real_time_streaming():
    spark = SparkSession \
            .builder \
            .appName("StreamingConsumer") \
            .getOrCreate()
    # Only display Error messages in the console.
    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from topic
    stream_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", kafka_topic_name) \
            .option("startingOffsets", "latest") \
            .load()
    # Select the value part of the kafka message and cast it to a string.
    stream_df = stream_df.selectExpr("CAST(value as STRING)")
    print(stream_df)
    stream_df_schema = StructType(fields = [StructField("category", StringType()),
                                            StructField("index", StringType()),
                                            StructField("unique_id", StringType()),
                                            StructField("title", StringType()),
                                            StructField("description", StringType()),
                                            StructField("follower_count", StringType()),
                                            StructField("tag_list", StringType()),
                                            StructField("is_img_or_video", StringType()),
                                            StructField("image_src", StringType()),
                                            StructField("downloaded", IntegerType()),
                                            StructField("save_location", IntegerType())
                                            ])
    stream_df = stream_df.withColumn("value", from_json(stream_df.value, stream_df_schema)).select("value.*")
    print(type(stream_df))
    #stream_df = process(stream_df, epoch)
    stream_df = stream_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .foreachBatch(process) \
        .start() \
        .awaitTermination()

real_time_streaming()
    
