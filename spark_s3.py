import boto3
import pyspark
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType, BooleanType
import multiprocessing
import operator
import numpy as np
import pandas as pd
from kafka import KafkaConsumer
from json import loads , dumps


s3 = boto3.resource('s3')
client = boto3.client('s3')
bucket = s3.Bucket('pinterestdata')

def create_spark_session():
    cfg = (
        pyspark.SparkConf()
        # Setting the master to run locally and with the maximum amount of cpu coresfor multiprocessing.
        .setMaster(f"local[{multiprocessing.cpu_count()}]")
        # Setting application name
        .setAppName("SparkS3")
        # Setting config value via string
        .set("spark.eventLog.enabled", False)
        # Setting environment variables for executors to use
        .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
        # Setting memory if this setting was not set previously
        .setIfMissing("spark.executor.memory", "1g")
    )

    session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()
    return session

def read_files_from_s3():
    message_list = []
    list_of_objects = bucket.objects.all()
    
    for message in list_of_objects:
        message_key = message.key
        message_value = client.get_object(Bucket='pinterestdata', Key=message_key)
        message_content = loads(message_value['Body'].read())
        message_list.append(message_content)
        #message_in_df = session.createDataFrame(message)
        #print(message_in_df)
        #session.read.json(message)
    return message_list

@pandas_udf(returnType = BooleanType())
def clean_message_downloaded(value= pd.Series):
    if value == 1:
        value = True
    elif value == 0:
        value = False
    return value
    

@pandas_udf(returnType=IntegerType()) 
def clean_follower_count(value=pd.Series):
    value = list(value)
    save_key = value.pop(-1)
    value = "".join(value)
    if save_key == "k":
        value = int(value) * 1000
    elif save_key == "M":
        value = int(value) * 1000000
    else:
        value = int(value)
    return value
    


def process_with_spark(message_data, session):
    spark_df = session.createDataFrame(pd.DataFrame(message_data))
    spark_df.select(clean_follower_count("follower_count")).show()
    #print(spark_df.agg(Func.clean_message_downloaded(spark_df.downloaded)))
    #spark_df.withColumn("follower_count", clean_follower_count(("follower_count")))


session = create_spark_session
data = read_files_from_s3
process_with_spark(session=session(), message_data=data())