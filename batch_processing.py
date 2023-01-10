from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, BooleanType, NullType, StringType
import os
from credentials import *


# Adding the packages required to get data from S3 
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"

# Creating our Spark configuration
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')

sc=SparkContext(conf=conf)

# Configure the setting to read from the S3 bucket
accessKeyId= key_id
secretAccessKey= secret_key
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

# Create our Spark session
spark=SparkSession(sc)

# Read from the S3 bucket
df = spark.read.option("multiline", "true").json("s3a://pinterestdata/consumer*.json")


@udf(returnType = IntegerType())
def clean_follower_count(value):
    val = value.isnumeric()
    if val == True:
        value = int(value)
    elif val == False:
        value = list(value)
        save_key = value.pop(-1)
        value = "".join(value)
        #if (save_key.isalpha() == True) and (save_key != "k") and (save_key != "M"):
            #value = None
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

# You may want to change this to read csv depending on the files your reading from the bucket

df = df.withColumn("follower_count", clean_follower_count(col("follower_count"))) \
.withColumn("downloaded", clean_message_downloaded(col("downloaded"))) \
.withColumn("tag_list", clean_tag_list(col("tag_list"))) \
.withColumn("title", clean_title(col("title"))) \
.withColumn("image_src", clean_img_src(col("image_src"))) \
.withColumn("save_location", clean_save_location(col("save_location"))) \
.withColumn("description", clean_description(col("description")))

clean_df = df.select(col("index"), col("category"), col("title"), col("description"), 
col("tag_list"), col("follower_count"), col("downloaded"), col("is_image_or_video"), 
col("image_src"), col("save_location"))

clean_df.show()