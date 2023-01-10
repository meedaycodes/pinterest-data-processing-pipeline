import boto3

aws_client = boto3.resource('s3')
aws_client.create_bucket(
    Bucket='pinterestdata',
    CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-1',
    },
)
