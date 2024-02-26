import boto3
import urllib.parse
import json


def lambda_handler(event, context):
    file_name = event['Records'][0]['s3']['object']['key']
    decoded_file_name = urllib.parse.unquote(file_name)
    bucketName=event['Records'][0]['s3']['bucket']['name']
    print(decoded_file_name)
    client = boto3.client('stepfunctions')
    response = client.start_execution(
    stateMachineArn='arn:aws:states:ap-south-1:730335234333:stateMachine:MyStateMachine-tl7ho7zu0',
    input= json.dumps({"file" : file_name,"bucketName":bucketName})
    )
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
