import json
import boto3
import urllib.parse
def lambda_handler(event, context):
    file_name = event["file"]
    bucketName = event["bucketName"]
    decoded_file_name = urllib.parse.unquote(file_name)
    glue=boto3.client('glue')
    try:
        response = glue.start_job_run(JobName = "movie-etl-job", Arguments={"--file_path":decoded_file_name,"--bucket_name":bucketName})
        
    except glue.exceptions.ConcurrentRunsExceededException as e:
        return {
        'statusCode': 200,
        'status': "ConcurrentRunsExceededException",
        'file': event['file'],
        'bucketName':event["bucketName"]
    }
    print(response)
    return {
        'statusCode': 200,
        'jobRunId': response["JobRunId"],
        'status': 'Running',
        'jobName': "movie-etl-job"
    }