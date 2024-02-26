import json
import boto3

def lambda_handler(event, context):
    athena_client = boto3.client('athena', region_name='ap-south-1')
    database_name = event['database_name']
    output_location = event['output_location']
    query = event['query']
    table_name = event['table_name']
    response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database_name
            },
            ResultConfiguration={
                'OutputLocation': output_location
            }
        )
    query_execution_id = response['QueryExecutionId']
    
    return {
        'statusCode': 200,
        'query_execution_id': query_execution_id
    }
