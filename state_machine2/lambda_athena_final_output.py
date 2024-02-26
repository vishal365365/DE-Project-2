import json
import boto3
def lambda_handler(event, context):
    athena_client = boto3.client('athena', region_name='ap-south-1')
    query_execution_id = event['query_execution_id']
    response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
    query_status = response['QueryExecution']['Status']['State']
    if query_status in ('QUEUED', 'RUNNING'):
        print(query_execution_id)
        return {
            'status' : 'wait',
            'query_execution_id' : query_execution_id,
            
        }
    elif query_status == 'FAILED':
        print("query failed")
        return {
                "status": "failed",
            }
    else:
        response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
        rows = response['ResultSet']['Rows']
        for row in rows:
            data = [field['VarCharValue'] for field in row['Data']]
            print(data)
        return {
        'status' : 'success'
    }
