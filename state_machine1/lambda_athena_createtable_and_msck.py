import boto3

def lambda_handler(event, context):
    athena_client = boto3.client('athena', region_name='ap-south-1')
    database_name = event['database_name']
    output_location = event['output_location']
    create_table_query = event['create_table_query']
    flag = event['flag']
    table_name = event['table_name']
    
    if flag:
        response = athena_client.start_query_execution(
            QueryString=create_table_query,
            QueryExecutionContext={
                'Database': database_name
            },
            ResultConfiguration={
                'OutputLocation': output_location
            }
        )
        query_execution_id = response['QueryExecutionId']
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        query_status = response['QueryExecution']['Status']['State']
        while query_status in ('QUEUED', 'RUNNING'):
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            query_status = response['QueryExecution']['Status']['State']
            print(query_status)

        if query_status == 'FAILED':
            print("create table query is failed")
            return {
                "status": "failed",
            }

    msck_query_id = event['msck_query_id']
    if not msck_query_id:
        sync_partition_query = f"MSCK REPAIR TABLE {table_name};"
        response = athena_client.start_query_execution(
        QueryString=sync_partition_query,
        QueryExecutionContext={
            'Database': database_name
        },
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )
        msck_query_id = response['QueryExecutionId']
    response = athena_client.get_query_execution(QueryExecutionId=msck_query_id)
    query_status = response['QueryExecution']['Status']['State']
    
    if query_status in ('QUEUED', 'RUNNING'):
        print(msck_query_id)
        print(query_status)
        return {
            'status': "wait",
            'create_table_query': create_table_query,
            'table_name': event['table_name'],
            'flag': False,
            'database_name': event['database_name'],
            'output_location': event['output_location'],
            'msck_query_id':msck_query_id
        }
        
    elif query_status == "FAILED":
        print(query_status)
        print("partition_query failed")
        return {
            'status': 'failed'
        }
    else:
        print(query_status)
        return {
            'status': 'success'
        }
