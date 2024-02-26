import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')

    response = glue.get_job_run(
        JobName=event["jobName"],
        RunId=event['jobRunId'],
        PredecessorsIncluded=False
    )
    print(response)

    status = {'STARTING': 1, 'RUNNING': 2, 'STOPPING': 3, 'WAITING': 4}

    if response['JobRun']['JobRunState'] == 'SUCCEEDED':
        create_table_query = """
            CREATE EXTERNAL TABLE IF NOT EXISTS movie_rating (
                movieId int,
                title string,
                rating int,
                timestamp string
            )
            PARTITIONED BY (year int, date string)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS PARQUET
            LOCATION 's3://etl-movie-datas/movie_list/movie_data_filtered/'
        """
        database_name = 'default'
        output_location = 's3://etl-movie-datas/movie_list/temp/'
        table_name = "movie_rating"

        return {
            'statusCode': 200,
            'job_status': 'SUCCEEDED',
            'create_table_query': create_table_query,
            'flag': True,
            'database_name': database_name,
            'output_location': output_location,
            'table_name': table_name,
            'msck_query_id' : ""
        }
    elif response['JobRun']['JobRunState'] not in status:
        return {
            'statusCode': 200,
            'job_status': 'Failed'
        }
    else:
        return {
            'statusCode': 200,
            'job_status': 'Running',
            'jobRunId': event['jobRunId'],
            'jobName': event["jobName"]
        }
