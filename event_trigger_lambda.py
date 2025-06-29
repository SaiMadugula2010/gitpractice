import boto3
import os
import time

dynamodb = boto3.client('dynamodb')
redshift_data = boto3.client('redshift-data')

# ENV VARS
DDB_TABLE = os.environ['DDB_TABLE']
CLUSTER_ID = os.environ['CLUSTER_ID']
DATABASE = os.environ['DATABASE']
SECRET_ARN = os.environ['SECRET_ARN']

def lambda_handler(event, context):
    try:
        # Parse the S3 object key
        s3_object_key = event['Records'][0]['s3']['object']['key']
        filename = s3_object_key.split('/')[-1]
        query_name = filename.replace('.csv', '')  # e.g., orders/load_orders_from_s3.csv → load_orders_from_s3

        print(f"Triggering query for: {query_name}")
    except Exception as e:
        return {"statusCode": 400, "body": f"Could not parse S3 event: {str(e)}"}

    # Get SQL from DynamoDB
    try:
        response = dynamodb.get_item(
            TableName=DDB_TABLE,
            Key={"query_name": {"S": query_name}}
        )
        sql = response['Item']['sql']['S']
    except Exception as e:
        return {"statusCode": 500, "body": f"Query not found: {str(e)}"}

    # Execute SQL
    try:
        exec_response = redshift_data.execute_statement(
            ClusterIdentifier=CLUSTER_ID,
            Database=DATABASE,
            SecretArn=SECRET_ARN,
            Sql=sql,
            WithEvent=True
        )

        statement_id = exec_response['Id']

        while True:
            desc = redshift_data.describe_statement(Id=statement_id)
            if desc['Status'] in ['FINISHED', 'FAILED', 'ABORTED']:
                break
            time.sleep(1)

        if desc['Status'] != 'FINISHED':
            return {
                "statusCode": 500,
                "body": f"Query failed: {desc['Status']} - {desc.get('Error')}"
            }

        if desc['HasResultSet']:
            result = redshift_data.get_statement_result(Id=statement_id)
            return {"statusCode": 200, "records": result['Records']}
        else:
            return {"statusCode": 200, "message": "Query executed successfully (no results)"}

    except Exception as e:
        return {"statusCode": 500, "body": f"Execution error: {str(e)}"}
