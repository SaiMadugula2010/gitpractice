import boto3
import os
import time
import json

# Clients
redshift_data = boto3.client('redshift-data')
dynamodb = boto3.client('dynamodb')

# ENV Variables
DDB_TABLE   = os.environ['DDB_TABLE']           # e.g., 'RedshiftSavedQueries'
CLUSTER_ID  = os.environ['CLUSTER_ID']          # e.g., 'redshift-cluster-1'
DATABASE    = os.environ['DATABASE']            # e.g., 'dev'
SECRET_ARN  = os.environ['SECRET_ARN']          # ARN of secret in Secrets Manager

def lambda_handler(event, context):
    try:
        print("RAW EVENT >>>", json.dumps(event, indent=2))

        # Parse S3 object key from event
        records = event.get("Records", [])
        if not records or "s3" not in records[0]:
            raise ValueError("Missing or invalid 'Records' from S3 event")

        s3_info = records[0]['s3']
        s3_object_key = s3_info['object']['key']
        filename = s3_object_key.split('/')[-1]              # Get file name
        query_name = filename.replace('.csv', '').strip()    # Derive query name

        print(f"Parsed query name: {query_name}")
    except Exception as e:
        return {"statusCode": 400, "body": f"Could not parse S3 event: {str(e)}"}

    # Fetch SQL from DynamoDB
    try:
        response = dynamodb.get_item(
            TableName=DDB_TABLE,
            Key={"query_name": {"S": query_name}}
        )
        if 'Item' not in response or 'sql' not in response['Item']:
            return {"statusCode": 404, "body": f"Query not found for '{query_name}'"}

        sql = response['Item']['sql']['S']
        print(f"Retrieved SQL for {query_name}: {sql}")
    except Exception as e:
        return {"statusCode": 500, "body": f"DynamoDB error: {str(e)}"}

    # Execute SQL using Redshift Data API with Secret
    try:
        exec_response = redshift_data.execute_statement(
            ClusterIdentifier=CLUSTER_ID,
            Database=DATABASE,
            SecretArn=SECRET_ARN,
            Sql=sql,
            WithEvent=True
        )

        statement_id = exec_response['Id']

        # Wait for execution
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
