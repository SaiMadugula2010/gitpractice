import boto3
import os
import time

dynamodb = boto3.client('dynamodb')
redshift_data = boto3.client('redshift-data')

# Environment Variables
DDB_TABLE   = os.environ['DDB_TABLE']           # e.g., 'RedshiftSavedQueries'
CLUSTER_ID  = os.environ['CLUSTER_ID']          # e.g., 'redshift-cluster-1'
DATABASE    = os.environ['DATABASE']            # e.g., 'dev'
SECRET_ARN  = os.environ['SECRET_ARN']          # ARN of secret in Secrets Manager


def lambda_handler(event, context):
    query_name = event.get("query_name")
    if not query_name:
        return {"statusCode": 400, "body": "Missing 'query_name' in event."}

    # Fetch SQL from DynamoDB
    try:
        response = dynamodb.get_item(
            TableName=DDB_TABLE,
            Key={"query_name": {"S": query_name}}
        )
        if 'Item' not in response or 'sql' not in response['Item']:
            return {"statusCode": 404, "body": f"No query found for '{query_name}'"}

        sql = response['Item']['sql']['S']
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

        # Wait for completion
        while True:
            status = redshift_data.describe_statement(Id=statement_id)['Status']
            if status in ['FINISHED', 'FAILED', 'ABORTED']:
                break
            time.sleep(1)

        if status != 'FINISHED':
            return {"statusCode": 500, "body": f"Query failed: {status}"}

        # Return result if SELECT
        desc = redshift_data.describe_statement(Id=statement_id)
        if desc['HasResultSet']:
            result = redshift_data.get_statement_result(Id=statement_id)
            return {"statusCode": 200, "records": result['Records']}
        else:
            return {"statusCode": 200, "message": "Query executed successfully (no results)"}

    except Exception as e:
        return {"statusCode": 500, "body": f"Execution error: {str(e)}"}
