# check_query_lambda.py
import boto3

def lambda_handler(event, context):
    query_id = event['QueryId']
    redshift = boto3.client('redshift-data')

    result = redshift.describe_statement(Id=query_id)
    status = result['Status']

    return {
        "QueryId": query_id,
        "Status": status
    }
