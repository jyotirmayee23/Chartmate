import json
import boto3
import uuid
import os
import time

ssm_client = boto3.client('ssm')
lambda_client = boto3.client('lambda')
secondary_lambda_arn = os.getenv('CHARTMATE_FUNCTION_ARN')

def invoke_secondary_lambda_async(payload):
    response = lambda_client.invoke(
        FunctionName=secondary_lambda_arn,
        InvocationType='Event',  # Asynchronous invocation
        Payload=json.dumps(payload)
    )
    return response

def lambda_handler(event, context):
    start_time = time.time()  # Start time

    body_dict = json.loads(event['body'])
    job_id = str(uuid.uuid4())
    processing_links = body_dict.get('links', [])
    links = [link.replace('+', ' ') for link in processing_links]
        
    print(f"Links received: {links}")

    ssm_client.put_parameter(
        Name=job_id,
        Value="In Progress",
        Type='String',
        Overwrite=True
    )

    end_time = time.time()  # End time
    total_time = end_time - start_time  # Calculate total time

    payload = {
        "job_id": job_id,
        "links": links,
        "total_time": total_time
    }

    # Invoke the secondary Lambda function asynchronously
    invoke_secondary_lambda_async(payload)

    # Log the total time taken
    print(f"Total time taken: {total_time} seconds")
    
    # Return the response immediately
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
        },
        "body": json.dumps({"job_id": job_id}),
    }
