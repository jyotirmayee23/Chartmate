import json
import boto3
import uuid
import os

ssm_client = boto3.client('ssm')
lambda_client = boto3.client('lambda')
secondary_lambda_arn = os.getenv('CHARTMATE_FUNCTION_ARN')
print("@",secondary_lambda_arn)



def invoke_secondary_lambda_async(payload):
    response = lambda_client.invoke(
        FunctionName=secondary_lambda_arn,
        InvocationType='Event',  # Asynchronous invocation
        Payload=json.dumps(payload)
    )
    return response

def lambda_handler(event, context):
    print("event",event)
    body_dict = json.loads(event['body'])
    job_id = str(uuid.uuid4())
    parameter_name = job_id
    print("1",parameter_name)
    processing_links = body_dict.get('links', [])
    links = []
    for link in processing_links:
        link = link.replace('+', ' ')
        links.append(link)

    
        
    print(f"Links received: {links}")
    # for link in links:
    #     print(f"Processing link: {link}")
    # Put the parameter to SSM
    ssm_client.put_parameter(
        Name=parameter_name,
        Value="In Progress",
        Type='String',
        Overwrite=True
    )

    payload = {
        "job_id": job_id,
        "event_data": event,
        "links": links,
    }

    print("2",payload)
    
    # Invoke the secondary Lambda function asynchronously
    invoke_secondary_lambda_async(payload)
    
    # Return the response immediately
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
        },
        "body": json.dumps(job_id),
    }
