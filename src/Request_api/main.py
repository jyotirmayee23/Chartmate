import json
import boto3
import uuid
import os
import base64
from botocore.exceptions import ClientError

ssm_client = boto3.client('ssm')
lambda_client = boto3.client('lambda')
secondary_lambda_arn = os.getenv('CHARTMATE_FUNCTION_ARN')
s3_client = boto3.client('s3')

def invoke_secondary_lambda_async(payload):
    response = lambda_client.invoke(
        FunctionName=secondary_lambda_arn,
        InvocationType='Event',  # Asynchronous invocation
        Payload=json.dumps(payload)
    )
    return response

def lambda_handler(event, context): 
    auth_header=event['headers'].get('Authorization')
    if auth_header!=None and auth_header.startswith("Basic "):
       
        encoded_str = auth_header[len("Basic "):]
       
        # Step 3: Decode the Base64 encoded part
        decoded_bytes = base64.b64decode(encoded_str)
        decoded_str = decoded_bytes.decode('utf-8')
       
        username, password = decoded_str.split(':', 1)
 
        client = boto3.client('cognito-idp')
   
        try:
           
            # Authenticate user to get the access token
            auth_response = client.initiate_auth(
                ClientId='5gi976bu03rm1uksd22ls0ita7',  # Replace with your app client ID
                AuthFlow='USER_PASSWORD_AUTH',
                AuthParameters={
                    'USERNAME': username,
                    'PASSWORD': password
                }
            )
           
            if 'ChallengeName' in  auth_response and auth_response['ChallengeName'] == 'NEW_PASSWORD_REQUIRED':
                response = client.admin_set_user_password(
                    UserPoolId='us-east-1_qZ7xirBOK',
                    Username=username,
                    Password=password,
                    Permanent=True
                )
               
                auth_response = client.initiate_auth(
                    ClientId='5gi976bu03rm1uksd22ls0ita7',  # Replace with your app client ID
                    AuthFlow='USER_PASSWORD_AUTH',
                    AuthParameters={
                        'USERNAME': username,
                        'PASSWORD': password
                    }
                )
               
            # Extract access token from the response
            access_token = auth_response['AuthenticationResult']['AccessToken']
            # Validate the access token
            user_response = client.get_user(AccessToken=access_token)
            print(user_response)
            bucket_name = "chartmate-idp" 
            body_dict = json.loads(event['body'])
            processing_links = body_dict.get('links', [])
            links = [link.replace('+', ' ') for link in processing_links]
                
            print(f"Links received: {links}")

            if 'uuid' in body_dict:
                job_id = body_dict['uuid']
                filenames = [os.path.splitext(os.path.basename(link))[0] for link in links]
                print(f"Filenames extracted: {filenames}")  # Assuming all links are from the same bucket
                log_key = f"{job_id}/log.json"
                
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=log_key)
                    print("Log file exists.")

                    # Read the log.json content
                    log_object = s3_client.get_object(Bucket=bucket_name, Key=log_key)
                    log_content = json.loads(log_object['Body'].read().decode('utf-8'))

                    # Assuming log_content has a 'filenames' key with a list of full links
                    existing_filenames = log_content.get('filenames', [])
                    print(f"Existing filenames in log: {existing_filenames}")

                    # Check for missing files by comparing full links
                    missing_files = [link for link in links if link not in existing_filenames]

                    if missing_files:
                        print(f"Missing files: {missing_files}")
                        # Append missing files to the filenames list
                        log_content['filenames'].extend(missing_files)
                        s3_client.put_object(
                            Bucket=bucket_name,
                            Key=log_key,
                            Body=json.dumps(log_content),
                            ContentType='application/json'
                        )
                        print(f"Log file updated with missing files appended: {missing_files}")
                        # Send extraction process for all missing file links in one payload
                        extraction_payload = {
                            "job_id": job_id,
                            "links": missing_files  # Include full links for missing files
                        }
                        invoke_secondary_lambda_async(extraction_payload)
                    else:
                        print("All files are present.")
                        # Return response indicating everything is up to date
                        return {
                            "statusCode": 200,
                            "headers": {
                                "Content-Type": "application/json",
                                "Access-Control-Allow-Headers": "*",
                                "Access-Control-Allow-Origin": "*",
                                "Access-Control-Allow-Methods": "*",
                            },
                            "body": json.dumps({"message": "Everything is up to date."}),
                        }

                except s3_client.exceptions.ClientError:
                    # Log file does not exist
                    print("Log file does not exist.")
                    return {
                        "statusCode": 404,
                        "headers": {
                            "Content-Type": "application/json",
                            "Access-Control-Allow-Headers": "*",
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Methods": "*",
                        },
                        "body": json.dumps({"message": "Please check the identifier."}),
                    }
            else:
                job_id = str(uuid.uuid4())

            # Update SSM parameter in both cases
            ssm_client.put_parameter(
                Name=job_id,
                Value="In Progress",
                Type='String',
                Overwrite=True
            )

            # Prepare the payload only when job_id is generated
            if 'uuid' not in body_dict:
                payload = {
                    "job_id": job_id,
                    "links": links
                }

                # Invoke the secondary Lambda function asynchronously
                invoke_secondary_lambda_async(payload)

                # Store log_content with full links
                log_content = {
                    "filenames": links  # Store as a flat list of full links
                }

                # Upload log.json to S3
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=f"{job_id}/log.json",
                    Body=json.dumps(log_content),
                    ContentType='application/json'
                )

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
        except ClientError as e:
                    # Return unauthorized if authentication fails or token is invalid
                    return {
                        'statusCode': 401,
                        'body': json.dumps({'error': 'Unauthorized: ' + str(e)})
                    }
        except Exception as e:
                    return {
                        'statusCode': 500,
                        'body': json.dumps({'error': str(e)})
                    }
    else:
        return {
            'statusCode': 401,
            'body': ('Unauthorized Missing Credentials')
        }  
