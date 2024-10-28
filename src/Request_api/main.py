import json
import boto3
import os
import base64
from botocore.exceptions import ClientError

# Initialize AWS clients
ssm_client = boto3.client('ssm')
s3_client = boto3.client('s3')

# Function to iterate through the JSON data and count 'Not Found' values
def iterate_json(data, path='', counts=None):
    if counts is None:
        counts = {'not_found': 0, 'total': 0}
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = f"{path}.{key}" if path else key
            if isinstance(value, (dict, list)):
                iterate_json(value, new_path, counts)
            else:
                counts['total'] += 1
                if value == 'Not Found' or value == '':
                    counts['not_found'] += 1
    elif isinstance(data, list):
        for index, item in enumerate(data):
            new_path = f"{path}[{index}]"
            if isinstance(item, (dict, list)):
                iterate_json(item, new_path, counts)
            else:
                counts['total'] += 1
                if item == 'Not Found' or item == '':
                    counts['not_found'] += 1
    return counts

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
            try:
                # Parse the incoming event body
                body_content = event['body']
                body_dict = json.loads(body_content)

                # Extract job_id from the parsed dictionary
                job_id = body_dict.get('job_id')
                print(f"Extracted job_id: {job_id}")

                # Retrieve job status from SSM Parameter Store
                response = ssm_client.get_parameter(Name=job_id)
                parameter_value = response['Parameter']['Value']

                if parameter_value == "Extraction completed":
                    print("The job status indicates that extraction is completed.")
                    bucket_name = "chartmate-idp"
                    file_name = "combined_responses.json"
                    local_file_path = os.path.join('/tmp', file_name)

                    # Download the file from S3
                    s3_client.download_file(bucket_name, f"{job_id}/{file_name}", local_file_path)
                    print(f"Downloaded {file_name} to {local_file_path}.")

                    # Load and process the JSON data from the downloaded file
                    with open(local_file_path, 'r') as f:
                        json_data = json.load(f)
                        responses = json_data.get("responses", {})

                    # Count 'Not Found' values
                    counts = {'not_found': 0, 'total': 0}
                    for key, response_data in responses.items():
                        counts = iterate_json(response_data, counts=counts)

                    total_fields_count = 83  # Explicitly set the total fields to 83
                    found_count = total_fields_count - counts['not_found']
                    found_percentage = (found_count / total_fields_count) * 100
                    print(f"Percentage of found values: {found_percentage:.2f}%")

                    # Clean and format responses
                    cleaned_responses = {}
                    for key, value in responses.items():
                        if isinstance(value, dict):
                            cleaned_responses[key] = value  # Store as key-value pairs in a dictionary
                        else:
                            try:
                                parsed_value = json.loads(value)
                                cleaned_responses[key] = parsed_value  # Store parsed value
                            except json.JSONDecodeError:
                                print(f"Error decoding JSON for value: {value}")

                    # Prepare final output
                    final_output = {
                        "responses": cleaned_responses,  # Now this is a dictionary instead of an array
                        "found_percentage": found_percentage
                    }

                    # Return the processed JSON data along with the found percentage
                    return {
                        "statusCode": 200,
                        "headers": {
                            "Content-Type": "application/json",
                            "Access-Control-Allow-Headers": "*",
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Methods": "*",
                        },
                        "body": json.dumps(final_output, indent=2),
                    }
                else:
                    # If extraction is not completed, return a message to try again later
                    return {
                        "statusCode": 202,
                        "headers": {
                            "Content-Type": "application/json",
                            "Access-Control-Allow-Headers": "*",
                            "Access-Control-Allow-Origin": "*",
                            "Access-Control-Allow-Methods": "*",
                        },
                        "body": json.dumps({
                            "message": "Extraction is not completed. Please try again after some time."
                        }),
                    }
            except Exception as e:
                print(f"Error occurred: {str(e)}")
                return {
                    "statusCode": 500,
                    "body": json.dumps({"error": str(e)})
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
