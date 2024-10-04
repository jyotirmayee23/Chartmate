import json
import boto3
import os

# Initialize AWS clients
ssm_client = boto3.client('ssm')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Parse the incoming event body
        body_content = event['body']
        body_dict = json.loads(body_content)

        # Extract job_id from the parsed dictionary
        job_id = body_dict.get('job_id')
        print(f"Extracted job_id: {job_id}")

        # Retrieve job status from SSM Parameter Store
        parameter_name = job_id
        response = ssm_client.get_parameter(Name=parameter_name)
        # print("response",response)

        if 'Parameter' in response and 'Value' in response['Parameter']:
            parameter_value = response['Parameter']['Value']
            print(f"Parameter value retrieved: {parameter_value}")

            # Check if the value is "Extraction completed"
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
                    for key, value in json_data.get("responses", {}).items():
                        try:
                            json_data["responses"][key] = json.loads(value)  # Parse string to JSON
                        except json.JSONDecodeError:
                            print(f"Error decoding JSON for key {key}: {value}")

                # Return the processed JSON data
                return {
                    "statusCode": 200,
                    "headers": {
                        "Content-Type": "application/json",
                        "Access-Control-Allow-Headers": "*",
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Methods": "*",
                    },
                    "body": json.dumps(json_data, indent=2),
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
