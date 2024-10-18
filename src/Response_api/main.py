import json
import boto3
import os

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
                    responses = json_data.get("responses", {})
                    for key, value in responses.items():
                        if isinstance(value, str):
                            try:
                                responses[key] = json.loads(value)
                            except json.JSONDecodeError:
                                print(f"Error decoding JSON for key {key}: {value}")

                # Count 'Not Found' values
                counts = {'not_found': 0, 'total': 0}
                for key, response_data in responses.items():
                    counts = iterate_json(response_data, counts=counts)

                total_fields_count = 83  # Explicitly set the total fields to 83
                found_count = total_fields_count - counts['not_found']
                found_percentage = (found_count / total_fields_count) * 100

                print(f"Percentage of found values: {found_percentage:.2f}%")

                json_data["found_percentage"] = found_percentage  # Attach percentage to the final JSON

                final_json = json.dumps(json_data, indent=2)

                # Return the processed JSON data along with the found percentage
                return {
                    "statusCode": 200,
                    "headers": {
                        "Content-Type": "application/json",
                        "Access-Control-Allow-Headers": "*",
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Methods": "*",
                    },
                    "body": final_json,
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
