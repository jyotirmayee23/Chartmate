import boto3
from io import BytesIO
import json
import datetime
import os
import fitz 
from PIL import Image
import tempfile
from botocore.config import Config
import requests
import io

lambda_client = boto3.client('lambda')
secondary_lambda_arn = os.getenv('CHARTMATE_EMBEDDING_FUNCTION_ARN')
print("@",secondary_lambda_arn)

def invoke_secondary_lambda_async(payload):
    response = lambda_client.invoke(
        FunctionName=secondary_lambda_arn,
        InvocationType='Event',  # Asynchronous invocation
        Payload=json.dumps(payload)
    )
    return response


images_dir = '/tmp/images'
supported_image_formats = ('.jpeg', '.jpg', '.png')

s3 = boto3.client('s3')
ssm_client = boto3.client('ssm')
textract = boto3.client('textract')



def lambda_handler(event, context):
    job_id = event['job_id']
    links = event.get('links', [])
    aggregated_text = ""
    confidence_scores = []

    for link in links:
        url_parts = link.split('/')
        bucket_name1 = url_parts[2]
        if '.s3.amazonaws.com' in bucket_name1:
            bucket_name = bucket_name1.rstrip('.s3.amazonaws.com')
        else:
            bucket_name = bucket_name1

        print("23", bucket_name)
        object_key = '/'.join(url_parts[3:])
                
        if object_key.lower().endswith('.pdf'):
            local_path = '/tmp/' + object_key.split('/')[-1]
            s3.download_file(bucket_name, object_key, local_path)
            base_name = os.path.splitext(object_key.split('/')[-1])[0]

            pdf_document = fitz.open(local_path)
            for page_number in range(len(pdf_document)):
                page = pdf_document.load_page(page_number)
                pix = page.get_pixmap()
                output_image_path = f'/tmp/{base_name}_page_{page_number + 1}.png'
                pix.save(output_image_path)

                with open(output_image_path, 'rb') as img_file:
                    img_bytes = img_file.read()
                    response = textract.analyze_document(
                        Document={'Bytes': img_bytes},
                        FeatureTypes=["TABLES", "FORMS"]  # Include features if needed
                    )

                    total_confidence = 0
                    block_count = 0
                    layout_types = set()  # To collect unique layout types

                    for item in response['Blocks']:
                        if item['BlockType'] == 'LINE':
                            aggregated_text += item['Text'] + ""
                            total_confidence += item['Confidence']
                            block_count += 1
                        # Collect unique layout types (e.g., LINE, TABLE, FORM)
                        layout_types.add(item['BlockType'])

                    # Calculate average confidence if there are any blocks
                    if block_count > 0:
                        average_confidence = total_confidence / block_count
                        confidence_scores.append(average_confidence)
                        # print(f"Page number {page_number + 1} (Page {page_number + 1})")
                        # print(f"Confidence score: {average_confidence:.2f}%")
                        # print(f"Layout types: {', '.join(layout_types)}")
                    else:
                        print(f"No blocks found on page {page_number + 1}.")
    # print("2323",confidence_scores)
    if confidence_scores:
        overall_average_confidence = sum(confidence_scores) / len(confidence_scores)
        # print(f"\nOverall Average Confidence Score: {overall_average_confidence:.2f}%")
    else:
        print("\nNo confidence scores were calculated.")

    # print("Aggregated Text:", aggregated_text.strip()) 

    output_filename = f"/tmp/output_{overall_average_confidence:.2f}".replace('.', '_').lower() + ".txt"


    print("Output Filename:", output_filename)

    object_name = f"{job_id}/{os.path.basename(output_filename)}"

    with open(output_filename, 'w') as output_file:
        output_file.write(aggregated_text)
    # s3.upload_file(output_filename, bucket_name, 'output_files/' + os.path.basename(output_filename))
    # s3.upload_file(file_name, bucket_name, object_name)
    s3.upload_file(output_filename, bucket_name, object_name)


    # combined_result = {
    #     "status": "Done",
    #     "response": "completed"
    # }

    # combined_result_str = json.dumps(combined_result)

    payload = {
        "job_id": job_id,
        "event_data": event,
        "links": links
    }
    
    invoke_secondary_lambda_async(payload)

    # print(result)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
        },
        "body": json.dumps("in progess"),
    }
