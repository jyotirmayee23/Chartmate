import boto3
import json
import os
import fitz
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize boto3 clients
lambda_client = boto3.client('lambda')
s3 = boto3.client('s3')
textract = boto3.client('textract')

secondary_lambda_arn = os.getenv('CHARTMATE_EMBEDDING_FUNCTION_ARN')
print("@", secondary_lambda_arn)

def invoke_secondary_lambda_async(payload):
    response = lambda_client.invoke(
        FunctionName=secondary_lambda_arn,
        InvocationType='Event',  # Asynchronous invocation
        Payload=json.dumps(payload)
    )
    return response

def extract_bucket_name(url):
    bucket_name = url.split('/')[2]
    if '.s3.amazonaws.com' in bucket_name:
        bucket_name = bucket_name.rstrip('.s3.amazonaws.com')
    return bucket_name

def process_page(page_number, local_path, textract_client):
    pdf_document = fitz.open(local_path)
    page = pdf_document.load_page(page_number)
    pix = page.get_pixmap()
    img_bytes = pix.tobytes("png")

    response = textract_client.analyze_document(
        Document={'Bytes': img_bytes},
        FeatureTypes=["TABLES", "FORMS"]  # Include features if needed
    )

    page_text = " ".join(item['Text'] for item in response['Blocks'] if item['BlockType'] == 'LINE')

    total_confidence = sum(item['Confidence'] for item in response['Blocks'] if item['BlockType'] == 'LINE')
    block_count = sum(1 for item in response['Blocks'] if item['BlockType'] == 'LINE')
    avg_confidence = total_confidence / block_count if block_count > 0 else 0

    return page_number, page_text, avg_confidence  # Return page number with the result

def process_pdf(local_path, textract_client):
    aggregated_text = ""
    confidence_scores = []

    with ThreadPoolExecutor() as executor:
        pdf_document = fitz.open(local_path)
        futures = {executor.submit(process_page, page_number, local_path, textract_client): page_number for page_number in range(len(pdf_document))}

        # Collect results and sort by page number
        results = sorted((future.result() for future in as_completed(futures)), key=lambda x: x[0])
        for page_number, text, avg_confidence in results:  # Unpack the sorted results
            aggregated_text += text + " "  # Append text in sequence with a space after each block
            confidence_scores.append(avg_confidence)

    overall_average_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
    
    return aggregated_text, overall_average_confidence

def lambda_handler(event, context):
    job_id = event['job_id']
    links = event.get('links', [])
    aggregated_text = ""
    overall_confidences = []

    for link in links:
        bucket_name = extract_bucket_name(link)
        object_key = '/'.join(link.split('/')[3:])
        
        if object_key.lower().endswith('.pdf'):
            local_path = f'/tmp/{os.path.basename(object_key)}'
            s3.download_file(bucket_name, object_key, local_path)

            text, avg_confidence = process_pdf(local_path, textract)
            aggregated_text += text
            overall_confidences.append(avg_confidence)

            os.remove(local_path)  # Clean up temporary file

    overall_average_confidence = sum(overall_confidences) / len(overall_confidences) if overall_confidences else 0

    output_filename = f"/tmp/output_{overall_average_confidence:.2f}".replace('.', '_').lower() + ".txt"
    with open(output_filename, 'w') as output_file:
        output_file.write(aggregated_text)
    
    object_name = f"{job_id}/{os.path.basename(output_filename)}"
    s3.upload_file(output_filename, bucket_name, object_name)
    os.remove(output_filename)  # Clean up text file

    payload = {
        "job_id": job_id,
        "links": links
    }
    invoke_secondary_lambda_async(payload)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
        },
        "body": json.dumps("in progress"),
    }
