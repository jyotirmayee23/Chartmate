import boto3
import json
import os
import time
import fitz
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize boto3 clients
lambda_client = boto3.client('lambda')
s3 = boto3.client('s3')
textract = boto3.client('textract')

secondary_lambda_arn = os.getenv('CHARTMATE_EMBEDDING_FUNCTION_ARN')

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

    total_confidence = sum(item['Confidence'] for item in response['Blocks'] if item['BlockType'] == 'LINE')
    block_count = sum(1 for item in response['Blocks'] if item['BlockType'] == 'LINE')

    page_text = "".join(item['Text'] for item in response['Blocks'] if item['BlockType'] == 'LINE')

    avg_confidence = total_confidence / block_count if block_count > 0 else 0
    return page_text, avg_confidence

def process_pdf(local_path, textract_client, pdf_name):
    aggregated_text = ""
    confidence_scores = []

    with ThreadPoolExecutor() as executor:
        pdf_document = fitz.open(local_path)
        futures = {executor.submit(process_page, page_number, local_path, textract_client): page_number for page_number in range(len(pdf_document))}

        for future in as_completed(futures):
            page_number = futures[future]
            try:
                text, avg_confidence = future.result()
                aggregated_text += f"Page {page_number + 1}:\n{text}\n\n"  # Append page number
                confidence_scores.append(avg_confidence)
            except Exception as e:
                print(f"Error processing page {page_number}: {e}")

    overall_average_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
    
    # Save the output file named after the PDF
    output_filename = f"/tmp/{pdf_name.replace('.pdf', '.txt')}"
    with open(output_filename, 'w') as output_file:
        output_file.write(aggregated_text)
    
    return output_filename, overall_average_confidence

def lambda_handler(event, context):
    # Capture the start time from the event
    start_time = event['total_time']
    print("1", start_time)

    job_id = event['job_id']
    links = event.get('links', [])
    overall_confidences = []

    for link in links:
        bucket_name = extract_bucket_name(link)
        object_key = '/'.join(link.split('/')[3:])
        
        if object_key.lower().endswith('.pdf'):
            local_path = f'/tmp/{os.path.basename(object_key)}'
            s3.download_file(bucket_name, object_key, local_path)

            output_filename, avg_confidence = process_pdf(local_path, textract, os.path.basename(object_key))
            overall_confidences.append(avg_confidence)

            # Upload the individual text file back to S3
            s3.upload_file(output_filename, bucket_name, f"{job_id}/{os.path.basename(output_filename)}")

            os.remove(local_path)  # Clean up temporary file
            os.remove(output_filename)  # Clean up text file

    overall_average_confidence = sum(overall_confidences) / len(overall_confidences) if overall_confidences else 0

    processing_end_time = time.time()
    total_time = processing_end_time - start_time

    payload = {
        "job_id": job_id,
        "links": links,
        "total_time": total_time
    }
    print(f"Total time taken: {total_time} seconds")
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
