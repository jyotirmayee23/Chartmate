import boto3
import json
import os
import time
from langchain.embeddings import BedrockEmbeddings
from langchain.indexes import VectorstoreIndexCreator
from langchain.vectorstores import FAISS
from langchain.document_loaders import TextLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter

# Initialize boto3 clients
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

third_lambda_arn = os.getenv('CHARTMATE_EXTRACTION_FUNCTION_ARN')

def invoke_secondary_lambda_async(payload):
    response = lambda_client.invoke(
        FunctionName=third_lambda_arn,
        InvocationType='Event',  # Asynchronous invocation
        Payload=json.dumps(payload)
    )
    return response

# Initialize BedrockEmbeddings and VectorstoreIndexCreator
bedrock_runtime = boto3.client(service_name="bedrock-runtime", region_name="us-east-1")

embeddings = BedrockEmbeddings(
    model_id="amazon.titan-embed-text-v1",
    client=bedrock_runtime,
    region_name="us-east-1",
)

index_creator = VectorstoreIndexCreator(
    vectorstore_cls=FAISS,
    embedding=embeddings,
)

def lambda_handler(event, context):
    # Capture the start time from the event
    start_time = event['total_time']
    print("1",start_time)
    job_id = event['job_id']
    bucket_name = "chartmate-idp"
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=job_id)
    print("response ",response)
    txt_file_key = next((obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.txt')), None)
    print("txt file key",txt_file_key)

    if txt_file_key:
        temp_dir = '/tmp'
        temp_path = os.path.join(temp_dir, os.path.basename(txt_file_key))
        s3.download_file(bucket_name, txt_file_key, temp_path)
        print(f"File downloaded successfully: {temp_path}")
    else:
        print("No text files found in the folder.")

    loader = TextLoader(temp_path)
    docs = loader.load()
    text_splitter = RecursiveCharacterTextSplitter()
    documents = text_splitter.split_documents(docs)
    vector = FAISS.from_documents(documents, embeddings)
    vector.save_local(folder_path="/tmp/", index_name='index')

    s3.upload_file(
        "/tmp/index.faiss", bucket_name, f"{job_id}/embeddings/index.faiss"
    )
    s3.upload_file("/tmp/index.pkl", bucket_name, f"{job_id}/embeddings/index.pkl")

    # Calculate the time taken for processing
    processing_end_time = time.time()
    total_time = processing_end_time - start_time

    payload = {
        "job_id": job_id,
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
        "body": json.dumps("hello"),
    }

