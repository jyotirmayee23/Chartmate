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
from langchain.chains import LLMMathChain, LLMChain
from langchain.agents.agent_types import AgentType
from langchain.agents import Tool, initialize_agent
from langchain.prompts import PromptTemplate
from langchain_aws import ChatBedrock


images_dir = '/tmp/images'
supported_image_formats = ('.jpeg', '.jpg', '.png')

s3 = boto3.client('s3')
ssm_client = boto3.client('ssm')
textract = boto3.client('textract')

bedrock_runtime = boto3.client( 
        service_name="bedrock-runtime",
        region_name="us-east-1",
    )

data_json = {
    "Patient Information": {
        "Full Name": "",
        "Date of Birth": "",
        "Gender": "",
        "Address": {
            "Street": "",
            "City": "",
            "State": "",
            "ZIP Code": ""
        },
        "Contact Information": {
            "Home Phone": "",
            "Mobile Phone": ""
        },
        "Insurance Information": {
            "Primary Insurance": {
                "Provider Name": "",
                "Plan Details": "",
                "Policy Number": "",
                "Contact Details": ""
            },
            "Secondary Insurance": {
                "Provider Name": "",
                "Policy Number": "",
                "Plan Details": ""
            }
        }
    },
    "Reason for Referral": {
        "Detailed Description": ""
    },
    "Requested Services": {
        "Specific Services Requested": [
            "Skilled Nursing",
            "Physical Therapy (PT)",
            "Occupational Therapy (OT)",
            "Speech Therapy (ST)",
            "Home Health Aide (HHA)",
            "Medical Social Worker (MSW)"
        ]
    },
    "Source of Referral": {
        "Referring Physician/Provider": {
            "Name": "",
            "Contact Information": {
                "Phone Number": "",
                "Fax Number": "",
                "Email Address": ""
            }
        }
    },
    "Clinical History": {
        "Comprehensive Medical History": {
            "Current Diagnoses": [],
            "Past Diagnoses": [],
            "Recent Surgeries": [],
            "Medications": [],
            "Relevant Lab Results": [],
            "Imaging Reports": [],
            "Diagnostic Studies": []
        }
    },
    "Current Medical Status": {
        "Summary": {
            "Vital Signs": {
                "Blood Pressure": "",
                "Heart Rate": "",
                "Oxygen Saturation": "",
                "Temperature": "",
                "Weight": ""
            },
            "Recent Hospitalizations": {
                "Date of Discharge": "",
                "Facility Type": "",
                "Acute/Chronic Issues": ""
            },
            "Functional Precautions": ""
        }
    },
    "Functional Status": {
        "Mobility": {
            "Ability to Walk or Transfer": "",
            "Assistance Needed": "",
            "Assistive Devices": []
        },
        "Activities of Daily Living (ADLs)": {
            "Assistance Needed": {
                "Dressing": "",
                "Bathing": "",
                "Toileting": "",
                "Feeding": ""
            }
        }
    },
    "Home Environment": {
        "Safety Concerns": "",
        "Primary Caregiver Availability": {
            "Caregiver Name": "",
            "Frequency of Support": "",
            "Type of Support": ""
        },
        "Home Modifications": []
    },
    "Care Team Information": {
        "List of Healthcare Providers": {
            "Primary Care Physician (PCP)": {
                "Name": "",
                "Contact Details": ""
            },
            "Specialists": [],
            "Other Providers": []
        }
    },
    "Medications": {
        "Medication List": [
            {
                "Name": "",
                "Dosage": "",
                "Route": "",
                "Frequency": ""
            }
        ],
        "Medication Reconciliation": ""
    },
    "Wound Care or IV/TPN Orders": {
        "Specific Orders": ""
    }
}
data_json_str = json.dumps(data_json)


def lambda_handler(event, context):
    job_id = event['job_id']
    links = event.get('links', [])
    all_table_info = []
    all_final_maps = {}
    aggregated_text = ""

    for link in links:
        url_parts = link.split('/')
        bucket_name1 = url_parts[2]
        if '.s3.amazonaws.com' in bucket_name1:
            bucket_name = bucket_name1.rstrip('.s3.amazonaws.com')
        else:
            bucket_name = bucket_name1

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
                    a_response = textract.detect_document_text(Document={'Bytes': img_bytes})

                    text = ""
                    for item in a_response['Blocks']:
                        if item['BlockType'] == 'LINE':
                            aggregated_text += item['Text'] + " " 


    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4000,
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": aggregated_text},
                    {"type": "text", "text": "You are a document entity extraction specialist. Given above the document content, your task is to extract the text value of the following entities:"},
                    {"type": "text", "text": data_json_str},
                    {"type": "text", "text": "The JSON schema must be followed during the extraction.\nThe values must only include text found in the document."},
                    {"type": "text", "text": "for the key 'Reason for Referral' take this as an example u have to answer 'Post-Surgical Care' for this type of description Patient recovering from a specific surgery, requiring wound care, mobility assistance, or physical therapy. and not only description"},
                    {"type": "text", "text": "Do not normalize any entity value."},
                    {"type": "text", "text": "For all thes list key , answer in list only "},
                    {"type": "text", "text": "If an entity is not found in the document, set the entity value to null."},
                    {"type": "text", "text": "only return the key and values dont add any extra words"},
                ],
            }
        ],
    })

    response = bedrock_runtime.invoke_model(
        modelId="anthropic.claude-3-haiku-20240307-v1:0",
        body=body
    )

    response_body = json.loads(response.get("body").read())

    input_tokens = response_body['usage']['input_tokens']
    output_tokens = response_body['usage']['output_tokens']
    result = response_body['content'][0]['text']

     # Parse the result string to JSON
    try:
        result_json = json.loads(result)
        # print(json.dumps(result_json, indent=4))
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        result_json = {"error": "Failed to parse JSON response"}
    

    file_name = "/tmp/extracted_information.json"
    object_key1 = '/extracted_information.json'
    parsed_content = json.loads(result)
    with open(file_name, 'w') as json_file:
        json.dump(parsed_content, json_file, indent=2)

    s3.upload_file(file_name, bucket_name, object_key1)


    combined_result = {
        "status": "Done",
        "response": "completed"
    }

    combined_result_str = json.dumps(combined_result)

    ssm_client.put_parameter(
        Name=job_id,
        Value=combined_result_str,
        Type='String',
        Overwrite=True
    )
    

    # print(result)

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
        },
        "body": json.dumps(combined_result_str),
    }
