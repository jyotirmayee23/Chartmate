import boto3
from io import BytesIO
import json
import datetime
import os
import tempfile
from botocore.config import Config
import requests
import io
from langchain.embeddings import BedrockEmbeddings
from langchain.indexes import VectorstoreIndexCreator
from langchain.vectorstores import FAISS
from langchain_core.prompts import ChatPromptTemplate
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain.chains import create_retrieval_chain
from langchain_community.chat_models import BedrockChat
import concurrent.futures
# import uuid


from langchain_core.prompts import ChatPromptTemplate
prompt = ChatPromptTemplate.from_template("""Please fill in the missing details in the following information::
<context>
{context}
</context>

please only return in json (fill the values).
please return evrything and dont stop at the middle.
please return in correct json format only.
retrieve the aprropriate values from the context.
Only Answer from the context.
return not found in case you do not found an answer instead of keeping blank.
Question: {input}""")

patient_info = {
    "patient Information": {
            "fullName": "",
            "dateOfBirth": "//should be in this format xx/xx/xxxx",
            "social security number":"",
            "gender": "////would be nice to have male, female or not-known",
            "address": {
                "streetNumber": "",
                "streetName": "",
                "apartment UnitNumber": "",
                "city": "",
                "state": "",
                "zipCode": ""
            },
            "contactInformation": {
                "homePhone": "",
                "mobilePhone": ""
            },
            "advanced Directive": "//if advance directive found , then check the respective file details also.",
        }
}

insurance = {
    "insurance Information": {
                "primary Insurance": {
                    "payor Name": "//Name of the insurance company",
                    "policyInsuranceHolder": "",
                    "planDetails": "",
                    "policy Number": "",
                    "group Number": "//group number of primary insurance ",
                    "contactDetails": "//phone number in primary insurance",
                },
                "secondary Insurance": {
                    "payor Name": "//Name of the insurance company/insurance name(secondary)",
                    "policyInsuranceHolder": "",
                    "planDetails": "",
                    "policy Number": "",
                    "group Number": "//group number of secondary insurance",
                    "contactDetails": "//phone number in secondary insurance",
                }
            }
}

reason_for_referral = {
    "reason for Referral": {
            "detailed Description": "///what is the reason for referral in order ?"
    }
        
}

requested_service = {
    "requested Services": {
            "specific Services Requested": ["//only return the mentioned service from the below."
                "Skilled Nursing",
                "Physical Therapy (PT)",
                "Occupational Therapy (OT)",
                "Speech Therapy (ST)",
                "Home Health Aide (HHA)",
                "Medical Social Worker (MSW)"
            ]
        }
}

s_o_r = {
    "sourceOfReferral": {
            "referringPhysicianProvider": {
                "name": "//find it from the order section",
                "address": {
                    "streetNumber": "",
                    "streetName": "",
                    "suiteNumber": "",
                    "city": "",
                    "state": "",
                    "zipCode": ""
                },
                "contactInformation": {
                    "phoneNumber": "",
                    "faxNumber": ""
                }
            }
        }
}


clinical_history_Current_Diagnoses = {
    "clinicalHistory": {
        "current Diagnoses": {
            "current Diagnoses": ["//only return current diagnoses.description,icd10code"]
        }
    }
}

clinical_history_Past_Diagnoses = {
    "clinicalHistory": {
        "Past Medical History": {
            "Past Medical History": ["//only return medical history. description,icd10code"]
        }
    }
}

clinical_history_recent_Surgical_History = {
    "clinicalHistory": {
        "comprehensiveMedicalHistory": {
            "recentSurgicalHistory": ["name,date"]
        }
    }
}

patient_pharmacy = {
    "patient pharmacy": ["//Please provide the names of the patient's pharmacies."],
    "pharmacy phone number":""
}

current_medical_statushpi = {
    "current Medical Status HPI": {
        "summary": {
            "vital Signs": [
                "//return all the vital signs for all the vitals found"
            ],
            "recent Inpatient Facility": {
                "date Of Discharge": "",
                "facility Type": "",
            }
        }
    }
}

functional_status = {
    "functional Status": {
            "mobility": {
                "assistive Devices": [
                    ""
                ]
            }
        }
}

home_env = {
    "home Environment": {
            "primary Caregiver Availability": {
                "caregiver Name": ""
            }
        }
}
care_team_info = {
    "care Team Information": {
            "list Of Healthcare Providers": {
                "primary Care Physician": {
                    "name": "",
                    "contact Details": ""
                }
            }
        }
}

medications = {
    "medications": {
            "medicationList": ["name,dosage,form,quantity,route,frequency,date,action"],
            "medicationReconciliation": ""
        }
}

wound_care = {
    "woundCare": {
        "hasWound": "",
        "woundDescription": ""
    }
}

Iv_line = {
    "ivLine": {
        "hasIVLine": "",
        "ivLineDescription": ""
    }
}

PICC_line = {
    "piccLine": {
        "hasPICCLine": "",
        "piccLineDescription": ""
    }

}

TPN = {
    "tpn": {
        "hasTPN": "",
        "tpnDescription": ""
    }

}

Weight_bearing_precautions ={
    "weightBearingPrecautions": {
        "categories": {
            "nonWeightBearing": {
                "description": ""
            },
            "toeTouchWeightBearing": {
                "description": ""
            },
            "partialWeightBearing": {
                "description": ""
            },
            "weightBearingAsTolerated": {
                "description": ""
            },
            "fullWeightBearing": {
                "description": ""
            }
        }
    }
}

s3 = boto3.client('s3')
ssm_client = boto3.client('ssm')


bedrock_runtime = boto3.client( 
        service_name="bedrock-runtime",
        region_name="us-east-1",
    )

embeddings = BedrockEmbeddings(
        model_id="amazon.titan-embed-text-v1",
        client=bedrock_runtime,
        region_name="us-east-1",
    )

index_creator = VectorstoreIndexCreator(
        vectorstore_cls=FAISS,
        embedding=embeddings,
    )

llm = BedrockChat(
    model_id="anthropic.claude-3-haiku-20240307-v1:0",
    client=bedrock_runtime,
    region_name="us-east-1",
    model_kwargs={"temperature": 0.0},
)

document_chain = create_stuff_documents_chain(llm,prompt)


def lambda_handler(event, context):
    print("Event:", event)
    bucket_name = "chartmate-idp" 
    job_id = event['job_id']

    s3.download_file(bucket_name, f"{job_id}/embeddings/index.faiss", "/tmp/index.faiss")
    s3.download_file(bucket_name, f"{job_id}/embeddings/index.pkl", "/tmp/index.pkl")

    faiss_index = FAISS.load_local("/tmp", embeddings, allow_dangerous_deserialization=True)
    # retriever = faiss_index.as_retriever()

    retriever = faiss_index.as_retriever(search_kwargs={"k":20})
    retrieval_chain = create_retrieval_chain(retriever, document_chain)

    def process_json(index, data_json):
        try:
            # Convert the JSON data to a string
            data_json_str = json.dumps(data_json, indent=2)

            response1 = retrieval_chain.invoke({"input": f"Understand and fill the answer for this {data_json_str}.Don't return anything extra other than the things mentioned in the context.return the answer as it is without modification.return Not Found as answer for each field incase of not getting answer.Do Not return blank or null values, empty strings incase you didnt find an answer."})
            response = response1["answer"]
            print(response)

            return index, response
        except Exception as e:
            print(f"Error in task {index}: {e}")
            return index, None, str(e)  # Ensure three values are always returned

    # Create a list of tasks with indices
    tasks = [
        patient_info,
        insurance,
        reason_for_referral,
        requested_service,
        s_o_r,
        clinical_history_Current_Diagnoses,
        clinical_history_Past_Diagnoses,
        clinical_history_recent_Surgical_History,
        # clinical_history_past_Medical_History,
        patient_pharmacy,
        current_medical_statushpi,
        functional_status,
        home_env,
        care_team_info,
        medications,
        wound_care,
        Iv_line,
        PICC_line,
        TPN,
        Weight_bearing_precautions
    ]

    responses = {}

    ssm_client.put_parameter(
        Name=job_id,
        Value="Starting Extraction",
        Type='String',
        Overwrite=True
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        # Submit all tasks sequentially
        futures = []
        for index, data_json in enumerate(tasks):
            future = executor.submit(process_json, index, data_json)
            futures.append(future)

        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            index = futures.index(future)
            try:
                _, response = future.result()
                
                if response is None:
                    error_message = f"Task with index {index} generated an exception: {str(future.exception())}"
                    print(error_message)
                    continue
                
                responses[str(index)] = response
            except Exception as e:
                print(f"Task with index {index} generated an exception: {e}")

    # Create the final JSON object
    final_json = {
        "responses": responses
    }

    output_file_path = '/tmp/combined_responses.json'
    with open(output_file_path, 'w') as f:
        json.dump(final_json, f, indent=2)

    print(f"All tasks completed. Results saved to {output_file_path}")

    # Upload the JSON file to S3
    s3.upload_file(output_file_path, bucket_name, f"{job_id}/combined_responses.json")
    print(f"File uploaded to S3: s3://{bucket_name}/{job_id}/combined_responses.json")

    ssm_client.put_parameter(
        Name=job_id,
        Value="Extraction completed",
        Type='String',
        Overwrite=True
    )

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "*",
        },
        "body": json.dumps(responses),
    }
