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


prompt = ChatPromptTemplate.from_template("""Please fill in the missing details in the following information::
<context>
{context}
</context>

please only return in json (fill the values)
retrieve the appropriate values from the context
Question: {input}""")

patient_info = {
    "patientInformation": {
            "fullName": "",
            "dateOfBirth": "",
            "gender": "////would be nice to have male, female or not-known",
            "address": {
                "streetNumber": "",
                "streetName": "",
                "apartmentUnitNumber": "",
                "city": "",
                "state": "",
                "zipCode": ""
            },
            "contactInformation": {
                "Emergency Contact": "",
                "Primary Contact": "",
                "homePhone": "",
                "mobilePhone": ""
            },
            "advancedDirective": "",
            "insuranceInformation": {
                "primaryInsurance": {
                    "providerName": "",
                    "policyInsuranceHolder": "",
                    "planDetails": "",
                    "policyNumber": "",
                    "groupNumber": "",
                    "contactDetails": ""
                },
                "secondaryInsurance": {
                    "providerName": "",
                    "policyInsuranceHolder": "",
                    "planDetails": "",
                    "policyNumber": "",
                    "groupNumber": "",
                    "contactDetails": ""
                }
            }
        }
}

prompt_patient_info = """
Return only the filled JSON object with all keys present. If a detail is not available, set its value to null. Do not include any introductory text or explanations.
get the answer for each value.
get correct answer for each of the key.
"""

reason_for_referral = {
    "reasonForReferral": {
            "detailedDescription": ""
    }
        
}

prompt_reason_for_referral = """
Return only the filled JSON object with all keys present. If a detail is not available, set its value to null. Do not include any introductory text or explanations.
get the answer for each value.

"""

requested_service = {
    "requested Services": {
            "specific Services Requested": [
                "Skilled Nursing",
                "Physical Therapy (PT)",
                "Occupational Therapy (OT)",
                "Speech Therapy (ST)",
                "Home Health Aide (HHA)",
                "Medical Social Worker (MSW)"
            ]
        }
}

prompt_requested_service = """
Return only the filled JSON object with all keys present. If a detail is not available, set its value to null. Do not include any introductory text or explanations.
get the answer for each value.
only return the required service from the context
"""

s_o_r = {
    "sourceOfReferral": {
            "referringPhysicianProvider": {
                "name": "",
                "address": {
                    "street Number": "",
                    "street Name": "",
                    "suite Number": "",
                    "city": "",
                    "state": "",
                    "zipCode": ""
                },
                "contactInformation": {
                    "phoneNumber": "",
                    "faxNumber": "",
                    "emailAddress": ""
                }
            }
        }
}



prompt_s_o_r = """
get the providers full address and answer from that only.
Return only the filled JSON object with all keys present. If a detail is not available, set its value to null. Do not include any introductory text or explanations.
get the answer for each value. 
"""


clinical_history = {
    "clinical History": {
            "comprehensive Medical History": {
                "current Diagnoses": [
                    {
                        "description": "",
                        "icd10 Code": "",
                        "onset Date": ""
                    }
                ],
                "past Diagnoses": [
                    {
                        "description": "",
                        "icd10 Code": ""
                    }
                ],
                "past Medical History": [
                    {}
                ],
                "recent Surgeries Surgical History": [
                    {
                        "name": "",
                        "date Year": ""
                    }
                ],
                "patient's Pharmacy": [
                    {
                        "name": "",
                        "phone Number": "",
                        "address": {
                            "street Number": "",
                            "street Name": "",
                            "suite Number": "",
                            "city": "",
                            "state": "",
                            "zipCode": ""
                        }
                    }
                ],
                "relevant Lab Results": [],
                "imaging Reports": [],
                "diagnostic Studies": []
            }
        }
}



prompt_clinical_history = """
Return only the filled JSON object with all keys present. If a detail is not available, set its value to null. Do not include any introductory text or explanations.
get the correct answer for each value.

"""

home_env = {
    "home Environment": {
            "safety Concerns": "",
            "primary Caregiver Availability": {
                "caregiver Name": "",
                "frequency Of Support": "",
                "type Of Support": ""
            },
            "home Modifications": [
                ""
            ]
        }
}


prompt_home_env = """
Return only the filled JSON object with all keys present. If a detail is not available, set its value to null. Do not include any introductory text or explanations.
get the answer for each value.
"""

medications = {
    "medications": {
            "medicationList": [
                {
                    "name": "Name of the medication.",
                    "dosage": "",
                    "form": "Form of medication (e.g., tablet, liquid).",
                    "quantity": "Quantity of medication prescribed)for eg 1 tablet )",
                    "route": "How the medication is administered (e.g., oral, intravenous).",
                    "frequency": "How often the medication should be taken.",
                    "date": "Date when the medication was prescribed.",
                    "action": "Action to be taken regarding the medication (e.g., continue, discontinue)."
                }
            ],
            "medicationReconciliation": "Comparison of patient's current medications with new prescriptions to prevent conflicts."
        }
}


prompt_medications = """
Return only the filled JSON object with all keys present. If a detail is not available, set its value to null. Do not include any introductory text or explanations.
get the answer for each value.
"""

current_medical_statushpi = {
    "current Medical Status HPI": {
            "summary": {
                "allergies": [],
                "vital Signs": {
                    "blood Pressure": "",
                    "heart Rate": "",
                    "oxygen Saturation": "",
                    "temperature": "",
                    "weight": "",
                    "height": ""
                },
                "recent Inpatient Facility": {
                    "date Of Discharge": "",
                    "facility Type": "",
                    "acute Chronic Issues": ""
                },
                "functional Precautions": ""
            }
        }
}


prompt_current_medical_statushpi = """
Return only the filled JSON object with all keys present. If a detail is not available, set its value to null. Do not include any introductory text or explanations.
get the answer for each value.
"""

functional_status = {
    "functional Status": {
            "mobility": {
                "ability To Walk Or Transfer": "",
                "assistance Needed": "",
                "assistive Devices": [
                    ""
                ]
            },
            "activities Of Daily LivingADLs": {
                "assistance Needed": {
                    "dressing": "",
                    "bathing": "",
                    "toileting": "",
                    "feeding": ""
                }
            }
        }
}


prompt_functional_status = """
Return only the filled JSON object with all keys present. If a detail is not available, set its value to null. Do not include any introductory text or explanations.
get the answer for each value.
"""


care_team_info = {
    "care Team Information": {
            "list Of Healthcare Providers": {
                "primary Care Physician": {
                    "name": "",
                    "contact Details": ""
                },
                "specialists": [],
                "other Providers": []
            }
        }
}


prompt_care_team_info = """
Return only the filled JSON object with all keys present. If a detail is not available, set its value to null. Do not include any introductory text or explanations.
get the answer for each value.
for contact information provide the contact details of the provider.
"""

woundcareorders = {
    "wound Care Orders": {
        "description": "",
        "orders": {
            "type Of Dressing": {
                "description": ""
            },
            "frequency Of Dressing Changes": {
                "description": ""
            },
            "cleaning Instructions": {
                "description": ""
            },
            "debridement": {
                "description": "",
                "performedBy": ""
            },
            "wound Monitoring": {
                "description": "",
                "parameters": [""],
                "signsOfInfection": [""]
            },
            "adjunctTherapies": {
                "description": ""
            }
        }
    }
}


prompt_woundcareorders = """
Return only the filled JSON object with all keys present. If a detail is not available, set its value to null. Do not include any introductory text or explanations.
get the answer for each value.
"""


iv_therapy = {
    "iv Therapy": {
      "description": "",
      "specifics": {
        "type Of Iv Fluid Or Medication": {
          "description": "",
          "examples": [
            "saline",
            "dextrose",
            "antibiotics",
            "pain medications"
          ]
        },
        "dosage": {
          "description": ""
        },
        "frequency And Duration": {
          "description": "",
          "examples": [
            "once a day",
            "continuously over a certain number of hours"
          ]
        },
        "method Of Administration": {
          "description": "",
          "options": [
            "peripheral IV line",
            "central line",
            "PICC line"
          ]
        },
        "monitoring": {
          "description": "",
          "parameters": [
            "vital signs",
            "signs of complications"
          ],
          "complications": [
            "infiltration",
            "phlebitis"
          ]
        }
      }
    },
    "picc Line": {
      "description": "",
      "care": {
        "piccLineMaintenance": {
          "description": "",
          "solutions": [
            "saline",
            "heparin"
          ]
        },
        "dressing Changes": {
          "description": "",
          "performed By": "",
          "frequency": ""
        },
        "medication Administration": {
          "description": "",
          "considerations": [
            "Checking compatibility of different medications before administration."
          ]
        },
        "signs Of Infection": {
          "description": "",
          "indicators": [
            "redness",
            "warmth",
            "swelling",
            "fever",
            "chills"
          ]
        },
        "labDraws": {
          "description": ""
        }
      }
    },
    "tpn": {
      "description": "",
      "specifics": {
        "composition": {
          "description": "",
          "components": [
            "glucose",
            "amino acids",
            "fats",
            "vitamins",
            "electrolytes"
          ]
        },
        "rate Of Administration": {
          "description": "",
          "method": ""
        },
        "monitoring": {
          "description": "",
          "parameters": [
            "blood sugar levels",
            "electrolytes",
            "liver function",
            "hydration status"
          ]
        },
        "complications": {
          "description": "",
          "examples": [
            "catheter-related infections",
            "metabolic imbalances",
            "hyperglycemia",
            "electrolyte disturbances"
          ]
        }
      }
    },
    "lab Orders": {
      "description": "",
      "tests": {
        "blood Tests": {
          "examples": {
            "cbc": "",
            "bmp": "",
            "cmp": "",
            "inr": "",
            "bloodGlucoseLevels": ""
          }
        },
        "urine Tests": {
          "description": ""
        },
        "cultures": {
          "description": "",
          "types": [
            "blood cultures",
            "urine cultures",
            "wound cultures"
          ]
        }
      },
      "timing And Frequency": {
        "description": "",
        "examples": [
          "weekly",
          "daily",
          "as needed"
        ]
      }
    },
    "weight Bearing Precautions": {
      "description": "",
      "categories": {
        "non Weight Bearing": {
          "description": ""
        },
        "toe Touch Weight Bearing": {
          "description": ""
        },
        "partial Weight Bearing": {
          "description": ""
        },
        "weight Bearing As Tolerated": {
          "description": ""
        },
        "full Weight Bearing": {
          "description": ""
        }
      }
    },
    "bed bound": {
      "description": "",
      "causes": {
        "severe Illness": {
          "description": ""
        },
        "post Surgery": {
          "description": ""
        },
        "chronic Conditions": {
          "description": ""
        }
      }
    },
    "wheel chair Bound": {
      "description": "",
      "causes": {
        "neurological Disorders": {
          "description": ""
        },
        "severe Arthritis Or Joint Problems": {
          "description": ""
        },
        "muscle Weakness": {
          "description": ""
        }
      },
      "mobility Status": {
        "description": "",
        "considerations": [
          "Pressure ulcer prevention and monitoring",
          "Safe transfers between wheelchair and bed",
          "Physical therapy for maintaining upper body strength"
        ]
      }
    },
    "aAndO": {
      "description": "",
      "levels": {
        "aAndOx1": {
          "description": ""
        },
        "aAndOx2": {
          "description": ""
        },
        "aAndOx3": {
          "description": ""
        },
        "aAndOx4": {
          "description": ""
        }
      },
      "implications": {
        "description": ""
      }
    }
  }



prompt_ivtherapy = """
Return only the filled JSON object with all keys present. If a detail is not available, set its value to null. Do not include any introductory text or explanations.
get the answer for each value.
"""



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
    region_name="us-east-1"
)

document_chain = create_stuff_documents_chain(llm,prompt)


def lambda_handler(event, context):
    print("Event:", event)
    bucket_name = "chartmate-idp" 
    job_id = event['job_id']

    s3.download_file(bucket_name, f"{job_id}/embeddings/index.faiss", "/tmp/index.faiss")
    s3.download_file(bucket_name, f"{job_id}/embeddings/index.pkl", "/tmp/index.pkl")

    faiss_index = FAISS.load_local("/tmp", embeddings, allow_dangerous_deserialization=True)
    retriever = faiss_index.as_retriever()
    retrieval_chain = create_retrieval_chain(retriever, document_chain)

    def process_json(index, data_json, prompt):
        try:
            # Convert the JSON data to a string
            data_json_str = json.dumps(data_json, indent=2)

            # response = retrieval_chain.invoke({"input":f"analyse and asnwer properly and return the whole answer . fill the answer for this {data_json_str}","prompt": prompt})
            response1 = retrieval_chain.invoke({"input":f"Understand and fill the answer for this {data_json_str}","prompt": prompt})
            # print(response["answer"])
            response = response1["answer"]
            # print("1",response)
            # print("2",index)

            return index, response
        except Exception as e:
            print(f"Error in task {index}: {e}")
            return index, None, str(e)
    

    # Create a list of tasks with indices
    tasks = [
        (patient_info, prompt_patient_info),
        (reason_for_referral, prompt_reason_for_referral),
        (requested_service, prompt_requested_service),
        (s_o_r, prompt_s_o_r),
        (clinical_history , prompt_clinical_history ),
        (current_medical_statushpi,prompt_current_medical_statushpi),
        (functional_status,prompt_functional_status),
        (home_env,prompt_home_env),
        (care_team_info, prompt_care_team_info),
        (medications, prompt_medications),
        (woundcareorders,prompt_woundcareorders)
    ]

    responses = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        # Submit all tasks sequentially
        futures = []
        for index, (data_json, prompt) in enumerate(tasks):
            future = executor.submit(process_json, index, data_json, prompt)
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
                
                print(f"Response saved for Task {index}")
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
