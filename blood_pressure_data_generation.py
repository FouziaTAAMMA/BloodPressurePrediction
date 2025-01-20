import json
from datetime import datetime, timedelta
import uuid
from confluent_kafka import Producer
from faker import Faker
import random

fake = Faker(locale="fr_FR")

def generate_medical_data(condition):
    if condition == "Hypotension":
        systolic_bp = random.randint(50, 89)
        diastolic_bp = random.randint(40, 59)
    elif condition == "Normal":
        systolic_bp = random.randint(90, 119)
        diastolic_bp = random.randint(60, 79)
    elif condition == "Elevated":
        systolic_bp = random.randint(120, 129)
        diastolic_bp = random.randint(60, 79)
    elif condition == "Hypertension Stage 1":
        systolic_bp = random.randint(130, 139)
        diastolic_bp = random.randint(80, 89)
    elif condition == "Hypertension Stage 2":
        systolic_bp = random.randint(140, 179)
        diastolic_bp = random.randint(90, 119)
    elif condition == "Hypertensive Crisis":
        systolic_bp = random.randint(180, 200)
        diastolic_bp = random.randint(120, 130)
    else:
        systolic_bp = random.randint(50, 200)
        diastolic_bp = random.randint(40, 130)
        condition = "Unknown"

    body_temp = round(random.uniform(36.0, 41.0), 1)
    return systolic_bp, diastolic_bp, body_temp, condition

def generate_fhir_data(patient_name, systolic_bp, diastolic_bp, body_temp, effective_date, condition):
    # Generate a unique ID for the observation
    observation_id = str(uuid.uuid4())

    # Blood Pressure Observation
    blood_pressure_observation = {
        "resourceType": "Observation",
        "id": "blood-pressure",
        "meta": {
            "profile": ["http://hl7.org/fhir/StructureDefinition/vitalsigns"]
        },
        "identifier": [{
            "system": "urn:ietf:rfc:3986",
            "value": f"urn:uuid:{observation_id}"
        }],
        "status": "final",
        "category": [{
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                "code": "vital-signs",
                "display": "Vital Signs"
            }]
        }],
        "code": {
            "coding": [{
                "system": "http://loinc.org",
                "code": "85354-9",
                "display": "Blood pressure panel with all children optional"
            }],
            "text": "Blood pressure systolic & diastolic"
        },
        "subject": {
            "reference": f"{patient_name}"
        },
        "effectiveDateTime": effective_date,
        "component": [
            {
                "code": {
                    "coding": [{
                        "system": "http://loinc.org",
                        "code": "8480-6",
                        "display": "Systolic blood pressure"
                    }]
                },
                "valueQuantity": {
                    "value": systolic_bp,
                    "unit": "mmHg",
                    "system": "http://unitsofmeasure.org",
                    "code": "mm[Hg]"
                }
            },
            {
                "code": {
                    "coding": [{
                        "system": "http://loinc.org",
                        "code": "8462-4",
                        "display": "Diastolic blood pressure"
                    }]
                },
                "valueQuantity": {
                    "value": diastolic_bp,
                    "unit": "mmHg",
                    "system": "http://unitsofmeasure.org",
                    "code": "mm[Hg]"
                }
            }
        ],
    }

    # Body Temperature Observation
    body_temp_observation = {
        "resourceType": "Observation",
        "id": "body-temperature",
        "meta": {
            "profile": ["http://hl7.org/fhir/StructureDefinition/vitalsigns"]
        },
        "status": "final",
        "category": [{
            "coding": [{
                "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                "code": "vital-signs",
                "display": "Vital Signs"
            }]
        }],
        "code": {
            "coding": [{
                "system": "http://loinc.org",
                "code": "8310-5",
                "display": "Body temperature"
            }],
            "text": "Body temperature"
        },
        "subject": {
            "reference": f"{patient_name}"
        },
        "effectiveDateTime": effective_date,
        "valueQuantity": {
            "value": body_temp,
            "unit": "C",
            "system": "http://unitsofmeasure.org",
            "code": "Cel"
        }
    }

    # Combine both observations into a Bundle
    bundle = {
        "resourceType": "Bundle",
        "type": "collection",
        "entry": [
            {
                "resource": blood_pressure_observation
            },
            {
                "resource": body_temp_observation
            }
        ]
    }

    return bundle

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def publish_to_kafka(data, topic, bootstrap_servers):
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    producer.produce(topic, value=json.dumps(data).encode('utf-8'), callback=delivery_report)
    producer.flush()

# Calculate the date range for the last 6 months
range_months_ago = datetime.now() - timedelta(days=190)

# Generate medical data for patients
def process_patient_data(condition, print_to_file=False):
    patient_name = fake.name()
    effective_date = fake.date_between(
        start_date=range_months_ago, 
        end_date='today'
    ).strftime("%Y-%m-%d")
    systolic_bp, diastolic_bp, body_temp, condition = generate_medical_data(condition)
    fhir_data = generate_fhir_data(patient_name, systolic_bp, diastolic_bp, body_temp, effective_date, condition)
    
    if print_to_file:
        with open(f"fhir_data_{condition}.json", "w") as file:
            json.dump(fhir_data, file, indent=2)

    # Publish to Kafka
    print(f"Patient Name: {patient_name}, Condition: {condition}, Systolic BP: {systolic_bp}, Diastolic BP: {diastolic_bp}, Body Temp: {body_temp}, Effective Date: {effective_date}")
    publish_to_kafka(fhir_data, 'observation', 'localhost:29092')

# Randomly select a condition
conditions = ["Hypotension", "Normal", "Elevated", "Hypertension Stage 1", "Hypertension Stage 2", "Hypertensive Crisis"]

# Generate 400 observations for test
for _ in range(400):
    selected_condition = random.choice(conditions)
    process_patient_data(selected_condition, print_to_file=False)


