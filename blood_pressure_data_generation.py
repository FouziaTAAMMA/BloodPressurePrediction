import json
from datetime import datetime, timedelta
import uuid
from confluent_kafka import Producer
from faker import Faker
import random

fake = Faker(locale="fr_FR")

def generate_medical_data(sick=False):
    if sick:
        # Generate anomalous values for either or both systolic and diastolic BP
        systolic_bp = random.choice([random.randint(50, 89), random.randint(141, 200)])
        diastolic_bp = random.choice([random.randint(40, 59), random.randint(91, 130)])
        # Ensure at least one of the values is anomalous
        if systolic_bp in range(90, 141) and diastolic_bp in range(60, 91):
            if random.choice([True, False]):
                systolic_bp = random.choice([random.randint(50, 89), random.randint(141, 200)])
            else:
                diastolic_bp = random.choice([random.randint(40, 59), random.randint(91, 130)])
        body_temp = round(random.uniform(38.0, 41.0), 1)  # Fever range for sick
    else:
        # Generate normal values
        systolic_bp = random.randint(90, 140)
        diastolic_bp = random.randint(60, 90)
        body_temp = round(random.uniform(36.0, 37.5), 1)  # Normal body temperature

    return systolic_bp, diastolic_bp, body_temp

def generate_fhir_data(patient_name, systolic_bp, diastolic_bp, body_temp, effective_date):
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
        ]
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

# Calculate the date range for the last two months
two_months_ago = datetime.now() - timedelta(days=60)

# Example usage
patient_name = fake.name()

# Generate observation date
effective_date = fake.date_between(
    start_date=two_months_ago, 
    end_date='today'
).strftime("%Y-%m-%d")

# Generate medical data for healthy and sick patients
def process_patient_data(sick, print_to_file=False):
    systolic_bp, diastolic_bp, body_temp = generate_medical_data(sick=sick)
    fhir_data = generate_fhir_data(patient_name, systolic_bp, diastolic_bp, body_temp, effective_date)
    
    if print_to_file:
        with open(f"fhir_data_sick_{sick}.json", "w") as file:
            json.dump(fhir_data, file, indent=2)


    # Publish to Kafka
    print(f"Patient Name: {patient_name},Sick: {sick} , Systolic BP: {systolic_bp}, Diastolic BP: {diastolic_bp}, Body Temp: {body_temp}, Effective Date: {effective_date}")
    publish_to_kafka(fhir_data, 'observation', 'localhost:29092')

# Example usage

process_patient_data(sick=random.choice([True, False]), print_to_file=False)


