import json
from datetime import datetime
import uuid
from confluent_kafka import Producer

def generate_fhir_data(patient_name, systolic_bp, diastolic_bp, body_temp):
    # Generate a unique ID for the observation
    observation_id = str(uuid.uuid4())

    # Current date and time for effectiveDateTime
    effective_date = datetime.now().strftime("%Y-%m-%d")

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
            "reference": f"Patient/{patient_name}"
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
            "reference": f"Patient/{patient_name}"
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


# Example usage
patient_name = "Patient 2"
systolic_bp = 140
diastolic_bp = 90
body_temp = 40

# Generate FHIR data
fhir_data = generate_fhir_data(patient_name, systolic_bp, diastolic_bp, body_temp)


# Publish to Kafka
publish_to_kafka(fhir_data, 'observation', 'localhost:9093')

# Write the JSON data to a file ( for local test )
# output_file = "fhir_data.json"
# with open(output_file, "w") as file:
#     json.dump(fhir_data, file, indent=2)

# print(f"FHIR data has been written to {output_file}")