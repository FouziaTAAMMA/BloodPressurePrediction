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
    # Générer un ID unique pour l'observation
    observation_id = str(uuid.uuid4())

    # Observation de la pression artérielle
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

    # Observation de la température corporelle
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

    # Combiner les deux observations dans un Bundle
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
        print('Échec de la livraison du message: {}'.format(err))
    else:
        print('Message livré à {} [{}]'.format(msg.topic(), msg.partition()))

def publish_to_kafka(data, topic, bootstrap_servers):
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    producer.produce(topic, value=json.dumps(data).encode('utf-8'), callback=delivery_report)
    producer.flush()

# Calculer la plage de dates pour les 6 derniers mois
range_months_ago = datetime.now() - timedelta(days=190)

# Générer des données médicales pour les patients
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

    # Publier sur Kafka
    print(f"Nom du patient: {patient_name}, Condition: {condition}, Pression systolique: {systolic_bp}, Pression diastolique: {diastolic_bp}, Température corporelle: {body_temp}, Date effective: {effective_date}")
    publish_to_kafka(fhir_data, 'observation', 'localhost:29092')

# Sélectionner aléatoirement une condition
conditions = ["Hypotension", "Normal", "Elevated", "Hypertension Stage 1", "Hypertension Stage 2", "Hypertensive Crisis"]

# Générer des observations pour le test
nombre_observations = 400
for _ in range(nombre_observations):
    selected_condition = random.choice(conditions)
    process_patient_data(selected_condition, print_to_file=False)


