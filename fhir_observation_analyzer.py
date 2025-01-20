import json
from confluent_kafka import Consumer, KafkaException, KafkaError
from elasticsearch import Elasticsearch

# Définir la fonction pour détecter les anomalies
def detect_condition(systolic_bp, diastolic_bp):
    if systolic_bp < 90 and diastolic_bp < 60:
        return "Hypotension"
    elif 90 <= systolic_bp <= 119 and 60 <= diastolic_bp <= 79:
        return "Normal"
    elif 120 <= systolic_bp <= 129 and 60 <= diastolic_bp <= 79:
        return "Elevated"
    elif 130 <= systolic_bp <= 139 and 80 <= diastolic_bp <= 89:
        return "Hypertension Stage 1"
    elif 140 <= systolic_bp <= 179 and 90 <= diastolic_bp <= 119:
        return "Hypertension Stage 2"
    elif systolic_bp >= 180 and diastolic_bp >= 120:
        return "Hypertensive Crisis"
    else:
        return "Unknown"

# Initialiser le client Elasticsearch
es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'http'}],
    headers={'Content-Type': 'application/json'}
)

# Enregistrer les données dans un fichier JSON local
def save_to_json(data, filename):
    with open(filename, 'a') as file:
        json.dump(data, file, indent=2)
        file.write('\n')

# Créer un consommateur Kafka
def consume_message():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:29092',  # Mis à jour pour refléter la configuration Docker
        'group.id': 'observation-group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['observation'])  # Nom du sujet

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Ajuster le délai d'attente si nécessaire
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'Fin de la partition atteinte: {msg.topic()} {msg.partition()} {msg.offset()}')
                else:
                    raise KafkaException(msg.error())
            else:
                message_value = msg.value().decode('utf-8')
                process_fhir_message(message_value)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Traiter le message FHIR et extraire les données
def process_fhir_message(fhir_message):
    try:
        data = json.loads(fhir_message)

        # Extraire le nom du patient et les observations
        patient_name = None
        systolic_bp = diastolic_bp = body_temp = effective_date = None

        for entry in data.get('entry', []):
            if entry['resource']['resourceType'] == 'Observation':
                if entry['resource']['id'] == 'blood-pressure':
                    for component in entry['resource'].get('component', []):
                        if 'Systolic blood pressure' in component['code']['coding'][0]['display']:
                            systolic_bp = component['valueQuantity']['value']
                        elif 'Diastolic blood pressure' in component['code']['coding'][0]['display']:
                            diastolic_bp = component['valueQuantity']['value']
                    effective_date = entry['resource']['effectiveDateTime']
                elif entry['resource']['id'] == 'body-temperature':
                    body_temp = entry['resource']['valueQuantity']['value']
                if not patient_name:
                    patient_name = entry['resource']['subject']['reference']

        if patient_name and systolic_bp is not None and diastolic_bp is not None and body_temp is not None:
            condition = detect_condition(systolic_bp, diastolic_bp)
            print(f"Patient: {patient_name}")
            print(f"Pression systolique: {systolic_bp} mmHg, Pression diastolique: {diastolic_bp} mmHg, Température corporelle: {body_temp}°C")
            print(f"Condition: {condition}")
            if condition != "Normal":
                print(f"Anomalies détectées: {condition}")
                # Indexer les données dans Elasticsearch
                response = es.index(index='patients', body={
                    'patient_name': patient_name,
                    'systolic_bp': systolic_bp,
                    'diastolic_bp': diastolic_bp,
                    'body_temp': body_temp,
                    'effective_date': effective_date,
                    'condition': condition
                })
                print(f"ID du document indexé: {response['_id']}")
            else:
                print("Aucune anomalie détectée.")
                # Enregistrer les données dans un fichier JSON local
                save_to_json(data, 'patients_normal.json')
        else:
            print("Certaines données requises manquent.")
    except Exception as e:
        print(f"Erreur lors du traitement du message FHIR: {str(e)}")

if __name__ == '__main__':
    consume_message()