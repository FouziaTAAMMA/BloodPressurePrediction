import json
from confluent_kafka import Consumer, KafkaException, KafkaError
from elasticsearch import Elasticsearch
import os

# Define the function to detect anomalies
def detect_anomalies(systolic_bp, diastolic_bp, body_temp):
    anomalies = []
    
    # Check for abnormal blood pressure values
    if systolic_bp < 90 or systolic_bp > 140:
        anomalies.append("Abnormal systolic blood pressure")
    if diastolic_bp < 60 or diastolic_bp > 90:
        anomalies.append("Abnormal diastolic blood pressure")
    
    # Check for abnormal body temperature
    # if body_temp < 35 or body_temp > 40:
    #     anomalies.append("Abnormal body temperature")
    
    return anomalies

# Initialize Elasticsearch client
es = Elasticsearch(
    [{'host': 'localhost', 'port': 9200, 'scheme': 'http'}],
    headers={'Content-Type': 'application/json'}
)

# Save data to local JSON file
def save_to_json(data, filename):
    with open(filename, 'a') as file:
        json.dump(data, file, indent=2)
        file.write('\n')

# Create Kafka consumer
def consume_message():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:29092',  # Updated to reflect Docker setup
        'group.id': 'observation-group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['observation'])  # Topic name

    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Adjust timeout as needed
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'End of partition reached: {msg.topic()} {msg.partition()} {msg.offset()}')
                else:
                    raise KafkaException(msg.error())
            else:
                message_value = msg.value().decode('utf-8')
                process_fhir_message(message_value)
    finally:
        consumer.close()

# Process the FHIR message and extract data
def process_fhir_message(fhir_message):
    try:
        data = json.loads(fhir_message)

        # Extract patient name (assume patient reference format "Patient/Patient 3")
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
            anomalies = detect_anomalies(systolic_bp, diastolic_bp, body_temp)
            print(f"Patient: {patient_name}")
            print(f"Systolic BP: {systolic_bp} mmHg, Diastolic BP: {diastolic_bp} mmHg, Body Temp: {body_temp}°C")
            if anomalies:
                print(f"Anomalies detected: {', '.join(anomalies)}")
                # Index data in Elasticsearch
                es.index(index='patients', body={
                    'patient_name': patient_name,
                    'systolic_bp': systolic_bp,
                    'diastolic_bp': diastolic_bp,
                    'body_temp': body_temp,
                    'effective_date': effective_date,
                    'anomalies': anomalies
                })
            else:
                print("No anomalies detected.")
                # Save data to local JSON file
                save_to_json(data, 'patients_normal.json')
        else:
            print("Missing some required data.")
    except Exception as e:
        print(f"Error processing FHIR message: {str(e)}")

if __name__ == '__main__':
    consume_message()
