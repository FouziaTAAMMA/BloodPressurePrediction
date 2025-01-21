# Blood Pressure Prediction

## Description

Ce projet utilise Kafka, Elasticsearch et Kibana pour analyser et visualiser les données de pression artérielle des patients. Les données sont générées, publiées sur Kafka, consommées et indexées dans Elasticsearch, puis visualisées dans Kibana.

## Prérequis

- Docker
- Docker Compose
- Python 3.x

## Installation

1. Clonez le dépôt :
   ```sh
   git clone <URL_DU_DEPOT>
   cd BloodPressurePrediction
   ```

2. Installez les dépendances Python :
   ```sh
   pip install -r requirements.txt
   ```

## Utilisation

### Étape 1 : Démarrer les services Docker Compose

Assurez-vous que tous les services (Zookeeper, Kafka, Kafka UI, Elasticsearch et Kibana) sont en cours d'exécution.

```sh
docker-compose up -d
```

### Étape 2 : Générer et publier des données

Exécutez le script `blood_pressure_data_generation.py` pour générer et publier des données sur Kafka.

```sh
python blood_pressure_data_generation.py
```

### Étape 2.1 : Vérifier les données dans Kafka UI ( Optionel )

Ouvrez votre navigateur web et accédez à `http://localhost:8080` pour ouvrir l'interface Kafka UI. Vérifiez que les données générées par le script `blood_pressure_data_generation.py` sont bien enregistrées dans les sujets de Kafka.

### Étape 3 : Consommer et indexer des données

Exécutez le script `fhir_observation_analyzer.py` pour consommer des données de Kafka et les indexer dans Elasticsearch.

```sh
python fhir_observation_analyzer.py
```

### Étape 4 : Accéder à Kibana

Ouvrez votre navigateur web et accédez à `http://localhost:5601` pour ouvrir l'interface Kibana.

### Étape 5 : Configurer Kibana pour utiliser l'index Elasticsearch

1. Dans Kibana, allez dans la section **Management** en cliquant sur l'icône d'engrenage dans le menu de gauche.
2. Sous **Kibana**, cliquez sur **Index Patterns**.
3. Cliquez sur **Create index pattern**.
4. Entrez `patients` comme modèle d'index et cliquez sur **Next step**.
5. Sélectionnez le champ de date (par exemple, `effective_date`) si disponible, et cliquez sur **Create index pattern**.

### Étape 6 : Créer des visualisations

1. Allez dans la section **Visualize** en cliquant sur l'icône de graphique dans le menu de gauche.
2. Cliquez sur **Create visualization**.
3. Choisissez le type de visualisation que vous souhaitez créer (par exemple
