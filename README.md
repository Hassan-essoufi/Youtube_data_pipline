# YouTube Data Pipeline

> Pipeline ETL (Extract, Transform, Load) pour extraire, transformer et charger les données YouTube (chaînes, vidéos, commentaires) dans une base de données PostgreSQL, avec orchestration via Apache Airflow.

##  Architecture


Ce projet suit une architecture ETL classique en 3 phases :
```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   EXTRACT   │ ───> │  TRANSFORM  │ ───> │    LOAD     │
│ YouTube API │      │  Nettoyage  │      │ PostgreSQL  │
└─────────────┘      └─────────────┘      └─────────────┘
```

## Flux de données
1. **Extract** : Extraction des données via l'API YouTube (chaînes, vidéos, commentaires)
1. **Extract** : Extraction des données via l'API YouTube (chaînes, vidéos, commentaires)
2. **Transform** : Nettoyage, normalisation et transformation des données
3. **Load** : Chargement dans PostgreSQL avec création automatique des tables 

##  Fonctionnalités:

- ✅ Extraction de données YouTube via API officielle
- ✅ Support multi-chaînes en parallèle
- ✅ Transformation et nettoyage automatique des données
- ✅ Chargement dans PostgreSQL avec gestion des schémas
- ✅ Orchestration via Apache Airflow (DAG)
- ✅ Logging détaillé avec fichiers horodatés
- ✅ Gestion des erreurs et validation des données
- ✅ Sauvegarde des données brutes et transformées


## Structure du projet
```
Youtube_Data_Pipline/
├── config/
│   ├── api_keys.yaml          # Clés API YouTube
│   └── settings.yaml          # Configuration générale (DB, chemins, etc.)
├── data/
│   ├── raw/                   # Données brutes extraites
│   └── processed/             # Données transformées
├── etl/
│   ├── extract.py             # Extraction depuis YouTube API
│   ├── transform.py           # Transformation et nettoyage
│   └── load.py                # Chargement dans PostgreSQL
├── utils/
│   ├── api_utils.py           # Utilitaires YouTube API
│   └── db_utils.py            # Utilitaires base de données
├── logs/                      # Logs d'exécution
├── main.py                    # Point d'entrée principal
├── youtube_etl_dag.py         # DAG Airflow pour orchestration 
├── requirements.txt           # Dépendances Python
└── README.md                  

```
## Requirements:
```
google-api-python-client==2.108.0
PyYAML==6.0.1
pandas==2.1.3
numpy==1.26.2
psycopg2-binary==2.9.9
apache-airflow==2.7.3
```
