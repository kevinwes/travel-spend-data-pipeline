 Pipeline de Données – Analyse des Dépenses de Voyage
 Présentation du projet

Ce projet est un pipeline de données complet permettant d’analyser les dépenses de voyage des équipes (Football, Handball, Judo).

Il comprend :

- L’extraction de données depuis des fichiers CSV
- La transformation des données avec Python (Pandas)
- L’orchestration avec Apache Airflow
- La visualisation avec Power BI
  
Technologies utilisées :

- Python (Pandas)
- SQL (SQLite)
- Apache Airflow
- Docker
- Power BI
  
Fonctionnalités principales:

- Suivi des dépenses totales de voyage
- Analyse des dépenses par équipe
- Analyse par type de réservation (flight, hotel)
- Identification des employés les plus coûteux
- Calcul du taux d’annulation
- Analyse des tendances dans le temps
  
Architecture du pipeline :

- Extract : Chargement des fichiers CSV
- Transform : Nettoyage et enrichissement des données
- Load : Stockage des données transformées
- Visualisation : Dashboard Power BI
  
Structure du projet
travel-spend-data-pipeline/
│
├── data/
│   ├── raw/        # Données brutes
│   └── processed/  # Données transformées
│
├── scripts/
│   └── etl_travel.py
│
├── dags/
│   └── travel_etl_pipeline.py
│
├── docker-compose.yml
└── README.md

Lancer le projet :

1. Démarrer Airflow
docker-compose up
2. Exécuter le pipeline
- Ouvrir Airflow : http://localhost:8080
- Lancer le DAG : travel_etl_pipeline
  
 Dashboard Power BI

Le dashboard permet de visualiser :

- Dépense totale
- Dépenses par équipe
- Dépenses par type de réservation
- Top employés
- Taux d’annulation
- Évolution des dépenses dans le temps
- Valeur métier

Ce projet permet de :

- Optimiser les coûts de déplacement
- Suivre les budgets
- Aider à la prise de décision
- Identifier les sources de dépenses élevées
👨‍💻 Auteur

Kevin N'tary
