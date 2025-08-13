# 🌦️ Weather ETL Pipeline (CSV → PostgreSQL)

## 📌 Description
Ce projet met en place un **pipeline ETL** complet utilisant **Apache Airflow**, **PostgreSQL** et **Docker** pour automatiser le traitement de données météorologiques à partir de fichiers CSV.

Le pipeline effectue les étapes suivantes :
1. **Extraction** : Lecture des données météorologiques depuis un fichier CSV.
2. **Transformation** : Nettoyage, formatage des dates, conversion des températures en Celsius, etc.
3. **Chargement** : Insertion des données transformées dans une base **PostgreSQL**.

---

## 🛠️ Technologies utilisées
- **Python** (pandas, psycopg2)
- **Apache Airflow**
- **PostgreSQL**
- **Docker & Docker Compose**

---

## 📂 Structure du projet
