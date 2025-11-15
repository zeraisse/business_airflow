# CMD 
```bash
python -m venv venv 
source ./venv/Scripts/activate
pip install -r requirements_scraper.txt
docker compose up -d
docker compose exec namenode bash
 hdfs dfs -mkdir -p /kbo/html
 hdfs dfs -chown airflow /kbo/html
 hdfs dfs -mkdir -p /kbo/parsed_results
 hdfs dfs -chown airflow /kbo/parsed_results
```

# Airflow Mongodb config local
```text
Si la config docker ne parviens pas a créer la connexion entre Airflow et MongoDb
Il faut le faire manuellement ici 
Dans Airflow > Admin > Connections > Add Connection
Y indiqué les information de la base de donnée
```

# Commande verification BDD
```text
Verifier la presence de données dans la bdd via docker 
```

```bash
docker compose exec {IMAGENAME} mongosh -u {USER} -p {PASSWORD} --authenticationDatabase {ROLE}
    use {DBNAME};
    db.{COLLECTION_NAME}.findOne();
    db.{COLLECTION_NAME}.countDocuments();
```
# Accès visualisation des données traité
```text
UI :  http://localhost:3000
API : http://localhost:5001/api/companies
```
❤️
