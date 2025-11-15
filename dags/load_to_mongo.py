import json
from datetime import datetime
from hdfs import InsecureClient
from airflow.sdk import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook

HDFS_FILE_PATH = "/kbo/parsed_results/kbo_data.json"
HDFS_CLIENT = InsecureClient('http://namenode:9870', user='airflow')

MONGO_CONN_ID = "mongo_default"
MONGO_DB = "kbo_data"
MONGO_COLLECTION = "companies"

@dag(
    dag_id="kbo_load_mongo_from_hdfs",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kbo", "mongo", "load"]
)
def kbo_load_mongo_pipeline():
    """
    Pipeline pour :
    1. Lire le fichier 'kbo_data.json' depuis HDFS.
    2. Charger ces données dans MongoDB (en écrasant la collection).
    """

    @task
    def load_json_from_hdfs_to_mongo():
        """
        Lit le fichier JSON depuis HDFS et l'insère dans MongoDB.
        """
        print(f"Début du chargement de {HDFS_FILE_PATH} vers MongoDB.")
        
        try:
            # --- 1. Lire le fichier depuis HDFS ---
            print(f"Lecture de HDFS: {HDFS_FILE_PATH}")
            with HDFS_CLIENT.read(HDFS_FILE_PATH, encoding='utf-8') as reader:
                json_string = reader.read()
            
            # Convertir le JSON (string) en une liste Python
            data_list = json.loads(json_string)
            print(f"{len(data_list)} documents lus depuis HDFS.")

            # --- 2. Charger les données dans Mongo ---
            print(f"Connexion à MongoDB (conn_id: {MONGO_CONN_ID})...")
            hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
            client = hook.get_conn()
            db = client[MONGO_DB]
            collection = db[MONGO_COLLECTION]

            print(f"Nettoyage de l'ancienne collection '{MONGO_COLLECTION}'...")
            collection.delete_many({}) # Vide la collection
            
            print("Insertion des nouveaux documents...")
            result = collection.insert_many(data_list)
            
            print(f"Chargement MongoDB terminé. {len(result.inserted_ids)} documents insérés.")

        except Exception as e:
            print(f"Échec du chargement HDFS vers Mongo: {e}")
            raise

    # --- Orchestration ---
    load_json_from_hdfs_to_mongo()


# Instanciation
kbo_load_mongo_pipeline()