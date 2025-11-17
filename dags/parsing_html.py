import json
from datetime import datetime
from hdfs import InsecureClient
from airflow.sdk import dag, task
from parser.kbo_html_parser import ParsingHTML
import os
from pathlib import Path

# --- Configuration (inchangée) ---
HDFS_BASE_DIR = "/kbo/html"
HDFS_OUTPUT_DIR = "/kbo/parsed_results"
HDFS_OUTPUT_FILE = f"{HDFS_OUTPUT_DIR}/kbo_data.json"
CLIENT_HDFS = InsecureClient('http://namenode:9870', user='airflow')

BATCH_SIZE = 100

@dag(
    dag_id="kbo_parsing_hdfs_pipeline", 
    start_date=datetime(2025, 1, 1),
    schedule="0 7 * * *",
    catchup=False
)
def kbo_parsing_hdfs_pipeline():
    """
    Pipeline pour :
    1. Lister TOUS les fichiers HDFS et les diviser en lots.
    2. Parser chaque LOT en parallèle (mapping).
    3. Sauvegarder tous les résultats dans un JSON unique sur HDFS.
    """

    @task
    def get_hdfs_file_batches() -> list[list[str]]:
        """
        Scanne le dossier HDFS et retourne une liste de LOTS de noms de fichiers.
        Ex: [['f1.html', 'f2.html'], ['f3.html', 'f4.html']]
        """
        print(f"Listing files from HDFS path: {HDFS_BASE_DIR}")
        try:
            files = CLIENT_HDFS.list(HDFS_BASE_DIR)
            if not files:
                raise ValueError(f"Aucun fichier trouvé dans HDFS : {HDFS_BASE_DIR}")
            
            print(f"Trouvé {len(files)} fichiers au total.")
            
            # Logique pour "chunker" la liste en lots de BATCH_SIZE
            batches = [files[i:i + BATCH_SIZE] for i in range(0, len(files), BATCH_SIZE)]
            
            print(f"Divisé en {len(batches)} lots de {BATCH_SIZE} fichiers chacun.")
            return batches
            
        except Exception as e:
            print(f"Échec de la lecture du dossier HDFS {HDFS_BASE_DIR}: {e}")
            raise

    # --- TÂCHE 2 (MODIFIÉE) : Traite un lot entier ---
    @task
    def parse_html_batch(file_batch: list[str]) -> list[dict]:
        """
        Prend UN LOT de noms de fichiers, les lit depuis HDFS, les parse
        et retourne une LISTE de dictionnaires de données.
        """
        print(f"Parsing d'un lot de {len(file_batch)} fichiers...")
        batch_results = []
        
        for filename in file_batch:
            hdfs_file_path = f"{HDFS_BASE_DIR}/{filename}"
            try:
                with CLIENT_HDFS.read(hdfs_file_path, encoding='utf-8') as reader:
                    content = reader.read()
                
                parsed_data = ParsingHTML.parse_kbo_html(content)
                parsed_data['source_file'] = filename
                batch_results.append(parsed_data)
                
            except Exception as e:
                print(f"Échec du parsing HDFS {hdfs_file_path}: {e}")
                batch_results.append({"error": str(e), "source_file": filename})
        
        print(f"Lot terminé. {len(batch_results)} résultats produits.")
        return batch_results # Retourne la liste des résultats pour CE lot

    @task
    def save_results_to_hdfs(list_of_lists_of_results: list[list[dict]]):
        """
        Collecte TOUS les résultats (une liste de listes),
        les aplatit, et les sauvegarde dans un seul JSON sur HDFS.
        """
        
        # 1. Matérialiser la LazySequence (comme avant)
        materialized_list_of_lists = list(list_of_lists_of_results)
        
        # 2. APLATIR la liste de listes
        #    On transforme [['res1', 'res2'], ['res3']] en ['res1', 'res2', 'res3']
        real_results_list = [item for batch in materialized_list_of_lists for item in batch]

        print(f"Sauvegarde de {len(real_results_list)} résultats (depuis {len(materialized_list_of_lists)} lots) vers {HDFS_OUTPUT_FILE}")

        try:
            json_data = json.dumps(real_results_list, indent=4, ensure_ascii=False)
            
            CLIENT_HDFS.write(HDFS_OUTPUT_FILE, 
                              data=json_data, 
                              encoding='utf-8', 
                              overwrite=True)
            
            print(f"Sauvegarde HDFS terminée.")
            
        except Exception as e:
            print(f"Échec de la sauvegarde JSON sur HDFS : {e}")
            raise

    # -----------------------------------------------------------------
    #  Orchestration
    # -----------------------------------------------------------------
    
    # 1. Obtenir la liste des lots
    file_batches = get_hdfs_file_batches()
    
    # 2. MAP : Appliquer 'parse_html_batch' à chaque lot
    list_of_parsed_batches = parse_html_batch.expand(file_batch=file_batches)
    
    # 3. REDUCE : Attendre que tout soit fini et sauvegarder
    save_results_to_hdfs(list_of_parsed_batches)

# Instanciation du DAG
kbo_parsing_hdfs_pipeline()