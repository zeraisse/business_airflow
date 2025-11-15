import json
from datetime import datetime
from hdfs import InsecureClient
from airflow.sdk import dag, task
from parser.kbo_html_parser import ParsingHTML
import os
from pathlib import Path


HDFS_BASE_DIR = "/kbo/html"
HDFS_OUTPUT_DIR = "/kbo/parsed_results"
HDFS_OUTPUT_FILE = f"{HDFS_OUTPUT_DIR}/kbo_data.json"

# Pointe vers l'API web du namenode (port 9870)
CLIENT_HDFS = InsecureClient('http://namenode:9870', user='airflow')


@dag(
    dag_id="kbo_parsing_hdfs_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
)
def kbo_parsing_hdfs_pipeline():
    """
    Pipeline pour :
    1. Lister tous les fichiers HTML depuis HDFS.
    2. Parser chaque fichier en parallèle (mapping).
    3. Sauvegarder tous les résultats dans un JSON unique sur HDFS.
    """

    @task
    def get_hdfs_files() -> list[str]:
        """
        Scanne le dossier HDFS /kbo/html et retourne la liste des
        noms de fichiers (ex: ['200065765.html', ...]).
        """
        print(f"Listing files from HDFS path: {HDFS_BASE_DIR}")
        try:
            # .list() ne retourne que les noms de fichiers, pas le chemin complet
            files = CLIENT_HDFS.list(HDFS_BASE_DIR)
            
            if not files:
                raise ValueError(f"Aucun fichier trouvé dans HDFS : {HDFS_BASE_DIR}")
            
            print(f"Trouvé {len(files)} fichiers à parser.")
            return files
            
        except Exception as e:
            print(f"Échec de la lecture du dossier HDFS {HDFS_BASE_DIR}: {e}")
            raise

    @task
    def parse_html_task(filename: str) -> dict:
        """
        Prend UN nom de fichier, le lit depuis HDFS, le parse
        et retourne un dictionnaire de données.
        """
        # Construit le chemin HDFS complet
        hdfs_file_path = f"{HDFS_BASE_DIR}/{filename}"
        print(f"Parsing HDFS file: {hdfs_file_path}")
        
        try:
            # --- MODIFICATION CLÉ : LECTURE DEPUIS HDFS ---
            with CLIENT_HDFS.read(hdfs_file_path, encoding='utf-8') as reader:
                content = reader.read()
            
            parsed_data = ParsingHTML.parse_kbo_html(content)
            parsed_data['source_file'] = filename
            
            return parsed_data
            
        except Exception as e:
            print(f"Échec du parsing HDFS {hdfs_file_path}: {e}")
            return {"error": str(e), "source_file": filename}

    @task
    def save_results_to_hdfs(parsed_data_list: list[dict]):
        
        real_results_list = list(parsed_data_list)
        
        print(f"Sauvegarde de {len(real_results_list)} résultats vers {HDFS_OUTPUT_FILE}")

        try:
            json_data = json.dumps(real_results_list, indent=4, ensure_ascii=False)
            
            # --- LA CORRECTION EST ICI ---
            # Pas besoin de "with" si vous fournissez l'argument "data"
            CLIENT_HDFS.write(HDFS_OUTPUT_FILE, 
                              data=json_data, 
                              encoding='utf-8', 
                              overwrite=True)
            # --- FIN DE LA CORRECTION ---
            
            print(f"Sauvegarde HDFS terminée.")
            
        except Exception as e:
            print(f"Échec de la sauvegarde JSON sur HDFS : {e}")
            raise

    # -----------------------------------------------------------------
    #  Orchestration (Dynamic Mapping)
    # -----------------------------------------------------------------
    
    # 1. Obtenir la liste des fichiers
    file_list = get_hdfs_files()
    
    # 2. MAP : Appliquer 'parse_html_task' à chaque fichier de la liste
    parsed_data = parse_html_task.expand(filename=file_list)
    
    # 3. REDUCE : Attendre que tout soit fini et sauvegarder
    save_results_to_hdfs(parsed_data)


# Instanciation du DAG
kbo_parsing_hdfs_pipeline()