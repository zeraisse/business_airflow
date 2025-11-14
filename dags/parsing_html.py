from airflow.sdk import dag, task
from datetime import datetime
from parser.kbo_html_parser import ParsingHTML
import os
from pathlib import Path



@dag(
    dag_id="parsing_html_dag_v1", 
    start_date=datetime(2025, 1, 1), 
    schedule=None,
    catchup=False
)
def parsing_html_dag():
    """DAG pour parser les fichiers HTML téléchargés depuis le KBO."""
    
    @task
    def parse_html_task(file_path: str) -> dict:
        """
        Lit un fichier HTML et le parse pour extraire les données de l'entreprise.
        """
        print(f"Début du parsing pour : {file_path}")
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = file.read()
            
            parsed_data = ParsingHTML.parse_kbo_html(content)
            
            print(f"Parsing réussi pour {file_path}. Données extraites : {parsed_data}")
            return parsed_data
            
        except Exception as e:
            print(f"Erreur lors du traitement du fichier {file_path}: {e}")
            raise

    
    # -----------------------------------------------------------------
    #  Exécution (statique pour le test)
    # TODO : remplacer par du dynamic task mapping plus tard
    # -----------------------------------------------------------------
    AIRFLOW_PROJECT_ROOT = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
    TEST_FILE_PATH = str(AIRFLOW_PROJECT_ROOT / "data" / "html" / "200065765.html")
    
    parse_html_task(file_path=TEST_FILE_PATH)

# Instanciation du DAG (nécessaire pour qu'Airflow le détecte)
parsing_html_dag()