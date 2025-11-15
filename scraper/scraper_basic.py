import os
import requests
from hdfs import InsecureClient

BASE_URL = "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer="
HDFS_BASE_DIR = "/kbo/html"
CLIENT_HDFS = InsecureClient('http://namenode:9870', user='airflow')
def build_kbo_url(number: str) -> str:
    """
    Construit l'URL KBO à partir d'un numéro d'entreprise.
    """
    return f"{BASE_URL}{number}"

def download_html(number: str) -> None:
    """
    Télécharge la page HTML pour un numéro d'entreprise
    et la sauvegarde dans HDFS kbo/html/<numero>.html
    """
    url = build_kbo_url(number)
    print(f"➡️  Téléchargement de : {url}")

    try:
        response = requests.get(url, timeout=10)
    except Exception as e:
        print(f"❌ Erreur de requête pour {number} : {e}")
        return

    if response.status_code != 200:
        print(f"❌ Erreur HTTP {response.status_code} pour {number}")
        return

    # S'assurer que le dossier existe
    #os.makedirs(os.path.join("data", "html"), exist_ok=True)

    #filepath = os.path.join("data", "html", f"{number}.html")

    # with open(filepath, "w", encoding="utf-8") as f:
    #     f.write(response.text)
    hdfs_filepath = f"{HDFS_BASE_DIR}/{number}.html"

    try:
        with CLIENT_HDFS.write(hdfs_filepath, encoding="utf-8", overwrite=True) as writer:
            writer.write(response.text)
        
        print(f"✅ Fichier sauvegardé dans HDFS : {hdfs_filepath}")
    except Exception as e:
        print(f"❌ Erreur lors de l'écriture HDFS pour {hdfs_filepath} : {e}")
        # Vous pouvez 'raise e' ici si vous voulez que la tâche Airflow
        # échoue en cas d'erreur d'écriture HDFS.

if __name__ == "__main__":
    print("Ce script doit être lancé via Airflow pour accéder à HDFS.")
    pass
