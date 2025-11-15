import os
import logging
import requests
import pandas as pd
from hdfs import InsecureClient


# URL en FR pour faciliter le parsing plus tard
BASE_URL = (
    "https://kbopub.economie.fgov.be/kbopub/"
    "toonondernemingps.html?lang=fr&ondernemingsnummer="
)
HDFS_BASE_DIR = "/kbo/html"
CLIENT_HDFS = InsecureClient('http://namenode:9870', user='airflow')

# --- CONFIG LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def normalize_enterprise_number(raw_number: str) -> str:
    """
    Normalise un numéro d'entreprise provenant du CSV :
    - enlève les guillemets et espaces
    - enlève les points du style 0200.065.765
    - enlève les zéros en trop au début (leading zeros)

    Exemple : "0200.065.765" -> "200065765"
    """
    if pd.isna(raw_number):
        return ""

    num = str(raw_number).strip().replace('"', '')

    # enlever les points
    num = num.replace(".", "")

    # enlever les zéros au début
    num = num.lstrip("0")

    return num


def build_kbo_url(number: str) -> str:
    """
    Construit l'URL KBO à partir d'un numéro d'entreprise.
    """
    return f"{BASE_URL}{number}"


def download_html(number: str) -> None:
    """
    Télécharge la page HTML pour UN numéro d'entreprise
    et la sauvegarde dans data/html/<numero>.html

    Utilisable depuis :
    - un script Python
    - une tâche Airflow dynamique (une task par entreprise)
    """
    # output_dir = os.path.join("data", "html")
    # os.makedirs(output_dir, exist_ok=True)

    # filepath = os.path.join(output_dir, f"{number}.html")

    # ✅ Ne pas retélécharger si on l'a déjà
    # if os.path.exists(filepath):
    #     logger.info(f"[SKIP] HTML déjà présent pour {number}")
    #     return

    url = build_kbo_url(number)
    logger.info(f"Téléchargement de : {url}")

    try:
        response = requests.get(url, timeout=10)
    except Exception as e:
        logger.error(f"Erreur de requête pour {number} : {e}")
        return

    if response.status_code != 200:
        logger.error(f"Erreur HTTP {response.status_code} pour {number} (URL: {url})")
        return

    # with open(filepath, "w", encoding="utf-8") as f:
    #     f.write(response.text)

    # logger.info(f"Fichier sauvegardé : {filepath}")
    hdfs_filepath = f"{HDFS_BASE_DIR}/{number}.html"

    try:
        with CLIENT_HDFS.write(hdfs_filepath, encoding="utf-8", overwrite=True) as writer:
            writer.write(response.text)
        
        print(f"✅ Fichier sauvegardé dans HDFS : {hdfs_filepath}")
    except Exception as e:
        print(f"❌ Erreur lors de l'écriture HDFS pour {hdfs_filepath} : {e}")
        # Vous pouvez 'raise e' ici si vous voulez que la tâche Airflow
        # échoue en cas d'erreur d'écriture HDFS.



def list_enterprise_numbers_from_csv(
    csv_path: str,
    max_companies: int | None = None,
) -> list[str]:
    """
    Lit le fichier enterprise.csv contenant une colonne 'EnterpriseNumber'
    et retourne une LISTE de numéros normalisés (strings).

    Adapté pour :
    - la génération dynamique de tâches Airflow (dynamic task mapping)
    - ou un traitement batch simple.

    max_companies : si défini, limite le nombre d'entreprises à traiter.
    """
    df = pd.read_csv(csv_path, sep=",")

    if max_companies is not None:
        df = df.head(max_companies)
        logger.info(f"Limitation à {max_companies} entreprises pour ce run.")

    numbers: list[str] = []

    for raw_number in df["EnterpriseNumber"]:
        normalized = normalize_enterprise_number(raw_number)
        if normalized:
            numbers.append(normalized)

    logger.info(f"{len(numbers)} numéros d'entreprises chargés depuis {csv_path}")
    return numbers


def download_from_csv(csv_path: str, max_companies: int | None = None) -> None:
    """
    Version 'batch' simple : lit le CSV et appelle download_html()
    pour chaque numéro.
    """
    numbers = list_enterprise_numbers_from_csv(csv_path, max_companies=max_companies)
    for number in numbers:
        download_html(number)


def run_scraping_from_csv(max_companies: int | None = None):
    """
    Point d'entrée principal pour :
    - exécution directe : python scraper_csv.py
    - DAG Airflow 'scrape_daily_kbo' (version non dynamique)

    Dans le conteneur Airflow, le répertoire de travail est /opt/airflow,
    donc 'data/enterprise.csv' => /opt/airflow/data/enterprise.csv.
    """
    csv_file = os.path.join("data", "enterprise.csv")
    logger.info(f"Lancement du scraping KBO depuis {csv_file}")
    download_from_csv(csv_file, max_companies=max_companies)


if __name__ == "__main__":
    # En mode script direct : on limite pour ne pas exploser le site
    run_scraping_from_csv(max_companies=100)
