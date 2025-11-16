import os
import requests
from hdfs import InsecureClient

BASE_URL = "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?lang=fr&ondernemingsnummer="

HDFS_BASE_DIR = "/kbo/html"
CLIENT_HDFS = InsecureClient('http://namenode:9870', user='airflow')

def build_kbo_url(number: str) -> str:
    """
    Construit l'URL KBO à partir d'un numéro d'entreprise.
    """
    return f"{BASE_URL}{number}"

def download_html(number: str, proxy: str | None = None) -> None:
    """
    Télécharge la page HTML pour un numéro d'entreprise
    et la sauvegarde dans HDFS kbo/html/<numero>.html  <-- On garde le docstring de 'main'
    Utilise un proxy si fourni.                     <-- On garde ce commentaire de 'anas'
    """
    url = build_kbo_url(number)
    proxy_info = f"via proxy {proxy}" if proxy else "sans proxy"
    print(f"➡️  Téléchargement de : {url} ({proxy_info})")

    proxies = {"http": proxy, "https": proxy} if proxy else None

    try:
        response = requests.get(url, timeout=15, proxies=proxies)
    except Exception as e:
        print(f"❌ Erreur de requête pour {number} : {e}")
        return

    if response.status_code != 200:
        print(f"❌ Erreur HTTP {response.status_code} pour {number}")
        return

    # 4.logique d'écriture HDFS

    hdfs_filepath = f"{HDFS_BASE_DIR}/{number}.html"

    try:
        with CLIENT_HDFS.write(hdfs_filepath, encoding="utf-8", overwrite=True) as writer:
            writer.write(response.text)
        
        print(f"✅ Fichier sauvegardé dans HDFS : {hdfs_filepath}")
    except Exception as e:
        print(f"❌ Erreur lors de l'écriture HDFS pour {hdfs_filepath} : {e}")

if __name__ == "__main__":
    print("Ce script doit être lancé via Airflow pour accéder à HDFS.")
    pass