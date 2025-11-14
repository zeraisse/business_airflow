import os
import requests

BASE_URL = "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer="

def build_kbo_url(number: str) -> str:
    """
    Construit l'URL KBO à partir d'un numéro d'entreprise.
    """
    return f"{BASE_URL}{number}"

def download_html(number: str) -> None:
    """
    Télécharge la page HTML pour un numéro d'entreprise
    et la sauvegarde dans data/html/<numero>.html
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
    os.makedirs(os.path.join("data", "html"), exist_ok=True)

    filepath = os.path.join("data", "html", f"{number}.html")

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(response.text)

    print(f"✅ Fichier sauvegardé : {filepath}")

if __name__ == "__main__":
    # Tu peux changer ce numéro si tu veux
    numero_test = "0203430576"
    download_html(numero_test)
