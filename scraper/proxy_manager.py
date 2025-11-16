import requests
from bs4 import BeautifulSoup
import logging
import concurrent.futures

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# URL de test fiable qui retourne l'IP de l'appelant
TEST_URL = "https://httpbin.org/ip"
REQUEST_TIMEOUT = 10


def scrape_from_free_proxy_list() -> set[str]:
    """Scrape les proxies depuis free-proxy-list.net."""
    url = "https://free-proxy-list.net/"
    proxies = set()
    try:
        logger.info(f"Scraping proxies from {url}")
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # La structure a peut-être changé, on cible la table directement
        table = soup.find("table", class_="table-striped")
        if not table:
            logger.warning("Could not find the proxy table on free-proxy-list.net")
            return proxies

        for row in table.find("tbody").find_all("tr"):
            cells = row.find_all("td")
            if len(cells) > 1:
                ip = cells[0].text.strip()
                port = cells[1].text.strip()
                # On ne garde que les proxies "elite" ou "anonymous"
                anonymity = cells[4].text.strip()
                if ip and port and anonymity in ["anonymous", "elite proxy"]:
                    proxies.add(f"{ip}:{port}")
    except requests.RequestException as e:
        logger.error(f"Failed to scrape {url}: {e}")
    return proxies


def scrape_from_proxyscrape() -> set[str]:
    """Scrape les proxies depuis proxyscrape.com."""
    url = "https://api.proxyscrape.com/v2/?request=getproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"
    proxies = set()
    try:
        logger.info(f"Scraping proxies from {url}")
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        proxies.update(response.text.strip().split("\r\n"))
    except requests.RequestException as e:
        logger.error(f"Failed to scrape {url}: {e}")
    return proxies


def check_proxy(proxy: str) -> str | None:
    """
    Vérifie si un proxy est fonctionnel en l'utilisant pour appeler TEST_URL.
    Retourne le proxy s'il est valide, sinon None.
    """
    try:
        response = requests.get(
            TEST_URL,
            proxies={"http": f"http://{proxy}", "https": f"http://{proxy}"},
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        # On vérifie que la réponse est bien un JSON et contient notre IP
        result_ip = response.json().get("origin")
        logger.info(f"✅ Proxy {proxy} is valid. Reported IP: {result_ip}")
        return proxy
    except (requests.RequestException, ValueError) as e:
        logger.warning(f"❌ Proxy {proxy} failed check: {e}")
        return None


def get_and_test_proxies(max_workers: int = 20) -> list[str]:
    """
    Fonction principale : scrape les sources et teste les proxies en parallèle.
    """
    logger.info("--- Starting proxy discovery ---")
    scraped_proxies = scrape_from_free_proxy_list()
    scraped_proxies.update(scrape_from_proxyscrape())
    
    logger.info(f"Found {len(scraped_proxies)} unique proxies. Now testing...")
    
    valid_proxies = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_proxy = {executor.submit(check_proxy, proxy): proxy for proxy in scraped_proxies}
        for future in concurrent.futures.as_completed(future_to_proxy):
            result = future.result()
            if result:
                valid_proxies.append(result)
    
    logger.info(f"--- Found {len(valid_proxies)} valid proxies ---")
    return valid_proxies