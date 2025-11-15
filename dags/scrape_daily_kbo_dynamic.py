from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.exceptions import AirflowFailException
# Code métier du scraper
from scraper.scraper_csv import (
    list_enterprise_numbers_from_csv,
)
# On importe la fonction de téléchargement depuis le bon module
from scraper.scraper_basic import download_html


# Dans le conteneur, ./data est monté sur /opt/airflow/data
AIRFLOW_DATA_PATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
CSV_PATH = os.path.join(AIRFLOW_DATA_PATH, "data", "enterprise.csv")

# 👉 On veut traiter jusqu'à 20 entreprises par run
MAX_COMPANIES_PER_RUN = 20

default_args = {
    "owner": "faycal",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="scrape_daily_kbo_dynamic",
    description="Scraping KBO avec tâches dynamiques (plusieurs entreprises par run, 1 entreprise = 1 task)",
    start_date=datetime(2025, 11, 13),
    # 👉 on exécute le DAG toutes les minutes
    schedule="* * * * *",
    catchup=False,
    default_args=default_args,
    tags=["kbo", "scraping", "dynamic"],
    max_active_runs=1,  # 1 run à la fois, mais plusieurs tasks en parallèle dans ce run
)
def scrape_daily_kbo_dynamic_dag():
    """
    DAG dynamique pour scraper les données KBO.
    1. Récupère la liste des proxies valides (préparée par `proxy_manager_dag`).
    2. Lit `enterprise.csv` pour trouver les entreprises à scraper.
    3. Crée une tâche de scraping par entreprise, en lui assignant un proxy.
    """

    @task(task_id="get_available_proxies")
    def _get_available_proxies() -> list[str]:
        """
        Récupère la liste des proxies depuis la Variable Airflow `proxy_list`.
        Cette variable est maintenue par le DAG `proxy_manager_dag`.
        """
        # On récupère la liste de proxies, qui a déjà été testée et validée.
        proxies_str = Variable.get("proxy_list", default_var=None)
        
        if not proxies_str:
            raise AirflowFailException("La variable 'proxy_list' est vide ou n'existe pas. Exécutez d'abord 'proxy_manager_dag'.")

        all_proxies = [p.strip() for p in proxies_str.split(',')]

        # La logique de quarantaine est maintenant dans le proxy_manager.
        # Ici, on utilise simplement ce qui est disponible.
        if not all_proxies:
            # Si aucun proxy n'est dispo, on fait échouer le DAG pour ne pas scraper sans.
            raise AirflowFailException("Aucun proxy disponible, arrêt du DAG.")

        print(f"{len(all_proxies)} proxies valides récupérés.")
        return all_proxies

    @task(task_id="read_next_enterprises")
    def _read_next_enterprises(
        max_companies: int = MAX_COMPANIES_PER_RUN,
    ) -> list[str]:
        """
        Lit le CSV, regarde quels HTML existent déjà,
        et renvoie une liste d'entreprises à scraper.
        """
        # Liste complète des numéros
        all_numbers = list_enterprise_numbers_from_csv(
            csv_path=CSV_PATH,
            max_companies=None,  # on lit tout, on filtrera après
        )

        # Le chemin est relatif à la racine du projet Airflow dans le conteneur
        html_dir = os.path.join(AIRFLOW_DATA_PATH, "data", "html")
        if not os.path.exists(html_dir):
            os.makedirs(html_dir)

        remaining: list[str] = []
        for number in all_numbers:
            html_path = os.path.join(html_dir, f"{number}.html")
            if not os.path.exists(html_path):
                remaining.append(number)
            if len(remaining) >= max_companies:
                break

        print(f"➡️ {len(remaining)} entreprise(s) à scraper sur ce run (max={max_companies}).")
        if not remaining:
            print("Toutes les entreprises semblent déjà scrapées. ✔️")
        else:
            print(f"Entreprises sélectionnées : {remaining}")
        return remaining

    @task(
        task_id="scrape_one_enterprise",
        retries=2, # On peut retenter avec un autre proxy
        # 👉 Pour limiter le nombre de scrapings simultanés, créez un Pool "scraping_pool"
        # dans l'UI Airflow (Admin -> Pools) et décommentez la ligne suivante.
        pool="scraping_pool",
    )
    def _scrape_one(number: str, proxy: str):
        """
        Tâche Airflow pour UNE entreprise.
        Si ça plante, seule cette task est en échec.
        Un proxy différent est utilisé pour chaque tentative.
        """
        try:
            print(f"Scraping entreprise: {number} avec proxy {proxy}")
            download_html(number, proxy=proxy)
        except Exception as e: # On capture une exception plus large
            print(f"Échec du scraping pour {number} avec proxy {proxy}. Erreur: {e}")
            # La logique de quarantaine est gérée par le `proxy_manager_dag`.
            # Si un proxy échoue ici, il sera probablement détecté comme invalide
            # lors du prochain run du `proxy_manager_dag` et sera retiré de la liste.
            # On pourrait aussi implémenter une quarantaine "en temps réel" avec Redis ici.
            raise # Fait échouer la tâche pour qu'Airflow la retente.

    # 1) On récupère la liste des proxies et des entreprises
    proxy_list = _get_available_proxies()
    numbers_list = _read_next_enterprises(max_companies=MAX_COMPANIES_PER_RUN)

    # 2) On prépare les arguments pour le mapping dynamique.
    #    On veut une paire (entreprise, proxy) pour chaque tâche.
    #    On s'assure de ne pas créer plus de tâches qu'on a de proxies ou d'entreprises.
    @task
    def map_arguments(numbers, proxies):
        import itertools
        num_tasks = min(len(numbers), len(proxies))
        # On crée un cycle sur la liste des proxies si on a plus d'entreprises que de proxies
        proxy_cycle = itertools.cycle(proxies)
        # On retourne un dictionnaire que .expand_kwargs peut utiliser
        return [{"number": num, "proxy": next(proxy_cycle)} for num in numbers[:num_tasks]]

    # 3) On crée dynamiquement une tâche pour chaque paire (entreprise, proxy)
    _scrape_one.expand_kwargs(map_arguments(numbers_list, proxy_list))

scrape_daily_kbo_dynamic_dag()
