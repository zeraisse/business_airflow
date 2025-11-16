from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.exceptions import AirflowFailException

from hdfs import InsecureClient
HDFS_BASE_DIR = "/kbo/html"
CLIENT_HDFS = InsecureClient('http://namenode:9870', user='airflow')

from scraper.scraper_csv import (
    list_enterprise_numbers_from_csv,
)
from scraper.scraper_basic import download_html


AIRFLOW_DATA_PATH = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
CSV_PATH = os.path.join(AIRFLOW_DATA_PATH, "data", "enterprise.csv")

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
    schedule="* * * * *",
    catchup=False,
    default_args=default_args,
    tags=["kbo", "scraping", "dynamic"],
    max_active_runs=1,
)
def scrape_daily_kbo_dynamic_dag():
    """
    DAG dynamique pour scraper les données KBO.
    1. Récupère la liste des proxies valides.
    2. Lit `enterprise.csv` et compare avec HDFS pour trouver les entreprises à scraper.
    3. Crée une tâche de scraping par entreprise, en lui assignant un proxy.
    """

    @task(task_id="get_available_proxies")
    def _get_available_proxies() -> list[str]:
        """
        Récupère la liste des proxies depuis la Variable Airflow `proxy_list`.
        """
        proxies_str = Variable.get("proxy_list", default_var=None)
        
        if not proxies_str:
            raise AirflowFailException("La variable 'proxy_list' est vide ou n'existe pas. Exécutez d'abord 'proxy_manager_dag'.")

        all_proxies = [p.strip() for p in proxies_str.split(',')]

        if not all_proxies:
            raise AirflowFailException("Aucun proxy disponible, arrêt du DAG.")

        print(f"{len(all_proxies)} proxies valides récupérés.")
        return all_proxies

    @task(task_id="read_next_enterprises")
    def _read_next_enterprises(
        max_companies: int = MAX_COMPANIES_PER_RUN,
    ) -> list[str]:
        """
        Lit le CSV, regarde quels HTML existent déjà sur HDFS,
        et renvoie une liste d'entreprises à scraper.
        """
        # 1. Liste complète des numéros du CSV
        #    On utilise un set() pour une comparaison rapide
        all_numbers_in_csv = set(list_enterprise_numbers_from_csv(
            csv_path=CSV_PATH,
            max_companies=None,  # on lit tout, on filtrera après
        ))

        # 2. Liste des fichiers déjà sur HDFS
        try:
            files_in_hdfs = CLIENT_HDFS.list(HDFS_BASE_DIR)
        except Exception as e:
            print(f"AVERTISSEMENT : Impossible de lister HDFS ({e}). On suppose que tout est à scraper.")
            files_in_hdfs = []

        # On garde juste le numéro, sans le '.html'
        existing_numbers_in_hdfs = {f.replace('.html', '') for f in files_in_hdfs}

        # 3. Calculer la différence
        remaining_to_scrape = list(all_numbers_in_csv - existing_numbers_in_hdfs)
        
        # 4. Limiter au maximum pour ce run
        to_scrape_this_run = remaining_to_scrape[:max_companies]

        print(f"➡️ {len(to_scrape_this_run)} entreprise(s) à scraper sur ce run (max={max_companies}).")
        if not to_scrape_this_run:
            print("Toutes les entreprises semblent déjà scrapées sur HDFS. ✔️")
        else:
            print(f"Entreprises sélectionnées : {to_scrape_this_run}")
        return to_scrape_this_run

    @task(
        task_id="scrape_one_enterprise",
        retries=2, 
        pool="scraping_pool",
    )
    def _scrape_one(number: str, proxy: str):
        """
        Tâche Airflow pour UNE entreprise.
        """
        try:
            print(f"Scraping entreprise: {number} avec proxy {proxy}")
            download_html(number, proxy=proxy)
        except Exception as e:
            print(f"Échec du scraping pour {number} avec proxy {proxy}. Erreur: {e}")
            raise 

    proxy_list = _get_available_proxies()
    numbers_list = _read_next_enterprises(max_companies=MAX_COMPANIES_PER_RUN)

    @task
    def map_arguments(numbers, proxies):
        import itertools
        
        if not numbers:
            print("Aucune entreprise à scraper.")
            return [] # Doit retourner une liste vide pour que le expand ne plante pas
        
        if not proxies:
            print("Aucun proxy disponible. Scraping sans proxy.")
            proxies = [None] * len(numbers)

        # On crée un cycle sur la liste des proxies si on a plus d'entreprises que de proxies
        proxy_cycle = itertools.cycle(proxies)
        
        return [{"number": num, "proxy": next(proxy_cycle)} for num in numbers]

    mapped_args = map_arguments(numbers_list, proxy_list)
    
    # On vérifie que mapped_args n'est pas vide avant de lancer expand_kwargs
    if mapped_args:
        _scrape_one.expand_kwargs(mapped_args)

scrape_daily_kbo_dynamic_dag()