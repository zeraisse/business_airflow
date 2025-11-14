from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

# Code métier du scraper
from scraper.scraper_csv import (
    list_enterprise_numbers_from_csv,
    download_html,
)

# Dans le conteneur, ./data est monté sur /opt/airflow/data
CSV_PATH = "/opt/airflow/data/enterprise.csv"

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
    DAG dynamique :
    1) lit enterprise.csv et trouve les entreprises PAS ENCORE scrapées
    2) sélectionne jusqu'à 20 entreprises
    3) crée une task de scraping par entreprise (dynamic task mapping)
    """

    @task(task_id="read_next_enterprises")
    def _read_next_enterprises(
        max_companies: int = MAX_COMPANIES_PER_RUN,
    ) -> list[str]:
        """
        Lit le CSV, regarde quels HTML existent déjà,
        et renvoie une petite liste (max 20) d'entreprises à scraper.
        """
        # Liste complète des numéros
        all_numbers = list_enterprise_numbers_from_csv(
            csv_path=CSV_PATH,
            max_companies=None,  # on lit tout, on filtrera après
        )

        html_dir = os.path.join("data", "html")

        remaining: list[str] = []
        for number in all_numbers:
            html_path = os.path.join(html_dir, f"{number}.html")
            if not os.path.exists(html_path):
                remaining.append(number)

        # On ne garde que max_companies entreprises pour ce run
        selected = remaining[:max_companies]

        print(f"➡️ {len(selected)} entreprise(s) à scraper sur ce run (max={max_companies}).")
        if remaining:
            print(f"Encore {len(remaining)} entreprise(s) sans HTML au total.")
        else:
            print("Tout est déjà scrapé ✔️")

        return selected

    @task(
        task_id="scrape_one_enterprise",
        # 👉 si plus tard tu ajoutes un pool "scraping_pool", tu pourras mettre: pool="scraping_pool"
    )
    def _scrape_one(number: str):
        """
        Tâche Airflow pour UNE entreprise.
        Si ça plante, seule cette task est en échec.
        """
        print(f"Scraping entreprise: {number}")
        download_html(number)

    # 1) On lit la liste des prochaines entreprises à scraper (0 à 20)
    numbers_list = _read_next_enterprises()

    # 2) On crée dynamiquement une tâche par numéro
    #    → 1 entreprise = 1 task = 1 log, 1 statut
    _scrape_one.expand(number=numbers_list)


scrape_daily_kbo_dynamic_dag = scrape_daily_kbo_dynamic_dag()
