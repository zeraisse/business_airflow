from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Import de ton scraper
from scraper.scraper_csv import run_scraping_from_csv

default_args = {
    "owner": "faycal",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="scrape_daily_kbo",
    description="Scraping quotidien KBO depuis enterprise.csv",
    default_args=default_args,
    schedule="0 6 * * *",  # ✅ Airflow 3 : 'schedule' et plus 'schedule_interval'
    start_date=datetime(2025, 11, 13),
    catchup=False,
    tags=["kbo", "scraping"],
) as dag:

    def _scrape():
        # Pour l’instant, on limite pour ne pas exploser le site
        run_scraping_from_csv(max_companies=100)

    run_kbo_scraping = PythonOperator(
        task_id="run_kbo_scraping",
        python_callable=_scrape,
    )
