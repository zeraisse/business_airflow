from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.variable import Variable

# Importe la logique métier de notre nouveau service
from scraper.proxy_manager import get_and_test_proxies

@dag(
    dag_id="proxy_manager_dag",
    description="DAG pour scraper, tester et mettre à jour la liste des proxies disponibles.",
    start_date=datetime(2024, 1, 1),
    schedule="*/30 * * * *",  # Toutes les 30 minutes
    catchup=False,
    tags=["proxy", "maintenance"],
    max_active_runs=1,
)
def proxy_manager_dag():
    """
    Ce DAG a pour unique but de maintenir une liste de proxies HTTP valides
    stockée dans une Variable Airflow nommée `proxy_list`.
    """

    @task(
        task_id="fetch_and_test_proxies",
        # On utilise un pool pour limiter la concurrence des tests de proxy
        # à 20 workers simultanés, comme demandé.
        pool="proxy_check_pool",
    )
    def _fetch_and_test_proxies() -> list[str]:
        """
        Appelle le service de proxy pour obtenir une liste de proxies valides.
        Le nombre de workers est géré par le pool Airflow.
        """
        # Le nombre de workers est défini dans la fonction, mais le pool Airflow
        # est la garantie que l'on ne dépassera pas 20 tâches en parallèle.
        valid_proxies = get_and_test_proxies(max_workers=20)
        return valid_proxies

    @task(task_id="update_proxy_variable")
    def _update_proxy_variable(proxies: list[str]):
        """
        Met à jour la Variable Airflow `proxy_list` avec les proxies fonctionnels.
        """
        if not proxies:
            print("Aucun proxy valide trouvé. La variable 'proxy_list' n'est pas mise à jour.")
            return
        
        # On stocke la liste sous forme de chaîne de caractères séparée par des virgules.
        Variable.set("proxy_list", ",".join(proxies))
        print(f"{len(proxies)} proxies valides ont été sauvegardés dans la variable 'proxy_list'.")

    # Définition du workflow
    valid_proxies_list = _fetch_and_test_proxies()
    _update_proxy_variable(valid_proxies_list)

proxy_manager_dag()