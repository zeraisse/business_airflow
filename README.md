# CMD 
```bash
python -m venv venv 
source ./venv/Scripts/activate
pip install -r requirements_scraper.txt
docker compose up -d
docker compose exec namenode bash
 hdfs dfs -mkdir -p /kbo/html
 hdfs dfs -chown airflow /kbo/html
 hdfs dfs -mkdir -p /kbo/parsed_results
 hdfs dfs -chown airflow /kbo/parsed_results
```
❤️
