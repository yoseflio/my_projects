from datetime import datetime
from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator

@dag(dag_id="wiki_stream", start_date=datetime(2026, 1, 1), schedule=None, catchup=False)
def wiki_stream_dag():
    BashOperator(
        task_id="run_wiki_stream",
    )   bash_command="python /opt/airflow/dags/wiki_stream.py",

wiki_stream_dag()