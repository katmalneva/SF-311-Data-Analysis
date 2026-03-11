from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

DAG_DIR = os.path.dirname(os.path.abspath(__file__))

with DAG(
    dag_id="sf_311_pipeline", 
    start_date=datetime(2026,3,10),
    schedule_interval="@daily", 
    catchup=False
) as dag:
    
    extract_load = BashOperator(
        task_id="extract_load",
        bash_command=f"python {DAG_DIR}/api/api_311.py"
    )

    aggregate = BashOperator(
        task_id="aggregate",
        bash_command=f"python {DAG_DIR}/aggregations.py"
    )

    map_aggregate = BashOperator(
        task_id="map_aggregate",
        bash_command=f"python {DAG_DIR}/map_aggs.py"
    )

    feature_eng = BashOperator(
        task_id="feature_eng",
        bash_command=f"python {DAG_DIR}/spark/feature_transform_cloud.py"
    )

    train = BashOperator(
        task_id="train",
        bash_command=f"python {DAG_DIR}/spark/train_model_cloud.py"
    )

    extract_load >> [aggregate, map_aggregate] >> feature_eng >> train