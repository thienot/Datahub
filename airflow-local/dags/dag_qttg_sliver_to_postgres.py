from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


# Lấy đúng tên từ lệnh: docker ps --format "table {{.Names}}"
SPARK_CONTAINER = "spark-master"
SPARK_MASTER = "spark://spark-master:7077"
APP_DIR = "/opt/spark/apps/qttg"


def spark_submit(app_name: str, app_args: str = "") -> str:
    return f"""
        set -e
        docker exec {SPARK_CONTAINER} \
        /opt/spark/bin/spark-submit \
        --master {SPARK_MASTER} \
        --deploy-mode client \
        --driver-memory 1g \
        --executor-memory 2g \
        --executor-cores 2 \
        --jars /opt/spark/jars/postgresql-42.7.3.jar \
        {APP_DIR}/{app_name} {app_args}
        """.strip()


default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="qttg_etl_postgres",
    description=(
        "Đọc Silver layer (master/detail) của QTTG và ghi vào "
        "postgres-dwh (qttg_header, qttg_detail)"
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["spark", "qttg", "postgres", "local"],
) as dag:
    etl_to_postgres = BashOperator(
        task_id="etl_silver_to_postgres",
        bash_command=spark_submit("etl_qttg_to_postgres.py"),
        execution_timeout=timedelta(hours=1),
    )

    etl_to_postgres