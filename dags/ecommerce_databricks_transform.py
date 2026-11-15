"""
PROJEKT ZALICZENIOWY — Extra B: DatabricksSubmitRunOperator
===========================================================
Autor: Aleksander Ortonowski

Wariant transformacji w Databricks (stub). To osobny DAG, uruchamiany tylko
ręcznie (schedule=None), żeby główny `ecommerce_daily_pipeline` dalej działał
lokalnie bez klastra i bez Connection `databricks_default`.

Notebook `/Shared/ecommerce/transform` dostaje datę przetwarzania jako parametr
i liczy w Sparku to samo co transform_group (sales_daily + RFM).

Wymaga:
    Connection databricks_default (host = workspace URL, password = PAT)
    Variable DATABRICKS_SPARK_VERSION / DATABRICKS_NODE_TYPE (opcjonalnie)
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

log = logging.getLogger(__name__)

DATABRICKS_CONN_ID = "databricks_default"
SPARK_VERSION = Variable.get("DATABRICKS_SPARK_VERSION", default_var="14.3.x-scala2.12")
NODE_TYPE = Variable.get("DATABRICKS_NODE_TYPE", default_var="Standard_DS3_v2")
NOTEBOOK_PATH = Variable.get("DATABRICKS_TRANSFORM_NOTEBOOK", default_var="/Shared/ecommerce/transform")

DEFAULT_ARGS = {
    "owner": "aleksander-ortonowski",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="ecommerce_databricks_transform",
    description="Extra B — transformacja sales_daily + RFM w Databricks (stub)",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "zaliczenie", "miesiac-4", "databricks", "extra"],
) as dag:

    transform_databricks = DatabricksSubmitRunOperator(
        task_id="transform_databricks",
        databricks_conn_id=DATABRICKS_CONN_ID,
        new_cluster={
            "spark_version": SPARK_VERSION,
            "node_type_id": NODE_TYPE,
            "num_workers": 2,
        },
        notebook_task={
            "notebook_path": NOTEBOOK_PATH,
            "base_parameters": {
                "execution_date": "{{ ds }}",
                # dzień przetwarzania — spójnie z głównym DAG (execution_date - 1)
                "target_date": "{{ macros.ds_add(ds, -1) }}",
            },
        },
        timeout_seconds=60 * 60,
        polling_period_seconds=30,
    )
