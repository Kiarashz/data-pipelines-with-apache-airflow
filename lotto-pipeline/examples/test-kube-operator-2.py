from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator
)
from datetime import datetime
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 12, 1),
    "retries": 1,
}

with DAG(
    dag_id="kubernetes_postgres_task_2",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    db_user = Secret(
        deploy_type="env",
        deploy_target="POSTGRES_USER",
        secret="postgres-credentials",
        key="POSTGRES_USER",
    )

    db_name = Secret(
        deploy_type="env",
        deploy_target="POSTGRES_DB",
        secret="postgres-credentials",
        key="POSTGRES_DB",
    )

    db_host = Secret(
        deploy_type="env",
        deploy_target="POSTGRES_HOST",
        secret="postgres-credentials",
        key="POSTGRES_HOST",
    )

    db_password = Secret(
        deploy_type="env",
        deploy_target="POSTGRES_PASSWORD",
        secret="postgres-credentials",
        key="POSTGRES_PASSWORD",
    )

    some_env_vars = [
        k8s.V1EnvVar(name="LOGICAL_DATE", value="{{ logical_date }}"),
        k8s.V1EnvVar(name="AIRFLOW_DS", value="{{ ds }}"),
        k8s.V1EnvVar(name="AIRFLOW_DS_NO_DASH", value="{{ ds_nodash }}"),
    ]

    curl_task_2 = KubernetesPodOperator(
        task_id="insert_postgres_record",
        name="insert-employee-task",
        namespace="airflow",
        image="kiarashz/pysert:latest",
        secrets=[db_host, db_name, db_user, db_password],
        is_delete_operator_pod=True,
        in_cluster=True,
        cmds=["env"],
        get_logs=True,
        env_vars=some_env_vars,
    )

    curl_task_2
