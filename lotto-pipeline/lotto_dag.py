import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.secret import Secret
from kubernetes.client import models as k8s
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

# Database connection details
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


with DAG(
    "tue_winning_numbers",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'on_skipped_callback': another_function, #or list of functions
        # 'trigger_rule': 'all_success'
    },
    # [END default_args]
    description="Check OzLotto wining numbers and create image + update db",
    schedule=timedelta(days=7),
    # schedule_interval="@daily",
    start_date=datetime(2006, 2, 7),
    catchup=True,
    tags=["ozlotto"],
) as dag:

    # docs for the operator: https://github.com/apache/airflow/blob/main/providers/src/airflow/providers/cncf/kubernetes/operators/pod.py
    fetch_draw_result = KubernetesPodOperator(
        task_id="fetch_draw_result",
        name="fetch_draw_result",
        namespace="airflow",
        image="kiarashz/fetch-lot:latest",
        is_delete_operator_pod=True,
        in_cluster=True,
        cmds=["python", "app.py"],
        arguments=["--logical_date", "{{ ds }}"],
        get_logs=True,
        do_xcom_push=True,
    )

    some_env_variables = [k8s.V1EnvVar(name="key1", value="value1")]
    save_draw_result_in_db_task = KubernetesPodOperator(
        task_id="insert_draw_result_2_postgres",
        name="save_draw_result_in_db",
        namespace="airflow",
        image="kiarashz/insert-db-record:latest",
        secrets=[db_host, db_name, db_user, db_password],
        is_delete_operator_pod=True,
        in_cluster=True,
        get_logs=True,
        env_vars=some_env_variables,
        cmds=["python", "app.py"],
        arguments=[
            "--draw_object",
            "{{ task_instance.xcom_pull(task_ids='fetch_draw_result') }}",
        ],
    )

    fetch_draw_result >> save_draw_result_in_db_task
