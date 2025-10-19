from airflow import DAG
from airflow.operator.python import PythonOperator
from datetime import datetime, timedelta


def default_args():
    return {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['hassanessoufi2004@gmail.com'],
    'retries': 2,
    'start_date': datetime(2025, 10, 20),
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
    'on_failure_callback': notify_failure,
    'on_success_callback': notify_success
}

def create_dag():
    default_arguments = default_args()
    with DAG(dag_i)

