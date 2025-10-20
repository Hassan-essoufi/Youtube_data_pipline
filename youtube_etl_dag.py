from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.extract import extract
from etl.transform import transform 
from etl.load import load


def notify_success(context):
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    message = f"SUCCESS: Task `{task_id}` in DAG `{dag_id}` "f"completed successfully at {execution_date}."
    print(message)

def notify_failure(context):
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    exception = context.get('exception')
    message = (f"FAILURE: Task `{task_id}` in DAG `{dag_id}` failed at {execution_date}.\n "f"Error: {exception}")
    print(message)

def get_default_args():
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
    default_arguments = get_default_args()
    with DAG(dag_id='youtube_pipline',
    default_args=default_arguments,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1) as dag:
        extract_task = PythonOperator(task_id='extract_data', python_callable=extract, dag=dag)
        
        transform_task = PythonOperator(task_id='transform_data', python_callable=transform, dag=dag)

        load_task = PythonOperator(task_id='load_data', python_callable=load, dag=dag)

        extract_task >> transform_task >> load_task
    
    return dag

dag = create_dag()

