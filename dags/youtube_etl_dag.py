import os
import sys
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils.api_utils import load_config
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

def ensure_directories():
    directories = ['data/raw', 'data/processed', 'logs']
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

def setup_logging():
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f'pipeline_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

channel_ids = [
    "UCW5YeuERMmlnqo4oq8vwUpg",  # The Net Ninja  
    "UCJskGeByzRRSvmOyZOz61ig",  # Striver_79 (Competitive programming)    
]
        
logger = setup_logging()

def extract_data():
    try:
        logger.info("=" * 60)
        logger.info("Starting YouTube ETL Pipeline")
        logger.info("=" * 60)
        
        # Ensure directories exist
        ensure_directories()
        
        # Load configurations
        logger.info("Loading configuration files...")
        api_config = load_config('dags/config/api_keys.yaml')
        data_types = ["channels", "videos", "comments"]

        logger.info("Phase 1: EXTRACT - Fetching data from YouTube API")
        logger.info(f"Target channels: {len(channel_ids)}")
        logger.info(f"Data types: {', '.join(data_types)}")
        logger.info("=" * 60)
        
        extract(config=api_config, channels=channel_ids, data_types=data_types, max_results=50) 
        
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        logger.error("Please ensure config/api_keys.yaml and config/settings.yaml exist")
        return False

def transform_data():
    logger.info("=" * 60)
    logger.info("Phase 2: TRANSFORM - Processing and cleaning data")
    logger.info("=" * 60)
    
    paths =['data/raw/channels_raw.json', 'data/raw/videos_raw.json', 'data/raw/comments_raw.json']
    transform(channels_path=paths[0], videos_path=paths[1], comments_path=paths[2])
    
    logger.info(f"Successfully transformed data")

def load_data():
    logger.info("=" * 60)
    logger.info("Phase 3: LOAD - Loading data to database")
    logger.info("=" * 60)
    
    file_names = {'channels':'channels_proc.csv',
                'videos':'videos_proc.csv', 
                'comments':'comments_proc.csv'}
    config_path = 'dags/config/settings.yaml'
    schemas = {
    'channels': {
        "channel_id": "VARCHAR(100) PRIMARY KEY",       
        "title": "TEXT",                 
        "description": "TEXT",                    
        "published_at": "TIMESTAMP",              
        "country": "VARCHAR(50)",              
        "custom_url": "TEXT",            
        "subscriber_count": "BIGINT DEFAULT 0",              
        "video_count": "BIGINT DEFAULT 0",                   
        "view_count": "BIGINT DEFAULT 0"
    },

    'videos': {
        'video_id': 'VARCHAR(50) PRIMARY KEY',
        'channel_id': 'VARCHAR(50) NOT NULL',
        'title': 'VARCHAR(500) NOT NULL',
        'description': 'TEXT',
        'channel_title': 'VARCHAR(255)',
        'category_id': 'VARCHAR(50)',
        'published_at': 'TIMESTAMPTZ NOT NULL',
        'created_at': 'TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP',
        'updated_at': 'TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP',
        'view_count': 'BIGINT DEFAULT 0',
        'like_count': 'BIGINT DEFAULT 0',
        'comment_count': 'BIGINT DEFAULT 0',
        'favorite_count': 'BIGINT DEFAULT 0',
        'duration': "VARCHAR(20) DEFAULT 'PT0S'",
        'dimension': "VARCHAR(20) DEFAULT '2d'",
        'definition': "VARCHAR(20) DEFAULT 'sd'",
        'caption': 'BOOLEAN DEFAULT FALSE',
        'licensed_content': 'BOOLEAN DEFAULT FALSE',
        'projection': "VARCHAR(20) DEFAULT 'rectangular'",
        'live_broadcast_content': "VARCHAR(20) DEFAULT 'none'"
    },

    'comments': {
        "comment_id": "VARCHAR(100) PRIMARY KEY",
        "parent_comment_id": "VARCHAR(100) DEFAULT NULL",
        "video_id": "VARCHAR(100)",
        "author_display_name": "TEXT",
        "author_channel_id": "VARCHAR(100)",
        "author_channel_url": "TEXT",
        "text_display": "TEXT",
        "text_original": "TEXT",
        "like_count": "BIGINT DEFAULT 0",
        "total_reply_count": "INT DEFAULT 0"
    }
}

    table_names = ['channels','videos', 'comments']
    load(file_names, config_path, table_names, schemas, data_types=["channels", "videos", "comments"])
        
        
    logger.info(f"Successfully loaded data to database")
        
def get_default_args():
    return {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 2,
    'start_date': datetime(2025, 10, 20),
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30)
}


def create_dag():
    default_arguments = get_default_args()
    with DAG(dag_id='youtube_etl_dag',
    default_args=default_arguments,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1) as dag:
        extract_task = PythonOperator(task_id='extract_data', python_callable=extract_data, dag=dag)
        
        transform_task = PythonOperator(task_id='transform_data', python_callable=transform_data, dag=dag)

        load_task = PythonOperator(task_id='load_data', python_callable=load_data, dag=dag)

        extract_task >> transform_task >> load_task
    
    return dag

dag = create_dag()

