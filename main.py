
import os
import sys
import logging
from datetime import datetime


from etl.extract import extract
from etl.transform import transform 
from etl.load import load
from utils.api_utils import load_config


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


def ensure_directories():
    directories = ['data/raw', 'data/processed', 'logs']
    for directory in directories:
        os.makedirs(directory, exist_ok=True)


def run_etl_pipeline(channel_ids=None, data_types=None, max_results=50):
    logger = setup_logging()
    
    try:
        logger.info("=" * 60)
        logger.info("Starting YouTube ETL Pipeline")
        logger.info("=" * 60)
        
        # Ensure directories exist
        ensure_directories()
        
        # Load configurations
        logger.info("Loading configuration files...")
        api_config = load_config('config/api_keys.yaml')
        if channel_ids is None:
            channel_ids = [
                "UC_x5XG1OV2P6uZZ5FSM9Ttw",  # Google Developers
                "UC29ju8bIPH5as8OGnQzwJyA",  # Traversy Media
                "UC8butISFwT-Wl7EV0hUK0BQ",  # freeCodeCamp.org
                "UCYO_jab_esuFRV4b17AJtAw",  # 3Blue1Brown
                "UCWv7vMbMWH4-V0ZXdmDpPBA"   # Programming with Mosh
            ]
        
        if data_types is None:
            data_types = ["channels", "videos", "comments"]
        
        # EXTRACT Phase
        logger.info("=" * 60)
        logger.info("Phase 1: EXTRACT - Fetching data from YouTube API")
        logger.info(f"Target channels: {len(channel_ids)}")
        logger.info(f"Data types: {', '.join(data_types)}")
        logger.info("=" * 60)
        
        extract(config=api_config, channels=channel_ids, data_types=data_types, max_results=max_results) 
        
        logger.info(f"Successfully extracted data")
        
        # TRANSFORM Phase
        logger.info("=" * 60)
        logger.info("Phase 2: TRANSFORM - Processing and cleaning data")
        logger.info("=" * 60)
        
        paths =['data/raw/channels_raw.json', 'data/raw/videos_raw.json', 'data/raw/comments_raw.json']
        transform(channels_path=paths[0], videos_path=paths[1], comments_path=paths[2])
        
        logger.info(f"Successfully transformed data")
        
        # LOAD Phase
        logger.info("=" * 60)
        logger.info("Phase 3: LOAD - Loading data to database")
        logger.info("=" * 60)
        
        file_names = {'channels':'channels_proc.csv',
                    'videos':'videos_proc.csv', 
                    'comments':'comments_proc.csv'}
        config_path = 'config/settings.yaml'
        schemas = {
            'channels':{
                "channel_id": "VARCHAR(100)",        
                "title": "TEXT",             
                "description": "TEXT",               
                "published_at": "TIMESTAMP",          
                "country": "VARCHAR(50)",          
                "custom_url": "TEXT",        
                "subscriber_count": "BIGINT",          
                "video_count": "BIGINT",               
                "view_count": "BIGINT"  },

            'videos':{
                # IDENTIFIANTS UNIQUES
                'video_id': 'VARCHAR(50) PRIMARY KEY',
                'channel_id': 'VARCHAR(50) NOT NULL',
                
                # MÉTADONNÉES DE BASE
                'title': 'VARCHAR(500) NOT NULL',
                'description': 'TEXT',
                'channel_title': 'VARCHAR(10) NOT NULL', 
                'category_id': 'VARCHAR(50)',
                
                # DATES ET TIMESTAMPS
                'published_at': 'TIMESTAMPTZ NOT NULL',
                'created_at': 'TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP',
                'updated_at': 'TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP',
                
                # STATISTIQUES D'ENGAGEMENT
                'view_count': 'BIGINT DEFAULT 0',
                'like_count': 'BIGINT DEFAULT 0',
                'comment_count': 'BIGINT DEFAULT 0', 
                'favorite_count': 'BIGINT DEFAULT 0',
                
                # DÉTAILS TECHNIQUES
                'duration': "VARCHAR(20) DEFAULT 'PT0S'",    
                'dimension': "VARCHAR(20) DEFAULT '2d'",     
                'definition': "VARCHAR(10) DEFAULT 'sd'",    
                'caption': 'BOOLEAN DEFAULT FALSE',
                'licensed_content': 'BOOLEAN DEFAULT FALSE',
                'projection': "VARCHAR(20) DEFAULT 'rectangular'",  
                'live_broadcast_content': "VARCHAR(20) DEFAULT 'none'" },
            'comments':{
                "comment_id": "VARCHAR(100))",
                "parent_comment_id": "VARCHAR(100))",
                "video_id": "VARCHAR(100))",
                "author_display_name": "TEXT",
                "author_channel_id": "VARCHAR(100))",
                "author_channel_url": "TEXT",
                "text_display": "TEXT",
                "text_original": "TEXT",
                'like_count': "BIGINT",
                "published_at": "TIMESTAMP",
                "updated_at": "TIMESTAMP",
                "is_reply": "BOOLEAN",
                "total_reply_count": "INT",
                "can_reply": "BOOLEAN",
                "is_public": "BOOLEAN"
            }
        }
        table_names = ['channels','videos', 'comments']
        load(file_names, config_path, table_names, schemas, data_types=["channels", "videos", "comments"])
        
        
        logger.info(f"Successfully loaded data to database")
        
        # Summary
        logger.info("=" * 60)
        logger.info("ETL Pipeline completed successfully!")
        logger.info("Summary:")
        logger.info(f"  - Channels processed: {len(channel_ids)}")
        logger.info(f"  - Data types: {', '.join(data_types)}")
        logger.info(f"  - Max results per request: {max_results}")
        logger.info("=" * 60)
        
        return True
        
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        logger.error("Please ensure config/api_keys.yaml and config/settings.yaml exist")
        return False
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
        return False


def main():
    channel_ids = [
        "UC_x5XG1OV2P6uZZ5FSM9Ttw",  # Google Developers
        "UC29ju8bIPH5as8OGnQzwJyA",  # Traversy Media
        "UC8butISFwT-Wl7EV0hUK0BQ",  # freeCodeCamp.org
        "UCYO_jab_esuFRV4b17AJtAw",  # 3Blue1Brown
        "UCWv7vMbMWH4-V0ZXdmDpPBA"   # Programming with Mosh
    ]
    
    run_etl_pipeline(
        channel_ids=channel_ids,
        data_types=["channels", "videos", "comments"],
        max_results=50
    )

if __name__ == "__main__":
    main()