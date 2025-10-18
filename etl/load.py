import os
import pandas as pd
import importlib.util
utils_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils', 'db_utils.py')
spec = importlib.util.spec_from_file_location("db_utils", utils_path)
db_utils = importlib.util.module_from_spec(spec)
spec.loader.exec_module(db_utils)


def load_processed_data(file_name):
    file_path = os.path.join('data/processed',file_name)
    data = pd.read_csv(file_path, sep='\t')
    return data

def load_data_to_table(file_name, con, table_name, schema):
    data = load_processed_data(file_name)
    db_utils.create_table_if_not_exists(table_name, schema, con)
    db_utils.insert_data(table_name, data, con)
    print("Operations effectuees avec succes")


def load(file_names,config_path, tables_name, schemas,data_types=["channels", "videos", "comments"]):
    con = db_utils.get_db_connection(config_path)
    if 'channels' in data_types:
        channels_data = load_processed_data(file_names['channels'])
        load_data_to_table(file_names['channels'], con, tables_name[0], schemas['channels'])
        channels_loaded_data =db_utils.fetch_data('SELECT * FROM channels',con)
        if channels_data.equals(channels_loaded_data) == True:
            print('sauvgarde des donnees des chaines effectuee avec succes')
        else : print('les donnees des chaines processed et loaded ne sont pas compatible')

    if 'videos' in data_types:
        videos_data = load_processed_data(file_names['videos'])
        load_data_to_table(file_names['videos'], con, tables_name[1], schemas['videos'])
        videos_loaded_data = db_utils.fetch_data('SELECT * FROM videos',con)
        if videos_data.equals(videos_loaded_data) == True:
            print('sauvgarde des donnees des videos effectuee avec succes')
        else : print('les donnees des videos processed et loaded ne sont pas compatible')

    if 'comments' in data_types:
        comments_data = load_processed_data(file_names['comments'])
        load_data_to_table(file_names['comments'], con, tables_name[2], schemas['comments'])     
        comments_loaded_data = db_utils.fetch_data('SELECT * FROM comments',con)
        if comments_data.equals(comments_loaded_data) == True:
            print('sauvgarde des donnees des commentaires effectuee avec succes')
        else : print('les donnees des commentaires processed et loaded ne sont pas compatible')  

    print('la sauvegarde totale est effectuee avec succes') 



config_path = 'config/settings.yaml'
con = db_utils.get_db_connection(config_path)

file_name = 'videos_2025-10-18_11h39m47s.csv'
videos_schema = {
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
    
    # DÉTAILS TECHNIQUES (CORRIGÉS)
    'duration': "VARCHAR(20) DEFAULT 'PT0S'",           # Guillemets ajoutés
    'dimension': "VARCHAR(20) DEFAULT '2d'",            # Guillemets ajoutés
    'definition': "VARCHAR(10) DEFAULT 'sd'",           # Guillemets ajoutés
    'caption': 'BOOLEAN DEFAULT FALSE',
    'licensed_content': 'BOOLEAN DEFAULT FALSE',
    'projection': "VARCHAR(20) DEFAULT 'rectangular'",  # Guillemets ajoutés
    'live_broadcast_content': "VARCHAR(20) DEFAULT 'none'"  # Guillemets ajoutés
    
       # Guillemets ajoutés
}
load_data_to_table(file_name, con, 'videos', videos_schema)

    
    





