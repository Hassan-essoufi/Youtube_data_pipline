import os
import yaml
from sqlalchemy import create_engine
import pandas as pd

def get_db_connection(config_path):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    db_user = config['database']['user']
    db_password = config['database']['password']
    host = config['database']['host']
    port = config['database']['port']
    db_name = config['database']['name']
    connection_url = f"postgresql://{db_user}:{db_password}@{host}:{port}/{db_name}"
    engine = create_engine(connection_url, echo=True)
    return engine

def create_table_if_not_exists(table_name, schema):   #schema: un dictionnaire contient les noms des colonnes et ses natures
    engine = get_db_connection('config/setting.yaml')
    empty_data = {col: [] for col in schema.keys()}
    df_empty = pd.DataFrame(empty_data)
    df_empty.to_sql(name=table_name, con=engine, if_exists='replace', index=False) 
    print(f"la table {table_name} est cree avec succes")

def insert_data(table_name, data):
    engine = get_db_connection('config/setting.yaml')
    df = pd.DataFrame(data)
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    print(f"Donnees inserees dans la table {table_name} avec succes!")
    
def fetch_data(query):
    engine = get_db_connection('config/setting.yaml')
    df = pd.read_sql(query, engine)
    return df

def execute(query)
    