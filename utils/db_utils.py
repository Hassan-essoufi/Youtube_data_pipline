import yaml
import psycopg2
import pandas as pd

def get_db_connection(config_path):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    db_config = config['database']
    conn = psycopg2.connect(
        host=db_config['host'],
        port=db_config['port'],
        database=db_config['name'],
        user=db_config['user'],
        password=db_config['password']
    )
    return conn

def create_table_if_not_exists(table_name, schema, conn):
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        );
    """)
    table_exists = cursor.fetchone()[0]
    
    if not table_exists:
        columns = []
        for col_name, col_type in schema.items():
            columns.append(f"{col_name} {col_type}")
        
        create_query = f"CREATE TABLE {table_name} ({', '.join(columns)});"
        cursor.execute(create_query)
        conn.commit()
        print(f"Table '{table_name}' créée avec succès")
    else:
        print(f"Table '{table_name}' existe déjà")
    cursor.close()

def insert_data(table_name, data, conn):
    cursor = conn.cursor()
    columns = ', '.join(data.columns)
    placeholders = ', '.join(['%s'] * len(data.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    for _, row in data.iterrows():
        cursor.execute(insert_query, tuple(row))
    conn.commit()
    print(f"Données insérées dans la table '{table_name}': {len(data)} lignes")
    cursor.close()
    
def fetch_data(query, conn):
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    cursor.close()
    df = pd.DataFrame(results, columns=colnames)
    print(f"Données récupérées: {len(df)} lignes")
    return df

def update_data(query, conn):
    cursor = conn.cursor()
    cursor.execute(query)
    print(f"Donnees modifiees avec succes")
    cursor.close()
    



    