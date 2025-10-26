import os
import pandas as pd
import importlib.util
utils_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils', 'db_utils.py')
spec = importlib.util.spec_from_file_location("db_utils", utils_path)
db_utils = importlib.util.module_from_spec(spec)
spec.loader.exec_module(db_utils)


def load_processed_data(file_name):
    file_path = os.path.join('data/processed',file_name)
    data = pd.read_csv(file_path, sep=';')
    return data

def load_data_to_table(file_name, con, table_name, schema):
    data = load_processed_data(file_name)
    db_utils.create_table_if_not_exists(table_name, schema, con)
    cursor = con.cursor()
    cursor.execute(f"DELETE FROM {table_name}")
    con.commit()
    cursor.close()
    db_utils.insert_data(table_name, data, con)
    print("Operations effectuees avec succes")

def load(file_names,config_path, tables_name, schemas,data_types=["channels", "videos", "comments"]):
    con = db_utils.get_db_connection(config_path)
    if 'channels' in data_types:
        # channels_data = 
        load_processed_data(file_names['channels'])
        load_data_to_table(file_names['channels'], con, tables_name[0], schemas['channels'])
        """channels_loaded_data =db_utils.fetch_data('SELECT * FROM channels',con)
        if channels_data.equals(channels_loaded_data) == True:
            print('sauvgarde des donnees des chaines effectuee avec succes')
        else : print('les donnees des chaines processed et loaded ne sont pas compatible')  """

    if 'videos' in data_types:
        #videos_data = 
        load_processed_data(file_names['videos'])
        load_data_to_table(file_names['videos'], con, tables_name[1], schemas['videos'])
        """videos_loaded_data = db_utils.fetch_data('SELECT * FROM videos',con)
        if videos_data.equals(videos_loaded_data) == True:
            print('sauvgarde des donnees des videos effectuee avec succes')
        else : print('les donnees des videos processed et loaded ne sont pas compatible')  """

    if 'comments' in data_types:
        #comments_data = 
        load_processed_data(file_names['comments'])
        load_data_to_table(file_names['comments'], con, tables_name[2], schemas['comments'])     
        """comments_loaded_data = db_utils.fetch_data('SELECT * FROM comments',con)
        if comments_data.equals(comments_loaded_data) == True:
            print('sauvgarde des donnees des commentaires effectuee avec succes')
        else : print('les donnees des commentaires processed et loaded ne sont pas compatible')  """

    print('la sauvegarde totale est effectuee avec succes') 



