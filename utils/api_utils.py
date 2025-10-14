from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import yaml

def load_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def validate_api_key(api_key):
    service = build_youtube_service(api_key)
    try:
        request = service.channels().list(part="id",forUsername="GoogleDevelopers")
        response = request.execute()
        if "items" in response and len(response["items"])>0: 
            return True
        else :
            return False       
    except HttpError as e :
         error_message = e.content.decode()
         return "Erreur API" ,{error_message}
    
def build_youtube_service(api_key):
    return build('youtube', 'v3', developerKey=api_key)

def is_quota_exceeded(func, *args, **kwargs):
    """
    Parameters:
        func : fonction représentant l'appel API à exécuter
        *args, **kwargs : arguments de la fonction API

    Returns:
        True si le quota est dépassé, False si l'appel est OK
    """
    try:
        func(*args, **kwargs)
        return False 
    except HttpError as e:
        error_content = e.content.decode()
        if "quotaExceeded" in error_content:
            return True 
        else:
            raise




