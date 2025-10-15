import os
import json
import importlib.util

utils_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'utils', 'api_utils.py')
spec = importlib.util.spec_from_file_location("api_utils", utils_path)
api_utils = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api_utils)

def get_channel_data(channel_id, service):
    request = service.channels().list(part="snippet,statistics,contentDetails", id=channel_id)
    response = request.execute()
    return response

def get_videos_from_channel(channel_id, max_results, service):
    request = service.search().list(part="snippet", channelId=channel_id, type='video', maxResults=max_results)
    response = request.execute()
    video_ids = []
    for item in response.get('items', []):
        if 'id' in item and 'videoId' in item['id']:
            video_ids.append(item['id']['videoId'])
    return video_ids

def get_videos_details(video_ids,service):
    videos_details = []
    for video_id in video_ids:
        request = service.videos().list(part='snippet', id=video_id)
        response = request.execute()
        videos_details.append(response)
    return videos_details

def get_comments(video_id, service, max_results):
    request = service.commentThreads().list(part='snippet', videoId=video_id, maxResults=max_results, order='relevance')
    response = request.execute()
    return response

def  save_raw_data(data, filname):
    os.makedirs("data/raw", exist_ok=True)
    file_path = os.path.join("data","raw",filname)
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    print(f"fichier sauvegardé: {file_path} ")


def extract(channels, data_types=["channels", "videos", "comments"], max_results=50):
    config = api_utils.load_config('config/api_keys.yaml')
    primary_key = config['youtube']['api_key']
    backup_key = config['youtube']['backup_key']
 
    if api_utils.validate_api_key(primary_key) == True:
        api_key = primary_key
    elif api_utils.validate_api_key(primary_key) == False and api_utils.validate_api_key(backup_key ==True):
        api_key = backup_key
    else : print("Erreur API")

    service = api_utils.build_youtube_service(api_key)
    if "channels" in data_types :
        channels_data = []
        for channel_id in channels:
            channel_data = get_channel_data(channel_id, service)
            channels_data.append(channel_data)
        save_raw_data(channels_data, 'channels.json')

    if "videos" in data_types :
        for channel_id in channels:
            video_ids = get_videos_from_channel(channel_id, max_results, service)
            videos_data = get_videos_details(video_ids, service)
        save_raw_data(videos_data, 'videos.json')
    
    if  "comments" in data_types:
        comments_data =[]
        for video_id in video_ids: 
            video_comments = get_comments(video_id, service, max_results)
            comments_data.append(video_comments)
        save_raw_data(comments_data, 'comments.json')
    
    print("Extraction terminée avec succès.")


         
                                                                 









api_key = 'AIzaSyCMnImcGCDSMY_xU2pskj6V8IbHK3c1iKQ'
service = api_utils.build_youtube_service(api_key)
max_results = 4
channels_id = ['UC_x5XG1OV2P6uZZ5FSM9Ttw',"UCsMica-v34Irf9KVTh6xx-g","UC5WjFrtBdufl6CZojX3D8dQ"]
channel_id = 'UC_x5XG1OV2P6uZZ5FSM9Ttw'
video_ids = ["kJQP7kiw5Fk", "XqZsoesa55w", "MmB9b5njVbA"]
video_id = video_ids[0]
#print(api_utils.validate_api_key(api_key))
#print(get_channel_data(channel_id,service))

print(get_videos_from_channel(channel_id, max_results,service))

#print(get_videos_details(video_ids, service))

#data = get_videos_from_channel(channel_id, max_results,service)
#save_raw_data(data,"videosdata.json")

extract(channels_id, data_types=["channels", "videos", "comments"], max_results=50)























