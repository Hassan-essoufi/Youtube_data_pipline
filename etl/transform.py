import os 
import json
import pandas as pd 
import numpy as np
from datetime import datetime
import re

def load_raw_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        json_data = json.load(file)
    return json_data

def transform_channels(file_path):
    raw_channels_data = load_raw_data(file_path)
    channels_data = []
    for response in raw_channels_data:
        for item in response['items']:
            channel_info = {
                'channel_id': item.get('id', 'N/A'),
                'title': item.get('snippet', {}).get('title', 'N/A'),
                'description': item.get('snippet', {}).get('description', 'N/A'), 
                'published_at': item.get('snippet', {}).get('publishedAt', 'N/A'),
                'country': item.get('snippet', {}).get('country', 'N/A'),
                'custom_url': item.get('snippet', {}).get('customUrl', 'N/A'),
                'subscriber_count': int(item.get('statistics', {}).get('subscriberCount', 0)),
                'video_count': int(item.get('statistics', {}).get('videoCount', 0)),
                'view_count': int(item.get('statistics', {}).get('viewCount', 0))
            }
            channels_data.append(channel_info)
    channels_df = pd.DataFrame(channels_data)
    return channels_df 

def transform_videos(file_path):
    raw_videos_data = load_raw_data(file_path)
    videos_data = []
    for response in raw_videos_data:
        for item in response['items']:
            video_info = {
                'video_id': item.get('id', 'N/A'),
                'title': item.get('snippet', {}).get('title', 'N/A'),
                'description': item.get('snippet', {}).get('description', 'N/A'),
                'published_at': item.get('snippet', {}).get('publishedAt', 'N/A'),
                'channel_id': item.get('snippet', {}).get('channelId', 'N/A'),
                'channel_title': item.get('snippet', {}).get('channelTitle', 'N/A'),
                'category_id': item.get('snippet', {}).get('categoryId', 'N/A'),
                'tags': item.get('snippet', {}).get('tags', []),
                'thumbnails': len(item.get('snippet', {}).get('thumbnails', {})),
                'live_broadcast_content': item.get('snippet', {}).get('liveBroadcastContent', 'none'),
                # Statistiques
                'view_count': int(item.get('statistics', {}).get('viewCount', 0)),
                'like_count': int(item.get('statistics', {}).get('likeCount', 0)),
                'comment_count': int(item.get('statistics', {}).get('commentCount', 0)),
                'favorite_count': int(item.get('statistics', {}).get('favoriteCount', 0)),
                # Détails du contenu
                'duration': item.get('contentDetails', {}).get('duration', 'PT0S'),
                'dimension': item.get('contentDetails', {}).get('dimension', 'N/A'),
                'definition': item.get('contentDetails', {}).get('definition', 'N/A'),
                'caption': item.get('contentDetails', {}).get('caption', 'false'),
                'licensed_content': item.get('contentDetails', {}).get('licensedContent', False),
                'projection': item.get('contentDetails', {}).get('projection', 'rectangular')}
            videos_data.append(video_info)
    videos_df = pd.DataFrame(videos_data)
    return videos_df
         
    
def transform_comments(file_path):
    comments_raw_data = load_raw_data(file_path)
    comments_data = []
    for response in comments_raw_data:
        for item in response['items']:
            snippet = item['snippet']['topLevelComment']['snippet']
            comment_info = {
                'comment_id': item['id'],
                'parent_comment_id': None,
                'video_id': snippet['videoId'],
                'author_display_name': snippet['authorDisplayName'],
                'author_channel_id': snippet.get('authorChannelId', {}).get('value', 'N/A'),
                'author_channel_url': snippet['authorChannelUrl'],
                'text_display': snippet['textDisplay'],
                'text_original': snippet.get('textOriginal', ''),
                'like_count': snippet['likeCount'],
                'published_at': snippet['publishedAt'],
                'updated_at': snippet['updatedAt'],
                'is_reply': False,
                'total_reply_count': item['snippet']['totalReplyCount'],
                'can_reply': item['snippet']['canReply'],
                'is_public': True
            }
            comments_data.append(comment_info) 
    comments_df = pd.DataFrame(comments_data)
    return comments_df

def clean_data(df):
    df_clean = df.copy()
    df_clean = df_clean.drop(columns=['colonne_inutile'], errors='ignore')    
    for col in df_clean.columns:
        if df_clean[col].dtype in ['int64', 'float64']:
            df_clean[col].fillna(df_clean[col].median(), inplace=True)
        else:
            df_clean[col].fillna('nothing', inplace=True)
    problematic_columns = []
    for col in df_clean.columns:
        # Vérifier si la colonne contient des listes
        if df_clean[col].apply(lambda x: isinstance(x, list)).any():
            problematic_columns.append(col)
    for col in problematic_columns:
        df_clean[col] = df_clean[col].apply(
            lambda x: str(x) if isinstance(x, list) else x)
    df_clean = df_clean.drop_duplicates()
    for col in problematic_columns:
        df_clean[col] = df_clean[col].apply(
            lambda x: eval(x) if isinstance(x, str) and x.startswith('[') else x)
    return df_clean  

def save_processed_data(data,file_name):
    os.makedirs('data/processed',exist_ok=True)
    file_path = os.path.join("data","processed",file_name)
    data.to_csv(file_path, sep='\t', index=False, encoding='utf-8')
    print(f"fichier sauvegardé: {file_path} ")

def transform(channels_path,videos_path,comments_path):
    #Transform, clean and save channels_data
    channels_data = transform_channels(channels_path)
    channels_proc_data = clean_data(channels_data)
    save_processed_data(channels_proc_data,'channels_pro.csv')
    #Transform, clean and save videos_data
    videos_data = transform_videos(videos_path)
    videos_proc_data = clean_data(videos_data)
    save_processed_data(videos_proc_data,'videos_pro.csv')
    #Transform, claen and save comments_data
    comments_data = transform_comments(comments_path)
    comments_processed_data = clean_data(comments_data)
    save_processed_data(comments_processed_data,'comments_pro.csv')
    print('Transformation effectuee avec succes!')







transform('data/raw/channels.json', 'data/raw/videos.json', 'data/raw/comments.json') 
























      
























