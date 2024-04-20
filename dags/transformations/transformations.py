import pandas as pd
import json
import logging
import random
import re

##SPOTIFY##

def delete_column(df_spotify):
    # Transformation 1: delete column unnamed:0
    df_spotify.drop(['Unnamed: 0'], axis=1, inplace=True)
    return df_spotify

def delete_duplicated_id(df_spotify):
    # Transformation 2: deleting data with duplicated track_id
    df_spotify = df_spotify.groupby('track_id').apply(lambda group: group.sample(n=1, random_state=random.seed())).reset_index(drop=True)
    return df_spotify

def duration_transformation(spotify):
    # Transformation 3: form miliseconds to min rounded
    spotify['duration_ms'] = (spotify['duration_ms'] / 60000).round().astype(int)
    spotify.rename(columns={'duration_ms': 'duration_min'}, inplace=True)
    logging.info(f"data transformed is: {spotify} with shape: {spotify.shape}")
    return spotify

    #Transformation 4: categorize genre and "track_genre" column drop
genre_cat = {
    'Rock': ['alt-rock', 'hard-rock', 'punk-rock', 'grunge', 'rock', 'j-rock', 'psych-rock', 'punk', 'rock-n-roll'],
    'Metal': ['black-metal', 'heavy-metal', 'metal', 'metalcore', 'death-metal'],
    'Pop': ['pop', 'power-pop', 'latin-pop'],
    'Hip-Hop/R&B': ['hip-hop', 'r-n-b'],
    'Electronic': ['chicago-house', 'minimal-techno', 'electronic', 'techno', 'detroit-techno', 'disco', 'edm', 'electro', 'trance', 'dubstep', 'house', 'deep-house', 'dance', 'dancehall', 'breakbeat', 'hardstyle'],
    'Latin': ['latin', 'salsa', 'reggaeton', 'tango', 'samba', 'pagode', 'sertanejo', 'forro'],
    'Other': [
        'club', 'comedy', 'soul', 'ska', 'bluegrass', 'happy', 'drum-and-bass', 'idm', 'sad', 'honky-tonk', 'industrial', 'j-dance', 'grindcore', 'french', 'world-music', 'indian', 'children', 'jazz', 'romance', 'study', 'funk', 'afrobeat', 'opera', 'show-tunes', 'progressive-house', 'acoustic', 'anime', 'ambient', 'iranian', 'songwriter', 'synth-pop', 'kids', 'blues', 'pop-film', 'gospel', 'mandopop', 'swedish', 'reggae', 'piano', 'spanish', 'turkish', 'malay', 'country', 'mpb', 'indie', 'disney', 'chill', 'emo', 'rockabilly', 'j-idol', 'psych-rock', 'guitar', 'dub', 'groove', 'hardcore', 'rock-n-roll', 'brazil', 'indie-pop', 'trip-hop', 'singer-songwriter', 'party', 'sleep', 'garage', 'classical', 'j-pop', 'cantopop', 'british', 'folk', 'new-age', 'alternative', 'latino', 'edm'
    ]
}

def cat_genre(spotify):
    def categorize_genre(track_genre):
        for key, value in genre_cat.items():
            if track_genre in value:
                return key
        return 'Other'  
    
    spotify['genre_cat'] = spotify['track_genre'].apply(categorize_genre)

    spotify.drop(columns=['track_genre'], inplace=True)

    return spotify


#Transformation 5: drop columns

def drop_transformation(spotify):
    columns_to_drop = ['track_id', 'album_name', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'tempo', 'time_signature']
    spotify.drop(columns=columns_to_drop, inplace=True)
    
    return spotify

##GRAMMYS##

def drop_columns(grammys):
    #1. Drop columns img, published_at, updated_at
    grammys = grammys.drop(['img', 'published_at', 'updated_at'], axis=1)
    return grammys


def parenthesis_transformation(grammys):
    #2. Extract from the "workers" parenthesis the name and put it in the column "artist".
    condition = grammys['artist'].isnull() & grammys['workers'].str.contains(r'\(.*\)')
    grammys.loc[condition, 'artist'] = grammys.loc[condition, 'workers'].apply(lambda x: re.search(r'\((.*?)\)', x).group(1) if isinstance(x, str) and re.search(r'\((.*?)\)', x) else None)
    return grammys



def fill_nulls_first(grammys):
    #3. Fill nulls in artist column with the first name in the workers column
    condition= grammys['workers'].str.contains('[;,]', na=False) & ~grammys['workers'].str.contains(r'\(.*\)', na=False) & grammys['artist'].isnull()
    grammys.loc[condition, 'artist'] = grammys.loc[condition, 'workers'].str.split('[;,]').str[0].str.strip()
    return grammys


def fill_nulls_worker(grammys):
    #4. Fill nulls in artist column with the workers column
    condition = grammys['artist'].isnull() & ~grammys['workers'].isnull()
    grammys.loc[condition, 'artist'] = grammys.loc[condition, 'workers']
    return grammys

def fill_nulls_arts(grammys):
    #5. Fill nulls in artist column with the nominee column
    grammys.loc[(grammys['artist'].isnull()), 'artist'] = grammys['nominee']
    return grammys

def drop_nulls(grammys):
    # 6. Delete the null values in "artist" and "nominee"
    grammys.dropna(subset=['artist', 'nominee'], inplace=True)
    #Delete the column "workers"
    grammys.drop(['workers'], axis=1, inplace=True)
    return grammys

def lower_case(grammys):
    #7 Convert all "category" characters to lowercase and remove special characters.
    grammys['category'] = [i.lower().replace('(', '').replace(')', '').replace('-', ' ').replace(',', '') for i in grammys['category']]
    return grammys


def rename_column(grammys):
    grammys.rename(columns={'winner': 'nominated'}, inplace=True)
    return grammys

# def columns_merge(df_merge):
#     columns = ['year','artists','track_name','popularity','category','duration_min',
#                'explicit','danceability','energy','valence','genre_cat','nominated','title']
#     df_merge = df_merge[columns]
#     return df_merge

def fill_na_merge(df_merge):
    df_merge.fillna({'nominated':'False'}, inplace=True)
    return df_merge

def fill_na_merge1(df_merge):
    df_merge.fillna({'year':0}, inplace=True)
    return df_merge

def delete_artist(df_merge):
    df_merge.drop(columns=['artist'], inplace=True)
    return df_merge

def category_na(df_merge):
    df_merge.fillna({'category':'No Category'}, inplace= True)
    return df_merge

def nominee(df_merge):
    df_merge.drop(columns=['nominee'], inplace= True)
    return df_merge

def title(df_merge):
    df_merge.fillna({'title':'No ANNUAL GRAMMY Awards'}, inplace=True)
    return df_merge
