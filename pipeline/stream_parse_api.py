#!/usr/bin/python
import json
import os
import requests
import time
import yaml
from glob import glob
from string import punctuation
from datetime import datetime
from imdb import IMDb
from apiclient.discovery import build
from apiclient.errors import HttpError
from boto.s3.connection import S3Connection
from boto.s3.key import Key


def pull_movies(url, filename, dict_name, genres_url, conn):

    payload1, payload2 = {}, {}
    dict_name, simply_dict = [], {}

    mov_response = requests.request("GET", url, data=payload1)
    genre_response = requests.request("GET", genres_url, data=payload2)
    ts = time.time()
    timestamp = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

    mov_payload = json.loads(mov_response.text)
    genre_payload = json.loads(genre_response.text)

    for genre in genre_payload['genres']:
        simply_dict[genre['id']] = genre['name'].encode('utf-8')

    for movie in mov_payload['results']:
        for key, val in simply_dict.items():
            if key == movie['genre_ids'][0]:
                genre = val
                mov = movie['title'].encode('utf-8')
                for p in punctuation:
                    mov = mov.replace(p, '')
                dict_name.append({'title': mov,
                                  'genre': genre,
                                  'timestamp': timestamp})
    dict_name = collect_mpaa(conn, dict_name)
    save_to_json(filename, dict_name)
    return(dict_name)


def collect_mpaa(conn, dict_name):
    for movie in dict_name:
        title = movie['title']
        try:
            mov_title = conn.search_movie(title)[0]
            conn.update(mov_title)
            rating = mov_title["mpaa"]
            mpaa = rating.split(' ')[1].encode('utf-8')
            movie['rated'] = mpaa
        except KeyError as e:
            continue
    return(dict_name)


def collect_trailers(youtube, filename, dict_name):
    # Call the search.list method to retrieve results
    # matching the specified query term.
    trailers = []
    for movie in dict_name:
        title = movie['title']
        search_result = youtube.search().list(q=title,
                                              part="id,snippet",
                                              maxResults=1).execute()
        # Add each result to the appropriate list,
        # and then display the lists of
        # matching videos, channels, and playlists.
        for result in search_result.get("items", []):
            if result["id"]["kind"] == "youtube#video":
                suffix = result["id"]["videoId"]
            trailers.append({"title": title,
                             "url": suffix.encode('utf-8')})
    return save_to_json(filename, trailers)


def send_to_s3(conn, filename):
    api_stream = conn.get_bucket('flickbot-api')
    k = Key(api_stream)
    k.key = str(filename)
    k.set_contents_from_filename(filename, policy='public-read')


def save_to_json(filename, dict_name):
    with open(filename, 'w') as fp:
        json.dump(dict_name, fp, ensure_ascii=False, encoding='utf-8',
                  sort_keys=True)


if __name__ == '__main__':
    conn = IMDb()
    conn_s3 = S3Connection()
    credentials = yaml.load(open(os.path.expanduser(
                                 '~/admin/bot-api.yml')))
    youtube = build(credentials['youtube']["YOUTUBE_API_SERVICE_NAME"],
                    credentials['youtube']["YOUTUBE_API_VERSION"],
                    developerKey=credentials['youtube']["DEVELOPER_KEY"])
    in_theaters = "https://api.themoviedb.org/3/movie/now_playing?api_key=" \
                  + credentials['themoviedb']['API_KEY'] \
                  + "&language=en-US&page=1"
    coming_soon = "https://api.themoviedb.org/3/movie/upcoming?api_key=" \
                  + credentials['themoviedb']['API_KEY'] \
                  + "&language=en-US&page=1"
    genres_url = "https://api.themoviedb.org/3/genre/movie/list?api_key=" \
                 + credentials['themoviedb']['API_KEY'] \
                 + "&language=en-US"
    try:
        now_playing = pull_movies(in_theaters, "now_playing.json",
                                  "now_playing", genres_url, conn)
        upcoming = pull_movies(coming_soon, "upcoming.json",
                               "upcoming", genres_url, conn)
        collect_trailers(youtube, "now_trailers.json", now_playing)
        collect_trailers(youtube, "coming_trailers.json", upcoming)
    except HttpError as e:
        print("An HTTP error %d occurred:\n%s" % (e.resp.status, e.content))
    for json_file in glob('*.json'):
        send_to_s3(conn_s3, json_file)
