#!/usr/bin/python
import json
import os
import requests
import yaml
from imdb import IMDb
from apiclient.discovery import build
from apiclient.errors import HttpError


def pull_movie_data(url, dict_name, genres_url):

    payload1 = {}
    payload2 = {}
    dict_name = {}
    simply_dict = {}

    mov_response = requests.request("GET", url, data=payload1)
    genre_response = requests.request("GET", genres_url, data=payload2)

    mov_payload = json.loads(mov_response.text)
    genre_payload = json.loads(genre_response.text)

    for genre in genre_payload['genres']:
        simply_dict[genre['id']] = genre['name'].encode('utf-8')

    for movie in mov_payload['results']:
        for key, val in simply_dict.items():
            if key == movie['genre_ids'][0]:
                genre = val
        dict_name[movie['title'].encode('utf-8')] = {'genre': genre}
    return(dict_name)


def youtube_search(options, youtube):
    # Call the search.list method to retrieve results
    # matching the specified query term.
    search_response = youtube.search().list(
                                            q=options.q,
                                            part="id,snippet",
                                            maxResults=options.max_results
                                            ).execute()
    videos = []
    # Add each result to the appropriate list, and then display the lists of
    # matching videos, channels, and playlists.
    for search_result in search_response.get("items", []):
        if search_result["id"]["kind"] == "youtube#video":
            videos.append("%s (%s)" % (search_result["id"]["videoId"],
                                       search_result["snippet"]["title"]))

    print("Videos:", "\n".join(videos), "\n")


if __name__ == '__main__':
    credentials = yaml.load(open(os.path.expanduser(
                                 '~/admin/bot-api.yml')))
    youtube = build(credentials['youtube']["YOUTUBE_API_SERVICE_NAME"],
                    credentials['youtube']["YOUTUBE_API_VERSION"],
                    developerKey=credentials['youtube']["DEVELOPER_KEY"])
    conn = IMDb()
    in_theaters = "https://api.themoviedb.org/3/movie/now_playing?api_key=" \
                  + credentials['themoviedb']['API_KEY'] + "&language=en-US&page=1"
    coming_soon = "https://api.themoviedb.org/3/movie/upcoming?api_key=" \
                  + credentials['themoviedb']['API_KEY'] + "&language=en-US&page=1"
    genres_url = "https://api.themoviedb.org/3/genre/movie/list?api_key=" \
                 + credentials['themoviedb']['API_KEY'] + "&language=en-US"
    try:
        youtube_search(options, youtube)
    except HttpError as e:
        print("An HTTP error %d occurred:\n%s" % (e.resp.status, e.content))
