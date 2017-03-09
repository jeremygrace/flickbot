#!/usr/bin/python
import os
import yaml
from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.tools import argparser

# Set DEVELOPER_KEY to the API key value from the APIs & auth > Registered apps
# tab of
#   https://cloud.google.com/console
# Please ensure that you have enabled the YouTube Data API for your project.


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


if __name__ == "__main__":
    credentials = yaml.load(open(os.path.expanduser(
                                 '~/admin/bot-api.yml')))
    youtube = build(credentials['youtube']["YOUTUBE_API_SERVICE_NAME"],
                    credentials['youtube']["YOUTUBE_API_VERSION"],
                    developerKey=credentials['youtube']["DEVELOPER_KEY"])
    argparser.add_argument("--q", help="Search term", default="Google")
    argparser.add_argument("--max-results", help="Max results", default=1)
    args = argparser.parse_args()
    try:
        youtube_search(args, youtube)
    except HttpError as e:
        print("An HTTP error %d occurred:\n%s" % (e.resp.status, e.content))
