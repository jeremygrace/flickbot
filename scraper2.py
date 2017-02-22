#!/usr/bin/env python
# python 3.5.2 EC2
import boto3
import re
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup


def pull_html(url):
    # Scrape html of the specific site
    try:
        html = urlopen(url)
    # Handle HTTPError issue
    except HTTPError as e:
        print(e)
    # Handle URLError issue
    except URLError as e:
        print('The server could not be found:' + e)
    # Parse HTML from scrape
    bsObj = BeautifulSoup(html.read(), "html.parser")
    return bsObj


def parse_todict(bsObj, dict_name):
    # d
    dict_name = {}
    # e
    exceptions = "^((?!Showtimes|Register|Home|Delete"\
                 "|Get\sthe|Share|Follow|\d\d*\sreview).)*$"
    # bs
    for movie in bsObj.find_all('a', {'title': re.compile(exceptions)}):
        # a,b
        a, b = movie['title'].split(' (')
        # dict_name
        dict_name[a] = {}
    # return
    return dict_name


if __name__ == '__main__':
    # Establish S3 connection
    s3 = boto3.client('s3')
    # Assign website URLs to variables
    playing = "http://www.imdb.com/movies-in-theaters/?ref_=cs_inth"
    coming = "http://www.imdb.com/movies-coming-soon/?ref_=inth_cs"
    # Call GET function to scrape each site for HTML
    inth = pull_html(playing)
    cs = pull_html(coming)
    # Parse titles from bsObj specific to site
    in_theaters = parse_todict(inth, 'in_theaters')
    coming_soon = parse_todict(cs, 'coming_soon')
    print(in_theaters)
    print(coming_soon)
