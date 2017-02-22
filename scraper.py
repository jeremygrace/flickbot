#!/usr/bin/env python
# python2.7 EC2 env
from urllib2 import urlopen, HTTPError, URLError
from boto.s3.connection import S3Connection
from bs4 import BeautifulSoup


def get_films(url, file, conn):
    '''Function daily retrieves html from a designated website
    and loads it into S3 bucket 'imdb-flickbot' as a .html file
    for future use.

    Parameters:
        Inputs: url - website URL,
                file - name of html file,
                conn - S3 daily Connection
        Output: HTML file into S3 bucket
    '''
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
    # Use S3 conn to grab destination bucket
    imdb_bucket = conn.get_bucket('imdb-flickbot')
    # Indicate file name to write into the bucket
    output_file = imdb_bucket.new_key(file + '.html')
    output_file.content_type = 'text/html'
    output_file.set_contents_from_string(bsObj, policy='public-read')


if __name__ == '__main__':
    # Establish S3 connection
    conn = S3Connection()
    # Assign website URLs to variables
    in_theaters = "http://www.imdb.com/movies-in-theaters/?ref_=cs_inth"
    coming_soon = "http://www.imdb.com/movies-coming-soon/?ref_=inth_cs"
    # Call GET function to scrape each site for HTML
    get_films(in_theaters, 'in_theathers', conn)
    get_films(coming_soon, 'coming_soon', conn)
