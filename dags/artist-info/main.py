import pendulum
pendulum.set_locale('ja')
import requests
from bs4 import BeautifulSoup

class Scraper:
    def __init__(self):
        self.response = None

    def get(self, url='test'):
        url = 'https://en.wikipedia.org/wiki/Jin_Akanishi'
        self.response = requests.get(url)

    def is_modified_since_last_time(self):
        # TODO: GET last modified time from s3 or somewhere
        dt = pendulum.datetime(2020, 1, 1)
        last_modified = self.response.headers['Last-Modified']
        if dt < last_modified:
            self.write_last_modified_t(last_modified, s3_dst="s3://test")

    def write_last_modified_t(self,last_modified, s3_dst):
        # TODO: Write last_modified value to s3 or somewhere
        pass






    


