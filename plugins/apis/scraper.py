import pendulum
pendulum.set_locale('ja')
import requests
from bs4 import BeautifulSoup

class Scraper:
    def __init__(self):
        self.response = None
        self.soup = None

    def get(self, url):
        # url = 'https://en.wikipedia.org/wiki/Jin_Akanishi'
        self.response = requests.get(url)
        # Create BeautifulSoup object from a responded HTML
        self.soup = BeautifulSoup(self.response.text, 'html.parser')

    def is_modified_since_last_time(self):
        # TODO: GET last modified time from s3 or somewhere
        dt = pendulum.datetime(2020, 1, 1)
        last_modified = self.response.headers['Last-Modified']
        if dt < last_modified:
            self.write_last_modified_t(last_modified, s3_dst="s3://test")

    def write_last_modified_t(self,last_modified, s3_dst):
        # TODO: Write last_modified value to s3 or somewhere
        pass

class ArtistScraper(Scraper):
    items = {
            "Born": str, 
            "Occupation": list, 
            "Genres":list, 
            "Instruments":list,
            "Years active":str
            }
    def __init__(self):
        super().__init__()
        self.items = ArtistScraper.items

    def get_table_data(self, text):
        table_data = self.soup.find("th", text=text).find_next_sibling("td")
        return table_data

    def fetch_artist_info(self, url):
        super().get(url=url)
        for header in self.soup.select(".infobox th")[:]:
            if header.text in self.items:
                table_data = self.get_table_data(header.text)
                if header.text == "Born":
                    data = table_data.find(class_="bday")
                    self.items["Born"] = data.text
                elif header.text == "Occupation":
                    lists = table_data.find_all("li")
                    data = [self.item.text for self.item in lists]
                    self.items["Occupation"] = data
                elif header.text == "Genres":
                    lists = table_data.find_all("li")
                    data = [item.text for item in lists]
                    self.items["Genres"] = data
                elif header.text == "Instruments":
                    lists = table_data.find_all("li")
                    data = [item.text for item in lists]
                    self.items["Instruments"] = data
                elif header.text == "Years active":
                    data = table_data.text
                    self.items["Years active"] = data
        print(self.items)
        return self.items


if __name__ == "__main__":
    scraper = ArtistScraper()
    url='https://en.wikipedia.org/wiki/Jin_Akanishi'
    url = 'https://en.wikipedia.org/wiki/Zara_Larsson'
    scraper.fetch_artist_info(url)


    


