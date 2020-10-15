import pendulum
pendulum.set_locale('ja')
import requests
from bs4 import BeautifulSoup
import wikipedia


class Scraper:
    """
    Parse HTML file given URL.

    Parameters
    ----------
        response: class:`requests.models.Response` object.
        soup: class:`bs4.BeautifulSoup` object. 
    """

    def __init__(self, response=None, soup=None):
        self.response = None
        self.soup = None

    def get(self, url):
        """Create BeautifulSoup object from a responded HTML

        Parameters
        ----------
        url: str
            URL of a page that is to be parsed.

        Returns
        -------
            None
        """

        # url = 'https://en.wikipedia.org/wiki/Jin_Akanishi'
        self.response = requests.get(url)
        # Create BeautifulSoup object from a responded HTML
        self.soup = BeautifulSoup(self.response.text, 'html.parser')

    def is_modified_since_last_time(self, s3_dst="s3://test"):
        """Check if there is a modification to a given HTML file
        since the last time the file was fetched.

        Parameters
        ----------
        s3_dst: str
            A file(JSON) existing in S3 whose content is the last time you fetched the file
        """

        # TODO: GET last modified time from s3 or somewhere
        dt = pendulum.datetime(2020, 1, 1)
        last_modified = self.response.headers['Last-Modified']
        if dt < last_modified:
            self.write_last_modified_t(last_modified, s3_dst="s3://test")
            return True 
        return False

    def write_last_modified_t(self,last_modified, s3_dst):
        """Write last_modified time to a given file 

        Parameters
        ----------
        last_modified: str
            Time at which a page is modified

        s3_dst: str
            A file(JSON) existing in S3 whose content is the last time you fetched the file
        """

        # TODO: Write last_modified value to s3 or somewhere
        pass

class ArtistScraper(Scraper):
    """
    A subclass of Scraper class.
    This class is used for parsing a Wikipedia page of a given artist, 
    and for retrieving some information(items) about that artist.

    Parameters
    ----------
        response: class:`requests.models.Response` object.
        soup: class:`bs4.BeautifulSoup` object. 
        items: dict
            Pieces of information that is to be got 

    Examples
    --------
    >>> from scraper import ArtistScraper
    >>> scraper = ArtistScraper()
    >>> artist = "Zara Larsson"
    >>> scraper.fetch_artist_info(artist)
    >>> scraper.items
    items = {
            "Born": ['1997-12-16'],
            "Occupation": ['Singer', 'songwriter'],
            "Genres":['Pop', 'dance-pop', 'R&B'],
            "Instruments":'Instruments': ['Vocals'], 
            "Years active":' Years active': ['2008–present']
            }
    """

    items = {
            "Born": list, 
            "Occupation": list, 
            "Genres":list, 
            "Instruments":list,
            "Years active":list
            }

    def __init__(self, response=None, soup=None):
        super().__init__(response, soup)
        self.items = ArtistScraper.items

    def get_table_data(self, text):
        table_data = self.soup.find("th", text=text).find_next_sibling("td")
        return table_data

    def map_list_to_dict(self, func, values):
        """
        Apply func to values and return it as a dictionary

        Parameters
        ----------
        func: <class 'function'>
            A function being applied to values

        values: list
            Values being applied to by a given function
        """

        return dict((func(v), v) for v in values)

    def get_artist_url(self, artist_name):
        """
        Get URL of an artist page 

        Parameters
        ----------
        artist_name: str
            Artist name that is to be fetched URL for

        Returns
        -------
        page.url: str 
            url of Wikipedia about an artist
        """
        num_of_token = 30
        start_index, end_index = 0, 1
        artist_name = "".join(artist_name.lower().split())
        print(f"artist_name : {artist_name}")
        while True:
            values = wikipedia.search(artist_name, results=num_of_token)[start_index*num_of_token:end_index*num_of_token]
            if len(values) == 0:
                return None
            # print(f"values {values}")
            suggested_values = self.map_list_to_dict(lambda s: "".join(s.lower().split()), values)
            print(f"values {suggested_values}")
            if artist_name in suggested_values:
                page = wikipedia.page(suggested_values[artist_name])
                print(f"page url is: {page.url}")
                return page.url
            start_index += 1
            end_index   += 1

    def fetch_artist_info(self, artist_name):
        """
        Get Artist information.

        Parameters
        ----------
        artist_name: str
            Artist name whose informtion is to be fetched

        Returns
        -------
        items: dict 
            Pieces of information about an artist
        """

        url = self.get_artist_url(artist_name)
        if url is None:
            return None
        super().get(url=url)
        for header in self.soup.select(".infobox th")[:]:
            tr = header.text.lower().encode('ascii', errors='ignore').decode('utf8')
            tr = "".join(tr.split())
               # if header.text in self.items:
            # table_data = self.get_table_data(header.text)
            # print(table_data)
            if tr == "born":
                table_data = self.get_table_data(header.text)
                data = table_data.find(class_="bday")
                self.items["Born"] = [data.text]
            elif tr == "occupation":
                table_data = self.get_table_data(header.text)
                lists = table_data.find_all("li")
                data = [item.text for item in lists]
                if len(data) == 0:
                    data = [table_data.text]
                self.items["Occupation"] = data
            elif tr == "genres":
                table_data = self.get_table_data(header.text)
                lists = table_data.find_all("li")
                data = [item.text for item in lists]
                if len(data) == 0:
                    data = [table_data.text]
                self.items["Genres"] = data
            elif tr == "instruments":
                table_data = self.get_table_data(header.text)
                lists = table_data.find_all("li")
                data = [item.text for item in lists]
                if len(data) == 0:
                    data = [table_data.text]
                self.items["Instruments"] = data
            elif tr == "yearsactive":
                table_data = self.get_table_data(header.text)
                data = [table_data.text]
                self.items["Years active"] = data
        return self.items


if __name__ == "__main__":
    scraper = ArtistScraper()
    url='https://en.wikipedia.org/wiki/Jin_Akanishi'
    url = 'https://en.wikipedia.org/wiki/Zara_Larsson'
    artist = "Zara Larsson"
    scraper.fetch_artist_info(artist)
    print(scraper.items)


    


