import pytest
from airflow import settings
import sys
# sys.path.append('../../../plugins/apis/')
sys.path.append(f'{settings.AIRFLOW_HOME}/plugins/apis/')

from scraper import ArtistScraper

@pytest.fixture(scope="module")
def scraper():
    return ArtistScraper()

class TestScraper:
    def test_get_artist_url(self, scraper):
        assert scraper.get_artist_url("Test Artist Name") == None
        assert scraper.get_artist_url("Zara Larsson") == "https://en.wikipedia.org/wiki/Zara_Larsson"
        assert scraper.get_artist_url(" zara LarSson ") == "https://en.wikipedia.org/wiki/Zara_Larsson"

    def test_fetch_artist_info(self, scraper):
        assert scraper.fetch_artist_info("Test Artist Name") == None
        assert scraper.fetch_artist_info(" zara LarSson ") ==\
                {
                'Born': ['1997-12-16'],
                'Occupation': ['Singer', 'songwriter'],
                'Genres': ['Pop', 'dance-pop', 'R&B'],
                'Instruments': ['Vocals'],
                'Years active': ['2008–present']
                }
        assert scraper.fetch_artist_info(" jin akanishi ") ==\
                {
                'Born': ['1984-07-04'],
                'Occupation': ['Singer', 'songwriter', 'arranger', 'actor', 'television personality'],
                'Genres': ['Pop', 'dance', 'R&B', 'hip hop', 'electronic'],
                'Instruments': ['Vocals', 'guitar', 'piano'],
                'Years active': ['1998–present']
                }
