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
