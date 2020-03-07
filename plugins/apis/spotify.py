"""
A wrapper class for spotipy
"""

from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOauthError


class AuthError(Exception):
    def __init__(self, msg):
        self.msg = msg 
    def __str__(self):
        return f"Authorization Error : {self.msg}"


class Spotipy(object):
    """
    A wrapper class for spotipy
    """
    def __init__(self, client_id, client_secret):
        self._sp = self.set_client(client_id, client_secret)

    def debug(self):
        return 'Hello'

    def set_client(self, client_id, client_secret):
        """
        Creates a Spotify API client
        """
        client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
        sp = Spotify(client_credentials_manager=client_credentials_manager)

        if not self.is_credential_valid(sp):
            msg = "Invalid credential"
            raise AuthError(msg)

        return sp

    def is_credential_valid(self, sp):
        """
        Validate credential
        """
        try:
            sp._auth_headers()
        except SpotifyOauthError:
            return False
        else:
            return True

    def get_artist_top_10_tracks(self, artist_id, country):
        """ Get Spotify catalog information about an artist's top 10 tracks
            by country.
            Parameters:
                - artist_id(str) - the artist ID
                - country(str) - limit the response to one particular country
            Returns:
                - Information about 10 tracks (list)
        """
        artist_uri = f'spotify:artist:{artist_id}'
        return  self._sp.artist_top_tracks(artist_uri, country)["tracks"]

    def get_audio_features(self, tracks):
        """
        Parameters:
            - tracks(list)
        Returns:
            - (list) audio features about each song corresponding to each track
        """
        return self._sp.audio_features(tracks)

