"""A wrapper class for spotipy
"""

from spotipy import Spotify, SpotifyException
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOauthError

class Spotipy(object):
    def __init__(self, client_id, client_secret):
        self._sp = self.get_client(client_id, client_secret)

    def get_client(self, client_id, client_secret):
        client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
        return Spotify(client_credentials_manager=client_credentials_manager)

    def get_artist_top_tracks(self, artist_id):
        artist_uri = f'spotify:artist:{artist_id}'
        return  self._sp.artist_top_tracks(artist_uri)

    def get_audio_features(self, song_id):
        return self._sp.audio_features(song_id)

