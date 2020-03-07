from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from apis.spotify import Spotipy
from lib.config import read_credential
import os

class SpotifyOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            conf: str,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # self.client_credential = read_credential(conf)
        # self._sp_client = self.create_instance(conf)
        self.conf = conf
     

    def create_instance(self):
        client_credential = read_credential(self.conf)
        return Spotipy(client_credential['client_id'], client_credential['client_secret'])

    def execute(self, context):
        #client_credential = read_credential(self.conf)
        #self._sp_client = Spotipy(client_credential['client_id'], client_credential['client_secret'])
        self._sp_client = self.create_instance()
        message = self._sp_client.debug()
        print(message)
        return message