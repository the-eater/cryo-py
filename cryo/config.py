from typing import List, Optional
import etcd3
import toml


class Config:
    pass


class Config:
    @staticmethod
    def load(path: str) -> Optional[Config]:
        try:
            with open(path) as f:
                return Config(toml.load(f))
        except:
            return None

    def __init__(self, dict):
        self.dict = dict

        self.set_defaults()

    def set_defaults(self):
        self.dict['etcd'] = {}

    def get_etcd_client(self) -> etcd3.Etcd3Client:
        return etcd3.Etcd3Client(**self.dict['etcd'])
