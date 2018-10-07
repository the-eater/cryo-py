import random
import string
from datetime import datetime
from typing import Optional

from etcd3 import Etcd3Client


class Queue:
    def __init__(self, etcd_client: Etcd3Client, client_name: Optional[str] = None):
        self.etcd_client = etcd_client
        self.client_name = client_name
        self.seq = -1

        if self.client_name is None:
            self.generate_client_name()

    def generate_client_name(self):
        self.client_name = ''.join(random.choice(string.ascii_lowercase) for _ in range(10))

    def generate_key(self):
        self.seq = (self.seq + 1) % 1000

        return '{}-{}-{}'.format(Queue.get_microtimestamp(), self.client_name, self.seq)

    @staticmethod
    def get_microtimestamp():
        delta = (datetime.now() - datetime(1970, 1, 1))
        return '{}{:0>6}'.format(delta.seconds + delta.days * 3600, delta.microseconds)

    def add_item(self, data: bytes, tries: int = 5) -> Optional[str]:
        key = self.generate_key()

        success, _ = self.etcd_client.transaction(
            compare=[
                self.etcd_client.transactions.create('/state/' + key + '/input') == 0
            ],
            success=[
                self.etcd_client.transactions.put('/state/' + key + '/input', data),
                self.etcd_client.transactions.put('/state/' + key, b'queued'),
                self.etcd_client.transactions.put('/state/' + key + '/key', key),
                self.etcd_client.transactions.put('/queued/' + key, key)
            ],
            failure=[]
        )

        if not success and tries >= 5:
            return None

        if not success:
            return self.add_item(data, tries=tries - 1)

        return key

    def select(self, tries=5):
        items = self.etcd_client.get_range(range_start=b'/queued/', range_end=b'/queued/\xff', limit=5)

        for (val, md) in items:
            key = md.key.split(b'/')[-1]
            new_key = self.generate_key()
            ts = Queue.get_microtimestamp()

            success, _ = self.etcd_client.transaction(
                compare=[
                    self.etcd_client.transactions.value(b'/state/' + val) == b'queued',
                    self.etcd_client.transactions.value(b'/state/' + val + b'/key') == key,
                    self.etcd_client.transactions.create('/live/' + new_key) == 0
                ],
                success=[
                    self.etcd_client.transactions.delete(b'/queued/' + key),
                    self.etcd_client.transactions.put(b'/state/' + val, b'live'),
                    self.etcd_client.transactions.put(b'/state/' + val + b'/key', new_key),
                    self.etcd_client.transactions.put('/live/' + new_key, val),
                    self.etcd_client.transactions.put(b'/state/' + val + b'/live', ts),
                    self.etcd_client.transactions.put(b'/state/' + val + b'/heartbeat', ts),
                ],
                failure=[]
            )

            if success:
                hb = self.etcd_client.get(b'/state/' + val + b'/heartbeat')
                hb = int.from_bytes(hb, 'le', signed=False)

                if hb != 0:
                    self.etcd_client.get()

                return val

        if tries > 0:
            return self.select(tries - 1)

        return None

    def keepalive(self, id):

    def complete(self, id: str, output: bytes = None, tries=5):
        success, result = self.etcd_client.transaction(
            compare=[
                self.etcd_client.transactions.value('/state/' + id) == b'live'
            ],
            success=[
                self.etcd_client.transactions.get('/state/{}/key'.format(id))
            ],
            failure=[]
        )

        if not success:
            return False

        (val, md) = result[0][0]

        new_key = self.generate_key()

        playbook = [
            self.etcd_client.transactions.put('/state/' + id, 'done'),
            self.etcd_client.transactions.put('/state/' + id + '/key', new_key),
            self.etcd_client.transactions.delete(b'/live/' + val),
            self.etcd_client.transactions.put('/done/' + new_key, id)
        ]

        if output is not None:
            playbook.append(self.etcd_client.transactions.put('/state/{}/result'.format(id), output))

        success, _ = self.etcd_client.transaction(
            compare=[
                self.etcd_client.transactions.value('/state/' + id) == b'live',
                self.etcd_client.transactions.value('/state/' + id + '/key') == val,
                self.etcd_client.transactions.create(b'/live/' + val) > 0,
                self.etcd_client.transactions.create('/done/' + new_key) == 0
            ],
            success=playbook,
            failure=[]
        )

        if not success and tries > 0:
            return self.complete(id, output, tries - 1)

        return success

    def wipe(self, id, tries=5):
        success, result = self.etcd_client.transaction(
            compare=[
                self.etcd_client.transactions.value('/state/' + id) == b'done'
            ],
            success=[
                self.etcd_client.transactions.get('/state/{}/key'.format(id))
            ],
            failure=[]
        )

        if not success:
            return False

        (val, md) = result[0][0]

        success, _ = self.etcd_client.transaction(
            compare=[
                self.etcd_client.transactions.value('/state/' + id) == b'done',
                self.etcd_client.transactions.value('/state/' + id + '/key') == val,
                self.etcd_client.transactions.create(b'/done/' + val) > 0,
            ],
            success=[
                self.etcd_client.transactions.delete(b'/done/' + val),
                self.etcd_client.transactions.delete('/state/' + id + '/key'),
                self.etcd_client.transactions.delete('/state/' + id + '/input'),
                self.etcd_client.transactions.delete('/state/' + id),
            ],
            failure=[]
        )

        if not success:
            return False

        self.etcd_client.transaction(
            compare=[
                self.etcd_client.transactions.create('/state/' + id + '/output') > 0
            ],
            success=[
                self.etcd_client.transactions.create('/state/' + id + '/output')
            ],
            failure=[]
        )

        return success

    def get_output(self, id):
        return self.etcd_client.get('/state/' + id + '/output')
