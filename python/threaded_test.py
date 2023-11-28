from tempfile import mkdtemp
from threading import Thread

from rocksq.blocking import PersistentQueueWithCapacity

path = mkdtemp()
queue = PersistentQueueWithCapacity(path, 100)


def push_data():
    while True:
        try:
            queue.push([b'123'])
        except RuntimeError as e:
            if 'Queue is full' not in e.args[0]:
                raise


def poll_data():
    while True:
        queue.pop()


threads = [Thread(target=push_data), Thread(target=poll_data)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
