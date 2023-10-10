import time
import os
from rocksq import remove_db
from rocksq.blocking import PersistentQueueWithCapacity

NUM = 1
OPS = 1000000
RELEASE_GIL = True
PATH = '/tmp/queue'

# if directory exists, remove it
if os.path.exists(PATH):
    remove_db(PATH)

q = PersistentQueueWithCapacity(PATH)

start = time.time()
for i in range(OPS):
    data = [bytes(str(i), 'utf-8')]
    q.push(data, no_gil=RELEASE_GIL)
    v = q.pop(max_elements=NUM, no_gil=RELEASE_GIL)
    assert len(v) == NUM
    assert v == data

end = time.time()

print("Time taken: %f" % (end - start))
