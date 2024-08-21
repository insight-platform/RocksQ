import time
import os
from rocksq import remove_mpmc_queue, StartPosition
from rocksq.blocking import MpmcQueue

NUM = 1
OPS = 1000
RELEASE_GIL = True
PATH = '/tmp/mpmc-queue'
TTL = 60
LABEL = 'label'

# if directory exists, remove it
if os.path.exists(PATH):
    remove_mpmc_queue(PATH)

q = MpmcQueue(PATH, TTL)

start = time.time()
for i in range(OPS):
    data = [bytes(str(i), 'utf-8')]
    q.add(data, no_gil=RELEASE_GIL)

for i in range(OPS):
    v = q.next(label=LABEL, start_position=StartPosition.Oldest, max_elements=NUM, no_gil=RELEASE_GIL)
    assert len(v[0]) == NUM
    assert v[0] == [bytes(str(i), 'utf-8')]
    assert not v[1]

end = time.time()

print("Time taken: %f" % (end - start))
