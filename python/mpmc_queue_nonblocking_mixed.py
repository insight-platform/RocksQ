import time
import os
from rocksq import remove_mpmc_queue, StartPosition
from rocksq.nonblocking import MpmcQueue

NUM = 1
OPS = 1000
RELEASE_GIL = True
PATH = '/tmp/mpmc-queue'
TTL = 60
LABEL_ONE = 'label1'
LABEL_TWO = 'label2'
LABEL_THREE = 'label3'

# if directory exists, remove it
if os.path.exists(PATH):
    remove_mpmc_queue(PATH)

q = MpmcQueue(PATH, TTL)

start = time.time()
for i in range(OPS):
    data = [bytes(str(i), 'utf-8')]
    q.add(data, no_gil=RELEASE_GIL).get()
    v = q.next(label=LABEL_ONE, start_position=StartPosition.Oldest, max_elements=NUM, no_gil=RELEASE_GIL).get().data
    assert len(v) == NUM
    assert v == data

end = time.time()

print("Time taken: %f" % (end - start))

v = q.next(label=LABEL_ONE, start_position=StartPosition.Oldest, max_elements=NUM, no_gil=RELEASE_GIL).get().data
assert len(v) == 0

v = q.next(label=LABEL_TWO, start_position=StartPosition.Newest, max_elements=NUM, no_gil=RELEASE_GIL).get().data
assert len(v) == NUM
assert v == [bytes(str(OPS-1), 'utf-8')]

labels = q.labels.get().labels
assert len(labels) == 2
assert LABEL_ONE in labels
assert LABEL_TWO in labels

r = q.remove_label(LABEL_THREE).get().removed_label
assert not r

r = q.remove_label(LABEL_ONE).get().removed_label
assert r

labels = q.labels.get().labels
assert len(labels) == 1
assert LABEL_ONE not in labels
assert LABEL_TWO in labels

v = q.next(label=LABEL_ONE, start_position=StartPosition.Oldest, max_elements=NUM, no_gil=RELEASE_GIL).get().data
assert len(v) == NUM
assert v == [bytes(str(0), 'utf-8')]
