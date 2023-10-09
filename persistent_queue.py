import time
from rocksq.sync import PersistentQueueWithCapacity

NUM = 1
OPS = 1000000
RELEASE_GIL = True

try:
    PersistentQueueWithCapacity.remove_db('/tmp/queue')
except:
    pass

q = PersistentQueueWithCapacity('/tmp/queue')

start = time.time()
for i in range(OPS):
    data = [bytes(str(i), 'utf-8')]
    q.push(data, no_gil=RELEASE_GIL)

for i in range(OPS):
    v = q.pop(max_elements=NUM, no_gil=RELEASE_GIL)
    assert len(v) == NUM
    assert v == [bytes(str(i), 'utf-8')]


end = time.time()

print("Time taken: %f" % (end - start))
