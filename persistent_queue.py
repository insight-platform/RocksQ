import time
from rocksq import PersistentQueueWithCapacity

# Create a queue that will persist to disk

# try:
#     PersistentQueue.remove_db('/tmp/queue')
# except:
#     pass

q = PersistentQueueWithCapacity('/tmp/queue')

data = [bytes(256 * 1024), bytes(256 * 1024), bytes(256 * 1024), bytes(256 * 1024)]

OPS = 8 * 30
RELEASE_GIL = True

start = time.time()
print("begin writing")
for i in range(OPS):
    q.push(data, no_gil=RELEASE_GIL)

print("begin reading")
for i in range(OPS):
    v = q.pop(max_elements=4, no_gil=RELEASE_GIL)
    assert len(v) == 4

end = time.time()

print("Time taken: %f" % (end - start))
