import time
from rocksq import PersistentQueueWithCapacity

# Create a queue that will persist to disk

# try:
#     PersistentQueue.remove_db('/tmp/queue')
# except:
#     pass

q = PersistentQueueWithCapacity('/tmp/queue')

start = time.time()
buf = bytes(256 * 1024)

OPS = 8 * 30
RELEASE_GIL = True

print("begin writing")
for i in range(OPS):
    q.push(buf, no_gil=RELEASE_GIL)

print("begin reading")
for i in range(OPS):
    v = q.pop(no_gil=RELEASE_GIL)


end = time.time()

print("Time taken: %f" % (end - start))

