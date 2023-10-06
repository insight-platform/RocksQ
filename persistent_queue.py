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

print("begin writing")
for i in range(OPS):
    q.push(buf)

print("begin reading")
for i in range(OPS):
    v = q.pop()


end = time.time()

print("Time taken: %f" % (end - start))

