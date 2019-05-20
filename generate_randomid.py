import csv
import uuid
import sys

def make_random_uuid(num_uids=2_000_000):
    for _ in range(num_uids):
        uid = uuid.uuid4()
        yield str(uid).replace('-', '')

for i in range(10):
    uid_container = set(i for i in make_random_uuid())
    uid_container = list(uid_container)
    container_batch = [uid_container[x:x+10] for x in range(len(uid_container))]
    del uid_container
    with open('random_uuid.csv', 'a') as fd:
        writer = csv.writer(fd)
        while len(container_batch) > 0:
            batch = container_batch.pop()
            writer.writerow(batch)
    sys.stdout.write('\rCompleted batch => %d' % (i+1))
    sys.stdout.flush()
