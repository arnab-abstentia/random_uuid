import random
import csv
import sys
from uuid import uuid4

def generate_radom_uid(num_uuid=20_000_000, len_uuid=8):
    chars = list('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
    uids = set()
    while len(uids) <= num_uuid:
        choice = random.choices(chars, k=len_uuid)
        uids.add(''.join(choice))
    return uids

TOTAL_UUID = 20_000_000
BATCH_SIZE = 20_000_000
NUM_BATCHES = TOTAL_UUID // BATCH_SIZE if TOTAL_UUID % BATCH_SIZE == 0 else TOTAL_UUID // BATCH_SIZE + 1
NUM_ROWS = 20

# running batch for every 20 million
for i in range(NUM_BATCHES):
    # gurantees uniqueness for every 20 million uuid
    # uid_container = [str(uuid4()).replace('-', '')[:8] for i in range(BATCH_SIZE)]
    uid_container = list(generate_radom_uid(num_uuid=TOTAL_UUID))[:TOTAL_UUID]
    # creating batch of 20 rows
    container_batch = [uid_container[x:x+NUM_ROWS] for x in range(0, len(uid_container), NUM_ROWS)]
    # reducing memory footprint by deleting the set object
    del uid_container
    headers = [f"column{i}" for i in range(1, NUM_ROWS+1)]
    # writing the csv file
    with open('random_uuid1.csv', 'w') as fd:
        # writer = csv.DictWriter(fd, fieldnames=headers)
        while len(container_batch) > 0:
            batch = container_batch.pop()
            fd.write(','.join(batch))
            fd.write('\n')
            #writer.writerow(dict(zip(headers, batch)))
    sys.stdout.write('\rCompleted batch => %d' % (i+1))
    sys.stdout.flush()
