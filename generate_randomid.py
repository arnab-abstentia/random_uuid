import csv
import sys
from uuid import uuid4

TOTAL_UUID = 20_000_000
BATCH_SIZE = 20_000_000
NUM_BATCHES = TOTAL_UUID // BATCH_SIZE if TOTAL_UUID % BATCH_SIZE == 0 else TOTAL_UUID // BATCH_SIZE + 1
NUM_ROWS = 20

# running batch for every 20 million
for i in range(NUM_BATCHES):
    # gurantees uniqueness for every 20 million uuid
    uid_container = [str(uuid4()).replace('-', '')[:8] for i in range(BATCH_SIZE)]
    # creating batch of 20 rows
    container_batch = [uid_container[x:x+NUM_ROWS] for x in range(0, len(uid_container), NUM_ROWS)]
    # reducing memory footprint by deleting the set object
    del uid_container
    # writing the csv file
    with open('random_uuid.csv', 'a') as fd:
        writer = csv.writer(fd)
        while len(container_batch) > 0:
            batch = container_batch.pop()
            writer.writerow(batch)
    sys.stdout.write('\rCompleted batch => %d' % (i+1))
    sys.stdout.flush()
