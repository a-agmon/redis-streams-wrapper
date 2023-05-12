import redis
import time
import argparse
import concurrent.futures
import random

stream = 'mystream'

def send_messages(num_messages, sleep_time, tid):
    r = redis.Redis(host='localhost', port=6379)
    for i in range(num_messages):
        r.xadd(stream, {'message': f'{tid}-message-{i}'})
        sleep_time = random.random()
        time.sleep(sleep_time )

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Send messages to Redis stream')
parser.add_argument('--threads', type=int, default=1,
                    help='Number of threads to use')
parser.add_argument('--messages', type=int, default=10,
                    help='Number of messages to send')
parser.add_argument('--sleep', type=float, default=0.1,
                    help='Sleep time between messages in seconds')
args = parser.parse_args()

# Create a thread pool
with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
    # Submit tasks to the thread pool
    futures = []
    for i in range(args.threads):
        futures.append(executor.submit(send_messages, args.messages, args.sleep, i))

    # Wait for all tasks to complete
    for future in futures:
        future.result()
