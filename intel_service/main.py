from confluent_kafka import Consumer, Producer
from shared.logger import log_event
import logging
import json
import pymongo

logger = logging.getLogger("intel service")
logging.basicConfig(filename='intel service', level=logging.INFO)


SERVER = 'localhost:9092'
conf = {'bootstrap.servers': SERVER,
        'group.id': 'intel',
        'auto.offset.reset': 'earliest'}

producer = Producer(SERVER)
consumer = Consumer(conf)
WRITING_TOPIC = 'dlq_signals_intel'
LISTENING_TOPIC = 'intel'
consumer.subscribe([LISTENING_TOPIC])

        # producer.produce(intel)

def validate(intel):
    if len(intel != 7): # if the intel has missing fields
        print('error')
        intel['error'] = 'missing fileds'


while True:
    print('listening')
    intel = consumer.poll(1)
    if intel is None: continue
    if intel.error():
        logger.error('error in intel service, trying to pull from kafka')
        log_event('error','error in intel service, trying to pull from kafka')
    intel = json.loads(intel.value())
    response = validate(intel)
    break





# python -m intel_service.main