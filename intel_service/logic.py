from confluent_kafka import Consumer, Producer
from shared.logger import log_event
import logging
import json
from pymongo import MongoClient
from intel_service.haversine import haversine_km
import os

logger = logging.getLogger("intel service")
logging.basicConfig(filename='intel service', level=logging.INFO)


SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
conf = {'bootstrap.servers': SERVER,
        'group.id': 'intel',
        'auto.offset.reset': 'earliest'}

producer = Producer({'bootstrap.servers': SERVER})
consumer = Consumer(conf)


WRITING_TOPIC = os.getenv('WRITING_TOPIC', 'dlq_signals_intel')
LISTENING_TOPIC = os.getenv('INTEL_SERVICE_LISTENING_TOPIC', 'intel')

consumer.subscribe([LISTENING_TOPIC])

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')

MONGO_URI = 'mongodb://localhost:27017'
client = MongoClient(MONGO_URI)

db = client['digital_hunter1']
destroyed_targets = db['destroyed_targets']
targets_bank = db['targets_bank']

def validate(intel):
    if len(intel) < 7: # if the intel has missing fields
        intel['error'] = 'missing fileds'
        logger.error(f'{intel},missing fields')
        log_event('error',f'error in intel service, {intel}, missing fields')

        print('missing fields')
        intel = json.dumps(intel)
        producer.produce(intel, WRITING_TOPIC)
        return False

    if intel['entity_id'].startswith('TGT-UNKNOWN-'):
        intel['error'] = 'false entity'
        logger.error(f'{intel['entity_id']}, false entity')
        log_event('error',f'error in intel service, {intel['entity_id']}, false entity')
        
        intel = json.dumps(intel)
        producer.produce(intel, WRITING_TOPIC)
        return False
        
    # TODO check if its in the destroyed targets
    return True

def targets_bank_validate(intel):
    entity_id = intel['entity_id']

    if not targets_bank.find_one({'entity_id': entity_id}): # if its not in the targets bank
        intel['priority_level'] = 99
        targets_bank.insert_one(intel)

        logger.info(f'sent {entity_id} to mongo')
        log_event('info',f'sent {entity_id} to mongo')
        

def distance_calc(intel):
    mongo_intel = targets_bank.find_one({'entity_id': intel['entity_id']})

    lat1, lon1 = intel['reported_lat'],intel['reported_lon']
    lat2, lon2 = mongo_intel['reported_lat'], mongo_intel['reported_lon']

    result = haversine_km(lat1, lon1, lat2, lon2)
    intel['distance_from_last_save'] = result
    targets_bank.update_one(
        {'_id': mongo_intel['_id']}, 
        {'$set': {'distance_from_last_save': result}}
    )
    





