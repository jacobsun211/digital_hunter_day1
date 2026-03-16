from confluent_kafka import Consumer, Producer
from shared.logger import log_event
import logging
import json
from pymongo import MongoClient
from intel_service.haversine import haversine_km
import os
from shared.type_validation import Intel
from shared.mongo_connection import targets_bank, destroyed_targets

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



def validate(intel: Intel):

    if len(intel) < 7: # if the intel has missing fields
        intel['error'] = 'missing fileds'
        logger.error(f'{intel},missing fields')
        log_event('error',f'error in intel service, {intel}, missing fields')

        damage = json.dumps(damage)
        producer.produce(damage, WRITING_TOPIC)
        return False # i have to return here, or it will crash trying to access intel['entity_id'] later, since it's missing :/


    if intel['entity_id'].startswith('TGT-UNKNOWN-'):
        error = True
        intel['error'] = 'false entity'
        logger.error(f'{intel['entity_id']}, false entity')
        log_event('error',f'error in intel service, {intel['entity_id']}, false entity')
        
        
    if not targets_bank.find_one({'entity_id': damage['entity_id']}): 
        error = True
        logger.error(f'{damage['entity_id']}, not exist in targets_bank, damage is impossible')
        log_event('error',f'{damage['entity_id']}, not exist in targets_bank, damage is impossible')
        
    if error:
        damage = json.dumps(damage)
        producer.produce(damage, WRITING_TOPIC)
        return False

    
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
    





