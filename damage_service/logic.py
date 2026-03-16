from confluent_kafka import Consumer, Producer
from shared.logger import log_event
import logging
import json
from pymongo import MongoClient
import os

logger = logging.getLogger("damage service")
logging.basicConfig(filename='damage service', level=logging.INFO)


SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
conf = {'bootstrap.servers': SERVER,
        'group.id': 'damage',
        'auto.offset.reset': 'earliest'}

producer = Producer({'bootstrap.servers': SERVER})
consumer = Consumer(conf)


WRITING_TOPIC = os.getenv('WRITING_TOPIC', 'dlq_signals_intel')
LISTENING_TOPIC = os.getenv('damage_SERVICE_LISTENING_TOPIC', 'damage')

consumer.subscribe([LISTENING_TOPIC])

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')

MONGO_URI = 'mongodb://localhost:27017'
client = MongoClient(MONGO_URI)

db = client['digital_hunter1']
destroyed_targets = db['destroyed_targets']
targets_bank = db['targets_bank']
damages = db['damages']

def validate(damage):
    if len(damage) < 4:
        damage['error'] = 'missing fileds'
        logger.error(f'{damage},missing fields')
        log_event('error',f' {damage}, missing fields')

        damage = json.dumps(damage)
        producer.produce(damage, WRITING_TOPIC)
        return False
    
    if not targets_bank.find_one({'entity_id': damage['entity_id']}): 
        logger.error(f'{damage['entity_id']}, not exist in targets_bank, damage is impossible')
        log_event('error',f'{damage['entity_id']}, not exist in targets_bank, damage is impossible')
        # send to kafka
        damage = json.dumps(damage)
        producer.produce(damage, WRITING_TOPIC)
        return False
        
    
    return True

def update(damage):
    if damage['result'] == 'destroyed':
        if not destroyed_targets.find_one({'entity_id': damage['entity_id']}):
            destroyed_targets.insert_one(damage)
    
    mongo_intel = targets_bank.find_one({'entity_id': damage['entity_id']})
    targets_bank.update_one(
        {'_id': mongo_intel['_id']}, 
        {'$set': {'status': damage['result']}}) # updating the existing collection of intel as well
    
    
    return
    



