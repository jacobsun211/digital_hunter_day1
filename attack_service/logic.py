from shared.mongo_connection import targets_bank, destroyed_targets, attacks
from shared.type_validation import Attack
from confluent_kafka import Consumer, Producer
from shared.logger import log_event
import logging
import json
import os


logger = logging.getLogger("attack service")
logging.basicConfig(filename='attack service', level=logging.INFO)


SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
conf = {'bootstrap.servers': SERVER,
        'group.id': 'attack',
        'auto.offset.reset': 'earliest'}

producer = Producer({'bootstrap.servers': SERVER})
consumer = Consumer(conf)


WRITING_TOPIC = os.getenv('WRITING_TOPIC', 'dlq_signals_intel')
LISTENING_TOPIC = os.getenv('ATTACK_SERVICE_LISTENING_TOPIC', 'attack')

consumer.subscribe([LISTENING_TOPIC])



def validate(attack: Attack):
    if len(attack) < 4:
        attack['error'] = 'missing fileds'
        logger.error(f'{attack},missing fields')
        log_event('error',f' {attack}, missing fields')

        attack = json.dumps(attack)
        producer.produce(attack, WRITING_TOPIC)
        return False
    
    if not targets_bank.find_one({'entity_id': attack['entity_id']}): # if its not in the targets bank
        logger.error(f'{attack['entity_id']}, not exist in targets_bank, attack is impossible')
        log_event('error',f'{attack['entity_id']}, not exist in targets_bank, attack is impossible')
        error = True
        
    if destroyed_targets.find({'entity_id': attack['entity_id']}):
        logger.error(f'{attack['entity_id']}, already destroyed')
        log_event('error',f'{attack['entity_id']}, already destroyed')
        error = True
        
    if error:
        attack = json.dumps(attack)
        producer.produce(attack, WRITING_TOPIC)
        return False

    return True

def update(attack):
    mongo_intel = targets_bank.find_one({'entity_id': attack['entity_id']})
    targets_bank.update_one(
        {'_id': mongo_intel['_id']}, 
        {'$set': {'attacked': True}}) # updating the existing collection of intel
    
    if not attacks.find_one({'entity_id': attack['entity_id']}): # if its not in attacks, add it
        attacks.insert_one(attack)
        return
    



