from confluent_kafka import Consumer, Producer
from shared.logger import log_event
import logging
import json
from pymongo import MongoClient
import os

logger = logging.getLogger("intel service")
logging.basicConfig(filename='attack service', level=logging.INFO)


SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS','localhost:9092')
conf = {'bootstrap.servers': SERVER,
        'group.id': 'intel',
        'auto.offset.reset': 'earliest'}

producer = Producer({'bootstrap.servers': SERVER})
consumer = Consumer(conf)


WRITING_TOPIC = os.getenv('ATTACK_SERVICE_WRITING_TOPIC', 'dlq_signals_intel')
LISTENING_TOPIC = os.getenv('ATTACK_SERVICE_LISTENING_TOPIC', 'attack')

consumer.subscribe([LISTENING_TOPIC])

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')

MONGO_URI = 'mongodb://localhost:27017'
client = MongoClient(MONGO_URI)

db = client['digital_hunter1']
destroyed_targets = db['destroyed_targets']
targets_bank = db['targets_bank']
attacks = db['attacks']

def validate(attack):
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
        # send to kafka
        attack = json.dumps(attack)
        producer.produce(attack, WRITING_TOPIC)
        return False
        
    
    # TODO check if its in the destroyed targets
    return True

def update(attack):
    mongo_intel = targets_bank.find_one({'entity_id': attack['entity_id']})
    targets_bank.update_one(
        {'_id': mongo_intel['_id']}, 
        {'$set': {'attacked': True}}) # updating the existing collection of intel
    
    if not attacks.find_one({'entity_id': attack['entity_id']}): # if its not in attacks, add it
        attacks.insert_one(attack)
        return
    

def main():
    while True:
        attack = consumer.poll(1)
        if attack is None: continue
        if attack.error():
            logger.error('error in intel service, trying to pull from kafka')
            log_event('error','error in intel service, trying to pull from kafka')
        try:
            attack = json.loads(attack.value().decode("utf-8"))
        except (json.JSONDecodeError): # to catch intel that was sent in bytes and not in json
            continue
        print(attack)
        if not validate(attack):
            continue
        update(attack)

main()

# python -m attack_service.main
