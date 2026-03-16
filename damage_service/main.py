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
        'group.id': 'damage',
        'auto.offset.reset': 'earliest'}

producer = Producer({'bootstrap.servers': SERVER})
consumer = Consumer(conf)


WRITING_TOPIC = os.getenv('WRITING_TOPIC', 'dlq_signals_intel')
LISTENING_TOPIC = os.getenv('DAMAGE_SERVICE_LISTENING_TOPIC', 'damage')

consumer.subscribe([LISTENING_TOPIC])

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')

MONGO_URI = 'mongodb://localhost:27017'
client = MongoClient(MONGO_URI)

db = client['digital_hunter1']
destroyed_targets = db['destroyed_targets']
targets_bank = db['targets_bank']

def validate(damage):
    if len(damage) < 4:
        damage['error'] = 'missing fileds'
        logger.error(f'{damage},missing fields')
        log_event('error',f' {damage}, missing fields')

        damage = json.dumps(damage)
        producer.produce(damage, WRITING_TOPIC)
        return False
    
    if not targets_bank.find_one({'entity_id': damage['entity_id']}): 
        logger.error(f'{damage['entity_id']}, not exist in targets_bank, attack is impossible')
        log_event('error',f'{damage['entity_id']}, not exist in targets_bank, attack is impossible')
        # send to kafka
        damage = json.dumps(damage)
        producer.produce(damage, WRITING_TOPIC)
        return False
        
    
    # TODO check if its in the destroyed targets
    return True

def update(damage):
    if damage['status'] == 'destroyed':
        destroyed_targets.insert_one(damage)

    mongo_intel = targets_bank.find_one({'entity_id': damage['entity_id']})
    targets_bank.update_one(
        {'_id': mongo_intel['_id']}, 
        {'$set': {'status': damage['status']}}) 
    
    
    



from shared.logger import log_event
import json
from attack_service.logic import validate, update, logger, consumer



    

def main():
    while True:
        print('listening')
        damage = consumer.poll(1)
        if damage is None: continue
        if damage.error():
            logger.error('error trying to pull from kafka')
            log_event('error','error trying to pull from kafka')
        try:
            damage = json.loads(damage.value().decode("utf-8"))
        except json.JSONDecodeError: 
            continue
        print(damage)
        if not validate(damage):
            continue
        update(damage)
        # break

main()

# python -m damage_service.main
