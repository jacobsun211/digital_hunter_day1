from confluent_kafka import Consumer, Producer
from shared.logger import log_event
import logging
import json
from pymongo import MongoClient
from intel_service.haversine import haversine_km

logger = logging.getLogger("intel service")
logging.basicConfig(filename='intel service', level=logging.INFO)


SERVER = 'localhost:9092'
conf = {'bootstrap.servers': SERVER,
        'group.id': 'intel',
        'auto.offset.reset': 'earliest'}

producer = Producer({'bootstrap.servers': SERVER})
consumer = Consumer(conf)
WRITING_TOPIC = 'dlq_signals_intel'
LISTENING_TOPIC = 'intel'
consumer.subscribe([LISTENING_TOPIC])


MONGO_URI = 'mongodb://localhost:27017'
client = MongoClient(MONGO_URI)
db = client['digital_hunter1']
destroyed_targets = db['destroyed_targets']
targets_bank = db['targets_bank']


def validate(intel):
    print('in validate')
    if len(intel) < 7: # if the intel has missing fields
        intel['error'] = 'missing fileds'
        producer.produce(intel, WRITING_TOPIC)
        print('missing fields')
        return False
    # TODO check if its in the destroyed targets
    return True

def targets_bank_validate(intel):
    intel_id = intel['signal_id']
    collection = db['targets_bank']

    if not collection.find_one({'signal_id': intel_id}): # if its not in the targets bank
        intel['priority_level'] = 99
        targets_bank.insert_one(intel)
        print('inserted')

        logger.info(f'sent {intel_id} to mongo')
        log_event('info',f'sent {intel_id} to mongo')
        

def distance_calc(intel):
    mongo_intel = targets_bank.find_one({'signal_id': intel['signal_id']})
    lat1, lon1 = intel['reported_lat'],intel['reported_lon']
    lat2, lon2 = mongo_intel['reported_lat'], mongo_intel['reported_lon']
    result = haversine_km(lat1, lon1, lat2, lon2)
    intel['distance_from_last_save'] = result
    targets_bank.update_one(intel,{'$set':{'distance_from_last_save': result}})
    print('done')
    # TODO add distance_from_last_save 

while True:
    print('listening')
    intel = {'timestamp': '2026-03-16T08:56:10.357155+00:00', 'signal_id': 'e1da0eb1-2bfb-457e-8ca2-9c12a14fb41e', 'entity_id': 'TGT-003', 'reported_lat': 32.036316, 'reported_lon': 34.778057, 'signal_type': 'VISINT', 'priority_level': 1}
    # intel = consumer.poll(1)
    # if intel is None: continue
    # if intel.error():
        # logger.error('error in intel service, trying to pull from kafka')
        # log_event('error','error in intel service, trying to pull from kafka')
    # intel = json.loads(intel.value())
    # print(intel)
    # if not validate(intel):
    #     continue
    if not validate(intel):
        continue
    targets_bank_validate(intel)
    distance_calc(intel)
    
    break





# python -m intel_service.main