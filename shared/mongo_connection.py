from pymongo import MongoClient
import os



MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017')

MONGO_URI = 'mongodb://localhost:27017'
client = MongoClient(MONGO_URI)

db = client['digital_hunter1']
destroyed_targets = db['destroyed_targets']
targets_bank = db['targets_bank']
attacks = db['attacks']