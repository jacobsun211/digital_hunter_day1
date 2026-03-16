from shared.logger import log_event
import json
from attack_service.logic import validate, update, logger, consumer



    

def main():
    while True:
        attack = consumer.poll(1)
        if attack is None: continue
        if attack.error():
            logger.error('error trying to pull from kafka')
            log_event('error','error trying to pull from kafka')
        try:
            attack = json.loads(attack.value().decode("utf-8"))
        except json.JSONDecodeError:
            continue
        print(attack)
        if not validate(attack):
            continue
        update(attack)

main()

# python -m attack_service.main
