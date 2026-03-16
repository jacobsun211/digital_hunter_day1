from shared.logger import log_event
import json
from damage_service.logic import validate, update, logger, consumer



    

def main():
    while True:
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

main()

# python -m damage_service_copy.main