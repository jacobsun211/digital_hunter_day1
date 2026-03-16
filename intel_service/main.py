from shared.logger import log_event
import json
from intel_service.logic import validate, targets_bank_validate, distance_calc, logger, consumer


def main():
    while True:
        intel = consumer.poll(1)
        if intel is None: continue
        if intel.error():
            logger.error('error in intel service, trying to pull from kafka')
            log_event('error','error in intel service, trying to pull from kafka')
        try:
            intel = json.loads(intel.value().decode("utf-8"))
        except (json.JSONDecodeError): # to catch intel that was sent in bytes and not in json
            continue
        print(intel)
        if not validate(intel):
            continue

        targets_bank_validate(intel)
        distance_calc(intel)

    


main()


# python -m intel_service.main



