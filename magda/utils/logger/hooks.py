import logging


def actor_logging_hook():
    logging.basicConfig()
    logging.getLogger('magda').setLevel(logging.DEBUG)
