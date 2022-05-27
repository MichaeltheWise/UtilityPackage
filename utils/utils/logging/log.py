""" @author: Michael Lin """
import logging


def setup_custom_logger(name):
    """
    Provide global custom logger
    :param name: name
    :return: logger
    """
    formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger
