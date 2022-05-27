""" @author: Michael Lin """
import functools
import logging
import os
import pandas as pd

logger = logging.getLogger('root')


def cache(func):
    """
    Decorator creating a cache
    This provides better performance than querying important but relatively static table multiple times
    Cache is stored as a csv file in the desired file path with a desired file name
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        file_name = kwargs['file_name'] + '.csv'
        path = os.path.join(kwargs['file_path'], file_name)
        if not os.path.isfile(path):
            logger.info(f'Writing data to cache: location {path}')
            result = func(*args, **kwargs)
        else:
            logger.info(f'Reading data from cache: location {path}')
            result = pd.read_csv(path, index_col=[0])
        return result
    return wrapper
