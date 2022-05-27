""" @author: Michael Lin """
import requests
import logging
import time

logger = logging.getLogger('root')


def make_request_with_retries(endpoint, headers=None, verify=False, retries=5, sleep_time=60):
    """ Make request with retries """
    json_response = None
    logger.info("Making get request for endpoint {endpoint}".format(endpoint=endpoint))
    while retries >= 0:
        try:
            resp = requests.get(endpoint, headers=headers, verify=verify)
            resp.raise_for_status()
            json_response = resp.json()
            break
        except (ValueError, requests.exceptions.HTTPError) as e:
            if retries != 0:
                retries -= 1
                time.sleep(sleep_time)
                logger.warning("Retrying {endpoint}".format(endpoint=endpoint))
                continue
            else:
                logger.error(
                    "Endpoint request failed with exception: {error}".format(error=e))
                raise e

    return json_response
