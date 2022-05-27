""" @author: Michael Lin """
import logging
import pandas as pd

from utils.utils.db_utils.api_helper import make_request_with_retries

logger = logging.getLogger('root')


def get_aws_data(sql, conn):
    """
    Extract data from AWS
    :param sql: sql
    :param conn: Redshift Connection
    :return: dataframe
    """
    try:
        with conn.connection as aws_conn:
            df = pd.read_sql_query(sql, aws_conn)
    except Exception as e:
        logger.warning("Having trouble getting data from AWS: {error}".format(error=e))
        logger.warning("Empty dataframe returned")
        df = pd.DataFrame()
    return df


def get_api_data(url):
    """
    Extract data from API
    :param url: url
    :return: dataframe
    """
    json_resp = make_request_with_retries(url)
    if not json_resp:
        raise ValueError('No data found')
    try:
        df = pd.DataFrame(json_resp['data'])
    except Exception as e:
        logger.warning("Having trouble getting data from API calls: {error}".format(error=e))
        logger.warning("Empty dataframe returned")
        df = pd.DataFrame()
    return df


def get_db_data(conn, sql):
    """
    Extract data from database using sqlalchemy
    :param conn: Sqlalchemy Connection
    :param sql: sql
    :return: dataframe
    """
    try:
        df = pd.read_sql_query(sql=sql, con=conn)
    except Exception as e:
        logger.warning("Having trouble getting data from DB: {error}".format(error=e))
        logger.warning("Empty dataframe returned")
        df = pd.DataFrame()
    return df


def get_csv_data(file_path):
    """
    Extract data from csv
    :param file_path: file_path
    :return: dataframe
    """
    try:
        df = pd.read_csv(file_path, index_col=0)
    except Exception as e:
        logger.warning("Having trouble getting data from CSV: {error}".format(error=e))
        logger.warning("Empty dataframe returned")
        df = pd.DataFrame()
    return df
