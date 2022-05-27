""" @author: Michael Lin """
import logging
import pandas as pd

from abc import ABC, abstractmethod
from utils.database.db_service import RedshiftConnection, SqlalchemyConnection
from utils.data_extractor.utils import get_db_data, get_csv_data, get_api_data, get_aws_data

logger = logging.getLogger('root')


class ExtractorFactory(ABC):
    """
    Extract abstract factory
    This interface is used to expand into a family of products defined below where depends on different methodology of
    extractor is efined as a different concrete factory
    """
    def __init__(self, entity, date):
        self._entity = entity
        self._date = date

    @abstractmethod
    def create_extractor(self):
        pass


class AwsExtractorFactory(ExtractorFactory):
    """
    Aws extractor concrete factory
    This factory produces a family of products related to Aws
    """
    def __init__(self, entity, date, sql):
        super(AwsExtractorFactory, self).__init__(entity, date)
        self._aws_sql = sql

    def create_extractor(self):
        return AwsDataExtractor(self._entity, self._date, self._aws_sql)


class DbExtractorFactory(ExtractorFactory):
    """
    Db extractor concrete factory
    This factory produces a family of products related to Db
    """
    def __init__(self, entity, date, dsn, sql):
        super(DbExtractorFactory, self).__init__(entity, date)
        self._dsn = dsn
        self._db_sql = sql

    def create_extractor(self):
        return DbDataExtractor(self._entity, self._date, self._dsn, self._db_sql)


class ApiExtractorFactory(ExtractorFactory):
    """
    Api extractor concrete factory
    This factory produces a family of products related to API
    """
    def __init__(self, entity, date, url):
        super(ApiExtractorFactory, self).__init__(entity, date)
        self._api_call = url

    def create_extractor(self):
        return ApiDataExtractor(self._entity, self._date, self._api_call)


class CsvExtractorFactory(ExtractorFactory):
    """
    Csv extractor concrete factory
    This factory produces a family of products related to CSV
    """
    def __init__(self, entity, date, path):
        super(CsvExtractorFactory, self).__init__(entity, date)
        self._file_path = path

    def create_extractor(self):
        return CsvDataExtractor(self._entity, self._date, self._file_path)


class DataExtractor(ABC):
    """ Data extractor base interface with each variant implementing it differently """
    def __init__(self, entity, date):
        self._entity = entity
        self._date = date

    @abstractmethod
    def extract_data(self) -> pd.DataFrame():
        pass


class AwsDataExtractor(DataExtractor):
    """ Aws data extractor """
    def __init__(self, entity, date, sql):
        super(AwsDataExtractor, self).__init__(entity, date)
        self._aws_conn = RedshiftConnection()
        self._aws_sql = sql

    def extract_data(self) -> pd.DataFrame():
        logger.info(f'Extracting data through AWS for {self._entity} on {self._date}')
        try:
            return get_aws_data(sql=self._aws_sql, conn=self._aws_conn)
        except Exception as e:
            logger.error(f'Having trouble extracting {self._entity} data through AWS')
            raise e


class DbDataExtractor(DataExtractor):
    """ Db data extractor """
    def __init__(self, entity, date, dsn, sql):
        super(DbDataExtractor, self).__init__(entity, date)
        self._dsn = dsn
        self._db_sql = sql
        self._engine = SqlalchemyConnection(dsn=self._dsn, app_name=__name__).setup()

    def extract_data(self) -> pd.DataFrame():
        logger.info(f'Extracting data through DB for {self._entity} on {self._date}')
        try:
            return get_db_data(sql=self._db_sql, conn=self._engine)
        except Exception as e:
            logger.error(f'Having trouble extracting {self._entity} data through DB')
            raise e


class ApiDataExtractor(DataExtractor):
    """ Api data extractor """
    def __init__(self, entity, date, url):
        super(ApiDataExtractor, self).__init__(entity, date)
        self._api_call = url

    def extract_data(self) -> pd.DataFrame():
        logger.info(f'Extracting data through API for {self._entity} on {self._date}')
        try:
            return get_api_data(url=self._api_call)
        except Exception as e:
            logger.error(f'Having trouble extracting {self._entity} data through API')
            raise e


class CsvDataExtractor(DataExtractor):
    """ Csv data extractor """
    def __init__(self, entity, date, path):
        super(CsvDataExtractor, self).__init__(entity, date)
        self._file_path = path

    def extract_data(self) -> pd.DataFrame():
        logger.info(f'Extracting data through CSV for {self._entity} on {self._date}')
        try:
            return get_csv_data(file_path=self._file_path)
        except Exception as e:
            logger.error(f'Having trouble extracting {self._entity} data through CSV')
            raise e


def run_extractor(factory: ExtractorFactory) -> pd.DataFrame():
    """ This function calls the abstract factory which instantiates the appropriate concrete factory """
    return factory.create_extractor()