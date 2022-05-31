""" @author: Michael Lin """
import logging
import pandas as pd

from factory import AwsExtractorFactory, ApiExtractorFactory, DbExtractorFactory, CsvExtractorFactory, run_extractor

logger = logging.getLogger('root')


class DataExtractor:
    """ Data extractor """

    def __init__(self, source, entity, date, dsn):
        self._source = source
        self._entity = entity
        self._date = date
        self._dsn = dsn

    def run(self, **kwargs):
        df = pd.DataFrame()
        if self._source == 'AWS':
            df = run_extractor(
                AwsExtractorFactory(entity=self._entity, date=self._date, sql=kwargs['sql'])).extract_data()
        elif self._source == 'DB':
            df = run_extractor(DbExtractorFactory(entity=self._entity, date=self._date, dsn=self._dsn,
                                                  sql=kwargs['sql'])).extract_data()
        elif self._source == 'API':
            df = run_extractor(ApiExtractorFactory(entity=self._entity, date=self._date,
                               url=kwargs['url'])).extract_data()
        elif self._source == 'CSV':
            df = run_extractor(
                CsvExtractorFactory(entity=self._entity, date=self._date, path=kwargs['path'])).extract_data()
        return df


if __name__ == '__main__':
    # Swap between sources
    test_data_extractor = DataExtractor(source='AWS', entity='GLOBAL_TRADING_DESK', date='2022-04-02', dsn=None)
    test_data_extractor.run()
