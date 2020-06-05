from typing import List

from influxdb_client import InfluxDBClient, Point
from pandas import concat, MultiIndex, Series, Timestamp, Timedelta, notnull

from qapio_influx_api.FluxQueryBuilder import FluxQueryBuilder

datasetCache = {}

def timestamp2str(date: Timestamp):
    return date.strftime('%Y-%m-%dT%H:%M:%SZ')


class DataSet:
    def __init__(self, data_frame):
        self.__data_frame = data_frame

    @property
    def data_frame(self):
        return self.__data_frame

    def series(self, measurement: str, field: str,
               from_date: Timestamp =
               None, to_date: Timestamp = None) -> Series:

        series = None

        try:
            if from_date is None and to_date is None:
                series = self.__data_frame.loc[(measurement, field)]

            if from_date is not None and to_date is not None:
                series = self.__data_frame.loc[measurement, field,
                         from_date:to_date]

            if from_date is not None and to_date is None:
                series = self.__data_frame.loc[measurement, field, from_date:]

            if from_date is None and to_date is not None:
                series = self.__data_frame.loc[measurement, field, :to_date]
        except:
            return None

        series = series.reset_index(drop=True)

        if len(series.index) == 0:
            return None

        series = series.pivot_table(
            index="_time",
            columns='_field',
            values='_value',
            aggfunc='first').rename_axis(None, axis=1)

        return series[field]

    def point_series(self, measurements: List[str], field: str,
                     date: Timestamp):

        data = []

        for ticker in measurements:
            p = self.point(
                ticker,
                field,
                date)

            data.append(p)

        return Series(data, index=measurements)

    def point(self, measurement: str, field: str, date: Timestamp):
        series = self.series(measurement, field, date, date)

        if series is None:
            return None

        try:
            point = series[date]
            return point
        except:
            return None

    def last(self, measurement: str, field: str, date: Timestamp):
        series = self.series(measurement, field, None, date)

        if series is None:
            return None

        return series.tail(1)[0]


class InfluxSource:
    def __init__(self, host: str, port: int, token: str, org: str):
        self.__org = org
        self.__influx_client = InfluxDBClient(
            url=f'{host}:{port}',
            token=token,
            org=org, enable_gzip=True)

    def query(self, query: str):
        query_client = self.__influx_client.query_api()

        response = query_client.query_data_frame(query)

        return response

    def dataset(self, database: str, tickers: List[str], fields:
    List[str],
                from_date: Timestamp,
                to_date: Timestamp, tags=dict(
                {})):

        _tags = []

        for tag in tags:
            _tags.append((tag, tags[tag]))

        ticker_string = '|'.join(tickers)
        field_string = '|'.join(fields)

        query = FluxQueryBuilder() \
            .bucket(database) \
            .range(timestamp2str(from_date), timestamp2str(to_date)) \
            .filters([("_measurement", [ticker_string]), ("_field",
                                                          [field_string])] +
                     _tags, equality="=~").do()

        query_client = self.__influx_client.query_api()

        cached = datasetCache.get(query)

        if cached is not None:
            return cached

        response = query_client.query_data_frame(query)

        if isinstance(response, list):
            response = concat(response)

        if response.empty:
            return None

        response = response.drop(columns=["table", "_start", "_stop",
                                          "result"])

        nonCatCols = ["_time", "_value"]

        for col in response.columns:
            if col not in nonCatCols:
                response[col] = response[col].astype("category")

        response = response.set_index(MultiIndex.from_frame(response[[
            "_measurement", "_field", "_time",
        ]], names=["_measurement", "_field", "_time", ]))

        response = response.sort_index()

        data = DataSet(response)
        datasetCache[query] = data
        return data

    def series(self, database: str, measurement: str, field: str, from_date:
    Timestamp, to_date: Timestamp,
               tags=dict({})):

        dataset = self.dataset(database, [measurement], [field], from_date,
                               to_date, tags)

        if dataset is None:
            return None

        return dataset.series(measurement, field, from_date, to_date)

    def point(self, database: str, measurement: str, field: str,
              date: Timestamp):

        series = self.series(database, measurement, field, date,
                             date + Timedelta(seconds=1))

        if series is None:
            return None

        try:
            point = series[date]
            return point
        except:
            return None
