import datetime
import csv
import json
import types

import yfinance

from kaxanuk.data_curator.entities import (
    Configuration,
    DividendData,
    FundamentalData,
    MarketData,
    MarketDataDailyRow,
    SplitData,
    Ticker,
)
from kaxanuk.data_curator.exceptions import (
    EntityFieldTypeError,
    EntityProcessingError,
    EntityTypeError,
    EntityValueError,
    MarketDataEmptyError,
    MarketDataRowError,
)
from kaxanuk.data_curator.data_providers.data_provider_interface import DataProviderInterface
from kaxanuk.data_curator.services import entity_helper


class YahooFinance(DataProviderInterface):

    _endpoints = types.MappingProxyType({
        'historical-price-full': 'https://query1.finance.yahoo.com/v7/finance/download',
    })

    _fields_market_data_daily_rows = types.MappingProxyType({
        'date': 'Date',
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'adjusted_close': 'Adj Close',
        'volume': 'Volume',
        'vwap': None
    })

    def __init__(self):
        self.stock_data = None

    def get_dividend_data(
        self,
        *,
        ticker: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> DividendData:
        return DividendData(
            ticker=Ticker(ticker),
            rows={}
        )

    def get_fundamental_data(
        self,
        *,
        ticker: str,
        period: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> FundamentalData:
        return FundamentalData(
            ticker=Ticker(ticker),
            rows={}
        )

    def get_market_data(
        self,
        *,
        ticker: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> MarketData:
        """
        Get the market data from the FMP web service wrapped in a MarketData entity.

        Parameters
        ----------
        ticker
            the stock's ticker
        start_date
        end_date

        Returns
        -------
        MarketData

        Raises
        ------
        ConnectionError
        """
        stock_data = yfinance.download(
            ticker,
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d")
        )

        market_raw_data = stock_data.to_csv()

        return self._create_market_data_from_raw_stock_response(ticker, market_raw_data)


    def get_market_data_old(
        self,
        *,
        ticker: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> MarketData:
        """
        Get the market data from the FMP web service wrapped in a MarketData entity.

        Parameters
        ----------
        ticker
            the stock's ticker
        start_date
        end_date

        Returns
        -------
        MarketData

        Raises
        ------
        ConnectionError
        """
        date1 = int(datetime.datetime(
            start_date.year,
            start_date.month,
            start_date.day,
            tzinfo=datetime.UTC
        ).timestamp())
        date2 = int(datetime.datetime(
            end_date.year,
            end_date.month,
            end_date.day,
            tzinfo=datetime.UTC
        ).timestamp())

        endpoint_id = "historical-price-full"
        market_raw_data = self._request_data(
            endpoint_id,
            self._endpoints[endpoint_id],
            ticker,
            {
                "period1": str(date1),
                "period2": str(date2),
                "interval": '1d',
                "events": 'history',
                "includeAdjustedClose": 'true',
            }
        )

        reader = csv.DictReader(market_raw_data.strip().splitlines())

        json_data = json.dumps(list(reader))

        market_raw_data = str({
            "symbol": f"{ticker}",
            "historical": f"{json_data}"
        }).replace("'", '"').replace('"[', '[').replace(']"', ']')

        return self._create_market_data_from_raw_stock_response(ticker, market_raw_data)

    def get_split_data(
        self,
        *,
        ticker: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> SplitData:
        return SplitData(
            ticker=Ticker(ticker),
            rows={}
        )

    def init_config(
        self,
        *,
        configuration: Configuration,
    ) -> None:
        self.stock_data = yfinance.download(
            configuration.tickers,
            start=configuration.start_date.strftime("%Y-%m-%d"),
            end=configuration.end_date.strftime("%Y-%m-%d"),
            actions=True,
            group_by='ticker',
        )

    def validate_api_key(
        self,
    ) -> bool:
        """
        Validate that the API key used to init the class is valid, in this case there's no key so always True

        Returns
        -------
        Always True
        """
        return True

    @classmethod
    def _create_market_data_from_raw_stock_response(
        cls,
        ticker: str,
        raw_stock_response: str
    ) -> MarketData:
        """
        Populate a MarketData entity from the web service raw data.

        Parameters
        ----------
        ticker : str
        raw_stock_response : str

        Returns
        -------
        MarketData

        Raises
        ------
        EntityProcessingError
        """
        market_data_rows = {}
        try:
            if raw_stock_response is None:
                raise MarketDataEmptyError("No data returned by market data endpoint")

            json_data = json.loads(raw_stock_response)
            if 'historical' not in json_data:
                raise MarketDataEmptyError("Historical market data missing")
            else:
                raw_stock_data = json_data['historical']

            stock_data = sorted(raw_stock_data, key=lambda x: x['Date'])
            min_date = None
            max_date = None
            for stock_row in stock_data:
                date = datetime.date.fromisoformat(stock_row['Date'])
                stock_row['vwap'] = None
                try:
                    attributes = entity_helper.fill_fields(
                        stock_row,
                        cls._fields_market_data_daily_rows,
                        MarketDataDailyRow
                    )
                    market_data_rows[stock_row['Date']] = MarketDataDailyRow(
                        **attributes
                    )
                except (
                        EntityFieldTypeError,
                        EntityTypeError,
                        EntityValueError,
                ) as error:
                    msg = f"date: {date}"
                    raise MarketDataRowError(msg) from error

                if (
                        min_date is None
                        or date < min_date
                ):
                    min_date = date
                if (
                        max_date is None
                        or date > max_date
                ):
                    max_date = date

            market_data = MarketData(
                start_date=min_date,
                end_date=max_date,
                ticker=Ticker(ticker),
                daily_rows=market_data_rows
            )
        except (
                MarketDataEmptyError,
                MarketDataRowError
        ) as error:
            raise EntityProcessingError("Market data processing error") from error

        return market_data
