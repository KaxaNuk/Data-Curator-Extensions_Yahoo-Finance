import datetime
import types
import typing

import pandas
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
    TickerNotFoundError,
)
from kaxanuk.data_curator.data_providers.data_provider_interface import DataProviderInterface
from kaxanuk.data_curator.services import entity_helper


class YahooFinance(DataProviderInterface):

    _config_to_yf_periods = types.MappingProxyType({
        'annual': '1y',
        'quarterly': '3mo',
    })

    _fields_market_data_daily_rows = types.MappingProxyType({
        'date': 'Date',
        'open': 'Open',
        'high': 'High',
        'low': 'Low',
        'close': 'Close',
        'adjusted_close': None,
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
        if (
            self.stock_data is None
            or ticker not in self.stock_data
        ):
            raise TickerNotFoundError(f"No market data for ticker {ticker}")

        return self._create_market_data_from_response_dataframe(
            ticker,
            self.stock_data[ticker]
        )

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
            period=self._config_to_yf_periods[configuration.period],
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
    def _create_market_data_from_response_dataframe(
        cls,
        ticker: str,
        response_dataframe: pandas.DataFrame
    ) -> MarketData:
        """
        Populate a MarketData entity from the web service raw data.

        Parameters
        ----------
        ticker
            The ticker symbol
        response_dataframe
            The ticker's dataframe

        Returns
        -------
        MarketData

        Raises
        ------
        EntityProcessingError
        """
        market_data_rows = {}
        try:
            if response_dataframe is None:
                raise MarketDataEmptyError("No data returned by market data endpoint")

            timestamps = response_dataframe.index.to_series()
            # dates = timestamps.dt.date

            min_date = None
            max_date = None

            for timestamp in timestamps:
                price_date = timestamp.date()
                price_date_string = price_date.isoformat()
                try:
                    date_indexed_row = response_dataframe.loc[timestamp]
                    # date_row = date_indexed_row.reset_index()
                    # date_row.index.astype(str)
                    # stock_row = date_row.to_dict()
                    stock_row = (
                        {cls._fields_market_data_daily_rows['date']: price_date_string}
                        | date_indexed_row.to_dict()
                    )
                    attributes = entity_helper.fill_fields(
                        stock_row,
                        dict(cls._fields_market_data_daily_rows),
                        MarketDataDailyRow
                    )
                    market_data_rows[price_date_string] = MarketDataDailyRow(
                        **attributes
                    )
                except (
                    EntityFieldTypeError,
                    EntityTypeError,
                    EntityValueError,
                ) as error:
                    msg = f"date: {price_date_string}"
                    raise MarketDataRowError(msg) from error

                if (
                    min_date is None
                    or price_date < min_date
                ):
                    min_date = price_date
                if (
                    max_date is None
                    or price_date > max_date
                ):
                    max_date = price_date

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

    @staticmethod
    def _cast_dict_keys_to_string(dictionary: dict) -> dict:
        """
        Cast the dictionary keys to string.

        This is just to help typecheckers understand that the keys of `dictionary` are strings.

        Parameters
        ----------
        dictionary
            The dict to typecast

        Returns
        -------
        dict
            The same dict after typecasting the keys to string
        """
        return typing.cast(
            dictionary,
            dict[str, typing.Any]
        )
