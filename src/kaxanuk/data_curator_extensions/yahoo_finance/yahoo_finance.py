import datetime
import decimal
import enum
import logging
import types

import pandas
import yfinance

from kaxanuk.data_curator.entities import (
    Configuration,
    DividendData,
    DividendDataRow,
    FundamentalData,
    MarketData,
    MarketDataDailyRow,
    SplitData,
    Ticker,
)
from kaxanuk.data_curator.exceptions import (
    DividendDataEmptyError,
    DividendDataRowError,
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
        'annual': 'yearly',
        'quarterly': 'quarterly',
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

    class MarketDataResponseHeaders(enum.StrEnum):
        DIVIDENDS = 'Dividends'

    def __init__(self):
        self.stock_general_data = None
        self.stock_market_data = None

    def get_dividend_data(
        self,
        *,
        ticker: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> DividendData:
        """
        Get the dividend data from the web service response, wrapped in a DividendData entity.

        Parameters
        ----------
        ticker
            the stock's ticker
        start_date
            the start date for the data
        end_date
            the end date for the data

        Returns
        -------
        DividendData

        """
        try:
            if (
                self.stock_market_data is None
                or ticker not in self.stock_market_data
            ):
                raise DividendDataEmptyError

            dividend_data = self._create_dividend_data_from_response_dataframe(
                ticker,
                self.stock_market_data[ticker]
            )
        except DividendDataEmptyError:
            msg = f"{ticker} has no dividend data obtained for the selected period, omitting its dividend data"
            logging.getLogger(__name__).warning(msg)
            dividend_data = DividendData(
                ticker=Ticker(ticker),
                rows={}
            )
        except DividendDataRowError as error:
            msg = f"{ticker} dividend data error: {error}"
            logging.getLogger(__name__).error(msg)
            dividend_data = DividendData(
                ticker=Ticker(ticker),
                rows={}
            )

        return dividend_data

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
        Get the market data from the web service wrapped in a MarketData entity.

        Parameters
        ----------
        ticker
            the stock's ticker
        start_date
            the start date for the data
        end_date
            the end date for the data

        Returns
        -------
        MarketData

        Raises
        ------
        EntityProcessingError
        TickerNotFoundError
        """
        if (
            self.stock_market_data is None
            or ticker not in self.stock_market_data
        ):
            raise TickerNotFoundError(f"No market data for ticker {ticker}")

        return self._create_market_data_from_response_dataframe(
            ticker,
            self.stock_market_data[ticker]
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
        """
        Download the ticker data required by the other interface implementation methods.

        Parameters
        ----------
        configuration
            The Configuration entity with the required data parameters
        """
        self.stock_general_data = yfinance.Tickers(
            " ".join(configuration.tickers)
        )
        self.stock_market_data = self.stock_general_data.history(
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
    def _create_dividend_data_from_response_dataframe(
        cls,
        ticker: str,
        response_dataframe: pandas.DataFrame
    ) -> DividendData:
        """
        Populate a DividendData entity from the web service raw data.

        Parameters
        ----------
        ticker
            The ticker symbol
        response_dataframe
            The ticker's dataframe

        Returns
        -------
        DividendData

        Raises
        ------
        DividendDataEmptyError
        DividendDataRowError
        """
        if cls.MarketDataResponseHeaders.DIVIDENDS not in response_dataframe:
            raise DividendDataEmptyError

        iteration_date = None
        try:
            dividends = response_dataframe.Dividends.loc[lambda x: x > 0]
            dividend_rows = {}

            for (timezone_date, dividend) in dividends.items():
                date = timezone_date.date()
                iteration_date = date.isoformat()
                dividend_rows[iteration_date] = DividendDataRow(
                    declaration_date=None,
                    ex_dividend_date=date,
                    record_date=None,
                    payment_date=None,
                    dividend=decimal.Decimal(
                        str(dividend)
                    ),
                    # @todo: the following field is not present in the data source, need to make the field optional:
                    adjusted_dividend=decimal.Decimal(
                        str(dividend)
                    ),
                )
        except (
            EntityFieldTypeError,
            EntityTypeError,
            EntityValueError,
        ) as error:
            msg = f"date: {iteration_date}"
            raise DividendDataRowError(msg) from error

        return DividendData(
            ticker=Ticker(ticker),
            rows=dividend_rows
        )

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
            if (
                response_dataframe is None
                or response_dataframe.empty
            ):
                raise MarketDataEmptyError("No data returned by market data endpoint")

            timestamps = response_dataframe.index.to_series()

            min_date = None
            max_date = None

            for timestamp in timestamps:
                price_date = timestamp.date()
                price_date_string = price_date.isoformat()
                try:
                    date_indexed_row = response_dataframe.loc[timestamp]
                    date_key = cls._fields_market_data_daily_rows['date']
                    entity_fields_row = date_indexed_row.to_dict()
                    entity_fields_row[date_key] = price_date_string
                    attributes = entity_helper.fill_fields(
                        entity_fields_row,
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
