import datetime
import decimal
import fractions
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
    SplitDataRow,
    MainIdentifier,
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
    SplitDataEmptyError,
    SplitDataRowError,
    IdentifierNotFoundError,
)
from kaxanuk.data_curator.data_providers.data_provider_interface import DataProviderInterface
from kaxanuk.data_curator.services import entity_helper


class YahooFinance(DataProviderInterface):

    _config_to_yf_periods = types.MappingProxyType({
        'annual': 'yearly',
        'quarterly': 'quarterly',
    })

    _field_correspondences_market_data_daily_rows = types.MappingProxyType({
        'date': 'Date',
        'open': None,
        'high': None,
        'low': None,
        'close': None,
        'volume': None,
        'vwap': None,
        'open_split_adjusted': 'Open',
        'high_split_adjusted': 'High',
        'low_split_adjusted': 'Low',
        'close_split_adjusted': 'Close',
        'volume_split_adjusted': 'Volume',
        'vwap_split_adjusted': None,
        'open_dividend_and_split_adjusted': None,
        'high_dividend_and_split_adjusted': None,
        'low_dividend_and_split_adjusted': None,
        'close_dividend_and_split_adjusted': 'Adj Close',
        'volume_dividend_and_split_adjusted': None,
        'vwap_dividend_and_split_adjusted': None,
    })

    def __init__(self):
        self.stock_general_data = None
        self.stock_market_data = None

    def get_dividend_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> DividendData:
        """
        Get the dividend data from the web service response, wrapped in a DividendData entity.

        Parameters
        ----------
        main_identifier
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
                self.stock_general_data is None
                or main_identifier not in self.stock_general_data.tickers
                or not hasattr(
                    self.stock_general_data.tickers[main_identifier],
                    'dividends'
                )
            ):
                raise DividendDataEmptyError

            dividend_data = self._create_dividend_data_from_response_dividends_series(
                main_identifier,
                self.stock_general_data.tickers[main_identifier].dividends,
                start_date=start_date,
                end_date=end_date,
            )
        except DividendDataEmptyError:
            msg = f"{main_identifier} has no dividend data obtained for the selected period, omitting its dividend data"
            logging.getLogger(__name__).warning(msg)
            dividend_data = DividendData(
                main_identifier=MainIdentifier(main_identifier),
                rows={}
            )
        except DividendDataRowError as error:
            msg = f"{main_identifier} dividend data error: {error}"
            logging.getLogger(__name__).error(msg)
            dividend_data = DividendData(
                main_identifier=MainIdentifier(main_identifier),
                rows={}
            )

        return dividend_data

    def get_fundamental_data(
        self,
        *,
        main_identifier: str,
        period: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> FundamentalData:
        return FundamentalData(
            main_identifier=MainIdentifier(main_identifier),
            rows={}
        )

    def get_market_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> MarketData:
        """
        Get the market data from the web service wrapped in a MarketData entity.

        Parameters
        ----------
        main_identifier
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
            or main_identifier not in self.stock_market_data
        ):
            raise IdentifierNotFoundError(f"No market data for ticker {main_identifier}")

        return self._create_market_data_from_response_dataframe(
            main_identifier,
            self.stock_market_data[main_identifier]
        )

    def get_split_data(
        self,
        *,
        main_identifier: str,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> SplitData:
        """
        Get the split data from the web service response, wrapped in a SplitData entity.

        Parameters
        ----------
        main_identifier
            the stock's ticker
        start_date
            the start date for the data
        end_date
            the end date for the data

        Returns
        -------
        SplitData
        """
        try:
            if (
                self.stock_general_data is None
                or main_identifier not in self.stock_general_data.tickers
                or not hasattr(
                    self.stock_general_data.tickers[main_identifier],
                    'splits'
                )
            ):
                raise SplitDataEmptyError

            split_data = self._create_split_data_from_response_splits_series(
                main_identifier,
                self.stock_general_data.tickers[main_identifier].splits,
                start_date=start_date,
                end_date=end_date,
            )
        except SplitDataEmptyError:
            msg = f"{main_identifier} has no split data obtained for the selected period, omitting its split data"
            logging.getLogger(__name__).warning(msg)
            split_data = SplitData(
                main_identifier=MainIdentifier(main_identifier),
                rows={}
            )
        except SplitDataRowError as error:
            msg = f"{main_identifier} split data error: {error}"
            logging.getLogger(__name__).error(msg)
            split_data = SplitData(
                main_identifier=MainIdentifier(main_identifier),
                rows={}
            )

        return split_data

    def initialize(
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
            " ".join(configuration.identifiers)
        )
        self.stock_market_data = self.stock_general_data.history(
            start=configuration.start_date.isoformat(),
            end=configuration.end_date.isoformat(),
            group_by='ticker',
            # actions=True,
            auto_adjust=False,
            back_adjust=False,
            period=None,
            interval="1d",
        )

    def validate_api_key(
        self,
    ) -> bool | None:
        """
        Validate that the API key used to init the class is valid, in this case there's no key so always None

        Returns
        -------
        Always None
        """
        return None

    @classmethod
    def _create_dividend_data_from_response_dividends_series(
        cls,
        ticker: str,
        dividends_series: pandas.DataFrame,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> DividendData:
        """
        Populate a DividendData entity from the web service raw data.

        Parameters
        ----------
        ticker
            The ticker symbol
        dividends_series
            The ticker's dividends as a Pandas Series
        start_date
            The start date for the data we're interested in
        end_date
            The end date for the data we're interested in

        Returns
        -------
        DividendData

        Raises
        ------
        DividendDataEmptyError
        DividendDataRowError
        """
        if dividends_series.empty:
            raise DividendDataEmptyError

        iteration_date = None
        try:
            # yfinance is assigning made-up date timezones, so we can't compare the date objects directly
            date_range = slice(
                start_date.isoformat(),
                end_date.isoformat()
            )
            dividends = dividends_series[date_range]
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
                    dividend_split_adjusted=decimal.Decimal(
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
            main_identifier=MainIdentifier(ticker),
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

            non_empty_rows_dataframe = response_dataframe.dropna(how='all')
            if non_empty_rows_dataframe.empty:
                raise MarketDataEmptyError("No non-empty data returned by market data endpoint")

            timestamps = non_empty_rows_dataframe.index.to_series()

            min_date = None
            max_date = None

            for timestamp in timestamps:
                price_date = timestamp.date()
                price_date_string = price_date.isoformat()
                try:
                    date_indexed_row = non_empty_rows_dataframe.loc[timestamp]
                    date_key = cls._field_correspondences_market_data_daily_rows['date']
                    entity_fields_row = date_indexed_row.to_dict()
                    entity_fields_row[date_key] = price_date_string
                    attributes = entity_helper.convert_data_row_into_entity_fields(
                        entity_fields_row,
                        dict(cls._field_correspondences_market_data_daily_rows),
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
                main_identifier=MainIdentifier(ticker),
                daily_rows=market_data_rows
            )
        except (
            MarketDataEmptyError,
            MarketDataRowError
        ) as error:
            raise EntityProcessingError("Market data processing error") from error

        return market_data

    @classmethod
    def _create_split_data_from_response_splits_series(
        cls,
        ticker: str,
        splits_series: pandas.Series,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> SplitData:
        """
        Populate a SplitData entity from the web service raw data.

        Parameters
        ----------
        ticker
            The ticker symbol
        splits_series
            The ticker's splits as a Pandas Series
        start_date
            The start date for the data we're interested in
        end_date
            The end date for the data we're interested in

        Returns
        -------
        SplitData

        Raises
        ------
        SplitDataEmptyError
        SplitDataRowError
        """
        if splits_series.empty:
            raise SplitDataEmptyError

        iteration_date = None
        try:
            # yfinance is assigning made-up date timezones, so we can't compare the date objects directly
            date_range = slice(
                start_date.isoformat(),
                end_date.isoformat()
            )
            splits = splits_series[date_range]
            split_rows = {}
            for (timezone_date, split) in splits.items():
                date = timezone_date.date()
                iteration_date = date.isoformat()
                split_fraction = fractions.Fraction(split).limit_denominator()

                split_rows[iteration_date] = SplitDataRow(
                    split_date=date,
                    numerator=float(split_fraction.numerator),
                    denominator=float(split_fraction.denominator),
                )
        except (
            EntityFieldTypeError,
            EntityTypeError,
            EntityValueError,
        ) as error:
            msg = f"date: {iteration_date}"
            raise SplitDataRowError(msg) from error

        return SplitData(
            main_identifier=MainIdentifier(ticker),
            rows=split_rows
        )
