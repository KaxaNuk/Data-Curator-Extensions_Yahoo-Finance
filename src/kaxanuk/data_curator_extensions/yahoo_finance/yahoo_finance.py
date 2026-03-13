import datetime
import enum
import fractions
import logging
import typing
import types

import pandas
import pyarrow
import yfinance

from kaxanuk.data_curator.data_blocks.dividends import DividendsDataBlock
from kaxanuk.data_curator.data_blocks.market_daily import MarketDailyDataBlock
from kaxanuk.data_curator.data_blocks.splits import SplitsDataBlock
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
    EntityProcessingError,
    IdentifierNotFoundError,
    SplitDataEmptyError,
)
from kaxanuk.data_curator.data_providers.data_provider_interface import DataProviderInterface
from kaxanuk.data_curator.services.data_provider_toolkit import (
    DataBlockEndpointTagMap,
    DataProviderToolkit,
    EndpointFieldMap,
)


class YahooFinance(DataProviderInterface):

    class Endpoints(enum.StrEnum):
        DIVIDEND_DATA = 'dividend_data'
        MARKET_DATA = 'market_data'
        SPLIT_DATA = 'split_data'

    _config_to_yf_periods = types.MappingProxyType({
        'annual': 'yearly',
        'quarterly': 'quarterly',
    })

    _dividend_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.DIVIDEND_DATA: {
            DividendDataRow.ex_dividend_date: 'Date',
            DividendDataRow.dividend: 'Dividends',
            DividendDataRow.dividend_split_adjusted: 'Dividends',
        },
    }

    _market_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.MARKET_DATA: {
            MarketDataDailyRow.date: 'Date',
            MarketDataDailyRow.open_split_adjusted: 'Open',
            MarketDataDailyRow.high_split_adjusted: 'High',
            MarketDataDailyRow.low_split_adjusted: 'Low',
            MarketDataDailyRow.close_split_adjusted: 'Close',
            MarketDataDailyRow.volume_split_adjusted: 'Volume',
            MarketDataDailyRow.close_dividend_and_split_adjusted: 'Adj Close',
        },
    }

    _split_data_endpoint_map: typing.Final[EndpointFieldMap] = {
        Endpoints.SPLIT_DATA: {
            SplitDataRow.split_date: 'Date',
            SplitDataRow.numerator: 'Numerator',
            SplitDataRow.denominator: 'Denominator',
        },
    }

    def __init__(self):
        self.stock_general_data = None
        self.stock_market_data = None

    @classmethod
    def get_data_block_endpoint_tag_map(cls) -> DataBlockEndpointTagMap:
        return {
            DividendsDataBlock: cls._dividend_data_endpoint_map,
            MarketDailyDataBlock: cls._market_data_endpoint_map,
            SplitsDataBlock: cls._split_data_endpoint_map,
        }

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
        empty_result = DividendData(
            main_identifier=MainIdentifier(main_identifier),
            rows={}
        )

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

            dividends_series = self.stock_general_data.tickers[main_identifier].dividends

            if dividends_series.empty:
                raise DividendDataEmptyError

            # yfinance assigns made-up timezones, so slice by ISO string
            date_range = slice(
                start_date.isoformat(),
                end_date.isoformat()
            )
            dividends_in_range = dividends_series[date_range]

            if dividends_in_range.empty:
                raise DividendDataEmptyError

            endpoint_table = self._convert_series_to_endpoint_table(
                dividends_in_range,
                value_column_name='Dividends',
            )

            endpoint_tables = {
                self.Endpoints.DIVIDEND_DATA: endpoint_table,
            }

            processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
                data_block=DividendsDataBlock,
                endpoint_field_map=self._dividend_data_endpoint_map,
                endpoint_tables=endpoint_tables,
            )
            consolidated_dividend_data = DataProviderToolkit.consolidate_processed_endpoint_tables(
                processed_endpoint_tables=processed_endpoint_tables,
                table_merge_fields=[DividendsDataBlock.clock_sync_field],
                predominant_order_descending=False,
            )
            dividend_data = DividendsDataBlock.assemble_entities_from_consolidated_table(
                consolidated_table=consolidated_dividend_data,
                common_field_data={
                    DividendData: {
                        DividendData.main_identifier: MainIdentifier(main_identifier),
                    }
                }
            )
        except DividendDataEmptyError:
            msg = f"{main_identifier} has no dividend data obtained for the selected period, omitting its dividend data"
            logging.getLogger(__name__).warning(msg)
            dividend_data = empty_result
        except EntityProcessingError as error:
            msg = f"{main_identifier} dividend data error: {error}"
            logging.getLogger(__name__).error(msg)
            dividend_data = empty_result

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
        IdentifierNotFoundError
        """
        if (
            self.stock_market_data is None
            or main_identifier not in self.stock_market_data
        ):
            raise IdentifierNotFoundError(f"No market data for ticker {main_identifier}")

        response_dataframe = self.stock_market_data[main_identifier]

        if (
            response_dataframe is None
            or response_dataframe.empty
        ):
            raise EntityProcessingError("No data returned by market data endpoint")

        non_empty_dataframe = response_dataframe.dropna(how='all')
        if non_empty_dataframe.empty:
            raise EntityProcessingError("No non-empty data returned by market data endpoint")

        endpoint_table = self._convert_ticker_dataframe_to_endpoint_table(
            non_empty_dataframe,
            columns_to_preserve=list(
                self._market_data_endpoint_map[self.Endpoints.MARKET_DATA].values()
            ),
        )

        # detect rows with empty data
        date_column_name = self._market_data_endpoint_map[self.Endpoints.MARKET_DATA][MarketDataDailyRow.date]
        value_column_names = [
            col
            for col in endpoint_table.column_names
            if col != date_column_name
        ]
        row_is_empty_or_zero_parts = [
            pyarrow.compute.or_(
                pyarrow.compute.is_null(endpoint_table[col]),
                pyarrow.compute.equal(endpoint_table[col], 0),
            ).combine_chunks().fill_null(fill_value=True)
            for col in value_column_names
        ]
        row_is_empty_or_zero = row_is_empty_or_zero_parts[0]
        for part in row_is_empty_or_zero_parts[1:]:
            row_is_empty_or_zero = pyarrow.compute.and_(row_is_empty_or_zero, part)

        if pyarrow.compute.any(row_is_empty_or_zero).as_py():
            problematic_dates_table = endpoint_table.select([date_column_name]).filter(row_is_empty_or_zero)
            dates_output = DataProviderToolkit.format_consolidated_discrepancy_table_for_output(
                discrepancy_table=problematic_dates_table,
                output_column_renames=['date'],
            )
            msg = "\n".join([
                "Market data contains rows with all-empty or zero values. Affected dates:",
                dates_output,
            ])

            raise EntityProcessingError(msg)

        endpoint_tables = {
            self.Endpoints.MARKET_DATA: endpoint_table,
        }

        processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
            data_block=MarketDailyDataBlock,
            endpoint_field_map=self._market_data_endpoint_map,
            endpoint_tables=endpoint_tables,
        )
        consolidated_market_data = DataProviderToolkit.consolidate_processed_endpoint_tables(
            processed_endpoint_tables=processed_endpoint_tables,
            table_merge_fields=[MarketDailyDataBlock.clock_sync_field],
            predominant_order_descending=False,
        )
        market_data = MarketDailyDataBlock.assemble_entities_from_consolidated_table(
            consolidated_table=consolidated_market_data,
            common_field_data={
                MarketData: {
                    MarketData.main_identifier: MainIdentifier(main_identifier),
                }
            }
        )

        return market_data  # noqa: RET504

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
        empty_result = SplitData(
            main_identifier=MainIdentifier(main_identifier),
            rows={}
        )

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

            splits_series = self.stock_general_data.tickers[main_identifier].splits

            if splits_series.empty:
                raise SplitDataEmptyError

            # yfinance assigns made-up timezones, so slice by ISO string
            date_range = slice(
                start_date.isoformat(),
                end_date.isoformat()
            )
            splits_in_range = splits_series[date_range]

            if splits_in_range.empty:
                raise SplitDataEmptyError

            endpoint_table = self._convert_splits_series_to_endpoint_table(splits_in_range)

            endpoint_tables = {
                self.Endpoints.SPLIT_DATA: endpoint_table,
            }

            processed_endpoint_tables = DataProviderToolkit.process_endpoint_tables(
                data_block=SplitsDataBlock,
                endpoint_field_map=self._split_data_endpoint_map,
                endpoint_tables=endpoint_tables,
            )
            consolidated_split_data = DataProviderToolkit.consolidate_processed_endpoint_tables(
                processed_endpoint_tables=processed_endpoint_tables,
                table_merge_fields=[SplitsDataBlock.clock_sync_field],
                predominant_order_descending=False,
            )
            split_data = SplitsDataBlock.assemble_entities_from_consolidated_table(
                consolidated_table=consolidated_split_data,
                common_field_data={
                    SplitData: {
                        SplitData.main_identifier: MainIdentifier(main_identifier),
                    }
                }
            )
        except SplitDataEmptyError:
            msg = f"{main_identifier} has no split data obtained for the selected period, omitting its split data"
            logging.getLogger(__name__).warning(msg)
            split_data = empty_result
        except EntityProcessingError as error:
            msg = f"{main_identifier} split data error: {error}"
            logging.getLogger(__name__).error(msg)
            split_data = empty_result

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

    @staticmethod
    def _convert_ticker_dataframe_to_endpoint_table(
        dataframe: pandas.DataFrame,
        columns_to_preserve: list[str],
    ) -> pyarrow.Table:
        """
        Convert a yfinance ticker DataFrame into a PyArrow table suitable for the toolkit.

        The DataFrame's DatetimeIndex is extracted as an ISO date string column named 'Date',
        and only the specified columns are carried over so they align with the tags declared
        in ``_market_data_endpoint_map``.

        Parameters
        ----------
        dataframe
            Non-empty ticker DataFrame from ``stock_market_data[ticker]``,
            with a DatetimeIndex and yfinance column names (Open, High, …).
        columns_to_preserve
            List of column names from the DataFrame to include in the output table.

        Returns
        -------
        pyarrow.Table
        """
        dataframe_with_date_column = dataframe.assign(
            Date=pandas.to_datetime(dataframe.index).date
        )

        return (
            dataframe_with_date_column[columns_to_preserve]
            .pipe(
                pyarrow.Table.from_pandas,
                preserve_index=False
            )
        )

    @staticmethod
    def _convert_series_to_endpoint_table(
        series: pandas.Series,
        value_column_name: str,
    ) -> pyarrow.Table:
        """
        Convert a yfinance Series with a DatetimeIndex into a PyArrow table.

        Extracts the index as an ISO date column named 'Date' and the values
        as a column with the given name.

        Parameters
        ----------
        series
            Pandas Series with a DatetimeIndex (e.g. dividends or splits from yfinance).
        value_column_name
            Name for the value column in the resulting table.

        Returns
        -------
        pyarrow.Table
        """
        dataframe = series.to_frame(name=value_column_name)
        dataframe_with_date = dataframe.assign(Date=pandas.to_datetime(dataframe.index).date)

        return (
            dataframe_with_date[['Date', value_column_name]]
            .pipe(
                pyarrow.Table.from_pandas,
                preserve_index=False
            )
        )

    @staticmethod
    def _convert_splits_series_to_endpoint_table(
        splits_series: pandas.Series,
    ) -> pyarrow.Table:
        """
        Convert a yfinance splits Series into a PyArrow table with Numerator/Denominator columns.

        Each split ratio is decomposed into its numerator and denominator via
        ``fractions.Fraction``, then stored alongside the ISO date.

        Parameters
        ----------
        splits_series
            Pandas Series of split ratios with a DatetimeIndex.

        Returns
        -------
        pyarrow.Table
        """
        rows = []
        for (timezone_date, ratio) in splits_series.items():
            fraction = fractions.Fraction(ratio).limit_denominator()
            rows.append({
                'Date': timezone_date.date(),
                'Numerator': float(fraction.numerator),
                'Denominator': float(fraction.denominator),
            })

        return pyarrow.Table.from_pylist(rows)
