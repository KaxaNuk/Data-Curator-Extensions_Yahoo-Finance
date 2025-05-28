import datetime
import pytest

from kaxanuk.data_curator.entities import (
    Configuration,
    DividendData,
    MarketData, SplitData,
)
from kaxanuk.data_curator.exceptions import (
    EntityProcessingError,
    IdentifierNotFoundError,
)
from kaxanuk.data_curator_extensions.yahoo_finance import YahooFinance


COLUMNS_TO_TEST = (
    'm_date',
    'm_open',
    'm_high',
    'm_low',
    'm_close',
    'm_adjusted_close',
    'm_volume',
    'm_vwap',
)
PERIOD_TO_TEST = 'annual'
INEXISTENT_TICKERS_TO_TEST = (
    'qwertyuiop',
    'poiuytrewq'
)
INVALID_TICKERS_TO_TEST = (
    '/*-^',
    'P/N*C',
)
VALID_TICKERS_TO_TEST = (
    'AAPL',
    'F',
    'JPM',
    'MSFT',
    'TSLA',
)
YESTERDAY = datetime.date.today() - datetime.timedelta(days=1)
START_DATE = datetime.date.fromisoformat('2020-01-01')


@pytest.fixture(scope="module")
def yahoo_finance_inexistent_tickers_instance():
    yahoo_finance = YahooFinance()
    yahoo_finance.initialize(
        configuration=Configuration(
            start_date=START_DATE,
            end_date=YESTERDAY,
            period=PERIOD_TO_TEST,
            identifiers=INEXISTENT_TICKERS_TO_TEST,
            columns=COLUMNS_TO_TEST,
        )
    )

    return yahoo_finance


@pytest.fixture(scope="module")
def yahoo_finance_invalid_tickers_instance():
    yahoo_finance = YahooFinance()
    yahoo_finance.initialize(
        configuration=Configuration(
            start_date=START_DATE,
            end_date=YESTERDAY,
            period=PERIOD_TO_TEST,
            identifiers=INVALID_TICKERS_TO_TEST,
            columns=COLUMNS_TO_TEST,
        )
    )

    return yahoo_finance


@pytest.fixture(scope="module")
def yahoo_finance_valid_tickers_instance():
    yahoo_finance = YahooFinance()
    yahoo_finance.initialize(
        configuration=Configuration(
            start_date=START_DATE,
            end_date=YESTERDAY,
            period=PERIOD_TO_TEST,
            identifiers=VALID_TICKERS_TO_TEST,
            columns=COLUMNS_TO_TEST,
        )
    )

    return yahoo_finance


def test_init_config_inexistent_tickers_general_data_loaded(yahoo_finance_invalid_tickers_instance):
    assert yahoo_finance_invalid_tickers_instance.stock_general_data is not None


def test_init_config_inexistent_tickers_market_data_loaded(yahoo_finance_invalid_tickers_instance):
    assert yahoo_finance_invalid_tickers_instance.stock_market_data is not None


def test_init_config_invalid_tickers_general_data_loaded(yahoo_finance_invalid_tickers_instance):
    assert yahoo_finance_invalid_tickers_instance.stock_general_data is not None


def test_init_config_invalid_tickers_market_data_loaded(yahoo_finance_invalid_tickers_instance):
    assert yahoo_finance_invalid_tickers_instance.stock_market_data is not None


def test_init_config_valid_tickers_general_data_loaded(yahoo_finance_valid_tickers_instance):
    assert yahoo_finance_valid_tickers_instance.stock_general_data is not None


def test_init_config_valid_tickers_market_data_loaded(yahoo_finance_valid_tickers_instance):
    assert yahoo_finance_valid_tickers_instance.stock_market_data is not None


@pytest.mark.parametrize(
    'ticker',
    INVALID_TICKERS_TO_TEST
)
def test_get_inexistent_market_data(
    yahoo_finance_inexistent_tickers_instance,
    ticker
):
    with pytest.raises(IdentifierNotFoundError):
        yahoo_finance_inexistent_tickers_instance.get_market_data(
            main_identifier=ticker,
            start_date=START_DATE,
            end_date=YESTERDAY,
        )


@pytest.mark.parametrize(
    'ticker',
    INVALID_TICKERS_TO_TEST
)
def test_get_invalid_market_data(
    yahoo_finance_invalid_tickers_instance,
    ticker
):
    with pytest.raises(EntityProcessingError):
        yahoo_finance_invalid_tickers_instance.get_market_data(
            main_identifier=ticker,
            start_date=START_DATE,
            end_date=YESTERDAY,
        )


@pytest.mark.parametrize(
    'ticker',
    VALID_TICKERS_TO_TEST
)
def test_get_valid_market_data(
    yahoo_finance_valid_tickers_instance,
    ticker
):
    market_data = yahoo_finance_valid_tickers_instance.get_market_data(
        main_identifier=ticker,
        start_date=START_DATE,
        end_date=YESTERDAY,
    )

    assert isinstance(market_data, MarketData)


@pytest.mark.parametrize(
    'ticker',
    VALID_TICKERS_TO_TEST
)
def test_get_valid_dividend_data(
    yahoo_finance_valid_tickers_instance,
    ticker
):
    dividend_data = yahoo_finance_valid_tickers_instance.get_dividend_data(
        main_identifier=ticker,
        start_date=START_DATE,
        end_date=YESTERDAY,
    )

    assert isinstance(dividend_data, DividendData)


@pytest.mark.parametrize(
    'ticker',
    VALID_TICKERS_TO_TEST
)
def test_get_valid_split_data(
    yahoo_finance_valid_tickers_instance,
    ticker
):
    split_data = yahoo_finance_valid_tickers_instance.get_split_data(
        main_identifier=ticker,
        start_date=START_DATE,
        end_date=YESTERDAY,
    )

    assert isinstance(split_data, SplitData)
