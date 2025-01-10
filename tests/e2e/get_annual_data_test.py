import datetime
import pytest

from kaxanuk.data_curator.entities import Configuration, MarketData
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
PEDIOD_TO_TEST = 'annual'
TICKERS_TO_TEST = (
    'AAPL',
    'F',
    'MSFT',
)
YESTERDAY = datetime.date.today() - datetime.timedelta(days=1)
YEAR_AGO_FROM_YESTERDAY = YESTERDAY - datetime.timedelta(days=365)


@pytest.fixture(scope="module")
def configuration():
    return Configuration(
        start_date=YEAR_AGO_FROM_YESTERDAY,
        end_date=YESTERDAY,
        period=PEDIOD_TO_TEST,
        tickers=TICKERS_TO_TEST,
        columns=COLUMNS_TO_TEST,
    )


@pytest.fixture(scope="module")
def yahoo_finance_instance(configuration):
    yahoo_finance = YahooFinance()
    yahoo_finance.init_config(
        configuration=configuration
    )

    return yahoo_finance


def test_init_config_data_loaded(yahoo_finance_instance):
    assert yahoo_finance_instance.stock_data is not None


def test_get_market_data(yahoo_finance_instance):
    market_data = yahoo_finance_instance.get_market_data(
        ticker=TICKERS_TO_TEST[0],
        start_date=YEAR_AGO_FROM_YESTERDAY,
        end_date=YESTERDAY,
    )

    assert isinstance(market_data, MarketData)
