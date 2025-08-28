# KaxaNuk Data Curator Extension: Yahoo Finance Data Provider

|                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [![Python](https://img.shields.io/badge/python-3.12%20%7C%203.13-blue?logo=python&logoColor=ffdd54)](https://www.python.org) [![PyPI - License](https://img.shields.io/pypi/l/kaxanuk.data-curator-extensions.yahoo-finance?color=blue)](LICENSE)                                                                                                                                                                                                                          |
| [![Build Status](https://github.com/KaxaNuk/Data-Curator-Extensions_Yahoo-Finance/actions/workflows/main.yml/badge.svg)](https://github.com/KaxaNuk/Data-Curator-Extensions_Yahoo-Finance/actions/workflows/main.yml)                                                                                                                                                                                                                                                              |
| [![PyPI - Version](https://img.shields.io/pypi/v/kaxanuk.data-curator-extensions.yahoo-finance?logo=pypi)](https://pypi.org/project/kaxanuk.data-curator-extensions.yahoo-finance) [![PyPI Downloads](https://static.pepy.tech/badge/kaxanuk-data-curator-extensions-yahoo-finance)](https://pepy.tech/projects/kaxanuk-data-curator-extensions-yahoo-finance) [![Powered by KaxaNuk](https://img.shields.io/badge/powered%20by-KaxaNuk-orange?colorB=orange)](https://kaxanuk.mx) |


Yahoo Finance data provider extension for the [KaxaNuk Data Curator](https://github.com/KaxaNuk/Data-Curator) component library.


# Installation
To install the Yahoo Finance data provider for the KaxaNuk Data Curator, you need to have the KaxaNuk Data Curator installed first.
Then, you can install the Yahoo Finance provider by running:

```bash
pip install kaxanuk.data_curator_extensions.yahoo_finance
```

This will allow you to use the Yahoo Finance data provider within the KaxaNuk Data Curator.


# Limitations
- The Yahoo Finance data provider is currently limited to only market data.
- The market data only includes the split-adjusted prices and volume, and the dividend-and-split-adjusted close price.
- The market data does not include any vwap, nor unadjusted prices or volume.
- Fundamental data is currently not supported because we haven't found a way to retrieve the filing date for each statement in the data returned by the yfinance library,
which the KaxaNuk Data Curator relies upon to match the dates when the release of that data actually started affecting the market prices. If you have any ideas on how to
retrieve those filing dates, please let us know by opening an issue.


# Support and Issues
Unfortunately we have no way of supporting or addressing any failures in the underlying yfinance library or the Yahoo Finance API,
so please report any issues with the yfinance library to its [GitHub repository](https://github.com/ranaroussi/yfinance).
