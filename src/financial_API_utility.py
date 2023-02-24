"""
This module contains class for scraping webpages from Yahoo Finance.

    Original Author: Mark D
    Date created: 09/28/2019
    Date Modified: 02/24/2023
    Python Version: 3.7

Note:
    This module depend on following third party library:
     - yfinance v0.2.12
     - pandas v0.25.0

Examples:
    test_df = Stock('AAPL')
    v_prev_close = test_df.get_previous_close()
    v_low_52wks = test_df.get_low_52wks()
    v_high_52wks = test_df.get_high_52wks()
    v_mkt_cap = test_df.get_market_cap()
    ...

"""

import yfinance as yf


class Stock(object):
    """
    The :class: Stock can be used to get latest Quotes and Finance information from Yahoo Finance.
    """
    def __init__(self, v_ticker):
        """
        constructor for :class: Stock. It will create a yfinance.Ticker object.

        Args:
            v_ticker (str): ticker for stock to get.
        """
        try:
            self.this_instance = yf.Ticker(v_ticker)
            self.this_ticker = v_ticker
        except Exception as e:
            raise RuntimeError(f"Failed to pull information from Yahoo Finance for ticker {v_ticker} -> "+str(e))

    def get_previous_close(self):
        """
        The :function: get_previous_close is used to get previous close price for a stock.
        """
        try:
            that_result = self.this_instance.fast_info['previous_close']
            return that_result
        except Exception as e:
            raise e

    def get_low_52wks(self):
        """
        The :function: get_low_52wks is used to get 52weeks lowest trading price for a stock.
        """
        try:
            that_result = self.this_instance.fast_info['year_low']
            if that_result is None:
                that_result = float('nan')
            return that_result
        except KeyError:
            that_result = float('nan')
            return that_result
        except Exception as e:
            raise e

    def get_high_52wks(self):
        """
        The :function: get_high_52wks is used to get 52weeks highest trading price for a stock.
        """
        try:
            that_result = self.this_instance.fast_info['year_high']
            if that_result is None:
                that_result = float('nan')
            return that_result
        except KeyError:
            that_result = float('nan')
            return that_result
        except Exception as e:
            raise e

    def get_market_cap(self):
        """
        The :function: get_market_cap is used to get latest Market Capitalization for a stock.
        """
        try:
            that_result = self.this_instance.fast_info['marketCap']
            if that_result is None:
                that_result = 0
            return that_result
        except KeyError:
            that_result = 0
            return that_result
        except Exception as e:
            raise e

    def get_pe(self):
        """
        The :function: get_pe is used to get trailing P/E ratio for a stock.
        """
        try:
            that_result = self.this_instance.info['trailingPE']
            if that_result is None:
                that_result = float('nan')
            return that_result
        except KeyError:
            that_result = float('nan')
            return that_result
        except Exception as e:
            raise e

    def get_forward_pe(self):
        """
        The :function: get_forward_pe is used to get forward P/E ratio for a stock.
        """
        try:
            that_result = self.this_instance.info['forwardPE']
            if that_result is None:
                that_result = float('nan')
            return that_result
        except KeyError:
            that_result = float('nan')
            return that_result
        except Exception as e:
            raise e

    def get_sector(self):
        """
        The :function: get_sector is used to get business sector for a stock.
        """
        try:
            that_result = self.this_instance.info['sector']
            if that_result is None:
                that_result = ''
            return that_result
        except KeyError:
            that_result = ''
            return that_result
        except Exception as e:
            raise e

    def get_dividend(self):
        """
        The :function: get_dividend is used to get dividend yield for a stock.
        """
        try:
            that_result = self.this_instance.info['trailingAnnualDividendYield']
            if that_result is None:
                that_result = float('nan')
            return that_result
        except KeyError:
            that_result = float('nan')
            return that_result
        except Exception as e:
            raise e

    def get_eps(self):
        """
        The :function: get_eps is used to get trailing Earning Per Share for a stock.
        """
        try:
            that_result = self.this_instance.info['trailingEps']
            if that_result is None:
                that_result = float('nan')
            return that_result
        except KeyError:
            that_result = float('nan')
            return that_result
        except Exception as e:
            raise e

    def get_forward_eps(self):
        """
        The :function: get_forward_eps is used to get forward Earning Per Share for a stock.
        """
        try:
            that_result = self.this_instance.info['forwardEps']
            if that_result is None:
                that_result = float('nan')
            return that_result
        except KeyError:
            that_result = float('nan')
            return that_result
        except Exception as e:
            raise e

    def get_short_float(self):
        """
        The :function: get_short_float is used to get short percentage of float for a stock.
        """
        try:
            that_result = self.this_instance.info['shortPercentOfFloat']
            if that_result is None:
                that_result = float('nan')
            return that_result
        except KeyError:
            that_result = float('nan')
            return that_result
        except Exception as e:
            raise e

    def get_beta(self):
        """
        The :function: get_beta is used to get beta ratio for a stock.
        """
        try:
            that_result = self.this_instance.info['beta']
            if that_result is None:
                that_result = float('nan')
            return that_result
        except KeyError:
            that_result = float('nan')
            return that_result
        except Exception as e:
            raise e

    def get_headquarter_country(self):
        """
        The :function: get_headquarter_country is used to get company location (country) for a stock.
        """
        try:
            that_result = self.this_instance.info['state']
            if that_result is None:
                that_result = ''
            return that_result
        except KeyError:
            that_result = ''
            return that_result
        except Exception as e:
            raise e

    def get_name(self):
        """
        The :function: get_name is used to get long/short name for a stock.
        """
        try:
            that_result = self.this_instance.info['longName']
            if that_result is None:
                that_result = ''
            return that_result
        except KeyError:
            that_result = ''
            return that_result
        except Exception as e:
            raise e


class ETF(Stock):
    """
    The :class: ETF is a child of :class: Stock, it can be used to get latest Quotes and Financial information for a
        Exchange Traded Fund from Yahoo Finance.
    """
    def get_total_assets(self):
        """
        The :function: get_total_assets is used to get Total Assets for an ETF.
        """
        try:
            that_result = self.this_instance.info['totalAssets']
            if that_result is None:
                that_result = 0
            return that_result
        except KeyError:
            that_result = 0
            return that_result
        except Exception as e:
            raise e

    def get_yield(self):
        """
        The :function: get_yield is used to get Yield for an ETF.
        """
        try:
            that_result = self.this_instance.info['yield']
            if that_result is None:
                that_result = float('nan')
            return that_result
        except KeyError:
            that_result = float('nan')
            return that_result
        except Exception as e:
            raise e

    def get_category(self):
        """
        The :function: get_category is used to get Category for an ETF.
        """
        try:
            that_result = self.this_instance.info['category']
            if that_result is None:
                that_result = ''
            return that_result
        except KeyError:
            that_result = ''
            return that_result
        except Exception as e:
            raise e
