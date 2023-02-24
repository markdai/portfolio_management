"""
This module contains class for Yahoo Finance API requests.

    Original Author: Mark D
    Date created: 02/24/2023
    Date Modified: 02/24/2023
    Python Version: 3.7

Note:
    This module depend on following third party library:
     - yahooquery v2.3.0
     - pandas v0.25.0
     - requests v2.28
        - urllib3 v1.16
     - requests-futures

Examples:
    test_df = Stock('aapl')
    v_prev_close = test_df.get_previous_close()
    v_low_52wks = test_df.get_low_52wks()
    v_high_52wks = test_df.get_high_52wks()
    v_mkt_cap = test_df.get_market_cap()
    ...

"""

from yahooquery import Ticker


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
            this_instance = Ticker(v_ticker)
            self.this_instance_summary = this_instance.summary_detail[v_ticker]
            self.this_ticker = v_ticker
        except Exception as e:
            raise RuntimeError(f"Failed to pull information from Yahoo Finance for ticker {v_ticker} -> "+str(e))

    def get_previous_close(self):
        """
        The :function: get_previous_close is used to get previous close price for a stock.
        """
        try:
            that_result = self.this_instance_summary['previousClose']
            return that_result
        except Exception as e:
            raise e

    def get_low_52wks(self):
        """
        The :function: get_low_52wks is used to get 52weeks lowest trading price for a stock.
        """
        try:
            that_result = self.this_instance_summary['fiftyTwoWeekLow']
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
            that_result = self.this_instance_summary['fiftyTwoWeekHigh']
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
            that_result = self.this_instance_summary['marketCap']
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
            that_result = self.this_instance_summary['trailingPE']
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
            that_result = self.this_instance_summary['forwardPE']
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
        that_result = ''
        return that_result

    def get_dividend(self):
        """
        The :function: get_dividend is used to get dividend yield for a stock.
        """
        try:
            that_result = self.this_instance_summary['dividendYield']
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
        that_result = float('nan')
        return that_result

    def get_forward_eps(self):
        """
        The :function: get_forward_eps is used to get forward Earning Per Share for a stock.
        """
        that_result = float('nan')
        return that_result

    def get_short_float(self):
        """
        The :function: get_short_float is used to get short percentage of float for a stock.
        """
        that_result = float('nan')
        return that_result

    def get_beta(self):
        """
        The :function: get_beta is used to get beta ratio for a stock.
        """
        try:
            that_result = self.this_instance_summary['beta']
            if that_result is None:
                that_result = float('nan')
            return that_result
        except KeyError:
            that_result = float('nan')
            return that_result
        except Exception as e:
            raise e

    def get_name(self):
        """
        The :function: get_name is used to get long/short name for a stock.
        """
        that_result = ''
        return that_result


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
            that_result = self.this_instance_summary['totalAssets']
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
            that_result = self.this_instance_summary['yield']
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
        that_result = ''
        return that_result
