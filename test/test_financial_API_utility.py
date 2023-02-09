"""
This :module: contains Test Calls to :module: src/financial_API_utility.py.

    Original Author: Mark D
    Date created: 07/31/2021
    Date Modified: 11/19/2021
    Python Version: 3.7

Note:
    none

Examples:
    python -m unittest test.test_financial_API_utility


"""

import unittest
from src.financial_API_utility import Stock, ETF


class TestFinAPI(unittest.TestCase):
    def test_init_stock(self):
        """
        TestCase for Stock.__init__().
        """
        try:
            _test_stock_instance = Stock('AAPL')
            self.assertEqual(_test_stock_instance.this_ticker, 'AAPL')
            self.assertTrue(isinstance(_test_stock_instance.this_instance.info, dict))
            self.assertTrue(len(_test_stock_instance.this_instance.info) > 2)
        except RuntimeError:
            self.fail(":class: Stock failed to initialize !")

    def test_get_previous_close_stock(self):
        """
        TestCase for Stock.get_previous_close().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_previous_close()
            self.assertTrue(isinstance(_test_output, float) or isinstance(_test_output, int))
        except RuntimeError:
            self.fail(":function: get_previous_close() raised RuntimeError unexpectedly !")

    def test_get_low_52wks(self):
        """
        TestCase for Stock.get_low_52wks().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_low_52wks()
            self.assertTrue(isinstance(_test_output, float) or isinstance(_test_output, int))
        except RuntimeError:
            self.fail(":function: get_low_52wks() raised RuntimeError unexpectedly !")

    def test_get_high_52wks(self):
        """
        TestCase for Stock.get_high_52wks().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_high_52wks()
            self.assertTrue(isinstance(_test_output, float) or isinstance(_test_output, int))
        except RuntimeError:
            self.fail(":function: get_high_52wks() raised RuntimeError unexpectedly !")

    def test_get_market_cap(self):
        """
        TestCase for Stock.get_market_cap().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_market_cap()
            self.assertTrue(isinstance(_test_output, int))
        except RuntimeError:
            self.fail(":function: get_market_cap() raised RuntimeError unexpectedly !")

    def test_get_pe(self):
        """
        TestCase for Stock.get_pe().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_pe()
            self.assertTrue(isinstance(_test_output, float) or isinstance(_test_output, int))
        except RuntimeError:
            self.fail(":function: get_pe() raised RuntimeError unexpectedly !")

    def test_get_forward_pe(self):
        """
        TestCase for Stock.get_forward_pe().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_forward_pe()
            self.assertTrue(isinstance(_test_output, float) or isinstance(_test_output, int))
        except RuntimeError:
            self.fail(":function: get_forward_pe() raised RuntimeError unexpectedly !")

    def test_get_sector(self):
        """
        TestCase for Stock.get_sector().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_sector()
            self.assertTrue(isinstance(_test_output, str))
        except RuntimeError:
            self.fail(":function: get_sector() raised RuntimeError unexpectedly !")

    def test_get_dividend(self):
        """
        TestCase for Stock.get_dividend().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_dividend()
            self.assertTrue(isinstance(_test_output, float) or isinstance(_test_output, int))
        except RuntimeError:
            self.fail(":function: get_dividend() raised RuntimeError unexpectedly !")

    def test_get_eps(self):
        """
        TestCase for Stock.get_eps().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_eps()
            self.assertTrue(isinstance(_test_output, float) or isinstance(_test_output, int))
        except RuntimeError:
            self.fail(":function: get_eps() raised RuntimeError unexpectedly !")

    def test_get_forward_eps(self):
        """
        TestCase for Stock.get_forward_eps().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_forward_eps()
            self.assertTrue(isinstance(_test_output, float) or isinstance(_test_output, int))
        except RuntimeError:
            self.fail(":function: get_forward_eps() raised RuntimeError unexpectedly !")

    def test_get_short_float(self):
        """
        TestCase for Stock.get_short_float().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_short_float()
            self.assertTrue(isinstance(_test_output, float))
        except RuntimeError:
            self.fail(":function: get_short_float() raised RuntimeError unexpectedly !")

    def test_get_short_ratio(self):
        """
        TestCase for Stock.get_short_ratio().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_short_ratio()
            self.assertTrue(isinstance(_test_output, float))
        except RuntimeError:
            self.fail(":function: get_short_ratio() raised RuntimeError unexpectedly !")

    def test_get_beta(self):
        """
        TestCase for Stock.get_beta().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_beta()
            self.assertTrue(isinstance(_test_output, float))
        except RuntimeError:
            self.fail(":function: get_beta() raised RuntimeError unexpectedly !")

    def test_get_headquarter_country(self):
        """
        TestCase for Stock.get_headquarter_country().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_headquarter_country()
            self.assertTrue(isinstance(_test_output, str))
        except RuntimeError:
            self.fail(":function: get_headquarter_country() raised RuntimeError unexpectedly !")

    def test_get_headquarter_city(self):
        """
        TestCase for Stock.get_headquarter_city().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_headquarter_city()
            self.assertTrue(isinstance(_test_output, str))
        except RuntimeError:
            self.fail(":function: get_headquarter_city() raised RuntimeError unexpectedly !")

    def test_get_name(self):
        """
        TestCase for Stock.get_name().
        """
        _test_stock_instance = Stock('AAPL')
        try:
            _test_output = _test_stock_instance.get_name()
            self.assertTrue(isinstance(_test_output, str))
        except RuntimeError:
            self.fail(":function: get_name() raised RuntimeError unexpectedly !")

    def test_init_etf(self):
        """
        TestCase for ETF.__init__.
        """
        try:
            _test_equity_etf_instance = ETF('VOO')
            self.assertEqual(_test_equity_etf_instance.this_ticker, 'VOO')
            self.assertTrue(isinstance(_test_equity_etf_instance.this_instance.info, dict))
            self.assertTrue(len(_test_equity_etf_instance.this_instance.info) > 2)
        except RuntimeError:
            self.fail(":class: ETF failed to initialize !")

    def test_get_previous_close_equity_etf(self):
        """
        TestCase for ETF.get_previous_close().
        """
        _test_etf_instance = ETF('VOO')
        try:
            _test_output = _test_etf_instance.get_previous_close()
            self.assertTrue(isinstance(_test_output, float) or isinstance(_test_output, int))
        except RuntimeError:
            self.fail(":function: get_previous_close() raised RuntimeError unexpectedly !")

    def test_get_previous_close_fixed_etf(self):
        """
        TestCase for ETF.get_previous_close().
        """
        _test_etf_instance = ETF('BSV')
        try:
            _test_output = _test_etf_instance.get_previous_close()
            self.assertTrue(isinstance(_test_output, float) or isinstance(_test_output, int))
        except RuntimeError:
            self.fail(":function: get_previous_close() raised RuntimeError unexpectedly !")

    def test_get_total_assets(self):
        """
        TestCase for ETF.get_total_assets().
        """
        _test_stock_instance = ETF('VOO')
        try:
            _test_output = _test_stock_instance.get_total_assets()
            self.assertTrue(isinstance(_test_output, int))
        except RuntimeError:
            self.fail(":function: get_total_assets() raised RuntimeError unexpectedly !")

    def test_get_yield(self):
        """
        TestCase for ETF.get_yield().
        """
        _test_stock_instance = ETF('VOO')
        try:
            _test_output = _test_stock_instance.get_yield()
            self.assertTrue(isinstance(_test_output, float))
        except RuntimeError:
            self.fail(":function: get_yield() raised RuntimeError unexpectedly !")

    def test_get_category(self):
        """
        TestCase for ETF.get_category().
        """
        _test_stock_instance = ETF('VOO')
        try:
            _test_output = _test_stock_instance.get_category()
            self.assertTrue(isinstance(_test_output, str))
        except RuntimeError:
            self.fail(":function: get_category() raised RuntimeError unexpectedly !")

    def test_get_fund_family(self):
        """
        TestCase for ETF.get_fund_family().
        """
        _test_stock_instance = ETF('VOO')
        try:
            _test_output = _test_stock_instance.get_fund_family()
            self.assertTrue(isinstance(_test_output, str))
        except RuntimeError:
            self.fail(":function: get_fund_family() raised RuntimeError unexpectedly !")