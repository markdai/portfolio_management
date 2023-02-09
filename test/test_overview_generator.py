"""
This :module: contains Test Calls to :module: src/overview_generator.

    Original Author: Mark D
    Date created: 11/29/2021
    Date Modified: 12/08/2021
    Python Version: 3.7

Note:
    none

Examples:
    python -m unittest test.test_overview_generator


"""

import unittest
from unittest.mock import patch
import pandas as pd

from src.overview_generator import SummaryTool
from src.eq_SQLite_utility import SQLiteRequest as eq_SQLiteRequest
from src.fixed_SQLite_utility import FixedSQLiteRequest as fixed_SQLiteRequest


class TestSummaryTool(unittest.TestCase):
    def test_init(self):
        """
        TestCase for SummaryTool.__init__().
        """
        _test_instance = SummaryTool()
        self.assertEqual(_test_instance.eq_db_file, 'databases/equity.db')
        self.assertEqual(_test_instance.fixed_db_file, 'databases/fixed_income.db')
        self.assertEqual(_test_instance.other_investment_file, 'databases/others.json')

    @patch.object(eq_SQLiteRequest, "get_table_transactions")
    def test_get_eq_transactions_data(self, mock_get_eq_transactions):
        """
        TestCase for SummaryTool._get_eq_transactions_data().
        """
        _test_instance = SummaryTool()
        mock_get_eq_transactions.return_value = [{'ID': None, 'SYMBOL': 'AAPL', 'TYPE': None, 'DATE': None,
                                                  'DOLLARS': 120.0, 'UNITS': None, 'INVESTMENT_TYPE': None,
                                                  'DESCRIPTION': None, 'ACCOUNT': None, 'TOTAL_DOLLARS': None}
                                                 ]
        _test_output = _test_instance._get_eq_transactions_data()
        self.assertTrue(mock_get_eq_transactions.called)
        self.assertTrue(isinstance(_test_output, pd.DataFrame))
        self.assertEqual(_test_output.shape[0], 1)
        self.assertEqual(_test_output.iloc[0]['SYMBOL'], 'AAPL')
        self.assertEqual(_test_output.iloc[0]['DOLLARS'], 120.0)

    @patch.object(eq_SQLiteRequest, "get_view_positions")
    def test_eq_positions_data(self, mock_get_eq_positions):
        """
        TestCase for SummaryTool._get_eq_positions_data().
        """
        _test_instance = SummaryTool()
        mock_get_eq_positions.return_value = [{'SYMBOL': 'VOO', 'DESCRIPTION': None, 'INVESTMENT_TYPE': None,
                                               'COST_DOLLARS': None, 'DOLLARS': 400.0, 'UNITS': None,
                                               'LAST_UPDATED': None, 'MKT_VALUE': None,
                                               'GAIN_PER_SHARE': None, 'GAIN_TOTAL': None, 'GAIN_PERCENTAGE': None}
                                              ]
        _test_output = _test_instance._get_eq_positions_data()
        self.assertTrue(mock_get_eq_positions.called)
        self.assertTrue(isinstance(_test_output, pd.DataFrame))
        self.assertEqual(_test_output.shape[0], 1)
        self.assertEqual(_test_output.iloc[0]['SYMBOL'], 'VOO')
        self.assertEqual(_test_output.iloc[0]['DOLLARS'], 400.0)

    @patch.object(fixed_SQLiteRequest, "get_view_positions_fixed")
    def test_fixed_positions_data(self, mock_get_fix_positions):
        """
        TestCase for SummaryTool._get_fixed_positions_data().
        """
        _test_instance = SummaryTool()
        mock_get_fix_positions.return_value = [{'NAME': None, 'SYMBOL': 'BLV', 'INVESTMENT_TYPE': None,
                                                'UNITS': None, 'FACE_VALUE': 5000.0, 'TOTAL_DOLLARS': None,
                                                'ADD_DATE': None, 'END_DATE': None, 'TOTAL_COST': None,
                                                'RETURN_RATE': None, 'RETURN_DOLLARS': None, 'IS_MATURED': None}
                                               ]
        _test_output = _test_instance._get_fixed_positions_data()
        self.assertTrue(mock_get_fix_positions.called)
        self.assertTrue(isinstance(_test_output, pd.DataFrame))
        self.assertEqual(_test_output.shape[0], 1)
        self.assertEqual(_test_output.iloc[0]['SYMBOL'], 'BLV')
        self.assertEqual(_test_output.iloc[0]['FACE_VALUE'], 5000.0)

    @patch.object(fixed_SQLiteRequest, "get_table_transactions_fixed")
    def test_get_fixed_transactions_data(self, mock_get_fixed_transactions):
        """
        TestCase for SummaryTool._get_fixed_transactions_data().
        """
        _test_instance = SummaryTool()
        mock_get_fixed_transactions.return_value = [{'ID': None, 'NAME': None, 'SYMBOL': 'VTIP',
                                                     'INVESTMENT_TYPE': None, 'UNITS': None, 'FACE_VALUE': 1000.0,
                                                     'TOTAL_DOLLARS': None, 'ADD_DATE': None, 'END_DATE': None,
                                                     'TOTAL_COST': None, 'APR': None, 'YTM': None, 'ACCOUNT': None}
                                                    ]
        _test_output = _test_instance._get_fixed_transactions_data()
        self.assertTrue(mock_get_fixed_transactions.called)
        self.assertTrue(isinstance(_test_output, pd.DataFrame))
        self.assertEqual(_test_output.shape[0], 1)
        self.assertEqual(_test_output.iloc[0]['SYMBOL'], 'VTIP')
        self.assertEqual(_test_output.iloc[0]['FACE_VALUE'], 1000.0)

    @patch.object(pd, "read_json")
    def test_get_other_investment_information(self, mock_pd_read_json):
        """
        TestCase for SummaryTool._get_other_investment_information().
        """
        _test_instance = SummaryTool()
        mock_pd_read_json.return_value = [{'DESCRIPTION': 'CD', 'MAJOR_TYPE': None, 'MINOR_TYPE': None,
                                           'DOLLARS': 500.0, 'ACCOUNT': None}
                                          ]
        _test_output = _test_instance._get_other_investment_information()
        self.assertTrue(mock_pd_read_json.called)
        self.assertTrue(isinstance(_test_output, pd.DataFrame))
        self.assertEqual(_test_output.shape[0], 1)
        self.assertEqual(_test_output.iloc[0]['DESCRIPTION'], 'CD')
        self.assertEqual(_test_output.iloc[0]['DOLLARS'], 500.0)

    @patch.object(SummaryTool, "_get_eq_positions_data")
    @patch.object(SummaryTool, "_get_fixed_positions_data")
    @patch.object(SummaryTool, "_get_other_investment_information")
    def test_generate_allocation_report_type(self, mock_get_other_investments, mock_get_fixed_positions,
                                             mock_get_eq_positions):
        """
        TestCase for SummaryTool.generate_allocation_report_type().
        """
        _test_instance = SummaryTool()
        _dict_eq_positions = {
            'SYMBOL': ['VOO', 'BLV', 'AAPL'],
            'INVESTMENT_TYPE': ['etf', 'etf', 'stock'],
            'MKT_VALUE': [10000.0, 5000.0, 2000.0]
        }
        _dict_fixed_positions = {
            'SYMBOL': ['TEST01', 'TEST02'],
            'INVESTMENT_TYPE': ['CD', 'CD'],
            'TOTAL_DOLLARS': [3000.0, 2000.0]
        }
        _dict_other_investments = {
            'SUFFIX': ['TEST03'],
            'DESCRIPTION': ['This is a test'],
            'MAJOR_TYPE': ['Cash Equivalent'],
            'MINOR_TYPE': ['Saving'],
            'DOLLARS': 10000.0,
            'ACCOUNT': None
        }
        _pd_eq_positions = pd.DataFrame(data=_dict_eq_positions)
        _pd_fixed_positions = pd.DataFrame(data=_dict_fixed_positions)
        _pd_other_investments = pd.DataFrame(data=_dict_other_investments)
        mock_get_eq_positions.return_value = _pd_eq_positions
        mock_get_fixed_positions.return_value = _pd_fixed_positions
        mock_get_other_investments.return_value = _pd_other_investments
        _test_output = _test_instance.generate_allocation_report_type()
        self.assertTrue(mock_get_eq_positions.called)
        self.assertTrue(mock_get_fixed_positions.called)
        self.assertTrue(mock_get_other_investments.called)
        self.assertEqual(_test_output.shape[0], 6)
        self.assertEqual(list(_test_output.columns),
                         ['MAJOR_TYPE', 'MAJOR_TOTAL_DOLLARS', 'MAJOR_ALLOCATION',
                          'MINOR_TYPE', 'MINOR_TOTAL_DOLLARS', 'MINOR_ALLOCATION'])
        self.assertEqual(list(_test_output.iloc[5]),
                         ['TOTAL', '$32,000', '100%', '', '$nan', 'nan%'])
        self.assertEqual(list(_test_output['MINOR_TOTAL_DOLLARS']),
                         ['$10,000', '$2,000', '$10,000', '$5,000', '$5,000', '$nan'])

    @patch.object(SummaryTool, "_get_fixed_transactions_data")
    def test_generate_mature_calender(self, mock_get_fixed_transactions):
        """
        TestCase for SummaryTool.generate_mature_calender().
        """
        _test_instance = SummaryTool()
        _dict_fixed_transactions = {
            'SYMBOL': ['TEST01', 'TEST02', 'TEST03', 'TEST04'],
            'END_DATE': ['2099-01-15', '2099-02-15', '2099-03-15', '2099-02-15'],
            'TOTAL_DOLLARS': [1000.0, 2000.0, 3000.0, 2000.0],
            'APR': [0.0, 0.0, 0.0, 0.0],
            'YTM': [0.01, 0.02, 0.03, 0.04],
            'ACCOUNT': ['Fidelity', 'Fidelity', 'Fidelity', 'Fidelity']
        }
        _pd_fixed_transactions = pd.DataFrame(data=_dict_fixed_transactions)
        mock_get_fixed_transactions.return_value = _pd_fixed_transactions
        _test_output = _test_instance.generate_mature_calender()
        self.assertTrue(mock_get_fixed_transactions.called)
        self.assertEqual(_test_output.shape[0], 3)
        self.assertEqual(list(_test_output.columns),
                         ['MATURE_DATE', 'TOTAL_DOLLARS', 'TOTAL_COUNT', 'YIELD', 'SYMBOL_REF'])
        self.assertEqual(list(_test_output['MATURE_DATE']), ['2099-01', '2099-02', '2099-03'])
        self.assertEqual(list(_test_output.iloc[0]), ['2099-01', '$1,000', 1, '1.00%', 'TEST01(Fidelity)'])

    @patch.object(SummaryTool, "_get_eq_positions_data")
    @patch.object(SummaryTool, "_get_eq_transactions_data")
    @patch.object(SummaryTool, "_get_fixed_transactions_data")
    @patch.object(SummaryTool, "_get_other_investment_information")
    def test_generate_allocation_report_account(self, mock_get_other_investment, mock_get_fixed_transactions,
                                                mock_get_eq_transactions, mock_get_eq_positions):
        """
        TestCase for SummaryTool.generate_allocation_report_account().
        """
        _test_instance = SummaryTool()
        _dict_eq_positions = {
            'SYMBOL': ['VOO', 'BLV'],
            'DOLLARS': [400.0, 100.0]
        }
        _dict_eq_transactions = {
            'SYMBOL': ['VOO', 'VOO', 'BLV', 'BLV'],
            'TYPE': ['BUY', 'BUY', 'BUY', 'SELL'],
            'UNITS': [20, 5, 100, 50],
            'ACCOUNT': ['TD', 'Fidelity', 'TD', 'TD']
        }
        _dict_fixed_transactions = {
            'TOTAL_DOLLARS': [5000.0, 2000.0],
            'END_DATE': ['2099-02-15', '2001-01-15'],
            'ACCOUNT': ['TD', 'TD']
        }
        _dict_other_investments = {
            'ACCOUNT': ['CITI', 'Fidelity'],
            'DOLLARS': [5000.0, 5000.0]
        }
        _pd_eq_positions = pd.DataFrame(data=_dict_eq_positions)
        _pd_eq_transactions = pd.DataFrame(data=_dict_eq_transactions)
        _pd_fixed_transactions = pd.DataFrame(data=_dict_fixed_transactions)
        _pd_other_investments = pd.DataFrame(data=_dict_other_investments)
        mock_get_eq_positions.return_value = _pd_eq_positions
        mock_get_eq_transactions.return_value = _pd_eq_transactions
        mock_get_fixed_transactions.return_value = _pd_fixed_transactions
        mock_get_other_investment.return_value = _pd_other_investments
        _test_output = _test_instance.generate_allocation_report_account()
        self.assertTrue(mock_get_eq_positions.called)
        self.assertTrue(mock_get_eq_transactions.called)
        self.assertTrue(mock_get_fixed_transactions.called)
        self.assertTrue(mock_get_other_investment.called)
        self.assertEqual(_test_output.shape[0], 3)
        self.assertEqual(list(_test_output.columns), ['ACCOUNT', 'TOTAL_DOLLARS', 'ALLOCATION'])
        self.assertEqual(list(_test_output['TOTAL_DOLLARS']), ['$18,000', '$7,000', '$5,000'])
        self.assertEqual(list(_test_output.iloc[0]), ['TD', '$18,000', '60%'])

    @patch.object(SummaryTool, "_get_eq_positions_data")
    def test_generate_allocation_report_equity_stock(self, mock_get_eq_positions):
        """
        TestCase for SummaryTool.generate_allocation_report_equity_stock().
        """
        _test_instance = SummaryTool()
        _dict_eq_positions = {
            'SYMBOL': ['VOO', 'VO', 'VB', 'BLV', 'VTIP', 'AAPL', 'MSFT'],
            'DESCRIPTION': [None, None, None, None, None, None, None],
            'INVESTMENT_TYPE': ['etf', 'etf', 'etf', 'etf', 'etf', 'stock', 'stock'],
            'MKT_VALUE': [20000.0, 2000.0, 2000.0, 10000.0, 5000.0, 2000.0, 3000.0]
        }
        _pd_eq_positions = pd.DataFrame(data=_dict_eq_positions)
        mock_get_eq_positions.return_value = _pd_eq_positions
        _test_output = _test_instance.generate_allocation_report_equity_stock()
        self.assertTrue(mock_get_eq_positions.called)
        self.assertEqual(_test_output.shape[0], 3)
        self.assertEqual(list(_test_output.columns), ['SYMBOL', 'DESCRIPTION', 'DOLLARS', 'STOCK_ALLOCATION'])
        self.assertEqual(list(_test_output['DOLLARS']), ['$3,000', '$2,000', '$5,000'])
        self.assertEqual(list(_test_output.iloc[0]), ['MSFT', None, '$3,000', '60.00%'])

    @patch.object(SummaryTool, "_get_fixed_transactions_data")
    @patch.object(SummaryTool, "_get_eq_transactions_data")
    @patch.object(SummaryTool, "_get_eq_positions_data")
    @patch.object(SummaryTool, "_get_other_investment_information")
    def test_generate_allocation_report_etf_w_account(self, mock_get_other_investments, mock_get_eq_positions, mock_get_eq_transactions, mock_get_fixed_transactions):
        """
        TestCase for SummaryTool.generate_allocation_report_etf_w_account().
        """
        _test_instance = SummaryTool()
        _dict_eq_transactions = {
            'SYMBOL': ['VOO', 'VOO', 'VB', 'VB', 'BND', 'AAPL'],
            'ACCOUNT': ['Fidelity', 'Vanguard', 'Fidelity', 'Fidelity', 'Vanguard', 'Fidelity'],
            'TYPE': ['BUY', 'BUY', 'BUY', 'SELL', 'BUY', 'BUY'],
            'UNITS': [30, 20, 100, 50, 100, 10]
        }
        _dict_eq_positions = {
            'SYMBOL': ['VOO', 'VB', 'BND', 'AAPL'],
            'DESCRIPTION': [None, None, None, None],
            'INVESTMENT_TYPE': ['etf', 'etf', 'etf', 'stock'],
            'DOLLARS': [400.0, 100.0, 100.0, 200.0]
        }
        _dict_other_investments = {
            'SUFFIX': ['n/a', 'n/a'],
            'MAJOR_TYPE': ['Cash Equivalent', 'Cash Equivalent'],
            'MINOR_TYPE': ['Cash', 'Cash'],
            'ACCOUNT': ['Fidelity', 'Fidelity'],
            'DOLLARS': [3000.0, 3000.0]
        }
        _dict_fixed_transactions = {
            
            'TOTAL_DOLLARS': [5000.0, 3000.0, 1000.0],
            'END_DATE': ['2099-01-01', '2010-01-01', '2099-01-01'],
            'INVESTMENT_TYPE': ['TREASURY', 'TREASURY', 'TREASURY'],
            'ACCOUNT': ['Fidelity', 'Fidelity', 'Vanguard'] 
        }
        _pd_eq_transactions = pd.DataFrame(data=_dict_eq_transactions)
        _pd_eq_positions = pd.DataFrame(data=_dict_eq_positions)
        _pd_other_investments = pd.DataFrame(data=_dict_other_investments)
        _pd_fixed_transactions = pd.DataFrame(data=_dict_fixed_transactions)
        mock_get_eq_transactions.return_value = _pd_eq_transactions
        mock_get_eq_positions.return_value = _pd_eq_positions
        mock_get_other_investments.return_value = _pd_other_investments
        mock_get_fixed_transactions.return_value = _pd_fixed_transactions
        _test_output = _test_instance.generate_allocation_report_etf_w_account('Fidelity')
        self.assertTrue(mock_get_eq_positions.called)
        self.assertEqual(_test_output.shape[0], 5)
        pd.set_option('display.expand_frame_repr', False)
        print(_test_output)
        self.assertEqual(list(_test_output.columns), ['ASSET_CLASS', 'ASSET_CLASS_TOTAL_DOLLARS',
                                                      'ASSET_CLASS_ALLOCATION', 'SUBCLASS',
                                                      'SUBCLASS_TOTAL_DOLLARS', 'SUBCLASS_ALLOCATION'])
        self.assertEqual(list(_test_output['SUBCLASS_TOTAL_DOLLARS']), ['$12,000', '$6,000', '$5,000', '$5,000', '$nan'])
        self.assertEqual(list(_test_output.iloc[0]), ['Large-Cap', '$12,000', '42.9%', 'Blend', '$12,000', '42.86%'])
