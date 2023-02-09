"""
This :module: contains Test Calls to :module: src/equity.

    Original Author: Mark D
    Date created: 11/27/2021
    Date Modified: 11/28/2021
    Python Version: 3.7

Note:
    none

Examples:
    python -m unittest test.test_equity


"""

from datetime import datetime
import unittest
from unittest.mock import patch

from src.equity import DbCommands
from src.eq_SQLite_utility import SQLiteRequest


class TestEquityCommands(unittest.TestCase):
    def test_init(self):
        """
        TestCase for DbCommands.__init__().
        """
        _test_instance = DbCommands()
        _current_date = datetime.now().strftime('%Y%m%d')
        self.assertEqual(_test_instance.production_db_file, 'databases/equity.db')
        self.assertEqual(_test_instance.backup_db_file, f'equity_transaction_backup_{_current_date}.csv')

    @patch.object(SQLiteRequest, "backup_table_transactions")
    def test_backup(self, mock_backup):
        """
        TestCase for DbCommands.backup().
        """
        _test_instance = DbCommands()
        _test_instance.backup()
        self.assertTrue(mock_backup.called)

    @patch.object(SQLiteRequest, "create_database")
    @patch.object(SQLiteRequest, "create_table_transactions")
    @patch.object(SQLiteRequest, "create_table_watch_list")
    @patch.object(SQLiteRequest, "create_table_holdings")
    @patch.object(SQLiteRequest, "create_view_positions")
    @patch.object(SQLiteRequest, "load_backup_to_table_transactions")
    def test_restore(self, mock_load_backup, mock_crt_position, mock_crt_holdings, mock_crt_watchlist,
                     mock_crt_transactions, mock_crt_database):
        """
        TestCase for DbCommands.restore().
        """
        _test_instance = DbCommands()
        _test_instance.restore()
        self.assertTrue(mock_crt_database.called)
        self.assertTrue(mock_crt_transactions.called)
        self.assertTrue(mock_crt_watchlist.called)
        self.assertTrue(mock_crt_holdings.called)
        self.assertTrue(mock_crt_position.called)
        self.assertTrue(mock_load_backup.called)

    @patch.object(SQLiteRequest, "insert_into_table_transactions")
    def test_add(self, mock_class_insert):
        """
        TestCase for DbCommands.add().
        """
        _test_instance = DbCommands()
        _test_instance.add('AAPL', 'BUY', '2018-12-31', 120.0, 10, 'stock', 'TD', 'Bought stock for Apple.inc')
        self.assertTrue(mock_class_insert.called)
