"""
This :module: contains Test Calls to :module: src/fixed_income.

    Original Author: Mark D
    Date created: 11/28/2021
    Date Modified: 11/28/2021
    Python Version: 3.7

Note:
    none

Examples:
    python -m unittest test.test_fixed_income


"""

from datetime import datetime
import unittest
from unittest.mock import patch

from src.fixed_income import DbCommands
from src.fixed_SQLite_utility import FixedSQLiteRequest


class TestFixedIncomeCommands(unittest.TestCase):
    def test_init(self):
        """
        TestCase for DbCommands.__init__().
        """
        _test_instance = DbCommands()
        _current_date = datetime.now().strftime('%Y%m%d')
        self.assertEqual(_test_instance.production_db_file, 'databases/fixed_income.db')
        self.assertEqual(_test_instance.backup_db_file, f'fixed_transaction_backup_{_current_date}.csv')

    @patch.object(FixedSQLiteRequest, "backup_table_transactions_fixed")
    def test_backup(self, mock_backup):
        """
        TestCase for DbCommands.backup().
        """
        _test_instance = DbCommands()
        _test_instance.backup()
        self.assertTrue(mock_backup.called)

    @patch.object(FixedSQLiteRequest, "create_database")
    @patch.object(FixedSQLiteRequest, "create_table_transactions_fixed")
    @patch.object(FixedSQLiteRequest, "create_view_positions_fixed")
    @patch.object(FixedSQLiteRequest, "load_backup_to_table_transactions_fixed")
    def test_restore(self, mock_load_backup, mock_crt_position, mock_crt_transactions, mock_crt_database):
        """
        TestCase for DbCommands.restore().
        """
        _test_instance = DbCommands()
        _test_instance.restore()
        self.assertTrue(mock_crt_database.called)
        self.assertTrue(mock_crt_transactions.called)
        self.assertTrue(mock_crt_position.called)
        self.assertTrue(mock_load_backup.called)

    @patch.object(FixedSQLiteRequest, "insert_into_table_transactions_fixed")
    def test_add(self, mock_class_insert):
        """
        TestCase for DbCommands.add().
        """
        _test_instance = DbCommands()
        _test_instance.add('US Treasury Notes', 'XXXXXXXX1', 'TREA', 100, 100.0, '2018-12-31', '2019-12-31',
                           9500.0, 'TD', YTM=0.025)
        self.assertTrue(mock_class_insert.called)
