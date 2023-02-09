"""
This :module: contains Test Calls to :module: src/eq_SQLite_utility.

    Original Author: Mark D
    Date created: 09/14/2019
    Date Modified: 01/28/2021
    Python Version: 3.7

Note:
    none

Examples:
    python -m unittest test.test_eq_SQLite_utility


"""

import unittest
import sqlite3
import os
from time import sleep
from datetime import datetime
import csv

from src.eq_SQLite_utility import SQLiteRequest


class TestSQLiteRequests(unittest.TestCase):
    def setUp(self):
        """
        setup variables before each TestCase executed.
        """
        self.test_db_file = 'test/test.db'
        self.test_backup_file = 'transaction_backup_test.csv'

    def tearDown(self):
        """
        drop test files after each TestCase finished.
        """
        if os.path.exists(self.test_db_file):
            sleep(5)
            os.remove(self.test_db_file)
        if os.path.exists('backup/' + self.test_backup_file):
            os.remove('backup/' + self.test_backup_file)

    def test_init(self):
        """
        TestCase for SQLiteRequest.__init__().
        """
        _test_instance = SQLiteRequest(self.test_db_file)
        self.assertEqual(_test_instance.db_file, self.test_db_file)
        self.assertEqual(_test_instance.table_schema_file, 'templates/equity_tables_schema.json')
        self.assertEqual(_test_instance.view_query_positions, 'templates/equity_positions_view_query.sql')

    def test_ready_json_schema_file(self):
        """
        TestCase for SQLiteRequest._read_json_schema_file().
        """
        _test_instance = SQLiteRequest(self.test_db_file)
        try:
            _test_instance._read_json_schema_file('TRANSACTIONS')
        except RuntimeError:
            self.fail(":function: _read_json_schema_file('TRANSACTIONS') raised RuntimeError unexpectedly!")
        try:
            _test_instance._read_json_schema_file('WATCH_LIST')
        except RuntimeError:
            self.fail(":function: _read_json_schema_file('WATCH_LIST') raised RuntimeError unexpectedly!")

    def test_connection(self):
        """
        TestCase for SQLiteRequest._create_connection().
        """
        _test_instance = SQLiteRequest(self.test_db_file)
        self.assertTrue(isinstance(type(_test_instance._create_connection()), type(sqlite3.Connection)))

    def test_create_db(self):
        """
        TestCase for SQLiteRequest.create_database().
        """
        _test_instance = SQLiteRequest(self.test_db_file)
        try:
            _test_instance.create_database()
        except Exception as e:
            self.fail(":function: create_database() raised exception unexpectedly ! -> "+str(e))

    def test_create_tables(self):
        """
        TestCase for SQLiteRequest.create_table_transactions(),
            SQLiteRequest.create_table_watch_list(),
            SQLiteRequest.create_view_positions().
        """
        _test_instance = SQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        try:
            _test_instance.create_table_transactions()
        except Exception as e:
            self.fail(":function: create_table_transactions() raised exception unexpectedly ! -> "+str(e))
        try:
            _test_instance.create_table_watch_list()
        except Exception as e:
            self.fail(":function: create_table_watch_list() raised exception unexpectedly ! -> "+str(e))
        try:
            _test_instance.create_table_holdings()
        except Exception as e:
            self.fail(":function: create_table_tmp_holding_cost() raised exception unexpectedly ! -> "+str(e))
        try:
            _test_instance.create_view_positions()
        except Exception as e:
            self.fail(":function: create_view_positions() raised exception unexpectedly ! -> "+str(e))

    def test_insert_into_table_transactions(self):
        """
        TestCase for SQLiteRequest.insert_into_table_transactions().
        """
        _test_instance = SQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        _test_instance.create_table_transactions()
        try:
            _test_instance.insert_into_table_transactions('AAPL', 'BUY', '2018-12-31', 120.0, 10, 'stock', 'TD',
                                                          'Apple Inc')
        except Exception as e:
            self.fail(":function: insert_into_table_transactions() raised exception unexpectedly ! -> "+str(e))
        with self.assertRaises(IOError):
            _test_instance.insert_into_table_transactions(1, 'BUY', '2018-12-31', 120.0, 10, 'stock', 'TD', 'Apple Inc')
        with self.assertRaises(IOError):
            _test_instance.insert_into_table_transactions('AAPL', 1, '2018-12-31', 120.0, 10, 'stock', 'TD',
                                                          'Apple Inc')

    def test_backup_table_transactions(self):
        """
        TestCase for SQLiteRequest.backup_table_transactions().
        """
        _test_instance = SQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        _test_instance.create_table_transactions()
        _test_instance.insert_into_table_transactions('AAPL', 'BUY', '2018-12-31', 120.0, 10, 'stock', 'TD',
                                                      'Apple Inc')
        try:
            _test_instance.backup_table_transactions(self.test_backup_file)
            with open('backup/'+self.test_backup_file, 'r', newline='') as rf:
                my_reader = csv.reader(rf)
                count_of_rows = 0
                for row in my_reader:
                    count_of_rows += 1
                    if count_of_rows == 1:
                        self.assertEqual(row, ['SYMBOL', 'TYPE', 'DATE', 'DOLLARS', 'UNITS', 'INVESTMENT_TYPE',
                                               'DESCRIPTION', 'ACCOUNT', 'TOTAL_DOLLARS'])
                    elif count_of_rows == 2:
                        self.assertEqual(row, ['AAPL', 'BUY', '2018-12-31', '120.0', '10', 'stock', 'Apple Inc',
                                               'TD', '1200.0'])
                if count_of_rows != 2:
                    raise RuntimeError('Error: Backup file should contains exactly 2 rows ! got ' +
                                       str(count_of_rows) + 'rows.')
        except Exception as e:
            self.fail(":function: backup_table_transactions() raised exception unexpectedly ! -> "+str(e))

    def test_load_backup_to_table_transactions(self):
        """
        TestCase for SQLiteRequest.load_backup_to_table_transactions().
        """
        _test_instance = SQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        _test_instance.create_table_transactions()
        _test_instance.insert_into_table_transactions('AAPL', 'BUY', '2018-12-31', 120.0, 10, 'stock', 'TD',
                                                      'Apple Inc')
        _test_instance.backup_table_transactions(self.test_backup_file)
        if os.path.exists(self.test_db_file):
            os.remove(self.test_db_file)
        try:
            _test_instance_re = SQLiteRequest(self.test_db_file)
            _test_instance_re.create_database()
            _test_instance_re.create_table_transactions()
            _test_instance_re.load_backup_to_table_transactions('backup/'+self.test_backup_file)
        except Exception as e:
            self.fail(":function: load_backup_to_table_transactions() raised exception unexpectedly ! -> "+str(e))

    def test_sync_table_watch_list(self):
        """
        TestCase for SQLiteRequest.sync_table_watch_list().
        """
        _test_instance = SQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        _test_instance.create_table_transactions()
        _test_instance.insert_into_table_transactions('AAPL', 'BUY', '2018-12-31', 120.0, 10, 'stock', 'TD',
                                                      'Apple Inc')
        _test_instance.create_table_watch_list()
        try:
            _test_instance.sync_table_watch_list()
        except Exception as e:
            self.fail(":function: sync_table_watch_list() raised exception unexpectedly ! -> "+str(e))

    def test_update_table_watch_list(self):
        """
        TestCase for SQLiteRequest.update_table_watch_list().
        """
        _test_instance = SQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        _test_instance.create_table_transactions()
        _test_instance.insert_into_table_transactions('AAPL', 'BUY', '2018-12-31', 200.0, 10, 'stock', 'TD',
                                                      'Apple Inc')
        _test_instance.create_table_watch_list()
        _test_instance.sync_table_watch_list()
        try:
            _test_instance.update_table_watch_list('AAPL', 'Apple Inc.', 'stock', 220.0, 140.0, 240.0,
                                                   100000000000, 0, 22.0, 18.0, 0.015, float('nan'), 3.05, 4.12,
                                                   1.21, 0.0042, 'Technology', '')
        except Exception as e:
            self.fail(":function: update_table_watch_list() raised exception unexpectedly ! -> " + str(e))

    def test_sync_table_holdings(self):
        """
        TestCase for SQLiteRequest.sync_table_holdings().
        """
        _test_instance = SQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        _test_instance.create_table_transactions()
        _test_instance.insert_into_table_transactions('AAPL', 'BUY', '2018-12-31', 120.0, 10, 'stock', 'TD',
                                                      'Apple Inc')
        _test_instance.create_table_holdings()
        try:
            _test_instance.sync_table_holdings()
        except Exception as e:
            self.fail(":function: sync_table_holdings() raised exception unexpectedly ! -> "+str(e))

    def test_get_table_transactions(self):
        """
        TestCase for SQLiteRequest.get_table_transactions().
        """
        _test_instance = SQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        _test_instance.create_table_transactions()
        _test_instance.insert_into_table_transactions('AAPL', 'BUY', '2018-12-31', 200.0, 10, 'stock', 'TD',
                                                      'Apple Inc')
        try:
            test_output = _test_instance.get_table_transactions()
            self.assertEqual(len(test_output), 1)
            self.assertEqual(test_output[0]['SYMBOL'], 'AAPL')
            self.assertEqual(test_output[0]['TYPE'], 'BUY')
            self.assertEqual(test_output[0]['DATE'], '2018-12-31')
            self.assertEqual(float(test_output[0]['DOLLARS']), 200.0)
            self.assertEqual(int(test_output[0]['UNITS']), 10)
            self.assertEqual(test_output[0]['INVESTMENT_TYPE'], 'stock')
            self.assertEqual(test_output[0]['DESCRIPTION'], 'Apple Inc')
            self.assertEqual(test_output[0]['ACCOUNT'], 'TD')
            self.assertEqual(float(test_output[0]['TOTAL_DOLLARS']), 2000.0)
        except Exception as e:
            self.fail(":function: get_table_transaction() raised exception unexpectedly ! -> " + str(e))

    def test_get_table_watch_list(self):
        """
        TestCase for SQLiteRequest.get_table_watch_list().
        """
        _test_instance = SQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        _test_instance.create_table_transactions()
        _test_instance.insert_into_table_transactions('AAPL', 'BUY', '2018-12-31', 200.0, 10, 'stock', 'TD',
                                                      'Apple Inc')
        _test_instance.create_table_watch_list()
        _test_instance.sync_table_watch_list()
        _test_instance.update_table_watch_list('AAPL', 'Apple Inc.', 'stock', 220.0, 140.0, 240.0,
                                               100000000000, 0, 22.0, 18.0, 0.015, float('nan'), 3.05, float('nan'),
                                               1.21, 0.0042199, 'Technology', '')
        try:
            test_output = _test_instance.get_table_watch_list()
            self.assertEqual(len(test_output), 1)
            self.assertEqual(test_output[0]['SYMBOL'], 'AAPL')
            self.assertEqual(test_output[0]['INVESTMENT_TYPE'], 'stock')
            self.assertEqual(test_output[0]['LAST_UPDATED'], datetime.now().strftime('%Y-%m-%d'))
            self.assertEqual(test_output[0]['FULL_NAME'], 'Apple Inc.')
            self.assertEqual(float(test_output[0]['PREV_CLOSE']), 220.0)
            self.assertEqual(float(test_output[0]['LOW_52WKS']), 140.0)
            self.assertEqual(float(test_output[0]['HIGH_52WKS']), 240.0)
            self.assertEqual(test_output[0]['MKT_CAP'], '100.0 bil')
            self.assertEqual(test_output[0]['TOTAL_ASSETS'], '0.0 bil')
            self.assertEqual(float(test_output[0]['PE']), 22.0)
            self.assertEqual(float(test_output[0]['FORWARD_PE']), 18.0)
            self.assertEqual(float(test_output[0]['DIV']), 0.015)
            self.assertEqual(test_output[0]['YIELD'], None)
            self.assertEqual(float(test_output[0]['EPS']), 3.05)
            self.assertEqual(test_output[0]['FORWARD_EPS'], None)
            self.assertEqual(float(test_output[0]['BETA']), 1.21)
            self.assertEqual(float(test_output[0]['SHORT_FLOAT']), 0.0042)
            self.assertEqual(test_output[0]['SECTOR'], 'Technology')
            self.assertEqual(test_output[0]['CATEGORY'], '')
            self.assertEqual(int(test_output[0]['ENABLED']), 1)
        except Exception as e:
            self.fail(":function: get_table_watch_list() raised exception unexpectedly ! -> " + str(e))

    def test_get_view_positions(self):
        """
        TestCase for SQLiteRequest.get_view_positions().
        """
        _test_instance = SQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        _test_instance.create_table_transactions()
        _test_instance.insert_into_table_transactions('AAPL', 'BUY', '2018-12-31', 200.0, 10, 'stock', 'TD',
                                                      'Apple Inc')
        _test_instance.create_table_watch_list()
        _test_instance.sync_table_watch_list()
        _test_instance.update_table_watch_list('AAPL', 'Apple Inc.', 'stock', 220.0, 140.0, 240.0,
                                               100000000000, 0, 22.0, 18.0, 0.015, float('nan'), 3.05, float('nan'),
                                               1.21, 0.0042999, 'Technology', '')
        _test_instance.create_table_holdings()
        _test_instance.sync_table_holdings()
        _test_instance.create_view_positions()
        try:
            test_output = _test_instance.get_view_positions()
            self.assertEqual(len(test_output), 1)
            self.assertEqual(test_output[0]['SYMBOL'], 'AAPL')
            self.assertEqual(test_output[0]['INVESTMENT_TYPE'], 'stock')
            self.assertEqual(float(test_output[0]['COST_DOLLARS']), 200.0)
            self.assertEqual(float(test_output[0]['DOLLARS']), 220.0)
            self.assertEqual(int(test_output[0]['UNITS']), 10)
            self.assertEqual(test_output[0]['LAST_UPDATED'], datetime.now().strftime('%Y-%m-%d'))
            self.assertEqual(int(test_output[0]['MKT_VALUE']), 2200.0)
            self.assertEqual(float(test_output[0]['GAIN_PER_SHARE']), 20.0)
            self.assertEqual(float(test_output[0]['GAIN_TOTAL']), 200.0)
            self.assertEqual(float(test_output[0]['GAIN_PERCENTAGE']), 0.1)
        except Exception as e:
            self.fail(":function: get_view_positions() raised exception unexpectedly ! -> " + str(e))
