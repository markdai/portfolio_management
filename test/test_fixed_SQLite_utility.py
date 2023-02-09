"""
This :module: contains Test Calls to :module: src/fixed_SQLite_utility.

    Original Author: Mark D
    Date created: 12/29/2019
    Date Modified: 12/29/2019
    Python Version: 3.7

Note:
    none

Examples:
    python -m unittest test.test_fixed_SQLite_utility


"""

import unittest
import sqlite3
import os
from time import sleep
import csv

from src.fixed_SQLite_utility import FixedSQLiteRequest


class TestFixedSQLiteRequests(unittest.TestCase):
    def setUp(self):
        """
        setup variables before each TestCase executed.
        """
        self.test_db_file = 'test/test.db'
        self.test_backup_file = 'transaction_backup_test.csv'

    def tearDown(self):
        """
        drop temporary files after each TestCase finished.
        """
        if os.path.exists(self.test_db_file):
            sleep(5)
            os.remove(self.test_db_file)
        if os.path.exists('backup/' + self.test_backup_file):
            os.remove('backup/' + self.test_backup_file)

    def test_init(self):
        """
        TestCase for FixedSQLiteRequest.__init__().
        """
        _test_instance = FixedSQLiteRequest(self.test_db_file)
        self.assertEqual(_test_instance.db_file, self.test_db_file)
        self.assertEqual(_test_instance.table_schema_file, 'templates/fixed_tables_schema.json')
        self.assertEqual(_test_instance.view_query_positions, 'templates/fixed_positions_view_query.sql')

    def test_ready_json_schema_file(self):
        """
        TestCase for FixedSQLiteRequest._read_json_schema_file().
        """
        _test_instance = FixedSQLiteRequest(self.test_db_file)
        try:
            _test_instance._read_json_schema_file('TRANSACTIONS')
        except RuntimeError:
            self.fail(":function: _read_json_schema_file('TRANSACTIONS') raised RuntimeError unexpectedly!")

    def test_connection(self):
        """
        TestCase for FixedSQLiteRequest._create_connection().
        """
        _test_instance = FixedSQLiteRequest(self.test_db_file)
        self.assertTrue(isinstance(type(_test_instance._create_connection()), type(sqlite3.Connection)))

    def test_create_db(self):
        """
        TestCase for FixedSQLiteRequest.create_database().
        """
        _test_instance = FixedSQLiteRequest(self.test_db_file)
        try:
            _test_instance.create_database()
        except Exception as e:
            self.fail(":function: create_database() raised exception unexpectedly ! -> "+str(e))

    def test_create_tables_fixed(self):
        """
        TestCase for FixedSQLiteRequest.create_table_transactions_fixed(),
            FixedSQLiteRequest.create_table_tmp_matured_calender(),
            FixedSQLiteRequest.create_view_positions_fixed().
        """
        _test_instance = FixedSQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        try:
            _test_instance.create_table_transactions_fixed()
        except Exception as e:
            self.fail(":function: create_table_transactions_fixed() raised exception unexpectedly ! -> "+str(e))
        try:
            _test_instance.create_view_positions_fixed()
        except Exception as e:
            self.fail(":function: create_view_positions_fixed() raised exception unexpectedly ! -> "+str(e))

    def test_insert_into_table_transactions_fixed(self):
        """
        TestCase for FixedSQLiteRequest.insert_into_table_transactions_fixed().
        """
        _test_instance = FixedSQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        _test_instance.create_table_transactions_fixed()
        try:
            _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX1', 'TREA', 150, 100.0,
                                                                '2018-12-31', '2019-12-31', 14000.0, 'TD', YTM=0.025)
        except Exception as e:
            self.fail(":function: insert_into_table_transactions_fixed() raised exception unexpectedly ! -> "+str(e))
        try:
            _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX2', 'TREA', 150, 100.0,
                                                                '2018-11-30', '2019-11-30', 14000.0, 'TD', APR=0.025)
        except Exception as e:
            self.fail(":function: insert_into_table_transactions_fixed() raised exception unexpectedly ! -> "+str(e))
        with self.assertRaises(IOError):
            _test_instance.insert_into_table_transactions_fixed(999, 'XXXXXXXX3', 'TREA', 150, 100.0,
                                                                '2018-10-31', '2019-10-31', 14000.0, 'TD', YTM=0.025)
        with self.assertRaises(IOError):
            _test_instance.insert_into_table_transactions_fixed('USTB', 999, 'TREA', 150, 100.0,
                                                                '2018-10-31', '2019-10-31', 14000.0, 'TD', YTM=0.025)
        with self.assertRaises(IOError):
            _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX3', 999, 150, 100.0,
                                                                '2018-10-31', '2019-10-31', 14000.0, 'TD', YTM=0.025)
        with self.assertRaises(IOError):
            _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX3', 'TREA', '150', 100.0,
                                                                '2018-10-31', '2019-10-31', 14000.0, 'TD', YTM=0.025)
        with self.assertRaises(IOError):
            _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX3', 'TREA', 150, '100',
                                                                '2018-10-31', '2019-10-31', 14000.0, 'TD', YTM=0.025)
        with self.assertRaises(IOError):
            _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX3', 'TREA', 150, 100.0,
                                                                999, '2019-10-31', 14000.0, 'TD', YTM=0.025)
        with self.assertRaises(IOError):
            _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX3', 'TREA', 150, 100.0,
                                                                '10-31-2018', '2019-10-31', 14000.0, 'TD', YTM=0.025)
        with self.assertRaises(IOError):
            _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX3', 'TREA', 150, 100.0,
                                                                '2018-10-31', 999, 14000.0, 'TD', YTM=0.025)
        with self.assertRaises(IOError):
            _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX3', 'TREA', 150, 100.0,
                                                                '2018-10-31', '10-31-2019', 14000.0, 'TD', YTM=0.025)
        with self.assertRaises(IOError):
            _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX3', 'TREA', 150, 100.0,
                                                                '2018-10-31', '2019-10-31', '14000', 'TD', YTM=0.025)
        with self.assertRaises(IOError):
            _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX3', 'TREA', 150, 100.0,
                                                                '2018-10-31', '2019-10-31', 14000.0, 999, YTM=0.025)

    def test_backup_table_transactions_fixed(self):
        """
        TestCase for FixedSQLiteRequest.backup_table_transactions_fixed().
        """
        _test_instance = FixedSQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        _test_instance.create_table_transactions_fixed()
        _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX1', 'TREA', 150, 100.0,
                                                            '2018-12-31', '2019-12-31', 14000.0, 'TD', YTM=0.025)
        try:
            _test_instance.backup_table_transactions_fixed(self.test_backup_file)
            with open('backup/'+self.test_backup_file, 'r', newline='') as rf:
                my_reader = csv.reader(rf)
                count_of_rows = 0
                for row in my_reader:
                    count_of_rows += 1
                    if count_of_rows == 1:
                        self.assertEqual(row, ['NAME', 'SYMBOL', 'INVESTMENT_TYPE', 'UNITS', 'FACE_VALUE',
                                               'TOTAL_DOLLARS', 'ADD_DATE', 'END_DATE', 'TOTAL_COST', 'APR',
                                               'YTM', 'ACCOUNT'])
                    elif count_of_rows == 2:
                        self.assertEqual(row, ['USTB', 'XXXXXXXX1', 'TREA', '150', '100.0', '15000.0',
                                               '2018-12-31', '2019-12-31', '14000.0', '0.0', '0.025', 'TD'])
                if count_of_rows != 2:
                    raise RuntimeError('Error: Backup file should contains exactly 2 rows ! got ' +
                                       str(count_of_rows) + 'rows.')
        except Exception as e:
            self.fail(":function: backup_table_transactions_fixed() raised exception unexpectedly ! -> "+str(e))

    def test_load_backup_to_table_transactions_fixed(self):
        """
        TestCase for FixedSQLiteRequest.load_backup_to_table_transactions_fixed().
        """
        _test_instance = FixedSQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        _test_instance.create_table_transactions_fixed()
        _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX1', 'TREA', 150, 100.0,
                                                            '2018-12-31', '2019-12-31', 14000.0, 'TD', YTM=0.025)
        _test_instance.backup_table_transactions_fixed(self.test_backup_file)
        if os.path.exists(self.test_db_file):
            os.remove(self.test_db_file)
        try:
            _test_instance_re = FixedSQLiteRequest(self.test_db_file)
            _test_instance_re.create_database()
            _test_instance_re.create_table_transactions_fixed()
            _test_instance_re.load_backup_to_table_transactions_fixed('backup/'+self.test_backup_file)
        except Exception as e:
            self.fail(":function: load_backup_to_table_transactions_fixed() raised exception unexpectedly ! -> "+str(e))

    def test_get_table_transactions_fixed(self):
        """
        TestCase for FixedSQLiteRequest.get_table_transactions_fixed().
        """
        _test_instance = FixedSQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        _test_instance.create_table_transactions_fixed()
        _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX1', 'TREA', 150, 100.0,
                                                            '2018-12-31', '2019-12-31', 14000.0, 'TD', YTM=0.025)
        try:
            test_output = _test_instance.get_table_transactions_fixed()
            self.assertEqual(len(test_output), 1)
            self.assertEqual(test_output[0]['NAME'], 'USTB')
            self.assertEqual(test_output[0]['SYMBOL'], 'XXXXXXXX1')
            self.assertEqual(test_output[0]['INVESTMENT_TYPE'], 'TREA')
            self.assertEqual(int(test_output[0]['UNITS']), 150)
            self.assertEqual(float(test_output[0]['FACE_VALUE']), 100.0)
            self.assertEqual(float(test_output[0]['TOTAL_DOLLARS']), 15000.0)
            self.assertEqual(test_output[0]['ADD_DATE'], '2018-12-31')
            self.assertEqual(test_output[0]['END_DATE'], '2019-12-31')
            self.assertEqual(float(test_output[0]['TOTAL_COST']), 14000.0)
            self.assertEqual(float(test_output[0]['APR']), 0.0)
            self.assertEqual(float(test_output[0]['YTM']), 0.025)
            self.assertEqual(test_output[0]['ACCOUNT'], 'TD')
        except Exception as e:
            self.fail(":function: get_table_transaction_fixed() raised exception unexpectedly ! -> " + str(e))

    def test_get_view_positions_fixed(self):
        """
        TestCase for FixedSQLiteRequest.get_view_positions_fixed().
        """
        _test_instance = FixedSQLiteRequest(self.test_db_file)
        _test_instance.create_database()
        _test_instance.create_table_transactions_fixed()
        _test_instance.insert_into_table_transactions_fixed('USTB', 'XXXXXXXX1', 'TREA', 150, 100.0,
                                                            '2018-12-31', '2099-12-31', 14000.0, 'TD', YTM=0.025)
        _test_instance.create_view_positions_fixed()
        try:
            test_output = _test_instance.get_view_positions_fixed()
            self.assertEqual(len(test_output), 1)
            self.assertEqual(test_output[0]['NAME'], 'USTB')
            self.assertEqual(test_output[0]['SYMBOL'], 'XXXXXXXX1')
            self.assertEqual(test_output[0]['INVESTMENT_TYPE'], 'TREA')
            self.assertEqual(int(test_output[0]['UNITS']), 150)
            self.assertEqual(float(test_output[0]['FACE_VALUE']), 100.0)
            self.assertEqual(float(test_output[0]['TOTAL_DOLLARS']), 15000.0)
            self.assertEqual(test_output[0]['ADD_DATE'], '2018-12-31')
            self.assertEqual(test_output[0]['END_DATE'], '2099-12-31')
            self.assertEqual(float(test_output[0]['TOTAL_COST']), 14000.0)
            self.assertEqual(float(test_output[0]['RETURN_RATE']), 0.025)
            self.assertEqual(float(test_output[0]['RETURN_DOLLARS']), 350.0)
            self.assertEqual(int(test_output[0]['IS_MATURED']), 0)
        except Exception as e:
            self.fail(":function: get_view_positions_fixed() raised exception unexpectedly ! -> " + str(e))
