"""
This :module: can be used to communicate with SQLite database.

    Original Author: Mark D
    Date created: 09/14/2019
    Date Modified: 01/05/2020
    Python Version: 3.7

Note:
    This module depend on following third-party Python library:
     - none

Examples:
    test_instance = SQLiteRequest('test/test.db')
    test_instance.create_database()
    test_instance.create_table_transactions()
    test_instance.create_table_watch_list()
    test_instance.create_table_holding_cost()
    test_instance.create_view_positions()
    test_instance.insert_into_table_transactions('AAPL', 'BUY', '2018-12-31', 120.0, 10, 'stock', 'TD')
    test_instance.backup_table_transactions('transaction_test.csv')
    test_instance.load_backup_to_table_transactions('backup/transaction_test.csv')
    test_instance.sync_table_watch_list()
    test_instance.update_table_watch_list('AAPL', 'stock', 220.0, 140.0, 240.0, '100M', 18.0, 0.015, 3.05, '2019-07-31')
    table_data_transactions = test_instance.get_table_transactions()
    table_data_watch_list = test_instance.get_table_watch_list()
    view_data_positions = test_instance.get_view_positions()

"""

import csv
import json
import sqlite3
import pandas as pd
from datetime import datetime

from .logger import UseLogging


class SQLiteRequest(object):
    """
    The :class: SQLiteRequest can be used for SQLite communications.
    """
    def __init__(self, v_db_filename):
        """
        constructor for :class: SQLiteRequest.
        """
        if not isinstance(v_db_filename, str):
            raise IOError("Constructor for :class: SQLiteUtility take a string argument. Got {}: {}".
                          format(str(type(v_db_filename)), str(v_db_filename))
                          )
        self.db_file = v_db_filename
        self.table_schema_file = "templates/equity_tables_schema.json"
        self.view_query_positions = "templates/equity_positions_view_query.sql"
        _logger_ref = UseLogging(__name__)
        self.logger = _logger_ref.use_loggers('portfolio_management')

    def _read_json_schema_file(self, v_table_name):
        """
        The :function: _read_json_schema_file is used to read Table Schema from JSON source file.

        Args:
            v_table_name (str): The table name to read from JSON file.

        Returns:
            :str: SQL Create statement to create table in SQLite.

        """
        self.logger.info("Loading Table metadata for {} from JSON schema file {} ...".format(
            v_table_name, self.table_schema_file)
        )
        column_headers = []
        try:
            with open(self.table_schema_file, 'r', newline='') as rf:
                schema_data = json.load(rf)
                for column in schema_data[v_table_name.upper()]:
                    column_headers.append(column['name'] + " " +
                                          column['type'] + " " +
                                          column['mode'].replace('NULLABLE', ''))
            that_result = "CREATE TABLE IF NOT EXISTS " + v_table_name.lower() + \
                          " ( " + \
                          ",".join([x.strip() for x in column_headers]) + \
                          " );"
        except Exception as e:
            self.logger.error("Failed to load table metadata for {} from JSON schema file {} ! -> {}".
                              format(v_table_name, self.table_schema_file, str(e))
                              )
            raise e
        return that_result

    def _create_connection(self):
        """
        The :function: _create_connection is used to initialize the SQLite connection.

        Args:

        Returns:
            sqlite3.Connection object.

        """
        try:
            this_conn = sqlite3.connect(self.db_file)
            self.logger.info("Connection to {} has been created ...".format(self.db_file))
            self.logger.info("SQLite version is: " + sqlite3.version)
        except sqlite3.Error as e:
            self.logger.error("Failed to connect to {} ! -> {}".format(self.db_file, str(e)))
            raise e
        return this_conn

    def create_database(self):
        """
        The :function: create_database is used to create the database file declared in __init__.

        Args:

        Returns:
            :boolean: True if job completed successfully.

        """
        try:
            this_conn = self._create_connection()
            self.logger.info(":database: {} has been created ...".format(self.db_file))
            if this_conn:
                this_conn.close()
                self.logger.info("Connection closed !")
            return True
        except Exception as e:
            self.logger.error("Failed to create :database: {} ! -> {}".format(self.db_file, str(e)))
            raise e

    def create_table_transactions(self):
        """
        The :function: create_table_transaction is used to create :table: 'transactions' in the SQLite DB file.

        Args:

        Returns:
            :boolean: True if job completed successfully.

        """
        create_table_sql = self._read_json_schema_file('TRANSACTIONS')
        try:
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.execute(create_table_sql)
            self.logger.info(":table: 'transactions' has been created ...")
            this_conn.close()
            self.logger.info("Connection closed !")
            return True
        except Exception as e:
            self.logger.error("Failed to create :table: 'transactions' ! -> " + str(e))
            raise e

    def create_table_watch_list(self):
        """
        The :function: create_table_watch_list is used to create :table: 'watch_list' in the SQLite DB file.

        Args:

        Returns:
            :boolean: True if job completed successfully.

        """
        create_table_sql = self._read_json_schema_file('WATCH_LIST')
        try:
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.execute(create_table_sql)
            self.logger.info(":table: 'watch_list' has been created ...")
            this_conn.close()
            self.logger.info("Connection closed !")
            return True
        except Exception as e:
            self.logger.error("Failed to create :table: 'watch_list' ! -> " + str(e))
            raise e

    def create_table_holdings(self):
        """
        The :function: create_table_holdings is used to create :table: 'tmp_holdings' in the SQLite DB file.

        Args:

        Returns:
            :boolean: True if job completed successfully.

        """
        create_table_sql = self._read_json_schema_file('TMP_HOLDINGS')
        try:
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.execute(create_table_sql)
            self.logger.info(":table: 'tmp_holdings' has been created ...")
            this_conn.close()
            self.logger.info("Connection closed !")
            return True
        except Exception as e:
            self.logger.error("Failed to create :table: 'tmp_holdings' ! -> " + str(e))
            raise e

    def create_view_positions(self):
        """
        The :function: create_view_positions is used to create :view: 'positions' in the SQLite DB file.

        Args:

        Returns:
            :boolean: True if job completed successfully.

        """
        try:
            self.logger.info("Loading Query Statement for :view: 'positions' from {} ...".format(
                self.view_query_positions)
            )
            with open(self.view_query_positions, 'r', newline='') as rf:
                create_view_sql = rf.read()
                this_conn = self._create_connection()
                this_cursor = this_conn.cursor()
                this_cursor.execute(create_view_sql)
                self.logger.info(":view: 'positions' has been created ...")
                this_conn.close()
                self.logger.info("Connection closed !")
            return True
        except Exception as e:
            self.logger.error("Failed to create :view: 'positions' ! -> " + str(e))
            raise e

    def insert_into_table_transactions(self, v_symbol, v_type, v_date, v_dollars, v_units,
                                       v_investment_type, v_account, v_description):
        """
        The :function: insert_into_table_transaction is used to insert new row into :table: 'transactions'.

        Args:
            v_symbol (str): The ticker symbol.
            v_type (str): transaction type, BUY/SELL.
            v_date (str): transaction Date in format 'YYYY-MM-DD'.
            v_dollars (float): dollars per share.
            v_units (int): number of shares.
            v_investment_type (str): The type of investment, e.g. stock/etf
            v_account (str): investment account.
            v_description (str): Name/Memo for transaction or symbol.

        Returns:
            :boolean: True if job completed successfully.

        """
        if not isinstance(v_symbol, str):
            raise IOError("1st :argument: v_symbol should be a string. Got {}: {}".format(
                str(type(v_symbol)), str(v_symbol))
            )
        if not isinstance(v_type, str) or str(v_type).upper() not in ['BUY', 'SELL']:
            raise IOError("2nd :argument: v_type should be 'BUY' or 'SELL'. Got {}: {}".format(
                str(type(v_type)), str(v_type))
            )
        if not isinstance(v_date, str):
            raise IOError("3rd :argument: v_date should be a string in 'YYYY-MM-DD' format. Got {}: {}".format(
                str(type(v_date)), str(v_date))
            )
        try:
            datetime.strptime(v_date, '%Y-%m-%d')
        except ValueError:
            raise IOError("3rd :argument: v_date should be in 'YYYY-MM-DD' format. Got {}: {}".format(
                str(type(v_date)), str(v_date))
            )
        if not isinstance(v_dollars, float):
            raise IOError("4th :argument: v_dollars should be a float. Got {}: {}".format(
                str(type(v_dollars)), str(v_dollars))
            )
        if not isinstance(v_units, int):
            raise IOError("5th :argument: v_units should be an integer. Got {}: {}".format(
                str(type(v_units)), str(v_units))
            )
        if not isinstance(v_investment_type, str):
            raise IOError("6th :argument: v_investment_type should be a string. Got {}: {}".format(
                str(type(v_investment_type)), str(v_investment_type))
            )
        if not isinstance(v_account, str):
            raise IOError("7th :argument: v_account should be a string. Got {}: {}".format(
                str(type(v_account)), str(v_account))
            )
        if not isinstance(v_description, str):
            raise IOError("8th :argument: v_description should be a string. Got {}: {}".format(
                str(type(v_description)), str(v_description))
            )
        try:
            self.logger.info("Inserting into :table: 'transactions' ...")
            this_total_dollars = v_dollars*v_units
            this_insert_values = (
                v_symbol, v_type.upper(), v_date, v_dollars, v_units,
                v_investment_type, v_description, v_account, this_total_dollars
            )
            insert_sql = ''' INSERT INTO transactions (
            SYMBOL, TYPE, DATE, DOLLARS, UNITS, INVESTMENT_TYPE, DESCRIPTION, ACCOUNT, TOTAL_DOLLARS) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);'''
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.execute(insert_sql, this_insert_values)
            this_conn.commit()
            self.logger.info("Entry has been inserted into :table: 'transactions' (SYMBOL={}, TYPE={}, DATE={}, "
                             "DOLLARS={}, UNITS={}, INVESTMENT_TYPE={}, DESCRIPTION={}, ACCOUNT={}, "
                             "TOTAL_DOLLARS={}".format(
                                 this_insert_values[0], this_insert_values[1], this_insert_values[2],
                                 this_insert_values[3], this_insert_values[4], this_insert_values[5],
                                 this_insert_values[6], this_insert_values[7], this_insert_values[8]))
            this_conn.close()
            if int(v_date.split('-')[0]) != int(datetime.today().year):
                self.logger.warning(f"A transaction for Year {v_date.split('-')[0]} was added !")
            return True
        except Exception as e:
            self.logger.error("Failed to insert into :table: 'transactions' ! -> "+str(e))
            raise e

    def backup_table_transactions(self,
                                  outfile_name='equity_transaction_backup_'+datetime.now().strftime('%Y%m%d_%H%M%S') +
                                               '.csv'):
        """
        The :function: backup_table_transaction is used to export data from :table: 'transactions'
            into a CSV backup file in backup folder.

        Args:
            outfile_name (str): The backup output filename, default to 'equity_transaction_backup_YYYYMMDD_HHMMSS.csv'.

        Returns:
            :boolean: True if job completed successfully.

        """
        list_of_header = ['SYMBOL', 'TYPE', 'DATE', 'DOLLARS', 'UNITS', 'INVESTMENT_TYPE', 'DESCRIPTION',
                          'ACCOUNT', 'TOTAL_DOLLARS']
        try:
            self.logger.info("Creating Backup for :table: 'transactions' ...")
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            query_sql = "SELECT {} FROM transactions;".format(', '.join(list_of_header))
            this_cursor.execute(query_sql)
            this_output = this_cursor.fetchall()
            with open('backup/'+outfile_name, 'w+', newline='') as wf:
                my_writer = csv.writer(wf)
                my_writer.writerow(list_of_header)
                for row in this_output:
                    my_writer.writerow(row)
            self.logger.info("Backup for :table: 'transactions' has been created ...")
            this_conn.close()
            return True
        except Exception as e:
            self.logger.error("Failed to backup :table: 'transactions' ! -> " + str(e))
            raise e

    def load_backup_to_table_transactions(self, infile_name):
        """
        The :function: load_backup_to_table_transaction is used to load backup file into :table: 'transactions'.

        Args:
            infile_name (str): The backup input filename.

        Returns:
            :boolean: True if job completed successfully.

        """
        try:
            self.logger.info("Loading from Backup into :table: 'transactions' ...")
            with open(infile_name, 'r', newline='') as rf:
                my_reader = csv.DictReader(rf)
                to_db = [(row['SYMBOL'], row['TYPE'], row['DATE'], row['DOLLARS'], row['UNITS'],
                          row['INVESTMENT_TYPE'], row['DESCRIPTION'], row['ACCOUNT'], row['TOTAL_DOLLARS']
                          ) for row in my_reader]
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            insert_sql = '''INSERT INTO transactions (
            SYMBOL, TYPE, DATE, DOLLARS, UNITS, INVESTMENT_TYPE, DESCRIPTION, ACCOUNT, TOTAL_DOLLARS
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);'''
            this_cursor.executemany(insert_sql, to_db)
            this_conn.commit()
            self.logger.info("Backup for :table: 'transactions' has been loaded ...")
            this_conn.close()
        except Exception as e:
            self.logger.error("Failed to load backup for :table: 'transactions' ! -> " + str(e))
            raise e

    def sync_table_watch_list(self, v_update_everything=0):
        """
        The :function: sync_table_watch_list is used to sync :table: watch_list to include
            all SYMBOLS in :table: transactions.

        Args:
            v_update_everything(int): 0 for False, 1 for True

        Returns:
            :boolean: True if job completed successfully.

        """
        try:
            self.logger.info("Syncing :table: 'watch_list' to include all SYMBOLS in :table: transactions ...")
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            update_sql_one = '''INSERT INTO watch_list (
            SYMBOL, INVESTMENT_TYPE, LAST_UPDATED, ENABLED
            ) SELECT DISTINCT symbol, investment_type, '{}', 1 
            FROM transactions AS t1 WHERE NOT EXISTS ( 
            SELECT 1 FROM watch_list AS t2 
            WHERE t1.symbol = t2.symbol AND t1.investment_type = t2.investment_type) 
            AND LOWER(t1.investment_type) <> "others";'''.format(
                datetime.now().strftime('%Y-%m-%d')
            )
            this_cursor.execute(update_sql_one)
            this_conn.commit()
            update_sql_two = '''UPDATE watch_list 
            SET enabled = ? 
            WHERE symbol NOT IN (
             SELECT symbol FROM (
              SELECT symbol,
               investment_type,
               IFNULL(SUM(CASE WHEN type = 'BUY' THEN units ELSE NULL END),0) AS BOUGHT_UNITS,
               IFNULL(SUM(CASE WHEN type = 'SELL' THEN units ELSE NULL END),0) AS SOLD_UNITS
              FROM transactions
              GROUP BY symbol, investment_type
             ) WHERE (BOUGHT_UNITS-SOLD_UNITS) > 0
            )'''
            this_cursor.execute(update_sql_two, (str(v_update_everything)))
            this_conn.commit()
            update_sql_three = '''UPDATE watch_list 
            SET enabled = 1 
            WHERE symbol IN (
             SELECT symbol FROM (
              SELECT symbol,
               investment_type,
               IFNULL(SUM(CASE WHEN type = 'BUY' THEN units ELSE NULL END),0) AS BOUGHT_UNITS,
               IFNULL(SUM(CASE WHEN type = 'SELL' THEN units ELSE NULL END),0) AS SOLD_UNITS
              FROM transactions
              GROUP BY symbol, investment_type
             ) WHERE (BOUGHT_UNITS-SOLD_UNITS) > 0
            )'''
            this_cursor.execute(update_sql_three)
            this_conn.commit()
            self.logger.info(":table: 'watch_list' has been synced with :table: 'transactions' ...")
            this_conn.close()
            return True
        except Exception as e:
            self.logger.error("Failed to sync :table: 'watch_list' ! -> " + str(e))
            raise e

    def update_table_watch_list(self, v_symbol, v_name, v_investment_type, v_prev_close, v_low_52wks, v_high_52wks,
                                v_mkt_cap, v_total_assets, v_pe, v_forward_pe, v_div, v_yield, v_eps, v_forward_eps,
                                v_beta, v_short_float, v_sector, v_category):
        """
        The :function: update_table_watch_list is used to update rows in :table: watch_list.

        Args:
            v_symbol (str): The ticker symbol.
            v_name (str): Long or Short name for the ticker.
            v_investment_type (str): The type of investment, e.g. stock/etf
            v_prev_close (float): Previous closed quote.
            v_low_52wks (float): Lowest quote in latest 52 weeks.
            v_high_52wks (float): Highest quote in latest 52 weeks.
            v_mkt_cap (int): Market Capitalization for a stock.
            v_total_assets (int): Total asset for the ETF in all classes.
            v_pe (float): Trailing PE ratio.
            v_forward_pe (float): Forward PE ratio.
            v_div (float): Dividend ratio.
            v_yield (float): Dividend yield.
            v_eps (float): Trailing Earning per share.
            v_forward_eps (float): Forward Earning per share.
            v_beta (float): Beta ratio.
            v_short_float (float): Percentage of shares shorted in total number of outstanding shares.
            v_sector (str): The business sector for this stock.
            v_category (str): The business category for this ETF.

        Returns:
            :boolean: True if job completed successfully.

        """
        if not isinstance(v_symbol, str):
            raise IOError("1st :argument: v_symbol should be a string. Got {}: {}".format(
                str(type(v_symbol)), str(v_symbol))
            )
        if not isinstance(v_name, str):
            raise IOError("2nd :argument: v_name should be a string. Got {}: {}".format(
                str(type(v_name)), str(v_name))
            )
        if not isinstance(v_investment_type, str):
            raise IOError("3rd :argument: v_investment_type should be a string. Got {}: {}".format(
                str(type(v_investment_type)), str(v_investment_type))
            )
        if not isinstance(v_prev_close, float) and not isinstance(v_prev_close, int):
            raise IOError("4th :argument: v_prev_close should be a float. Got {}: {}".format(
                str(type(v_prev_close)), str(v_prev_close))
            )
        if not isinstance(v_low_52wks, float) and not isinstance(v_low_52wks, int):
            raise IOError("5th :argument: v_low_52wks should be a float. Got {}: {}".format(
                str(type(v_low_52wks)), str(v_low_52wks))
            )
        if not isinstance(v_high_52wks, float) and not isinstance(v_high_52wks, int):
            raise IOError("6th :argument: v_high_52wks should be a float. Got {}: {}".format(
                str(type(v_high_52wks)), str(v_high_52wks))
            )
        if not isinstance(v_mkt_cap, int):
            raise IOError("7th :argument: v_mkt_cap should be an integer. Got {}: {}".format(
                str(type(v_mkt_cap)), str(v_mkt_cap))
            )
        if not isinstance(v_total_assets, int):
            raise IOError("8th :argument: v_total_assets should be an integer. Got {}: {}".format(
                str(type(v_total_assets)), str(v_total_assets))
            )
        if not isinstance(v_pe, float) and not isinstance(v_pe, int):
            raise IOError("9th :argument: v_pe should be a float. Got {}: {}".format(
                str(type(v_pe)), str(v_pe))
            )
        if not isinstance(v_forward_pe, float) and not isinstance(v_forward_pe, int):
            raise IOError("10th :argument: v_forward_pe should be a float. Got {}: {}".format(
                str(type(v_forward_pe)), str(v_forward_pe))
            )
        if not isinstance(v_div, float) and not isinstance(v_div, int):
            raise IOError("11th :argument: v_div should be a float or integer. Got {}: {}".format(
                str(type(v_div)), str(v_div))
            )
        if not isinstance(v_yield, float):
            raise IOError("12th :argument: v_yield should be a float. Got {}: {}".format(
                str(type(v_yield)), str(v_yield))
            )
        if not isinstance(v_eps, float) and not isinstance(v_eps, int):
            raise IOError("13th :argument: v_eps should be a float. Got {}: {}".format(
                str(type(v_eps)), str(v_eps))
            )
        if not isinstance(v_forward_eps, float) and not isinstance(v_forward_eps, int):
            raise IOError("14th :argument: v_forward_eps should be a float. Got {}: {}".format(
                str(type(v_forward_eps)), str(v_forward_eps))
            )
        if not isinstance(v_beta, float) and not isinstance(v_beta, int):
            raise IOError("15th :argument: v_beta should be a float. Got {}: {}".format(
                str(type(v_beta)), str(v_beta))
            )
        if not isinstance(v_short_float, float):
            raise IOError("16th :argument: v_short_float should be a float. Got {}: {}".format(
                str(type(v_short_float)), str(v_short_float))
            )
        if not isinstance(v_sector, str):
            raise IOError("17th :argument: v_sector should be a string. Got {}: {}".format(
                str(type(v_sector)), str(v_sector))
            )
        if not isinstance(v_category, str):
            raise IOError("18th :argument: v_category should be a string. Got {}: {}".format(
                str(type(v_category)), str(v_category))
            )
        try:
            _current_date = datetime.now().strftime('%Y-%m-%d')
            self.logger.info("Update :table: 'watch_list' data ...")
            update_sql = ''' UPDATE watch_list 
            SET LAST_UPDATED = ?, FULL_NAME = ?, PREV_CLOSE = ?, LOW_52WKS = ?, HIGH_52WKS = ?, MKT_CAP = ?, 
            TOTAL_ASSETS = ?, PE = ?, FORWARD_PE = ?, DIV = ?, YIELD = ?, EPS = ?, FORWARD_EPS = ?, 
            BETA = ?, SHORT_FLOAT = ?, SECTOR = ?, CATEGORY = ? 
            WHERE SYMBOL = ? AND INVESTMENT_TYPE = ? '''
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.execute(update_sql, (_current_date,
                                             v_name,
                                             round(float(v_prev_close), 2),
                                             round(float(v_low_52wks), 2),
                                             round(float(v_high_52wks), 2),
                                             str(round(v_mkt_cap/1000000000, 2))+' bil',
                                             str(round(v_total_assets/1000000000, 2))+' bil',
                                             round(float(v_pe), 2),
                                             round(float(v_forward_pe), 2),
                                             round(float(v_div), 4),
                                             round(v_yield, 4),
                                             round(float(v_eps), 2),
                                             round(float(v_forward_eps), 2),
                                             round(float(v_beta), 2),
                                             round(v_short_float, 4),
                                             v_sector,
                                             v_category,
                                             v_symbol,
                                             v_investment_type)
                                )
            this_conn.commit()
            self.logger.info('''SYMBOL '{}', INVESTMENT_TYPE '{}' has been updated in :table: 'watch_list' (
            LAST_UPDATED = '{}', FULL_NAME = '{}', PREV_CLOSE = '{}', LOW_52WKS = '{}', HIGH_52WKS = '{}', 
            MKT_CAP = '{}', TOTAL_ASSETS = '{}', PE = '{}', FORWARD_PE = '{}', DIV = '{}', YIELD = '{}', EPS = '{}', 
            FORWARD_EPS = '{}', BETA = '{}', SHORT_FLOAT = '{}', SECTOR = '{}', 
            CATEGORY = '{}' '''.format(
                str(v_symbol).upper(), str(v_investment_type).upper(), _current_date, str(v_name), str(v_prev_close),
                str(v_low_52wks), str(v_high_52wks), str(v_mkt_cap), str(v_total_assets), str(v_pe), str(v_forward_pe),
                str(v_div), str(v_yield), str(v_eps), str(v_forward_eps), str(v_beta), str(v_short_float),
                str(v_sector), str(v_category))
            )
            this_conn.close()
            return True
        except Exception as e:
            self.logger.error("Failed to update :table: 'watch_list' ! -> " + str(e))
            raise e

    def sync_table_holdings(self):
        """
        The :function: sync_table_holdings is used to update :table: tmp_holdings based on :table: transactions.

        Args:

        Returns:
            :boolean: True if job completed successfully.

        """
        try:
            self.logger.info("Syncing :table: tmp_holdings ...")
            self.logger.info("Querying data from table: 'transactions' ...")
            query_sql = "SELECT ID, SYMBOL, INVESTMENT_TYPE, TYPE, DATE, DOLLARS, UNITS, DESCRIPTION " \
                        "FROM transactions WHERE LOWER(INVESTMENT_TYPE) <> 'others'"
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.execute(query_sql)
            this_result = this_cursor.fetchall()
            this_conn.close()
            _that_output = []
            for row in this_result:
                _that_output.append(list(row))
            self.logger.info("Constructing DataFrame for transactions data ...")
            df_transactions = pd.DataFrame(_that_output,
                                           columns=['ID', 'SYMBOL', 'INVESTMENT_TYPE', 'TRANS_TYPE', 'TRANS_DATE',
                                                    'DOLLARS', 'UNITS', 'DESCRIPTION'])
            df_transactions["TOTAL_COST"] = 0.0
            df_transactions.sort_values(by=["TRANS_DATE"], inplace=True)
            self.logger.info("Constructing DataFrame for :table: tmp_holdings ...")
            df_tmp_holding = pd.DataFrame(columns=["SYMBOL", "DESCRIPTION", "INVESTMENT_TYPE", "UNITS", "COST_DOLLARS"])
            df_tmp_holding["SYMBOL"] = df_transactions["SYMBOL"].unique()
            for i in range(df_tmp_holding.shape[0]):
                df_tmp_holding.at[i, "DESCRIPTION"] = df_transactions.loc[
                    (df_transactions["SYMBOL"] == df_tmp_holding.iloc[i]["SYMBOL"]) &
                    (df_transactions["TRANS_TYPE"] == 'BUY')]["DESCRIPTION"].min()
                df_tmp_holding.at[i, "INVESTMENT_TYPE"] = df_transactions.loc[
                    df_transactions["SYMBOL"] == df_tmp_holding.iloc[i]["SYMBOL"]]["INVESTMENT_TYPE"].min()
            df_tmp_holding["UNITS"] = 0
            df_tmp_holding["COST_DOLLARS"] = 0.0

            self.logger.info("Calculating :field: UNITS & COST_DOLLARS for :table: tmp_holding, "
                             "and GAIN/LOSS of 'SELL' transactions ...")
            for i in range(df_tmp_holding.shape[0]):
                df_added = df_transactions.loc[(df_transactions["SYMBOL"] == df_tmp_holding.iloc[i]["SYMBOL"]) & (
                            df_transactions["TRANS_TYPE"] == 'BUY')][["ID", "DOLLARS", "UNITS"]].reset_index()
                df_reduced = df_transactions.loc[(df_transactions["SYMBOL"] == df_tmp_holding.iloc[i]["SYMBOL"]) & (
                            df_transactions["TRANS_TYPE"] == 'SELL')][["ID", "DOLLARS", "UNITS"]].reset_index()
                df_reduced["TOTAL_COST"] = 0.0
                for j in range(df_reduced.shape[0]):
                    this_delta_units = df_reduced.iloc[j]["UNITS"]
                    this_cond = True
                    this_idx = 0
                    while this_cond and this_idx < df_added.shape[0]:
                        if df_added.iloc[this_idx]["UNITS"] >= this_delta_units:
                            df_reduced.at[j, "TOTAL_COST"] = df_reduced.iloc[j]["TOTAL_COST"] + \
                                                             df_added.iloc[this_idx]["DOLLARS"] * this_delta_units
                            df_added.at[this_idx, "UNITS"] = df_added.iloc[this_idx]["UNITS"] - this_delta_units
                            this_cond = False
                        else:
                            df_reduced.at[j, "TOTAL_COST"] = df_reduced.iloc[j]["TOTAL_COST"] + \
                                                             df_added.iloc[this_idx]["DOLLARS"] * \
                                                             df_added.iloc[this_idx]["UNITS"]
                            this_delta_units = this_delta_units - df_added.iloc[this_idx]["UNITS"]
                            df_added.at[this_idx, "UNITS"] = 0
                            this_idx += 1
                df_added["TOTAL_DOLLARS"] = df_added["DOLLARS"] * df_added["UNITS"]
                for k in range(df_reduced.shape[0]):
                    df_transactions.loc[df_transactions["ID"] == df_reduced.iloc[k]["ID"], "TOTAL_COST"] = \
                        df_reduced.iloc[k]["TOTAL_COST"]
                df_tmp_holding.at[i, "UNITS"] = df_added["UNITS"].sum()
                if df_added["UNITS"].sum() == 0:
                    df_tmp_holding.at[i, "COST_DOLLARS"] = 0.0
                else:
                    df_tmp_holding.at[i, "COST_DOLLARS"] = df_added["TOTAL_DOLLARS"].sum() / df_added["UNITS"].sum()
            df_output_holding = df_tmp_holding.loc[df_tmp_holding["UNITS"] != 0]
            df_transactions.loc[df_transactions["TRANS_TYPE"] == 'SELL', "TOTAL_GAIN"] = \
                df_transactions["DOLLARS"] * df_transactions["UNITS"] - df_transactions["TOTAL_COST"]
            df_output_transactions = df_transactions[["ID", "TOTAL_GAIN"]]

            self.logger.info("Truncate :table: tmp_holdings ...")
            _truncate_sql = "DELETE FROM tmp_holdings;"
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.execute(_truncate_sql)
            this_conn.commit()
            this_conn.close()
            self.logger.info("Loading result into :table: tmp_holdings ...")
            insert_sql = "INSERT INTO tmp_holdings (SYMBOL, DESCRIPTION, INVESTMENT_TYPE, UNITS, COST_DOLLARS) " \
                         "VALUES (?, ?, ?, ?, ?);"
            insert_data = [tuple([x[0], x[1], x[2], x[3], round(x[4], 2)]) for x in df_output_holding.values]
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.executemany(insert_sql, insert_data)
            this_conn.commit()
            this_conn.close()
            self.logger.info("Updating :column: TOTAL_GAIN in :table: transactions ...")
            insert_sql = "UPDATE transactions SET TOTAL_GAIN = ? WHERE ID = ?;"
            insert_data = [tuple([round(x[1], 2), x[0]]) for x in df_output_transactions.values]
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.executemany(insert_sql, insert_data)
            this_conn.commit()
            this_conn.close()
            return True
        except Exception as e:
            self.logger.error("Failed to sync :table: tmp_holdings ! -> " + str(e))
            raise e

    def __sync_table_holdings(self):
        """
        The :function: sync_table_holdings is used to update :table: tmp_holdings based on :table: transactions.

        Args:

        Returns:
            :boolean: True if job completed successfully.

        """
        try:
            self.logger.info("Syncing :table: tmp_holdings ...")
            self.logger.info("Getting cost of purchase from table: 'transactions' ...")
            query_sql = """SELECT t1.SYMBOL AS SYMBOL, 
             t3.DESCRIPTION AS DESCRIPTION,
             t1.INVESTMENT_TYPE AS INVESTMENT_TYPE, 
             t1.DOLLARS AS DOLLARS, 
             t1.UNITS AS UNITS, 
             t1.DATE AS DATE, 
             t1.TYPE AS TYPE 
            FROM transactions t1 
            JOIN ( 
             SELECT SYMBOL, 
              INVESTMENT_TYPE, 
              IFNULL(SUM(CASE WHEN TYPE = 'BUY' THEN UNITS ELSE NULL END),0) AS BOUGHT_UNITS, 
              IFNULL(SUM(CASE WHEN TYPE = 'SELL' THEN UNITS ELSE NULL END),0) AS SOLD_UNITS
             FROM transactions
             GROUP BY SYMBOL, INVESTMENT_TYPE
            ) t2 ON t1.SYMBOL = t2.SYMBOL AND t1.INVESTMENT_TYPE = t2.INVESTMENT_TYPE
            JOIN (
             SELECT SYMBOL, 
              MAX(DESCRIPTION) AS DESCRIPTION,
              INVESTMENT_TYPE
             FROM transactions
             WHERE TYPE = 'BUY'
             GROUP BY SYMBOL, INVESTMENT_TYPE
            ) t3 ON t1.SYMBOL = t3.SYMBOL AND t1.INVESTMENT_TYPE = t3.INVESTMENT_TYPE
            WHERE (t2.BOUGHT_UNITS-t2.SOLD_UNITS) > 0"""
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.execute(query_sql)
            this_result = this_cursor.fetchall()
            this_conn.close()
            _that_output = []
            for row in this_result:
                _that_output.append(list(row))
            self.logger.info("Calculating cost of holdings ...")
            df = pd.DataFrame([list(y) for y in list(set([(x[0], x[1], x[2], 0, 0.0) for x in _that_output]))],
                              columns=['SYMBOL', 'DESCRIPTION', 'INVESTMENT_TYPE', 'UNITS', 'DOLLARS'])
            _raw_data = sorted(_that_output, key=lambda x: x[5])
            for i in range(len(_raw_data)):
                _current_unit = df.loc[df['SYMBOL'] == _raw_data[i][0]].iloc[0]['UNITS']
                _current_dollars = df.loc[df['SYMBOL'] == _raw_data[i][0]].iloc[0]['DOLLARS']
                if _raw_data[i][6].upper() == 'BUY':
                    df.loc[df['SYMBOL'] == _raw_data[i][0], 'UNITS'] = _current_unit + _raw_data[i][4]
                    df.loc[df['SYMBOL'] == _raw_data[i][0], 'DOLLARS'] = ((_current_dollars * _current_unit) +
                                                                          (_raw_data[i][3] * _raw_data[i][4])) / \
                                                                         (_current_unit + _raw_data[i][4])
                else:
                    df.loc[df['SYMBOL'] == _raw_data[i][0], 'UNITS'] = _current_unit - _raw_data[i][4]
            self.logger.info("Truncate :table: tmp_holdings ...")
            _truncate_sql = "DELETE FROM tmp_holdings;"
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.execute(_truncate_sql)
            this_conn.commit()
            this_conn.close()
            self.logger.info("Loading result into :table: tmp_holdings ...")
            insert_sql = "INSERT INTO tmp_holdings (SYMBOL, DESCRIPTION, INVESTMENT_TYPE, UNITS, COST_DOLLARS) " \
                         "VALUES (?, ?, ?, ?, ?);"
            insert_data = [tuple([x[0], x[1], x[2], x[3], round(x[4], 2)]) for x in df.values]
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.executemany(insert_sql, insert_data)
            this_conn.commit()
            this_conn.close()
            return True
        except Exception as e:
            self.logger.error("Failed to sync :table: tmp_holdings ! -> " + str(e))
            raise e

    def get_table_transactions(self):
        """
        The :function: get_table_transaction is used to query all data from :table: 'transaction' into a list of
            dictionary, use column name as dictionary key.

        Args:

        Returns:
            :list: of dictionary, can be read by column name.

        """
        list_of_header = ['ID', 'SYMBOL', 'TYPE', 'DATE', 'DOLLARS', 'UNITS', 'INVESTMENT_TYPE', 'DESCRIPTION',
                          'ACCOUNT', 'TOTAL_DOLLARS']
        try:
            self.logger.info("Attempt to get :table: 'transactions' data ...")
            query_sql = "SELECT {} FROM transactions;".format(', '.join(list_of_header))
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.execute(query_sql)
            this_result = this_cursor.fetchall()
            this_conn.close()
            that_output = []
            for row in this_result:
                this_dict = {}
                for i in range(len(list_of_header)):
                    this_dict[list_of_header[i]] = row[i]
                that_output.append(this_dict)
            return that_output
        except Exception as e:
            self.logger.error("Failed to get :table: 'transactions' data ! -> " + str(e))
            raise e

    def get_table_watch_list(self):
        """
        The :function: get_table_watch_list is used to query all data from :table: 'watch_list' into a list of
            dictionary, use column name as dictionary key.

        Args:

        Returns:
            :list: of dictionary, can be read by column name.

        """
        list_of_header = ['SYMBOL', 'FULL_NAME', 'INVESTMENT_TYPE', 'LAST_UPDATED', 'PREV_CLOSE', 'LOW_52WKS',
                          'HIGH_52WKS', 'MKT_CAP', 'TOTAL_ASSETS', 'PE', 'FORWARD_PE', 'DIV', 'YIELD', 'EPS',
                          'FORWARD_EPS', 'BETA', 'SHORT_FLOAT', 'SECTOR', 'CATEGORY', 'ENABLED']
        try:
            self.logger.info("Attempt to get :table: 'watch_list' data ...")
            query_sql = "SELECT {} FROM watch_list;".format(', '.join(list_of_header))
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.execute(query_sql)
            this_result = this_cursor.fetchall()
            this_conn.close()
            that_output = []
            for row in this_result:
                this_dict = {}
                for i in range(len(list_of_header)):
                    this_dict[list_of_header[i]] = row[i]
                that_output.append(this_dict)
            return that_output
        except Exception as e:
            self.logger.error("Failed to get :table: 'watch_list' data ! -> " + str(e))
            raise e

    def get_view_positions(self):
        """
        The :function: get_view_positions is used to query all data from :view: 'positions' into a list of
            dictionary, use column name as dictionary key.

        Args:

        Returns:
            :list: of dictionary, can be read by column name.

        """
        list_of_header = ['SYMBOL', 'DESCRIPTION', 'INVESTMENT_TYPE', 'COST_DOLLARS', 'DOLLARS', 'UNITS',
                          'LAST_UPDATED', 'MKT_VALUE', 'GAIN_PER_SHARE', 'GAIN_TOTAL', 'GAIN_PERCENTAGE']
        try:
            self.logger.info("Attempt to get :view: 'positions' data ...")
            query_sql = "SELECT {} FROM positions;".format(', '.join(list_of_header))
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.execute(query_sql)
            this_result = this_cursor.fetchall()
            this_conn.commit()
            this_conn.close()
            that_output = []
            for row in this_result:
                this_dict = {}
                for i in range(len(list_of_header)):
                    this_dict[list_of_header[i]] = row[i]
                that_output.append(this_dict)
            return that_output
        except Exception as e:
            self.logger.error("Failed to get :view: 'positions' data ! -> " + str(e))
            raise e
