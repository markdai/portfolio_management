"""
This :module: can be used to communicate with SQLite database.

    Original Author: Mark D
    Date created: 12/08/2019
    Date Modified: 12/29/2019
    Python Version: 3.7

Note:
    This module depend on following third-party Python library:
     - none

Examples:
    test_instance = FixedSQLiteRequest('test/test.db')
    test_instance.create_database()
    test_instance.create_table_transactions_fixed()
    test_instance.create_view_positions_fixed()

"""

import csv
from datetime import datetime

from .logger import UseLogging
from .eq_SQLite_utility import SQLiteRequest


class FixedSQLiteRequest(SQLiteRequest):
    """
    The :class: FixedSQLiteRequest can be used for SQLite communications.
    """
    def __init__(self, v_db_filename):
        """
        constructor for :class: FixedSQLiteRequest.
        """
        if not isinstance(v_db_filename, str):
            raise IOError("Constructor for :class: FixedSQLiteUtility take a string argument. Got {}: {}".
                          format(str(type(v_db_filename)), str(v_db_filename))
                          )
        super().__init__(v_db_filename)
        self.table_schema_file = "templates/fixed_tables_schema.json"
        self.view_query_positions = "templates/fixed_positions_view_query.sql"
        _logger_ref = UseLogging(__name__)
        self.logger = _logger_ref.use_loggers('portfolio_management')

    def _read_json_schema_file(self, v_table_name):
        return super()._read_json_schema_file(v_table_name)

    def _create_connection(self):
        return super()._create_connection()

    def create_database(self):
        return super().create_database()

    def create_table_transactions_fixed(self):
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

    def create_view_positions_fixed(self):
        """
        The :function: create_view_positions_fixed is used to create :view: 'positions' in the SQLite DB file.

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

    def insert_into_table_transactions_fixed(self, v_name, v_symbol, v_investment_type, v_units, v_face_value,
                                             v_add_date, v_end_date, v_total_cost, v_account, **kwargs):
        """
        The :function: insert_into_table_transaction is used to insert new row into :table: 'transactions'.

        Args:
            v_name (str): Name of Fix Income product.
            v_symbol (str): The ticker symbol.
            v_investment_type (str): The type of investment, CD/TREA(sury)/CORP
            v_units (int): Number of shares.
            v_face_value (float): Face value of each share..
            v_add_date (str): Transaction Date in format 'YYYY-MM-DD'.
            v_end_date (str): Matured Data in format 'YYYY-MM-DD'.
            v_total_cost (float): Total cost of the transaction.
            v_account (str): investment account.
            **kwargs:
                APR (float): Annualized-percentage-return, apply to CD.
                YTM (float): Yield-to-matured, apply to Treasury and Bonds.

        Returns:
            :boolean: True if job completed successfully.

        """
        if not isinstance(v_name, str):
            raise IOError("1st :argument: v_name should be a string. Got {}: {}".format(
                str(type(v_name)), str(v_name))
            )
        if not isinstance(v_symbol, str):
            raise IOError("2nd :argument: v_symbol should be a string. Got {}: {}".format(
                str(type(v_symbol)), str(v_symbol))
            )
        if not isinstance(v_investment_type, str):
            raise IOError("3rd :argument: v_investment_type should be a string. Got {}: {}".format(
                str(type(v_investment_type)), str(v_investment_type))
            )
        if not isinstance(v_units, int):
            raise IOError("4th :argument: v_units should be an integer. Got {}: {}".format(
                str(type(v_units)), str(v_units))
            )
        if not isinstance(v_face_value, float):
            raise IOError("5th :argument: v_face_value should be a float. Got {}: {}".format(
                str(type(v_face_value)), str(v_face_value))
            )
        if not isinstance(v_add_date, str):
            raise IOError("6th :argument: v_add_date should be a string in 'YYYY-MM-DD' format. Got {}: {}".format(
                str(type(v_add_date)), str(v_add_date))
            )
        try:
            datetime.strptime(v_add_date, '%Y-%m-%d')
        except ValueError:
            raise IOError("6th :argument: v_add_date should be in 'YYYY-MM-DD' format. Got {}: {}".format(
                str(type(v_add_date)), str(v_add_date))
            )
        if not isinstance(v_end_date, str):
            raise IOError("7th :argument: v_end_date should be a string in 'YYYY-MM-DD' format. Got {}: {}".format(
                str(type(v_end_date)), str(v_end_date))
            )
        try:
            datetime.strptime(v_end_date, '%Y-%m-%d')
        except ValueError:
            raise IOError("7th :argument: v_end_date should be in 'YYYY-MM-DD' format. Got {}: {}".format(
                str(type(v_end_date)), str(v_end_date))
            )
        if not isinstance(v_total_cost, float):
            raise IOError("8th :argument: v_total_cost should be a float. Got {}: {}".format(
                str(type(v_total_cost)), str(v_total_cost))
            )
        if not isinstance(v_account, str):
            raise IOError("9th :argument: v_account should be a string. Got {}: {}".format(
                str(type(v_account)), str(v_account))
            )
        if 'APR' in kwargs:
            this_apr = float(kwargs.get('APR'))
        else:
            this_apr = 0.0
        if 'YTM' in kwargs:
            this_ytm = float(kwargs.get('YTM'))
        else:
            this_ytm = 0.0
        try:
            self.logger.info("Inserting into :table: 'transactions' ...")
            this_total_dollars = v_face_value*v_units
            this_insert_values = (
                v_name, v_symbol, v_investment_type.upper(), v_units, v_face_value, this_total_dollars,
                v_add_date, v_end_date, v_total_cost, this_apr, this_ytm, v_account
            )
            insert_sql = ''' INSERT INTO transactions (
            NAME, SYMBOL, INVESTMENT_TYPE, UNITS, FACE_VALUE, TOTAL_DOLLARS, 
            ADD_DATE, END_DATE, TOTAL_COST, APR, YTM, ACCOUNT) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'''
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            this_cursor.execute(insert_sql, this_insert_values)
            this_conn.commit()
            self.logger.info("Entry has been inserted into :table: 'transactions' ("
                             "NAME={}, SYMBOL={}, INVESTMENT_TYPE={}, UNITS={}, FACE_VALUE={}, TOTAL_DOLLARS={}, "
                             "ADD_DATE={}, END_DATE={}, TOTAL_COST={}, APR={}, YTM={}, ACCOUNT={}".format(
                                 this_insert_values[0], this_insert_values[1], this_insert_values[2],
                                 this_insert_values[3], this_insert_values[4], this_insert_values[5],
                                 this_insert_values[6], this_insert_values[7], this_insert_values[8],
                                 this_insert_values[9], this_insert_values[10], this_insert_values[11]))
            this_conn.close()
            return True
        except Exception as e:
            self.logger.error("Failed to insert into :table: 'transactions' ! -> "+str(e))
            raise e

    def backup_table_transactions_fixed(self,
                                        outfile_name='fixed_transaction_backup_' +
                                                     datetime.now().strftime('%Y%m%d_%H%M%S') + '.csv'):
        """
        The :function: backup_table_transaction_fixed is used to export data from :table: 'transactions'
            into a CSV backup file in backup folder.

        Args:
            outfile_name (str): The backup output filename, default to 'fixed_transaction_backup_YYYYMMDD_HHMMSS.csv'.

        Returns:
            :boolean: True if job completed successfully.

        """
        list_of_header = ['NAME', 'SYMBOL', 'INVESTMENT_TYPE', 'UNITS', 'FACE_VALUE', 'TOTAL_DOLLARS',
                          'ADD_DATE', 'END_DATE', 'TOTAL_COST', 'APR', 'YTM', 'ACCOUNT']
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

    def load_backup_to_table_transactions_fixed(self, infile_name):
        """
        The :function: load_backup_to_table_transaction_fixed is used to load backup file into :table: 'transactions'.

        Args:
            infile_name (str): The backup input filename.

        Returns:
            :boolean: True if job completed successfully.

        """
        try:
            self.logger.info("Loading from Backup into :table: 'transactions' ...")
            with open(infile_name, 'r', newline='') as rf:
                my_reader = csv.DictReader(rf)
                to_db = [(row['NAME'], row['SYMBOL'], row['INVESTMENT_TYPE'], row['UNITS'], row['FACE_VALUE'],
                          row['TOTAL_DOLLARS'],
                          row['ADD_DATE'], row['END_DATE'], row['TOTAL_COST'], row['APR'], row['YTM'],
                          row['ACCOUNT']) for row in my_reader]
            this_conn = self._create_connection()
            this_cursor = this_conn.cursor()
            insert_sql = ''' INSERT INTO transactions (
                        NAME, SYMBOL, INVESTMENT_TYPE, UNITS, FACE_VALUE, TOTAL_DOLLARS, 
                        ADD_DATE, END_DATE, TOTAL_COST, APR, YTM, ACCOUNT) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);'''
            this_cursor.executemany(insert_sql, to_db)
            this_conn.commit()
            self.logger.info("Backup for :table: 'transactions' has been loaded ...")
            this_conn.close()
        except Exception as e:
            self.logger.error("Failed to load backup for :table: 'transactions' ! -> " + str(e))
            raise e

    def get_table_transactions_fixed(self):
        """
        The :function: get_table_transaction_fixed is used to query all data from :table: 'transaction' into a list of
            dictionary, use column name as dictionary key.

        TO-DO

        Args:

        Returns:
            :list: of dictionary, can be read by column name.

        """
        list_of_header = ['ID', 'NAME', 'SYMBOL', 'INVESTMENT_TYPE', 'UNITS', 'FACE_VALUE', 'TOTAL_DOLLARS',
                          'ADD_DATE', 'END_DATE', 'TOTAL_COST', 'APR', 'YTM', 'ACCOUNT']
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

    def get_view_positions_fixed(self):
        """
        The :function: get_view_positions_fixed is used to query all data from :view: 'positions' into a list of
            dictionary, use column name as dictionary key.

        TO-DO

        Args:

        Returns:
            :list: of dictionary, can be read by column name.

        """
        list_of_header = ['NAME', 'SYMBOL', 'INVESTMENT_TYPE', 'UNITS', 'FACE_VALUE', 'TOTAL_DOLLARS', 'ADD_DATE',
                          'END_DATE', 'TOTAL_COST', 'RETURN_RATE', 'RETURN_DOLLARS', 'IS_MATURED']
        try:
            self.logger.info("Attempt to get :view: 'positions' data ...")
            query_sql = "SELECT {} FROM positions WHERE IS_MATURED = 0;".format(', '.join(list_of_header))
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
