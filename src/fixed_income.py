"""
This module is the master for fixed_SQLite_utility

    Original Author: Mark D
    Date created: 12/28/2019
    Date Modified: 01/05/2020
    Python Version: 3.7

Note:
    This module depend on following third party library:
     - pandas v0.25.0

Examples:
    -- Backup SQLite Database 'fixed_income' to 'backup/' directory.
        from src.fixed_income import DbCommands as fixed_DbCommands
        this_instance = fixed_DbCommands()
        this_instance.backup()

    -- Restore SQLite Database 'fixed_income' from 'backup/' directory.
        from src.fixed_income import DbCommands as fixed_DbCommands
        this_instance = fixed_DbCommands()
        this_instance.restore()

    -- Add new transaction into SQLite Database 'fixed_income' Table 'transactions'.
        from src.fixed_income import DbCommands as fixed_DbCommands
        this_instance = fixed_DbCommands()
        this_instance.add(
          :str: Product NAME,
          :str: SYMBOL,
          :str: INVESTMENT_TYPE(TREASURY, CD, COPR BOND, HIGHYIELD, TIPS),
          :int: UNITS,
          :float: FACE_VALUE,
          :str: ADDED_DATE (YYYY-MM-DD),
          :str: MATURE_DATE (YYYY-MM-DD),
          :float: TOTAL_COST,
          :str: BROKER_NAME,
          ** kwargs: float: YTM / APR
        )
        this_instance.add('US Treasury Notes', 'XXXXXXXX1', 'TREASURY', 100, 100.0, '2018-12-31', '2019-12-31',
          9500.0, 'Trading Center', YTM=0.025
        )

"""

from datetime import datetime
import os

from .logger import UseLogging
from .fixed_SQLite_utility import FixedSQLiteRequest


class DbCommands(object):
    """
    The :class: DbCommands can be used to get latest Quotes and Financial information for an Equity from
    Yahoo Finance website.
    """
    def __init__(self):
        """
        constructor for :class: DbCommands. It will read from website html and build a pandas dataframe
        with financial data.
        """
        _logger_ref = UseLogging(__name__)
        self.logger = _logger_ref.use_loggers('portfolio_management')
        self.production_db_file = 'databases/fixed_income.db'
        self._current_date = datetime.now().strftime('%Y%m%d')
        self.backup_db_file = f'fixed_transaction_backup_{self._current_date}.csv'

    def backup(self):
        """Call fixed_SQLite_utility to create a backup from current fixed income database.

        Return: CSV file.

        """
        self.logger.info('Creating backup for current database...')
        try:
            _instance = FixedSQLiteRequest(self.production_db_file)
            _instance.backup_table_transactions_fixed(self.backup_db_file)
        except Exception as e:
            self.logger.error('Failed to backup current database -> '+str(e))
            raise e
        self.logger.info(f'.. Backup has been created as: backup/{self.backup_db_file}')

    def restore(self):
        """Call fixed_SQLite_utility to re-create fixed income database from the latest backup file.

        Return: none.

        """
        self.logger.info('Re-creating database...')
        try:
            _instance = FixedSQLiteRequest(self.production_db_file)
            _instance.create_database()
            _instance.create_table_transactions_fixed()
            _instance.create_view_positions_fixed()
            backup_file = [x for x in sorted(os.listdir('backup/'), reverse=True)
                           if x.startswith('fixed_transaction_')][0]
            _instance.load_backup_to_table_transactions_fixed('backup/' + backup_file)
        except Exception as e:
            self.logger.error('Failed to restore database -> '+str(e))
            raise e
        self.logger.info(f'.. Database has been restored from: backup/{backup_file}')

    def add(self, v_name, v_symbol, v_investment_type, v_units, v_face_value, v_add_date, v_end_date, v_total_cost,
            v_account, **kwargs):
        """Call fixed_SQLite_utility to add a new entry into fixed income database.

        e.g.
         'US Treasury Notes', 'XXXXXXXX1', 'TREA', 100, 100.0, '2018-12-31', '2019-12-31', 9500.0, 'TD', YTM=0.025
         '12-Month CD', 'XXXXXXXX2', 'CD', 10000, 1.0, '2019-01-31', '2020-01-31', 10000.0, 'TD', APR=0.026
        """
        self.logger.info('Adding new record to :table: transaction ...')
        if 'APR' in kwargs:
            v_apr = float(kwargs.get('APR'))
        else:
            v_apr = 0.0
        if 'YTM' in kwargs:
            v_ytm = float(kwargs.get('YTM'))
        else:
            v_ytm = 0.0
        try:
            _instance = FixedSQLiteRequest(self.production_db_file)
            _instance.insert_into_table_transactions_fixed(v_name, v_symbol, v_investment_type, v_units, v_face_value,
                                                           v_add_date, v_end_date, v_total_cost, v_account,
                                                           YTM=v_ytm, APR=v_apr)
        except Exception as e:
            self.logger.error(f'Failed to add new record to :table: transaction : NAME={v_name}, SYMBOL={v_symbol}, '
                              f'INVESTMENT_TYPE={v_investment_type}, UNITS={v_units}, FACE_VALUE={v_face_value}, '
                              f'ADD_DATE={v_add_date}, END_DATE={v_end_date}, '
                              f'TOTAL_COST={v_total_cost}, APR={v_apr}, YTM={v_ytm}, ACCOUNT={v_account} -> ' + str(e))
            raise e
        self.logger.info(f'.. New Record has been added: NAME={v_name}, SYMBOL={v_symbol}, '
                         f'INVESTMENT_TYPE={v_investment_type}, UNITS={v_units}, FACE_VALUE={v_face_value}, '
                         f'ADD_DATE={v_add_date}, END_DATE={v_end_date}, '
                         f'TOTAL_COST={v_total_cost}, APR={v_apr}, YTM={v_ytm}, ACCOUNT={v_account}')

