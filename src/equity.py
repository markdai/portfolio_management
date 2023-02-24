"""
This module is the master for FinAPI_utility, eq_SQLite_utility

    Original Author: Mark D
    Date created: 09/28/2019
    Date Modified: 01/05/2020
    Python Version: 3.7

Note:
    This module depend on following third party library:
     - pandas v0.25.0

Examples:
    -- Update SQLite Database 'equity' based on Yahoo Finance.
        from src.equity import DbCommands as eq_DbCommands
        this_instance = eq_DbCommands()
        this_instance.update()

    -- Backup SQLite Database 'equity' to 'backup/' directory.
        from src.equity import DbCommands as eq_DbCommands
        this_instance = eq_DbCommands()
        this_instance.backup()

    -- Restore SQLite Database 'equity' from 'backup/' directory.
        from src.equity import DbCommands as eq_DbCommands
        this_instance = eq_DbCommands()
        this_instance.restore()

    -- Add new transaction into SQLite Database 'equity' Table 'transactions'.
        from src.equity import DbCommands as eq_DbCommands
        this_instance = eq_DbCommands()
        this_instance.add(
          :str: SYMBOL,
          :str: ACTION (BUY/SELL),
          :str: TRANSACTION_DATE (YYYY-MM-DD),
          :float: PRICE,
          :int: UNITS,
          :str: INVESTMENT_TYPE(stock/ETF),
          :str: BROKER_NAME,
          :str: SHORT_DESCRIPTION
        )
        this_instance.add('APPL', 'BUY', '3000-01-01', 200.0, 10, 'stock', 'TC', 'Apple Inc.')
        this_instance.add('VHT', 'BUY', '2020-01-23', 195.81, 25, 'ETF', 'Vanguard', 'Vanguard Health Care ETF')
        this_instance.add('VNQ', 'BUY', '2020-01-23', 95.69, 52, 'ETF', 'Vanguard', 'Vanguard Real Estate Index ETF')

"""

from datetime import datetime
import os

from .logger import UseLogging
from .eq_SQLite_utility import SQLiteRequest
# from .financial_API_utility import Stock, ETF
from .financial_API_utility_alternative import Stock, ETF


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
        self.production_db_file = 'databases/equity.db'
        self._current_date = datetime.now().strftime('%Y%m%d')
        self.backup_db_file = f'equity_transaction_backup_{self._current_date}.csv'

    def _get_stock_information(self, v_ticker):
        """
        The :function: _get_stock_information is used to get specific financial data for an individual stock.

        Args:
            v_ticker (str): The Key to get.

        Returns:
            :str: The value for that key.

        """
        self.logger.info(f'Retrieving stock information for ticker: {v_ticker}...')
        _df = Stock(v_ticker)
        v_prev_close = _df.get_previous_close()
        v_low_52wks = _df.get_low_52wks()
        v_high_52wks = _df.get_high_52wks()
        v_mkt_cap = _df.get_market_cap()
        v_pe = _df.get_pe()
        v_forward_pe = _df.get_forward_pe()
        v_div = _df.get_dividend()
        v_eps = _df.get_eps()
        v_forward_eps = _df.get_forward_eps()
        v_sector = _df.get_sector()
        v_beta = _df.get_beta()
        v_short_float = _df.get_short_float()
        v_name = _df.get_name()
        self.logger.info(f'.. Got: PREV_CLOSE={v_prev_close}, 52WEEKS_LOW={v_low_52wks}, 52WEEKS_HIGH={v_high_52wks}, '
                         f'MARKET_CAP={v_mkt_cap}, trailing P/E={v_pe}, forward P/E={v_forward_pe}, '
                         f'DIVIDEND_RATIO={v_div}, trailing EPS={v_eps}, forward EPS={v_forward_eps}, '
                         f'sector={v_sector}, beta ratio={v_beta}, short float={v_short_float}, Name={v_name}'
                         )
        return v_prev_close, v_low_52wks, v_high_52wks, v_mkt_cap, v_pe, v_div, v_eps, v_forward_pe, v_forward_eps, \
            v_sector, v_beta, v_short_float, v_name

    def _get_etf_information(self, v_ticker):
        """
        The :function: _get_etf_information is used to get specific financial data for an ETF fund.

        Args:
            v_ticker (str): The Key to get.

        Returns:
            :str: The value for that key.

        """
        self.logger.info(f'Retrieving ETF information for ticker: {v_ticker}...')
        _df = ETF(v_ticker)
        v_prev_close = _df.get_previous_close()
        v_low_52wks = _df.get_low_52wks()
        v_high_52wks = _df.get_high_52wks()
        v_mkt_cap = _df.get_market_cap()
        v_pe = _df.get_pe()
        v_forward_pe = _df.get_forward_pe()
        v_div = _df.get_dividend()
        v_eps = _df.get_eps()
        v_forward_eps = _df.get_forward_eps()
        v_sector = _df.get_sector()
        v_beta = _df.get_beta()
        v_short_float = _df.get_short_float()
        v_name = _df.get_name()
        v_total_assets = _df.get_total_assets()
        v_yield = _df.get_yield()
        v_category = _df.get_category()
        self.logger.info(f'.. Got: PREV_CLOSE={v_prev_close}, Total Assets={v_total_assets}, Yield={v_yield}, '
                         f'Category={v_category}, Name={v_name}'
                         )
        return v_prev_close, v_low_52wks, v_high_52wks, v_mkt_cap, v_pe, v_div, v_eps, v_forward_pe, v_forward_eps, \
            v_sector, v_beta, v_short_float, v_name, v_total_assets, v_yield, v_category

    def backup(self):
        """Call eq_SQLite_utility to create a backup from current equity database.

        Return: CSV file.

        """
        self.logger.info('Creating backup for current database...')
        try:
            _instance = SQLiteRequest(self.production_db_file)
            _instance.backup_table_transactions(self.backup_db_file)
        except Exception as e:
            self.logger.error('Failed to backup current database -> '+str(e))
            raise e
        self.logger.info(f'.. Backup has been created as: backup/{self.backup_db_file}')

    def restore(self):
        """Call eq_SQLite_utility to re-create equity database from the latest backup file.

        Return: none.

        """
        self.logger.info('Re-creating database...')
        try:
            _instance = SQLiteRequest(self.production_db_file)
            _instance.create_database()
            _instance.create_table_transactions()
            _instance.create_table_watch_list()
            _instance.create_table_holdings()
            _instance.create_view_positions()
            backup_file = [x for x in sorted(os.listdir('backup/'), reverse=True)
                           if x.startswith('equity_transaction_')][0]
            _instance.load_backup_to_table_transactions('backup/' + backup_file)
        except Exception as e:
            self.logger.error('Failed to restore database -> '+str(e))
            raise e
        self.logger.info(f'.. Database has been restored from: backup/{backup_file}')

    def update(self):
        """Call eq_SQLite_utility to update :table: tmp_holdings and :table: watch_list in equity database.

        Return: none.

        """
        _instance = SQLiteRequest(self.production_db_file)
        self.logger.info('Updating :table: tmp_holdings ...')
        try:
            _instance.sync_table_holdings()
        except Exception as e:
            self.logger.error('Failed to update :table: tmp_holdings -> ' + str(e))
            raise e
        self.logger.info('Updating :table: watch_list ...')
        try:
            _instance.sync_table_watch_list()
            data_watch_list = [x for x in _instance.get_table_watch_list() if int(x['ENABLED']) == 1]
            for i in range(len(data_watch_list)):
                v_symbol = data_watch_list[i]['SYMBOL']
                v_investment_type = data_watch_list[i]['INVESTMENT_TYPE']
                if v_investment_type.lower() == 'stock':
                    v_prev_close, v_low_52wks, v_high_52wks, v_mkt_cap, v_pe, v_div, v_eps, v_forward_pe, \
                        v_forward_eps, v_sector, v_beta, v_short_float, v_name = self._get_stock_information(v_symbol)
                    v_total_assets, v_yield, v_category = 0, float('nan'), ''
                    _instance.update_table_watch_list(v_symbol, v_name, v_investment_type, v_prev_close,
                                                      v_low_52wks, v_high_52wks, v_mkt_cap, v_total_assets, v_pe,
                                                      v_forward_pe, v_div, v_yield, v_eps, v_forward_eps, v_beta,
                                                      v_short_float, v_sector, v_category
                                                      )
                elif v_investment_type.lower() == 'etf':
                    v_prev_close, v_low_52wks, v_high_52wks, v_mkt_cap, v_pe, v_div, v_eps, v_forward_pe, \
                        v_forward_eps, v_sector, v_beta, v_short_float, v_name, v_total_assets, v_yield, \
                        v_category = self._get_etf_information(v_symbol)
                    _instance.update_table_watch_list(v_symbol, v_name, v_investment_type, v_prev_close,
                                                      v_low_52wks, v_high_52wks, v_mkt_cap, v_total_assets,
                                                      v_pe, v_forward_pe, v_div, v_yield, v_eps, v_forward_eps,
                                                      v_beta, v_short_float, v_sector, v_category
                                                      )
                else:
                    self.logger.error("Investment type should be :string: stock/etf. Got {}: {}".format(
                        str(type(v_investment_type)), str(v_investment_type)
                    ))
                    raise IOError("Investment type should be :string: stock/etf. Got {}: {}".format(
                        str(type(v_investment_type)), str(v_investment_type)
                    ))
        except Exception as e:
            self.logger.error('Failed to update :table: watch_list -> ' + str(e))
            raise e
        self.logger.info(f'.. :table: watch_list and :table: tmp_holding_cost have been '
                         f'updated on {self._current_date}')

    def add(self, v_symbol, v_type, v_date, v_dollars, v_units, v_investment_type, v_account, v_memo=''):
        """Call eq_SQLite_utility to add a new entry into equity database.
        e.g. 'AAPL', 'BUY', '2018-12-31', 120.0, 10, 'stock', 'TD', 'Bought stock for Apple.inc'
        """
        self.logger.info('Adding new record to :table: transaction ...')
        try:
            _instance = SQLiteRequest(self.production_db_file)
            _instance.insert_into_table_transactions(v_symbol, v_type, v_date, v_dollars, v_units, v_investment_type,
                                                     v_account, v_description=v_memo)
        except Exception as e:
            self.logger.error(f'Failed to add new record to :table: transaction : SYMBOL={v_symbol}, TYPE={v_type}, '
                              f'DATE={v_date}, DOLLARS={v_dollars}, UNITS={v_units}, '
                              f'INVESTMENT_TYPE={v_investment_type}, ACCOUNT={v_account} -> ' + str(e))
            raise e
        self.logger.info(f'.. New Record has been added: SYMBOL={v_symbol}, TYPE={v_type}, DATE={v_date}, '
                         f'DOLLARS={v_dollars}, UNITS={v_units}, INVESTMENT_TYPE={v_investment_type}, '
                         f'ACCOUNT={v_account}')
