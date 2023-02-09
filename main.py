"""
This is the master module for this package.

    Original Author: Mark D
    Date created: 12/08/2019
    Date Modified: 11/19/2021
    Python Version: 3.7

Examples:
    To get help information:
        python main.py -h

    To generate Overview Snapshot:
        python main.py overview

    To manage equity investment Database:
        python main.py equity -m update
        python main.py equity -m backup
        python main.py equity -m restore
        python main.py equity -m add -ee 'APPL,BUY,2099-99-99,250.0,10,ETF,TD,Apple Inc'

    To manage fixed income investment Database:
        python main.py fixed -m backup
        python main.py fixed -m restore
        python main.py fixed -m add -fe 'US Treasury Notes,XXXXXXXX1,TREASURY,10,100.0,2018-12-31,2019-12-31,1000.0,
            Trading Center,YTM=0.025'

    To pull fund data for Investment Fund excel spreadsheet:
        python main.py get-fund-data

"""

import argparse
from datetime import datetime


def master_equity(v_mode, row=None):
    """ Master script for Equity Management, include: UPDATE, BACKUP, RESTORE, ADD.

    Args:
        v_mode (str): UPDATE/BACKUP/RESTORE/ADD
        row (list): new transaction entry, default to None [
            :str: SYMBOL,
            :str: ACTION (BUY/SELL),
            :str: TRANSACTION_DATE (YYYY-MM-DD),
            :float: PRICE,
            :int: UNITS,
            :str: INVESTMENT_TYPE(stock/ETF),
            :str: BROKER_NAME,
            :str: SHORT_DESCRIPTION
        ]
        e.g. ['APPL', 'BUY', '2099-99-99', 250.0, 10, 'ETF', 'TD', 'Apple Inc']

    Returns:
        True if job completed successfully, False otherwise.

    """
    from src.equity import DbCommands as eq_DbCommands
    print('[..] Calling master_equity() ...')
    try:
        this_instance = eq_DbCommands()
        if v_mode.upper() == 'UPDATE':
            this_instance.update()
        elif v_mode.upper() == 'BACKUP':
            this_instance.backup()
        elif v_mode.upper() == 'RESTORE':
            this_instance.restore()
        elif v_mode.upper() == 'ADD':
            if isinstance(row, list) and len(row) == 8:
                this_instance.add(row[0], row[1], row[2], float(row[3]), int(row[4]), row[5], row[6], row[7])
            elif row is None:
                raise IOError('Error: input :row: is required for equity ADD command.')
            else:
                if not isinstance(row, list):
                    raise IOError('Error: input :row: is not valid ! -> got {}: {}'.format(str(type(row)), str(row)))
                else:
                    raise IOError('Error: input :row: is not valid ! -> got {}({}): {}'.format(str(type(row)),
                                                                                               str(len(row)),
                                                                                               ','.join(row))
                                  )
        else:
            raise IOError('Error: input :v_mode: is not valid ! -> expect update/backup/restore/add, got {}: {}'.format(
                str(type(v_mode)), str(v_mode)))
        return True
    except Exception as e:
        raise RuntimeError('Error: Failed to run master_equity() -> '+str(e))


def master_fixed(v_mode, row=None):
    """ Master script for Fixed Income Management, include: BACKUP, RESTORE, ADD.

    Args:
        v_mode (str): BACKUP/RESTORE/ADD
        row (list): new transaction entry, default to None [
            :str: Product NAME,
            :str: SYMBOL,
            :str: INVESTMENT_TYPE(TREASURY, CD, COPR BOND, HIGHYIELD, TIPS),
            :int: UNITS,
            :float: FACE_VALUE,
            :str: ADDED_DATE (YYYY-MM-DD),
            :str: MATURE_DATE (YYYY-MM-DD),
            :float: TOTAL_COST,
            :str: BROKER_NAME,
            :str:=:float: YTM/APR
        ]
        e.g. ['US Treasury Notes', 'XXXXXXXX1', 'TREASURY', 10, 100.0, '2018-12-31', '2019-12-31',
            1000.0, 'Trading Center', 'YTM=0.025']
    )

    Returns:
        True is job completed successfully, False otherwise.

    """
    from src.fixed_income import DbCommands as fixed_DbCommands
    print('[..] Calling master_fixed() ...')
    try:
        this_instance = fixed_DbCommands()
        if v_mode.upper() == 'BACKUP':
            this_instance.backup()
        elif v_mode.upper() == 'RESTORE':
            this_instance.restore()
        elif v_mode.upper() == 'ADD':
            if isinstance(row, list) and len(row) == 10:
                if row[9].split('=')[0].upper() == 'YTM':
                    this_instance.add(row[0], row[1], row[2], int(row[3]), float(row[4]), row[5], row[6],
                                      float(row[7]), row[8], YTM=float(row[9].split('=')[1]))
                elif row[9].split('=')[0].upper() == 'APR':
                    this_instance.add(row[0], row[1], row[2], int(row[3]), float(row[4]), row[5], row[6],
                                      float(row[7]), row[8], APR=float(row[9].split('=')[1]))
                else:
                    raise IOError('Error: input :row:[9] is not valid ! -> expect "APR/YTM=n", got {}: {}'.format(
                        str(type(row[9])), str(row)[9]))
            elif row is None:
                raise IOError('Error: input :row: is required for fixed income ADD command.')
            else:
                if not isinstance(row, list):
                    raise IOError('Error: input :row: is not valid ! -> got {}: {}'.format(str(type(row)), str(row)))
                else:
                    raise IOError('Error: input :row: is not valid ! -> got {}({}): {}'.format(str(type(row)),
                                                                                               str(len(row)),
                                                                                               ','.join(row)))
        else:
            raise IOError('Error: input :v_mode: is not valid ! -> expect backup/restore/add, got {}: {}'.format(
                str(type(v_mode)), str(v_mode)))
        return True
    except Exception as e:
        raise RuntimeError('Error: Failed to run master_fixed() -> '+str(e))


def master_overview():
    """ Master script for Overview Generator, include: ALLOCATION_TYPE, ALLOCATION_ACCOUNT, MATURE_CALENDER

    Returns:
        True is job completed successfully, False otherwise.

    """
    from src.overview_generator import SummaryTool as SummaryTool
    print('[..] Calling master_overview() ...')
    try:
        out_filename = 'snapshots/snapshot_' + datetime.now().strftime('%Y%m%d') + '.html'
        this_instance = SummaryTool()
        # call function to generate master investment allocation report
        this_allocation_report_type = this_instance.generate_allocation_report_type()
        this_allocation_report_type.iloc[-1] = this_allocation_report_type.iloc[-1].apply(
            lambda x: '//strong/' + str(x) + '/strong//')
        # call function to generate account allocation report
        this_allocation_report_account = this_instance.generate_allocation_report_account()
        '''
        # call function to generate equity ETF allocation report
        this_allocation_report_equity_etf = this_instance.generate_allocation_report_equity_etf()
        this_allocation_report_equity_etf.iloc[-1] = this_allocation_report_equity_etf.iloc[-1].apply(
            lambda x: '//strong/' + str(x) + '/strong//')
        '''
        # call function to generate ETF allocation report in :broker: Charles Schwab
        this_allocation_report_etf_at_schwab = this_instance.generate_allocation_report_etf_w_account(
            'Schwab')
        this_allocation_report_etf_at_schwab.iloc[-1] = this_allocation_report_etf_at_schwab.iloc[-1].apply(
            lambda x: '//strong/' + str(x) + '/strong//')
        # call function to generate ETF allocation report in :broker: Fidelity
        this_allocation_report_etf_at_fidelity = this_instance.generate_allocation_report_etf_w_account(
            'Fidelity')
        this_allocation_report_etf_at_fidelity.iloc[-1] = this_allocation_report_etf_at_fidelity.iloc[-1].apply(
            lambda x: '//strong/' + str(x) + '/strong//')
        # call function to generate ETF allocation report in :broker: Vanguard
        this_allocation_report_etf_at_vanguard = this_instance.generate_allocation_report_etf_w_account(
            'Vanguard')
        this_allocation_report_etf_at_vanguard.iloc[-1] = this_allocation_report_etf_at_vanguard.iloc[-1].apply(
            lambda x: '//strong/' + str(x) + '/strong//')
        '''
        # call function to generate Fixed Income ETF allocation report
        this_allocation_report_fixed_etf = this_instance.generate_allocation_report_fixed_etf()
        this_allocation_report_fixed_etf.iloc[-1] = this_allocation_report_fixed_etf.iloc[-1].apply(
            lambda x: '//strong/' + str(x) + '/strong//')
        '''
        # call function to generate individual Stock holding list
        this_allocation_report_equity_stock = this_instance.generate_allocation_report_equity_stock()
        this_allocation_report_equity_stock.iloc[-1] = this_allocation_report_equity_stock.iloc[-1].apply(
            lambda x: '//strong/' + str(x) + '/strong//')
        # call function to generate Fixed Income mature calender
        this_mature_calender = this_instance.generate_mature_calender()
        with open(out_filename, 'w+') as wf:
            tmp_html_data = '<h1>Investment Portfolio Overview - ' + datetime.now().strftime('%b %d, %Y') + '</h1>' + \
                            '\n<h3>Allocation Report - Investment Type </h3>' + \
                            this_allocation_report_type.to_html(index=False) + \
                            '\n<br>\n<h3>Allocation Report - Broker </h3>' + \
                            this_allocation_report_account.to_html(index=False) + \
                            '\n<br>\n<h3>Allocation Report - Charles Schwab </h3>' + \
                            this_allocation_report_etf_at_schwab.to_html(index=False) + \
                            '\n<br>\n<h3>Allocation Report - Fidelity </h3>' + \
                            this_allocation_report_etf_at_fidelity.to_html(index=False) + \
                            '\n<br>\n<h3>Allocation Report - Vanguard </h3>' + \
                            this_allocation_report_etf_at_vanguard.to_html(index=False) + \
                            '\n<br>\n<h3>Allocation Report - Individual Stock </h3>' + \
                            this_allocation_report_equity_stock.to_html(index=False) + \
                            '\n<br>\n<h3>Fixed Income Mature Calender</h3>' + \
                            this_mature_calender.to_html(index=False) + '\n<br>\n'
            out_data = tmp_html_data.replace('<th>', '<th align="center"; bgcolor="E0FFFF">').\
                replace('//strong/', '<strong>').\
                replace('/strong//', '</strong>')
            wf.write(out_data)
        return True
    except Exception as e:
        raise RuntimeError('Error: Failed to run master_overview() -> '+str(e))


# Executable arguments handler
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('type', type=str, help='Execution Type: equity/fixed/overview')
    parser.add_argument('-m', '--mode', type=str, help='Execution Mode: update/backup/restore/add')
    parser.add_argument('-ee', '--eq_entry', type=str,
                        help='Transaction Entry to add, len=19):\n e.g. "SYMBOL,ACTION(BUY/SELL),'
                             'TRANSACTION_DATE(YYYY-MM-DD),PRICE,UNITS,INVESTMENT_TYPE(stock/ETF),'
                             'BROKER_NAME,SHORT_DESCRIPTION"'
                        )
    parser.add_argument('-fe', '--fixed_entry', type=str,
                        help='Transaction Entry to add, len=8):\n e.g. "NAME,SYMBOL,'
                             'INVESTMENT_TYPE(TREASURY/CD/COPR BOND/HIGHYIELD),UNITS,FACE_VALUE,'
                             'ADDED_DATE(YYYY-MM-DD),MATURE_DATE(YYYY-MM-DD),TOTAL_COST,BROKER_NAME,YTM/APR=n"'
                        )
    args = parser.parse_args()
    if args.type.lower() == 'equity':
        if args.mode.lower() == 'add':
            master_equity(args.mode, args.eq_entry.split(','))
        else:
            master_equity(args.mode)
    elif args.type.lower() == 'fixed':
        if args.mode.lower() == 'add':
            master_fixed(args.mode, args.fixed_entry.split(','))
        else:
            master_fixed(args.mode)
    elif args.type.lower() == 'overview':
        master_overview()
    else:
        raise IOError('Error in Executable arguments handler: Execution type is not valid -> '
                      'expect equity/fixed/overview/get-fund-data, got {}: {}'.format(str(type(args.type)),
                                                                                      str(args.type)))
