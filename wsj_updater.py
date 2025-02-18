import logging
from logging.handlers import TimedRotatingFileHandler
import requests
import pandas as pd
import argparse
import os
import itertools
import time
from dotenv import load_dotenv
from pathlib import Path
from bs4 import BeautifulSoup
from decimal import Decimal
from ratelimit import limits, sleep_and_retry
from datetime import datetime as dt
from supabase import create_client
from wsj_cleaner import WSJCleaner

balance_metrics = {
    ### Cash and Equivalents
    'Cash & Short Term Investments':'cash_and_short_term_investments',
    'Cash Only':'cash_only',
    'Total Cash & Due from Banks':'total_cash_and_due_from_banks',
    ### Assets
    'Total Assets':'total_assets',
    'Total Current Assets':'total_current_assets',
    ### Liabilities
    'Total Liabilities':'total_liabilities',
    'Total Current Liabilities':'total_current_liabilities',
    ### Debt
    'Total Debt':'total_debt',
    'Long-Term Debt':'long_term_debt',
    'Short Term Debt':'short_term_debt_excluding_current_portion_lt_debt',
    'ST Debt & Current Portion LT Debt':'short_term_debt',
    'ST Debt & Current Portion of LTD':'short_term_debt',
    ### Others
    'Total Equity':'total_equity',
    "Total Shareholders' Equity":'stockholders_equity',
    # 'Net Property, Plant & Equipments':'net_property_plant_and_equipment',
    # 'Net Property, Plant & Equipment':'net_property_plant_and_equipment',
    # 'Current Portion of LT Debt':'current_portion_of_lt_debt',
    # 'Current Portion of Long Term Debt':'current_portion_of_lt_debt',
    # 'Deferred Taxes - Credit':'deferred_taxes_credits',
    # 'Deferred Taxes - Credits':'deferred_taxes_credits',
    # 'Long-Term Debt excl. Capitalized Leases':'long_term_debt_excluding_capitalized_leases',
    # 'LT Debt excl. Capitalized Leases':'long_term_debt_excluding_capitalized_leases',
    # 'LT Debt excl. Capital Lease Obligations':'long_term_debt_excluding_capitalized_leases',
    # 'Other Liabilities':'other_liabilities',
    # 'Intangible Assets':'intangible_assets',
    # 'Other Investments':'other_investments',
    # 'Loan Loss Provision':'loan_loss_provision',
}

income_metrics = {
    # 'Interest Income':'interest_income',
    ### Total Revenue
    'Net Interest Income':'net_interest_income',
    'Non-Interest Income':'non_interest_income',
    'Sales/Revenue':'total_revenue',
    ### Interest Expense
    'Total Internest Expense':'interest_expense',
    'Total Interest Expense':'interest_expense',
    'Interest Expense':'interest_expense_non_operating',
    'Interest Expense, Net of Interest Capitalized':'interest_expense_net_of_interest_capitalized',
    'Interest Expense (excl. Interest Capitalized)':'interest_expense_net_of_interest_capitalized',
    ### Operating Income
    'Operating Income':'operating_income',
    'Operating Income Before Interest Expense':'operating_income_before_interest_expense',
    ## Cols used for calculating it
    'Selling, General & Admin. Expenses & Other':'selling_general_and_admin_expenses_and_other',
    'Gross Income':'gross_income',
    'Selling, General & Admin. Expenses':'selling_general_and_administration_expense',
    'SG&A Expense':'selling_general_and_administration_expense',
    'Other Operating Expense':'other_operating_expenses',
    'Cost of Goods Sold (COGS) incl. D&A':'cogs_including_depreciation_amortization',
    ### Income Taxes
    'Income Taxes':'income_taxes',
    'Income Tax':'income_taxes',
    ### Others
    'Pretax Income':'pretax_income', # -> For EBIT
    'Depreciation & Amortization Expense':'depreciation_and_amortization_expense', # -> For EBITDA
    'EBITDA':'ebitda',
    'Diluted Shares Outstanding':'diluted_shares_outstanding',
    'Net Income':'net_income',
    # 'EPS (Basic)':'basic_eps',
    # 'Total Expense':'total_expenses',
    # 'Non-Operating Income':'non_operating_income',
}

cashflow_metrics = {
    'Net Operating Cash Flow':'net_operating_cash_flow',
    'Free Cash Flow':'free_cash_flow'
}

fiscal_year = {
    'February-January':'31-Jan-',
    'April-March':'31-Mar-',
    'July-June':'30-Jun-',
    'January-December':'31-Dec-'
}

CALLS = 9
TIME_PERIOD = 5

def handle_error(logger, msg: str, exit=False) -> None:
    """
    Handle errors by logging the error message and deciding whether to continue or stop execution based on the severity of the error.

    Args:
        logger: The logger object used to log the error message.
        msg: The error message to be logged.
        exit: Whether to stop execution after logging the error. Defaults to False.
    """
    if exit:
        logger.error(msg)
        raise SystemExit(1)
    else:
        logger.warning(msg)

def read_csv_file(file_path):
    try:
        data = pd.read_csv(file_path)
        return data
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Could not find the file '{file_path}'. Please make sure the file exists and the path is correct.") from e

class WSJScraper:
    def __init__(self, symbols: list, quarter: bool, target_metrics: list, logger, max_retry=3,
                 save_every_symbol=False, append_file=None, completed_symbols_file=None, latest_date_df=None) -> None:
        self.symbols = symbols
        self.quarter = quarter
        self.target_metrics = target_metrics
        self.logger = logger
        self.max_retry = max_retry
        self.save_every_symbol = save_every_symbol
        self.result_dict = {
            'symbol':[],
            'date':[]
        }
        self.done_symbols_outfile = 'temp/scraped_symbols.csv'
        self.completed_symbols = set()
        self.missing_symbols_outfile = 'temp/missing_symbols.csv'
        self.missing_symbols = set()
        self.append_file = append_file
        self.completed_symbols_file = completed_symbols_file
        self.latest_date_df = latest_date_df  
        self.latest_date = None
        self.raw_data = None 
        self.proxies = {
            'http': 'http://brd-customer-hl_ef20981d-zone-unblocker:sx124tuu8xlr@brd.superproxy.io:22225',
            'https': 'http://brd-customer-hl_ef20981d-zone-unblocker:sx124tuu8xlr@brd.superproxy.io:22225'
        }
            
    @sleep_and_retry               
    @limits(calls=CALLS, period=TIME_PERIOD)
    def scrape_all_statements(self) -> None:
        symbol_dd=None
        df=pd.DataFrame()
        def _check_dbdate_is_latest(symbol, wsj_date):
            if self.latest_date_df is None:
                return False, None
            if symbol not in self.latest_date_df['symbol'].unique().tolist():
                self.logger.info(f'Found new symbol that was not in the database. {symbol}')
                return False, None
            
            dblatest_date = self.latest_date_df.loc[self.latest_date_df['symbol']==symbol, 'last_date'].max()
            if wsj_date<=dblatest_date:
                return True, None
            
            return False, dblatest_date
        def _convert_abbr(row: str, eps_true: bool) -> float:
            """
            Convert abbreviated values in a row to their corresponding numerical values.

            Args:  
                row (str): The abbreviated value to be converted.
                eps_true (bool): A boolean specifying if column is an eps column.

            Returns:
                float: The numerical value obtained after converting the abbreviated value.
            """
            if row == '-' or row == '':
                return row
            temp_row = row.replace(',', '')
            if eps_true:
                temp_row = '-'+temp_row.strip('()') if temp_row.startswith('(') else temp_row
                return float(temp_row)
            
            if '%' in temp_row:
                temp_row = temp_row.replace('%', '')
                temp_row = float(temp_row) / 100
                return temp_row
            
            if temp_row.startswith('(') and temp_row.endswith(')'):
                temp_row = '-' + temp_row.strip('()')
            try:
                n = Decimal(temp_row) * 1000000
            except Exception as e:
                handle_error(self.logger, f'Error converting value: {temp_row}, error: {e}')
                return float(temp_row)
            
            return float(n)
        def _get_rowsdata(symbol_dict: dict, rows: list, data_length: int) -> None:
            """
            Extracts data from the rows of a table in the HTML response of a web page.
            Populates a symbol dictionary and a result dictionary with the extracted data.

            Args:
                symbol_dict: A dictionary containing the symbol and column information for a specific symbol.
                rows: A list of HTML table rows.
                data_length: The expected length of the data rows.
            """
            for row in rows:
                try:
                    if len(row.attrs['class'])<1:
                        data_rows = row.find_all('td')
                        col_name = data_rows[0].text.strip()  # take the column name
                        if col_name in self.target_metrics:
                            col_name = self.target_metrics[col_name]
                        else:
                            continue
                        col_values = [value.text.strip() for value in data_rows[1:data_length+1]]  # take the subsequent values determined by date
                        try:
                            assert len(col_values) == data_length
                        except AssertionError:
                            handle_error(self.logger, f'Mismatch length of column values and headers length for {col_name}. Skipping...')
                    else:
                        continue
                except Exception as e:
                    handle_error(self.logger, f'Cannot extract rows, error: {e}')
                    continue

                if col_name not in self.result_dict:
                    self.result_dict[col_name] = []  # create a column name in our result dict
                    if len(self.result_dict['symbol']) > 0:  # if this is a new column name that was not found in previous tickers
                        # append '-' to all other tickers
                        self.result_dict[col_name] += [None] * len(self.result_dict['symbol'])
                        # self.logger.debug(f'Found new column head in table1: {col_name}')

                symbol_dict[col_name] = []
                eps_true = True if 'eps' in col_name.lower() else False
                # get the column values for this symbol 
                symbol_dict[col_name] += [_convert_abbr(col_values[i], eps_true) for i in range(data_length)]
                
        def _get_statement_data(tables_list, symbol_dict, values_length):
            for n,table in enumerate(tables_list):
                try:
                    rows = table.find('tbody').find_all('tr')
                    _get_rowsdata(symbol_dict, rows, values_length)
                except Exception as e:
                    handle_error(self.logger, f'Cannot find table{n} for {symbol}, error: {e}')
                
        period = 'quarter' if self.quarter else 'annual'
        if self.append_file:
            done_symbols = set(read_csv_file(self.completed_symbols_file)['symbol'].to_list())
            self.symbols = list(set(self.symbols) - done_symbols)
        for symbol in self.symbols:
            symbolw = symbol.split('.')[0]
            statements_div = []
            notfound_flag = False
            latest_flag = False
            for statement in ['income-statement','balance-sheet','cash-flow']:
                if latest_flag:
                    break
                url = f'https://www.wsj.com/market-data/quotes/ID/XIDX/{symbolw}/financials/{period}/{statement}'
                if not notfound_flag:
                    try:
                        i = 0
                        while i < self.max_retry:
                            response = requests.get(url, allow_redirects=True, verify=False, proxies=self.proxies)
                            soup = BeautifulSoup(response.text, 'lxml')
                            table_div = soup.find('div', {'data-module-zone':statement.replace('-','_')}).find('div', {'id':'cr_cashflow'})
                            if table_div.find_all('div', recursive=False) is not None:
                                tables = table_div.find_all('div', recursive=False)[1:]
                                statements_div.append(tables)
                                i = self.max_retry
                                break
                            i += 1
                            self.logger.debug(f'Retrying request to url for {symbol}')
                            time.sleep(5)
                    except AttributeError as e:
                        handle_error(self.logger, f'Page does not exist for {symbol}')
                        notfound_flag = True
                        break
                else:
                    self.missing_symbols.add(symbol)
                    break
            if notfound_flag:
                continue
            try:
                # extract headers
                table = statements_div[0][0].find('table')
                colheaders = table.find('thead').find_all('th')[:-1]
                fiscalYr = ''
                if not self.quarter:
                    fiscalYr = colheaders[0].text.split('.')[0].replace('Fiscal year is','').strip()
                    fiscalYr = fiscal_year[fiscalYr]
                dates = [dt.strptime(fiscalYr+col_head.text.strip(), "%d-%b-%Y") for col_head in colheaders[1:] if col_head.text.strip()!='']
                dblatest_true, dblatest_date = _check_dbdate_is_latest(symbol, dates[0])
                if dblatest_true:
                    # self.logger.debug(f"Data is already up to date for {symbol}")
                    latest_flag = True
                    continue 
                dates = list(itertools.takewhile(lambda x: x>dblatest_date, dates)) if dblatest_date else dates
                # make a local dict for 'symbol'
                symbol_dd = {
                    'symbol':list(itertools.repeat(symbol, len(dates))),
                    'date':dates
                }
                if len(statements_div) != 3:
                    self.logger.warning(f"There are 3 statements but WSJUpdater class found {len(statements_div)}.")
                ### extract statements
                # self.logger.debug(f"Trying to scrape {symbol}")
                for tables_list in statements_div:
                    _get_statement_data(tables_list, symbol_dd, len(dates))       
                    
            except Exception as e:
                if isinstance(e, IndexError):
                    msg = f'Data is not available for {symbol}'
                else:
                    msg = f'Could not identify error for {symbol}: {e}'
                handle_error(self.logger, msg)
                self.missing_symbols.add(symbol)
                continue
                
            if symbol_dd:
                result_colnames = set(list(self.result_dict.keys()))
                symbol_colnames = set(list(symbol_dd.keys()))
                # get the column names difference between the result and the current ticker
                nonexist_colnames = list(result_colnames.difference(symbol_colnames))
                self.result_dict['symbol'] += symbol_dd['symbol']
                self.result_dict['date'] += symbol_dd['date']
                cur_len = len(self.result_dict['symbol'])
                for col in list(symbol_dd.keys()):
                    if col=='symbol' or col=='date':
                        continue
                    elif (len(self.result_dict[col])+len(symbol_dd[col]))!=cur_len:
                        self.result_dict[col] += list(itertools.repeat(None, (cur_len - len(self.result_dict[col]))))
                        self.result_dict[col] += symbol_dd[col]
                    else:   
                        self.result_dict[col] += symbol_dd[col]
                # check for columns that doesn't exist for this symbol
                if len(nonexist_colnames)>0:
                    # self.logger.debug('Found non-existing columns')
                    for col in nonexist_colnames:
                        self.result_dict[col] += list(itertools.repeat(None, (len(self.result_dict['symbol']) - len(self.result_dict[col]))))
                
                df = pd.DataFrame(self.result_dict)
                if self.append_file:
                    df = df.loc[df['symbol']==symbol]
                    comp_df = read_csv_file(self.append_file)
                    cols = comp_df.columns.to_list()
                    flag = False
                    try:
                        df=df[cols]
                        assert df.columns==comp_df.columns
                    except AssertionError as e:
                        handle_error(self.logger, 'Columns do not match (1)')
                        flag=True
                    if flag:
                        cur_colnames = set(df.columns.to_list())
                        data_colnames = set(cols)
                        # get the column names difference between the result and the current ticker
                        nonexist_colnames = list(data_colnames.difference(cur_colnames))
                        if len(nonexist_colnames)>0:
                            for col in nonexist_colnames:
                                df[col] = list(itertools.repeat(None,len(df['symbol'])))
                            df = df[cols]
                        try:
                            assert all(df.columns==comp_df.columns)
                        except AssertionError:
                            handle_error(self.logger, 'Columns do not match (2). Cancelling append to file. Check code to fix', exit=True)
                # else: 
                #     self.completed_symbols.add(symbol)
                #     complete_df = pd.DataFrame({'symbol':list(self.completed_symbols)})
                #     complete_df.to_csv(self.done_symbols_outfile, index=False)
                if self.save_every_symbol:    
                    df.to_csv(f'temp/wsj_financials_{self.statement}.csv', index=False)
                # miss_symbols = pd.DataFrame({'symbol':list(self.missing_symbols)})
                # miss_symbols.to_csv(self.missing_symbols_outfile, index=False)
            else:
                continue
        self.raw_data = df
        if not df.empty:
            self.raw_data['date'] = pd.to_datetime(self.raw_data['date'])
        
def create_required_directories():
    for dir in ['logs','temp','data']:
        Path(dir).mkdir(parents=True, exist_ok=True)
    return

def init_args() -> argparse.Namespace:
    """
    Initializes and parses command line arguments using the argparse module.

    Returns:
        argparse.Namespace: Parsed command line arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--infile", nargs="?", default=None,
                        help='The path to a CSV file containing a list of symbols to scrape')
    parser.add_argument("-db", "--save_to_db", action='store_true', default=False,
                        help='Specifies whether to save the cleaned file to db or not. Defaults to not saving to DB, CSV files are always saved')
    parser.add_argument("-q", "--quarter", action='store_true', default=False,
                        help='Specifies whether to scrape annually or quarterly financial data. Defaults to annually')
    parser.add_argument("-a", "--append", nargs="?", 
                        help="The path to a CSV file to append to. Used for resuming scraping.")
    parser.add_argument("--save_every_symbol", action='store_true', default=False,
                        help="Specifies whether to save CSV file everytime data is scraped for a symbol.")

    return parser.parse_args()

def init_logger(filename='logs/wsj_scraping.log') -> logging.Logger:
    """
    Initializes a logger for logging error messages to a file.
    
    Args:
        filename (str): The filename specifying the path including the filename

    Returns:
        logger (logging.Logger): A logger object that can be used to log messages to the specified log file.
    """
    if not os.path.exists(filename):
        filepath = '/'.join(filename.split('/')[:-1])
        Path(filepath).mkdir(parents=True, exist_ok=True)
        fp = open(filename, 'x')
        fp.close()
    logger = logging.Logger("wsj_logger")
    logger.setLevel(logging.DEBUG)
    
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    
    file_handler = TimedRotatingFileHandler(filename, when='D', interval=21, backupCount=1, encoding='utf-8')
    file_handler.setLevel(logging.WARNING)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logger

def scrape_wsj(symbols: list, args, logger, latest_date_df) -> pd.DataFrame:
    metrics = income_metrics | balance_metrics | cashflow_metrics
    
    scraper = WSJScraper(
        symbols=symbols,
        quarter=args.quarter,
        target_metrics=metrics,
        logger=logger,
        latest_date_df=latest_date_df
    )
    scraper.scrape_all_statements()
    logger.info(f'Finished scraping')
    result_df = scraper.raw_data
    if result_df.empty:
        logger.info('No latest data is available. All data in database are up-to-date')
        return result_df
    result_df = result_df.sort_values('symbol')
    metrics = set(list(metrics.values()))
    columns = set(result_df.columns.to_list())
    non_exist_columns = metrics.difference(columns)
    for col in non_exist_columns:
        result_df[col] = None
    return result_df

def main():
    load_dotenv()    
    create_required_directories()
    args = init_args()
    logger = init_logger()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    # url,key = None, None
    supabase_client = None
    try:
        supabase_client = create_client(url, key)
    except Exception as e:
        handle_error(logger, f'Failed to initialize Supabase client. Error caught: {e}')
    logger.info("Starting Program")
    symbols = None
    data = None
    if args.infile:
        try:
            data = read_csv_file(args.infile)
        except FileNotFoundError as e:
            handle_error(logger, str(e), exit=True)
    # else:
    #     try:
    #         response = supabase_client.table("idx_active_company_profile").select("symbol").eq('current_source',2).execute()
    #         data = pd.DataFrame(response.data)
    #     except Exception as e:
    #         handle_error(logger, f'Could not obtain symbols from Supabase client. Error: {e}', exit=True)
    if args.append:
        try:
            temp_data = read_csv_file(args.append)
            del temp_data
        except FileNotFoundError as e:
            handle_error(logger, str(e), exit=True)   
    
    table_name = 'idx_financials_quarterly' if args.quarter else 'idx_financials_annual'
    latest_date_df = None
    if not args.infile:
        try:
            response = (
                supabase_client.rpc("get_outdated_symbols",params={"table_name":table_name, 'source':2}).execute()
            )
            latest_date_df = pd.DataFrame(response.data)
            latest_date_df['last_date'] = pd.to_datetime(latest_date_df['last_date'])
        except Exception as e:
            handle_error(logger, f'Last date table could not be retrieved. Error caught: {e}', exit=True)
    if latest_date_df: # From db       
        symbols = latest_date_df['symbol'].to_list()
    else: # From input
        symbols = data['symbol'].to_list()
    del data
    logger.info(f'Found {len(symbols)} symbols in the file')
    logger.info('Start scraping for quarterly data') if args.quarter else logger.info('Start scraping for annual data')
    result_df = scrape_wsj(symbols, args, logger, latest_date_df)
    if result_df.empty:
        raise SystemExit(0)
    logger.info('Finished scraping all statements')
    try:
        logger.info('Start cleaning')
        wsj_cleaner = WSJCleaner(result_df, supabase_client=supabase_client, 
                                quarter=args.quarter, table_name=table_name, logger=logger)
        wsj_cleaner.clean()
        logger.info('Finished cleaning process')
        if wsj_cleaner.changed_flag:
            logger.warning('Cleaner identified change in wsj_format')
        logger.info('Saving data to CSV')
        wsj_cleaner.save_data_to_csv()
    except Exception as e:
        result_df.to_csv(f"data/wsj_raw_data_{pd.Timestamp.now(tz='Asia/Jakarta').strftime('%Y%m%d_%H%M%S')}.csv", index=False)
        handle_error(logger, f'Error caught during cleaning process. Error: {e}', exit=True)
        
    if args.save_to_db:
        logger.info('Saving data to database')
        db_success_flag = wsj_cleaner.upsert_data_to_database()
    if not db_success_flag:
        logger.info('Saving raw data to CSV due to unsuccesful upsert')
        result_df.to_csv(f"data/wsj_raw_data_{pd.Timestamp.now(tz='Asia/Jakarta').strftime('%Y%m%d_%H%M%S')}.csv", index=False)
    for fname in os.listdir('temp'):
        if 'financials' in fname:
            fpath = os.path.join('temp',fname)
            Path.unlink(fpath)
            
if __name__=='__main__':   
    main()
