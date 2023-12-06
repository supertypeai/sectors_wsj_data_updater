import pandas as pd
import requests
import logging
import time
import os
import yfinance as yf
from supabase import create_client
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from wsj_updater import handle_error, init_logger
from pyrate_limiter import Duration, Limiter, RequestRate
from requests import Session
from requests_ratelimiter import LimiterMixin, MemoryQueueBucket

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'}

wsj_formats = {
    'Total Cash & Due from Banks':4,
    'Operating Income Before Interest Expense':3
}

class LimiterSession(LimiterMixin, Session):
    def __init__(self):
        super().__init__(
            limiter=Limiter(
                RequestRate(2, Duration.SECOND * 5)
            ),  # max 2 requests per 5 seconds
            bucket_class=MemoryQueueBucket,
        )
        
class SourceFormatChecker():
    def __init__(self, supabase_client=None, max_retry=2, logger=logging.getLogger(__name__)):
        self.supabase_client = supabase_client
        self.session = LimiterSession()
        self.logger = logger
        self.check_symbols = set()
        self.missing_symbols = set()
        self.found_data = {'symbol':[], 'current_source':[]}
        if not supabase_client:
            self.logger.error("Supabase client not provided")
            raise SystemExit(1)
        else:
            ### Get symbols to check for wsj_format
            client_table = self.supabase_client.table("idx_company_profile")
            # Check undefined wsj_format on DB
            response = client_table.select("symbol").eq('wsj_format',-1).execute()
            temp_data = pd.DataFrame(response.data)
            self.check_symbols.update(pd.DataFrame(response.data).symbol.tolist()) if not temp_data.empty else []
            # Check symbols with wsj_format 3 or 4 and source from YF on DB
            response = client_table.select("symbol").lt('current_source',2).in_('wsj_format',[3,4]).execute()
            temp_data = pd.DataFrame(response.data)
            self.check_symbols.update(pd.DataFrame(response.data).symbol.tolist()) if not temp_data.empty else []
            ### Get symbols with no source
            response = client_table.select("symbol").eq('current_source',-1).execute()
            temp_data = pd.DataFrame(response.data)
            self.missing_symbols.update(pd.DataFrame(response.data).symbol.tolist()) if not temp_data.empty else []
        del temp_data
        self.max_retry = max_retry
        self.format_data = {'symbol':[], 'wsj_format':[]}
        
    def _scrape_wsj_data(self, symbol, statement, period):
        def try_from_url(url, statement):
            response = requests.get(url, allow_redirects=True, headers=headers)
            soup = BeautifulSoup(response.text, 'lxml')
            table_div = soup.find('div', {'data-module-zone':statement.replace('-','_')}).find('div', {'id':'cr_cashflow'})
            if table_div.find_all('div', recursive=False) is not None:
                tables = table_div.find_all('div', recursive=False)[1:]
                return tables
            return None
        symbolw = symbol.split('.')[0]
        tables = None
        try:    
            i = 0
            while i < self.max_retry:
                url = f'https://www.wsj.com/market-data/quotes/ID/XIDX/{symbolw}/financials/{period}/{statement}'
                if tables:= try_from_url(url, statement):
                    return tables
                i += 1
                # self.logger.debug(f'Retrying request to {period} url for {symbol}')
                time.sleep(1)
            # Page exists but table doesn't, usually a refresh would fix it or it just doesn't exist
            if tables is None:
                raise ValueError
        # Page doesn't exist (404)
        except AttributeError as e:
            raise AttributeError
        
    def check_wsj_format(self):
        ### extract data from tables
        for symbol in self.check_symbols:
            found_flag = False
            for period in ['quarter','annual']:
                if found_flag: break
                for statement, metric in zip(['balance-sheet','income-statement'],list(wsj_formats.keys())):
                    if found_flag: break
                    try:
                        tables = self._scrape_wsj_data(symbol, statement, period)
                        table = tables[0].find('table')
                        rows = table.find('tbody').find_all('tr') 
                        rows = [row.find('td').text.strip() for row in rows if len(row.attrs['class'])<1]
                        for row in rows:
                            if row == metric:
                                self.logger.info(f'Found wsj_format {wsj_formats.get(metric)} for {symbol}')
                                self.format_data['symbol'].append(symbol)
                                self.format_data['wsj_format'].append(wsj_formats.get(metric))
                                found_flag = True
                                break
                    except Exception as e:
                        # sourced from tables[0]
                        if isinstance(e, TypeError):
                            msg = f'Data is not available for {symbol} in {period}'
                        elif isinstance(e, AttributeError):
                            msg = f'Sourced from error: Page does not exist for {symbol}'
                            handle_error(self.logger, msg)
                            found_flag = True # contradicting to the var name but to indicate not to search through again
                            break
                        elif isinstance(e, ValueError):
                            msg = f'Page exists but table does not exist for {symbol}'
                        else:
                            msg = f'Could not identify error: {e}'
                        handle_error(self.logger, msg)
                        break
            if not found_flag and tables:
                self.logger.info(f'Assigned wsj_format 1 for {symbol}')
                self.format_data['symbol'].append(symbol)
                self.format_data['wsj_format'].append(1)
                found_flag=True
     
    def check_null_source(self):
        def check_symbol_in_yf(symbol, session):
            ticker = yf.Ticker(symbol, session)
            if ticker.quarterly_income_stmt.empty:
                return False
            else:
                return True
        def check_symbol_in_wsj(symbol):
            tables = self._scrape_wsj_data(symbol, 'income-statement', 'quarter')
            if tables:
                return True
            return False
        if len(self.missing_symbols)>0:
            temp_symbols = list(self.missing_symbols)
            for symbol in temp_symbols: 
                if check_symbol_in_yf(symbol, self.session):
                    self.found_data['symbol'].append(symbol)
                    self.found_data['current_source'].append(1)
                elif symbol not in self.check_symbols:
                    if check_symbol_in_wsj(symbol):
                        self.found_data['symbol'].append(symbol)
                        self.found_data['current_source'].append(2)
                    
    def save_to_csv(self, df, column, error=None):
        if error:
            self.logger.warning(f'Updating {column} data with Supabase client failed. Saving to CSV file. Error:{error}')
            self.logger.info(f"Saving changed {column} data to a local directory")  
        df.to_csv(f"data/{column}_data.csv", index=False) 
        return
                
    def update_format_to_database(self):
        if len(self.format_data['symbol'])>0:
            temp_df = pd.DataFrame(self.format_data)
            temp_df['wsj_format'] = temp_df['wsj_format'].astype(int)
            for format in temp_df.wsj_format.unique().tolist():
                symbols = temp_df.loc[temp_df['wsj_format']==format].symbol.unique().tolist()
                self.logger.info("Updating wsj_format with Supabase client")
                try:
                    self.supabase_client.table("idx_company_profile")\
                    .update({'wsj_format': format})\
                    .in_('symbol', symbols)\
                    .execute()
                except Exception as e:
                    self.save_to_csv(temp_df, 'wsj_format', e)
                    return -1
            self.logger.info(f'Sucessfully update wsj_format data with Supabase client')
            ### Check for symbols with wsj_format 3 and 4 and update current_source to 2
            temp_df = temp_df.loc[temp_df['wsj_format']>2]
            if len(temp_df['symbol'])>0:
                self.logger.info("Updating current_source to 2 for wsj_format 3 and 4 with Supabase client")
                try:
                    self.supabase_client.table("idx_company_profile")\
                    .update({'current_source': 2})\
                    .in_('symbol', temp_df.symbol.unique().tolist())\
                    .execute()
                    self.logger.info("Sucessfully update current_source with Supabase client")  
                except Exception as e:
                    temp_df = pd.DataFrame({'symbol':symbols})
                    self.save_to_csv(temp_df, 'current_source', e)
                    return -1
            return 1
        return 0   
    
    def update_source_to_database(self):
        if len(self.found_data['symbol'])>0:
            temp_df = pd.DataFrame(self.found_data)
            temp_df['current_source'] = temp_df['current_source'].astype(int)
            self.logger.info(f'Updating current_source data with Supabase client')
            for source in temp_df.current_source.unique().tolist():
                symbols = temp_df.loc[temp_df['current_source']==source].symbol.unique().tolist()
                try:
                    self.supabase_client.table("idx_company_profile")\
                    .update({'current_source': source})\
                    .in_('symbol', symbols)\
                    .execute()
                except Exception as e:
                    temp_df = pd.DataFrame({'symbol':symbols})
                    self.save_to_csv(temp_df, 'current_source', e)
                    return -1
                return 1
        return 0
                
if __name__=='__main__':
    load_dotenv()
    logger = init_logger('logs/wsj_format_checker.log')
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    # url,key = None, None
    supabase_client = None
    try:
        supabase_client = create_client(url, key)
    except Exception as e:
        handle_error(logger, f'Failed to initialize Supabase client. Error caught: {e}', exit=True)
        
    checker = SourceFormatChecker(supabase_client, logger=logger)
    checker.check_wsj_format() 
    checker.check_null_source()
    if success_flag := checker.update_format_to_database() == 0:
        logger.info('No new format found')
    elif success_flag == 1:
        logger.info('Finished updating format to database')
    elif success_flag == -1:
        logger.info('Updating format to database failed')
    
    if success_flag := checker.update_source_to_database() == 0:
        logger.info('No new source found')
    elif success_flag == 1:
        logger.info('Finished updating source to database')
    elif success_flag == -1:
        logger.info('Updating source to database failed')
            