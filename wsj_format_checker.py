import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import logging
import time
import os
from wsj_updater import handle_error, init_logger
from supabase import create_client
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36'}

wsj_formats = {
    'Total Cash & Due from Banks':4,
    'Operating Income Before Interest Expense':3
}

class WSJFormatChecker():
    def __init__(self, supabase_client=None, max_retry=2, logger=logging.getLogger(__name__)) -> None:
        self.supabase_client = supabase_client
        self.logger = logger
        if not supabase_client:
            self.logger.error("Supabase client not provided")
            raise SystemExit(1)
        else:
            response = (
                self.supabase_client.table("idx_company_profile").select("symbol, wsj_format")\
                .eq('wsj_format',5).execute()
            )
            self.symbols = pd.DataFrame(response.data).symbol.tolist()
            response = (
                self.supabase_client.table("idx_company_profile").select("symbol, wsj_format")\
                .is_('wsj_format','null').execute()
            )
            self.symbols += pd.DataFrame(response.data).symbol.tolist()
        self.max_retry = max_retry
        self.profile_data = {'symbol':[], 'wsj_format':[]}
        
    def check_availability(self):
        def try_from_url(url, statement):
            response = requests.get(url, allow_redirects=True, headers=headers)
            soup = BeautifulSoup(response.text, 'lxml')
            table_div = soup.find('div', {'data-module-zone':statement.replace('-','_')}).find('div', {'id':'cr_cashflow'})
            if table_div.find_all('div', recursive=False) is not None:
                tables = table_div.find_all('div', recursive=False)[1:]
                return tables
            return None
        for symbol in self.symbols:
            symbolw = symbol.split('.')[0]
            found_flag = False
            for period in ['annual', 'quarter']:
                for statement, metric in zip(['balance-sheet','income-statement'],list(wsj_formats.keys())):
                    if found_flag:
                        break
                    tables = None
                    try:    
                        i = 0
                        while i < self.max_retry:
                            url = f'https://www.wsj.com/market-data/quotes/ID/XIDX/{symbolw}/financials/{period}/{statement}'
                            if tables:= try_from_url(url, statement):
                                break
                            i += 1
                            self.logger.debug(f'Retrying request to quarter url for {symbol}')
                            time.sleep(1)
                        if tables is None:
                            handle_error(self.logger, f'Page does not exist for {symbol}')
                            continue
                    except AttributeError as e:
                        handle_error(self.logger, f'Sourced from error: page does not exist for {symbol}. Error: {e}')
                        continue
                    ### extract data from tables
                    try:
                        if table:= tables[0].find('table'):
                            rows = table.find('tbody').find_all('tr') 
                            rows = [row.find('td').text.strip() for row in rows if len(row.attrs['class'])<1]
                            for row in rows:
                                if row == metric:
                                    self.logger.info(f'Found wsj_format for {symbol}')
                                    self.profile_data['symbol'].append(symbol)
                                    self.profile_data['wsj_format'].append(wsj_formats.get(metric))
                                    found_flag = True
                                    break
                    except Exception as e:
                        if isinstance(e, IndexError):
                            msg = f'Data is not available for {symbol}'
                        else:
                            msg = f'Could not identify error: {e}'
                        handle_error(self.logger, msg)
                        break
                if not found_flag and tables:
                    self.logger.info(f'Flag: Found wsj_format for {symbol}')
                    self.profile_data['symbol'].append(symbol)
                    self.profile_data['wsj_format'].append(1)
                    found_flag=True
                
    def upsert_profile_to_database(self):
        if len(self.profile_data['symbol'])>0:
            temp_df = pd.DataFrame(self.profile_data)
            temp_df['wsj_format'] = temp_df['wsj_format'].astype(int)
            for format in temp_df.wsj_format.unique().tolist():
                symbols = temp_df.loc[temp_df['wsj_format']==format].symbol.unique().tolist()
                try:
                    self.supabase_client.table("idx_company_profile")\
                    .update({'wsj_format': format})\
                    .in_('symbol', symbols)\
                    .execute()
                except Exception as e:
                    self.logger.warning(f'Upserting wsj_format data with Supabase client failed. Saving to CSV file. Error:{e}')
                    self.logger.info("Saving changed wsj_format data to a local directory")
                    temp_df.to_csv(f"data/wsj_format_data.csv", index=False) 
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
        
    checker = WSJFormatChecker(supabase_client, logger=logger)
    checker.check_availability()
    checker.upsert_profile_to_database()
            