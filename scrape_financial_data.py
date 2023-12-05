from wsj_updater import create_required_directories, init_logger, scrape_wsj, handle_error
from wsj_cleaner import WSJCleaner
from dotenv import load_dotenv
from pathlib import Path
import argparse
import pandas as pd
import os
from supabase import create_client

def main(args):
    load_dotenv()
    create_required_directories()    
    logger = init_logger()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    # url,key = None, None
    ## initiate client
    supabase_client = None
    try:
        supabase_client = create_client(url, key)
    except Exception as e:
        handle_error(logger, f'Failed to initialize Supabase client. Error caught: {e}')
    logger.info("Starting Program")
    ## get symbols
    try:
        response = supabase_client.table("idx_active_company_profile").select("symbol").eq('current_source',2).execute()
        data = pd.DataFrame(response.data)
    except Exception as e:
        handle_error(logger, f'Could not obtain symbols from Supabase client. Error: {e}', exit=True)
    ## get last date from db
    table_name = 'idx_financials_quarterly' if args.quarter else 'idx_financials_annual'
    try:
        response = (
            supabase_client.rpc("get_last_date",params={"table_name":table_name}).execute()
        )
        latest_date_df = pd.DataFrame(response.data)
        latest_date_df['last_date'] = pd.to_datetime(latest_date_df['last_date'])
    except Exception as e:
        handle_error(logger, f'Last date table could not be retrieved. Error caught: {e}')
        latest_date_df = None
    symbols = None
    symbols = data['symbol'].to_list()
    del data
    logger.info(f'Found {len(symbols)} symbols in the file')
    ## Scraping
    logger.info('Start scraping for quarterly data') if args.quarter else logger.info('Start scraping for annual data')
    result_df = scrape_wsj(symbols, args, logger, latest_date_df)
    if result_df.empty:
        raise SystemExit(0)
    logger.info('Finished scraping all statements. Raw data saved')
    ## Cleaning
    logger.info('Start cleaning')
    wsj_cleaner = WSJCleaner(result_df, supabase_client=supabase_client, 
                             quarter=args.quarter, table_name=table_name, logger=logger)
    wsj_cleaner.clean()
    logger.info('Finished cleaning process')
    if wsj_cleaner.changed_flag:
        logger.warning('Cleaner identified change in wsj_format')
    ## Upsert
    logger.info('Saving data to database')
    db_success_flag = wsj_cleaner.upsert_data_to_database(batch_size=10)
    logger.info('Sucessfully upsert data with Supabase client') if db_success_flag else logger.warning('Failed to upsert data with Supabase client')
    for fname in os.listdir('temp'):
        if 'financials' in fname:
            fpath = os.path.join('temp',fname)
            Path.unlink(fpath)
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update financial data.")
    parser.add_argument("-a", "--annual", action="store_true", default=False, help="Update annual financial data")
    parser.add_argument("-q", "--quarter", action="store_true", default=False, help="Update quarterfinancial data")

    args = parser.parse_args()
    if args.annual and args.quarter:
        print("Error: Please specify either -a or -q, not both.")
        raise SystemExit(1)
    main(args)