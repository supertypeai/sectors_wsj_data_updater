import pandas as pd
import numpy as np
import logging
import time

class WSJCleaner: 
    def __init__(self, data, company_profile_csv_path=None, supabase_client=None, quarter=True, table_name=None,
                 logger=logging.getLogger(__name__)) -> None:
        self.logger = logger
        if not company_profile_csv_path and not supabase_client:
            msg = "At least one of the company_profile_csv_path or supabase_client must be provided"
            self.logger.error(msg)
            raise ValueError(msg)
        elif company_profile_csv_path and supabase_client:
            self.logger.warning("Only one of company_profile_csv_path or supabase_client should be provided. Proceeding with supabase_client.")
        
        if supabase_client:
            response = (
                supabase_client.table("idx_company_profile").select("symbol, wsj_format").execute()
            )
            ticker_info = pd.DataFrame(response.data)
        elif company_profile_csv_path:
            try:
                ticker_info = pd.read_csv(company_profile_csv_path)['symbol','wsj_format']
            except Exception as e:
                if isinstance(e, FileNotFoundError):
                    logger.error("Could not found the file provided. Are you sure if you provided the correct path or is does the file exists?")
                if isinstance(e, KeyError):
                    logger.error(f"Either symbol or wsj_format was not found in the csv file. Error: {e}")
                else:
                    logger.critical(f'Error caught: {e}')
                raise SystemExit(1)
        self.columns = [
            'symbol','date','net_operating_cash_flow','total_assets','total_liabilities','total_current_liabilities',
            'total_equity','total_revenue','net_income','total_debt','stockholders_equity','ebit','ebitda',
            'cash_and_short_term_investments','cash_only','total_cash_and_due_from_banks','diluted_eps','diluted_shares_outstanding',
            'gross_income','pretax_income','income_taxes','total_non_current_assets','free_cash_flow',
            'interest_expense_non_operating','operating_income'
        ]
        self.supabase_client = supabase_client
        self.raw_data = pd.merge(data, ticker_info, on='symbol', how='left') 
        self.ticker_info = self.raw_data[['symbol','wsj_format']].drop_duplicates().rename(columns={'wsj_format':'wsj_format_db'})
        self.profile_data = None
        self.clean_data = None
        self.clean_flag = False
        self.changed_symbols = set()
        self.changed_flag = False
        self.period_id = 'q' if quarter else 'a'
        self.table_name = table_name
        self.csv_outfile = f"data/wsj_clean_data_{self.period_id}_{pd.Timestamp.now(tz='Asia/Jakarta').strftime('%Y%m%d_%H%M%S')}.csv"
    
    def _create_wsj_format(self, data):
        def test_duplication():
            try:
                changed_symbols = self.profile_data.loc[self.profile_data.duplicated('symbol')]
                assert len(changed_symbols)<1
                return True, changed_symbols.symbol.unique().tolist()
            except AssertionError as e:
                self.logger.error(f'Found symbol(s) with more than 1 wsj_format: {changed_symbols}')
                return False, changed_symbols.symbol.unique().tolist()
        
        temp_df = data.copy()
        temp_df.loc[temp_df['total_cash_and_due_from_banks'].notna(), ['wsj_format']] = 4
        temp_df.loc[temp_df['operating_income_before_interest_expense'].notna(), ['wsj_format']] = 3
        temp_df.loc[~temp_df['wsj_format'].isin([3,4]), ['wsj_format']] = 1
        self.profile_data = temp_df[['symbol','wsj_format']].drop_duplicates().copy()
        # check if there is symbol with more than 1 format
        try:
            unique_flag, changed_symbols = test_duplication()
            assert unique_flag==True
        except AssertionError as e:
            self.logger.warning(f'Found symbol(s) with more than 1 format: {changed_symbols}')
            self.changed_symbols = self.changed_symbols.union(set(changed_symbols))
            self.changed_flag = True
        
        return temp_df
    
    def _clean_nulls(self, data):
        """
        notes:
        [1] fill '-' with null, assume its not reported on WSJ (can't assume its 0).
        [2] formula: other operating expenses = "Selling, General & Admin. Expenses & Other" - "selling_general_and_administration_expense"
        """     
        temp_data = data.copy() 
        ### Cleaning debt columns
        debt_cols = ['short_term_debt','long_term_debt']
        debt_condition = ((temp_data['short_term_debt']=='-')|temp_data['short_term_debt'].isna()) & ((temp_data['long_term_debt']=='-')|temp_data['long_term_debt'].isna())
        temp_data.loc[debt_condition, debt_cols] = None
        
        debt_condition = ((temp_data['short_term_debt']=='-')|temp_data['short_term_debt'].isna()) & temp_data['long_term_debt'].notna()
        temp_data.loc[debt_condition, 'short_term_debt'] = 0
        ## and vice-versa
        debt_condition = ((temp_data['long_term_debt']=='-')|temp_data['long_term_debt'].isna()) & temp_data['short_term_debt'].notna()
        temp_data.loc[debt_condition, 'long_term_debt'] = 0

        ### [1] 
        fill_hyphen_cols = [
            'selling_general_and_administration_expense', 'other_operating_expenses', 'non_interest_income', 'net_interest_income',
            'gross_income','depreciation_and_amortization_expense','ebitda','total_assets', 'total_current_assets', 'pretax_income',
            'total_current_liabilities','total_liabilities', 'total_revenue','cogs_including_depreciation_amortization',
            'interest_expense_non_operating','interest_expense_net_of_interest_capitalized'
        ]
        temp_data[fill_hyphen_cols] = temp_data[fill_hyphen_cols].replace('-',None)
        
        ## [2]
        temp_data['other_operating_expenses'] = np.where(
            (temp_data['wsj_format']==3) & temp_data['other_operating_expenses'].isna(),
            pd.to_numeric(temp_data['selling_general_and_admin_expenses_and_other']) - pd.to_numeric(temp_data['selling_general_and_administration_expense']),
            temp_data['other_operating_expenses']
        )
        ## if gross_income is not null fill other_operating_expenses with 0
        temp_data['other_operating_expenses'] = np.where(
            temp_data['other_operating_expenses'].isna() & temp_data['gross_income'].notna(), 
            0, temp_data['other_operating_expenses'])
        ## [2]
        temp_data['selling_general_and_admin_expenses_and_other'] = np.where(
            temp_data['selling_general_and_admin_expenses_and_other'].isna(),
            pd.to_numeric(temp_data['selling_general_and_administration_expense']) + pd.to_numeric(temp_data['other_operating_expenses']),
            temp_data['selling_general_and_admin_expenses_and_other']
        )

        ## operating income for format 3
        temp_data['operating_income'] = np.where(
            temp_data['wsj_format']==3,
            temp_data['operating_income_before_interest_expense'],
            temp_data['operating_income']
        )
        ## For format 1
        temp_data['operating_income'] = np.where(
            temp_data['operating_income'].isna() & (temp_data['wsj_format']==1),
            pd.to_numeric(temp_data['gross_income']) - pd.to_numeric(temp_data['selling_general_and_administration_expense']) - pd.to_numeric(temp_data['other_operating_expenses']),
            temp_data['operating_income']
        )

        ## Interest expense for format 3
        temp_data['interest_expense_non_operating'] = np.where(
            temp_data['wsj_format']==3,
            temp_data['interest_expense_net_of_interest_capitalized'],
            temp_data['interest_expense_non_operating']
        )
        
        ### gross_income
        temp_data['gross_income'] = np.where(
            temp_data['gross_income'].isna(),
            pd.to_numeric(temp_data['total_revenue']) - pd.to_numeric(temp_data['cogs_including_depreciation_amortization']),
            temp_data['gross_income']
        )
        
        return temp_data
    
    def _enrich_columns(self, data):
        temp_data = data.copy()
        temp_data['total_debt'] = np.where(
            temp_data['total_debt'].isna(), 
            pd.to_numeric(temp_data['short_term_debt'])+pd.to_numeric(temp_data['long_term_debt']), 
            temp_data['total_debt'])
        ## should fill for banking 
        temp_data['total_revenue'] = np.where(
            temp_data['total_revenue'].isna(), 
            pd.to_numeric(temp_data['net_interest_income'])+pd.to_numeric(temp_data['non_interest_income']), 
            temp_data['total_revenue'])
        ### general filling
        temp_data['ebit'] = np.where(
            temp_data['wsj_format'].isin([1,3]),
            pd.to_numeric(temp_data['pretax_income']) + pd.to_numeric(temp_data['interest_expense_non_operating']),
            None
        )

        temp_data['ebitda'] = np.where(
            temp_data['ebitda'].notna(),
            pd.to_numeric(temp_data['ebit']) + pd.to_numeric(temp_data['depreciation_and_amortization_expense']),
            None
        )
        ### For non_current_assets (fixed assets)
        ## General: total_non_current_assets = total_assets - total_current_assets
        temp_data['total_non_current_assets'] = pd.to_numeric(temp_data['total_assets']) - pd.to_numeric(temp_data['total_current_assets'])

        return temp_data
    
    def clean(self):
        self.raw_data = self._create_wsj_format(self.raw_data)    
        try:
            self.clean_data = self._clean_nulls(self.raw_data)
        except Exception as e:
            self.logger.error(f'Failed to clean nulls columns. Error: {e}')
            return
        try:
            self.clean_data = self._enrich_columns(self.clean_data)
            self.clean_data = self.clean_data[self.columns]
            self.clean_data = self.clean_data.replace('-',None)
            ### Casting for upsert to db
            cast_cols = set(self.columns)
            float_cols = ['diluted_eps']
            int_cols = list(cast_cols.difference(set(['symbol','date']+float_cols)))
            self.clean_data[int_cols] = self.clean_data[int_cols].astype(float).astype('Int64')
            self.clean_data[float_cols] = self.clean_data[float_cols].astype('float64')
            self.clean_data = self.clean_data.drop_duplicates()
            self.clean_flag = True
            self.logger.info('Finished cleaning')
        except Exception as e:
            self.logger.error(f'Failed to enrich columns. Error: {e}')
            return
    
    def upsert_data_to_database(self):
        if self.clean_flag and self.supabase_client:
            def convert_df_to_records(data):
                temp_df = data.copy()
                for cols in temp_df.columns:
                    if temp_df[cols].dtype == "datetime64[ns]":
                        temp_df[cols] = temp_df[cols].astype(str)
                temp_df = temp_df.replace({np.nan: None})
                records = temp_df.to_dict("records")
                return records
            def batch_upsert(records, batch_size=100, max_retry=3):
                for i in range(0, len(records), batch_size):
                    retry_count = 0
                    while retry_count < max_retry:
                        try:
                            self.supabase_client.table(self.table_name).upsert(
                                records[i:i+batch_size], returning="minimal", on_conflict="symbol, date"
                            ).execute()
                            break
                        except Exception as e:
                            self.logger.warning(f'Upserting financial data with Supabase client failed. Retrying ...')
                            retry_count += 1
                            if retry_count == max_retry:
                                self.save_data_to_csv()
                                return False, str(e)
                            time.sleep(5)
                return True, None
            temp_df = self.clean_data.copy()
            records = convert_df_to_records(temp_df)
            if len(records)<1000:
                try:
                    self.supabase_client.table(self.table_name).upsert(
                        records, returning="minimal", on_conflict="symbol, date"
                    ).execute()
                    return True
                except Exception as e:
                    self.logger.warning(f'Upserting financial data with Supabase client failed. Saving to CSV. Error:{e}')
                    self.save_data_to_csv()
                    return False
            else:
                db_success_flag, msg = batch_upsert(records)
                if db_success_flag:
                    return True
                else:
                    self.logger.warning(f'Failed to upsert data to db. Error: {msg}')
                    return False
        else:
            self.logger.info("Cannot upsert data to db, cleaning was unsuccessful. Saving to CSV file ...")
            self.save_data_to_csv()
            return False
    
    def save_data_to_csv(self):
        if self.clean_flag:
            self.logger.info("Saving cleaned CSV file to a local directory")
            self.clean_data.to_csv(self.csv_outfile, index=False)  
        elif self.clean_data is not None:
            self.logger.info("Saving partially cleaned CSV file to a local directory")
            self.clean_data.to_csv(f"data/wsj_partialclean_data_{pd.Timestamp.now(tz='Asia/Jakarta').strftime('%Y%m%d_%H%M%S')}.csv", index=False)  
        else:
            self.logger.warning("Cleaning was unsucessful. No cleaned data was saved")  
            