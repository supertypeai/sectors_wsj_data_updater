import pandas as pd
import numpy as np
import logging
from datetime import datetime as dt

class WSJCleaner: 
    def __init__(self, data, company_profile_csv_path=None, supabase_client=None, quarter=True,
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
            'total_equity','total_revenue','net_income','total_debt','stockholders_equity','ebit','ebitda','interest_expense',
            'cash_and_short_term_investments','cash_only','total_cash_and_due_from_banks','diluted_eps','diluted_shares_outstanding',
            'gross_income','pretax_income','income_taxes','total_current_assets','total_non_current_assets','free_cash_flow',
            'interest_expense_non_operating','operating_income'
        ]
        self.supabase_client = supabase_client
        self.raw_data = pd.merge(data, ticker_info, on='symbol', how='left') 
        self.ticker_info = self.raw_data[['symbol','wsj_format']].drop_duplicates().rename(columns={'wsj_format':'wsj_format_db'})
        self.profile_data = None
        self.clean_data = None
        self.clean_flag = False
        self.clashed_symbols = set()
        self.new_format_symbols = set()
        self.changed_flag = False
        self.quarter = quarter
        self.csv_outfile = f"data/wsj_clean_data_{pd.Timestamp.now(tz='Asia/Jakarta').strftime('%Y%m%d_%H%M%S')}.csv"
    
    def _create_wsj_format(self, data):
        def test_duplication():
            try:
                clashed_symbols = self.profile_data.loc[self.profile_data.duplicated('symbol')]
                assert len(clashed_symbols)<1
                return True, clashed_symbols.symbol.unique().tolist()
            except AssertionError as e:
                self.logger.error(f'Found symbol(s) with more than 1 wsj_format: {clashed_symbols}')
                return False, clashed_symbols.symbol.unique().tolist()
        def test_equality():
            """
            Checks wsj_format between database and scraped data excluding new symbols.
            Returns true if nothing is changed.
            """
            df_merge = pd.merge(self.profile_data, self.ticker_info, on='symbol', how='left')
            # check existing db format with this scraped format
            df_merge['equal'] = df_merge['wsj_format']==df_merge['wsj_format_db']
            # store the changed format
            filter_df = df_merge.loc[df_merge['equal']==False]
            changed_df = filter_df.loc[filter_df['wsj_format_db'].notna()&(filter_df['wsj_format_db']!=5)]
            nonequal_df = filter_df.loc[filter_df['wsj_format_db'].isna()|(filter_df['wsj_format_db']==5)]
            
            return changed_df.symbol.to_list(), nonequal_df.symbol.to_list()
        
        temp_df = data.copy()
        temp_df.loc[temp_df['total_cash_and_due_from_banks'].notna(), ['wsj_format']] = 4
        temp_df.loc[temp_df['operating_income_before_interest_expense'].notna(), ['wsj_format']] = 3
        temp_df.loc[~temp_df['wsj_format'].isin([3,4]), ['wsj_format']] = 1
        self.profile_data = temp_df[['symbol','wsj_format']].drop_duplicates().copy()
        # check if there is symbol with more than 1 format
        try:
            unique_flag, clashed_symbols = test_duplication()
            assert unique_flag==True
        except AssertionError as e:
            self.logger.warning(f'Found symbol(s) with more than 1 format: {clashed_symbols}')
            self.clashed_symbols = self.clashed_symbols.union(set(clashed_symbols))
            self.changed_flag = True
        # check if there is symbols with changed format (only check for non new symbols)
        try:
            changed_format_symbols, new_format_symbols = test_equality()
            self.new_format_symbols = self.new_format_symbols.union(set(new_format_symbols))
            assert len(changed_format_symbols)<1
        except AssertionError as e:
            self.logger.warning(f'Found symbol(s) with changed format: {changed_format_symbols}')
            self.changed_flag = True
            self.clashed_symbols = self.clashed_symbols.union(set(changed_format_symbols))
        
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
            'interest_expense_non_operating'
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
            self.save_data_to_csv()
            ### Casting for upsert to db
            cast_cols = set(self.columns)
            float_cols = ['diluted_eps']
            int_cols = list(cast_cols.difference(set(['symbol','date']+float_cols)))
            self.clean_data[int_cols] = self.clean_data[int_cols].astype(float).astype('Int64')
            self.clean_data[float_cols] = self.clean_data[float_cols].astype(float)
            self.clean_flag = True
            self.logger.info('Finished cleaning')
        except Exception as e:
            self.logger.error(f'Failed to enrich columns. Error: {e}')
            return
    
    def save_data_to_database(self):
        if self.clean_flag and self.supabase_client:
            def convert_df_to_records(data):
                temp_df = data.copy()
                for cols in temp_df.columns:
                    if temp_df[cols].dtype == "datetime64[ns]":
                        temp_df[cols] = temp_df[cols].astype(str)
                temp_df["updated_on"] = pd.Timestamp.now(tz="GMT").strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                temp_df = temp_df.replace({np.nan: None})
                records = temp_df.to_dict("records")
                return records
            temp_df = self.clean_data.copy()
            records = convert_df_to_records(temp_df)
            try:
                self.supabase_client.table("idx_financials_quarterly_wsj").upsert(
                    records, returning="minimal", on_conflict="symbol, date"
                ).execute()
                return True
            except Exception as e:
                self.logger.warning('Upserting financial data with Supabase client failed. Saving to CSV file ...')
                self.save_data_to_csv()
                return False
        else:
            self.logger.info("Cannot upsert data to db, cleaning was unsuccessful. Saving to CSV file ...")
            self.save_data_to_csv()
            return False
    
    def save_profile_to_database(self):
        if len(self.new_format_symbols)>0:
            records = self.profile_data.loc[
                self.profile_data['symbol'].isin(list(self.new_format_symbols))
                ].to_dict("records")
            try:
                self.supabase_client.table('idx_company_profile').upsert(records, returning="minimal", on_conflict="symbol").execute()
                return 1
            except Exception as e:
                self.logger.warning('Upserting wsj_format data with Supabase client failed. Saving to CSV file ...')
                self.save_data_to_csv()
                return -1
        return 0
    
    def save_data_to_csv(self):
        if self.clean_flag:
            self.logger.info("Saving cleaned CSV file to a local directory")
            self.clean_data.to_csv(self.csv_outfile, index=False)  
        elif self.clean_data is not None:
            self.logger.info("Saving partially cleaned CSV file to a local directory")
            self.clean_data.to_csv(f"data/wsj_partialclean_data_{pd.Timestamp.now(tz='Asia/Jakarta').strftime('%Y%m%d_%H%M%S')}.csv", index=False)  
        else:
            self.logger.warning("Cleaning was unsucessful. No cleaned data was saved")
            
        if self.changed_flag:
            self.logger.info("Saving changed wsj_format data to a local directory")
            df = self.profile_data.loc[self.profile_data['symbol'].isin(list(self.clashed_symbols))]
            self.profile_data.to_csv(f"data/wsj_format_data.csv", index=False)  
            