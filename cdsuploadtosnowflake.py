import pandas as pd
import snowflake.connector
from cdswritereadfunctions import load_input_file as rjf, write_json_to_file as wjf
import gc
import sf_creds
import os
from functools import reduce
from datetime import datetime, timedelta
from snowflake.connector.pandas_tools import write_pandas


def upload_to_sf(run=False):
    """
       Uploads files to snowflake.

       Args:
           run (bool): If True, the function will execute the upload process. If False, the function will not execute
           the upload process.

       Returns:
           None
       """
    if run:
        inputs = rjf('DAX_ControlFile.json')
        start_date = datetime.today().strftime('%Y-%m-%d')
        dir_path = 'Output_Data_Files'
        seg_list = inputs['segment_to_run']

        label_date = (datetime.strptime(inputs["start_date"], '%Y-%m-%d') -
                      timedelta(weeks=4)).strftime('%Y-%m-%d')

        col_name_map = {'GIGYA_ID': 'CUSTOMER_ID', 'AI': 'AUT',
                        'TI': 'TRA', 'CCI': 'CRE', 'TE': 'TEC', 'HO': 'HOM', 'HWE': 'HEL',
                        'CARR': 'CAR', 'EDU': 'EDU', 'NP': 'NAP', 'PC': 'PCL', 'PET': 'PET', 'MH': 'MED',
                        'LE': 'LEV', 'AE': 'ART', 'ATT': 'ATT', 'BL': 'BAL', 'RE': 'REA', 'SCI': 'SCI',
                        'SHOP': 'SHO', 'SF': 'SAF', 'VG': 'GAM', 'EC': 'ENV', 'MOV': 'ENT', 'CA': 'CLO',
                        'CPG': 'CPG', 'TV': 'TEL', 'NON': 'NON', 'FOOD': 'FAD', 'FI': 'FIN', 'HOL': 'HOL',
                        'BO': 'BUS', 'SE': 'SPO', 'HWC': 'PAR'}

        files = []
        for dirpath, dirnames, filenames in os.walk(dir_path):
            for filename in filenames:
                if '.csv' in filename and label_date in filename:
                    files.append(os.path.join(dirpath, filename))

        df_dir = []

        for i in range(len(files)):
            if 'csv' in files[i]:
                for segs in seg_list:
                    if f'_{segs}_OUTPUT' in files[i]:
                        print(files[i])
                        data = pd.read_csv(files[i])
                        data.set_index('GIGYA_ID', inplace=True)
                        df = data.copy()
                        df.rename(columns={'Label': segs}, inplace=True)
                        df_label = pd.DataFrame(df[segs])
                        globals()[f'df_{segs}'] = df_label.copy()
                        df_dir.append(globals()[f'df_{segs}'])

        all_df = reduce(lambda left, right: pd.merge(left, right,
                                                     left_index=True,
                                                     right_index=True,
                                                     how='outer'), df_dir)

        all_df.reset_index(inplace=True, drop=False)

        all_df['DATE_ADDED'] = start_date

        all_df.rename(columns=col_name_map, inplace=True)

        ctx = snowflake.connector.connect(
            user=sf_creds.user,
            password=sf_creds.password,
            account=sf_creds.account,
            role=sf_creds.role,
            warehouse=sf_creds.warehouse,
            database=sf_creds.database,
            schema=sf_creds.schema
        )

        print(all_df.head())
        print(f'df size : {len(all_df)}')

        retrieve_query = 'SELECT * FROM RD_RM.SANDBOX.DS_META_PROD_MP_FIRST_PARTY_SEGMENTS WHERE DATE_ADDED LIKE' \
                         '(SELECT MAX(DATE_ADDED) FROM RD_RM.SANDBOX.DS_META_PROD_MP_FIRST_PARTY_SEGMENTS)'
        mp_table = pd.read_sql(retrieve_query, ctx)

        mp_table.set_index('CUSTOMER_ID', inplace=True, drop=True)
        all_df.set_index('CUSTOMER_ID', inplace=True, drop=True)

        col_actual = list(set(mp_table.columns.to_list()) - {'DATE_ADDED'})
        cols = list(set(all_df.columns.to_list()) - {'DATE_ADDED'})

        fill_df = [all_df]
        for col in cols:
            if all_df[col].sum() == 0:
                all_df.drop('col', inplace=True)
                fill_df.append(pd.DataFrame(mp_table[col]))

        unique_cols = list(set(col_actual) - set(cols))
        sub_df = pd.DataFrame(mp_table[unique_cols])
        fill_df.append(sub_df)

        complete_df = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True,
                                                          how='outer'), fill_df)

        complete_df.reset_index(inplace=True, drop=False)
        complete_df.fillna(value=0, inplace=True)
        complete_df['DATE_ADDED'] = start_date

        complete_df.to_csv(f'complete_df_{start_date}.csv', index=False)
        print(f'complete_df_{start_date}.csv')

        success, _, numrows, _ = write_pandas(ctx, complete_df,
                                              table_name='DS_META_PROD_MP_FIRST_PARTY_SEGMENTS',
                                              quote_identifiers=False)

        for name in list(globals().keys()):
            if not name.startswith('_'):  # Don't delete system variables.
                value = globals()[name]
                if isinstance(value, pd.DataFrame):  # Only delete DataFrame objects.
                    del globals()[name]

        gc.collect()

        del df_dir

        print('table uploaded successfully')


def main():
    upload_to_sf(True)


if __name__ == '__main__':
    main()
