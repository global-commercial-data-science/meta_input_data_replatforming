import snowflake.connector
import pandas as pd
import sf_creds
from tqdm import tqdm
from cdswritereadfunctions import load_input_file as rjf
from snowflake.connector.pandas_tools import write_pandas

def main():
    """
        NOTE: All parameters are read from DAX_ControlFile.json
        :param start_date (str) : Starting date of the dataset
        :param segment (str) : DMP segment to build the datset for
        :return: Snowflake Table
    """

    inputs = rjf('DAX_ControlFile.json')
    start_date = inputs['start_date']
    version = inputs['version']
    segment_to_run = inputs['segment_to_run']

    ctx = snowflake.connector.connect(
        user=sf_creds.user,
        password=sf_creds.password,
        account=sf_creds.account,
        role=sf_creds.role,
        warehouse=sf_creds.warehouse,
        database=sf_creds.database,
        schema=sf_creds.schema
    )

    for i in tqdm(range(len(segment_to_run))):

        segment = segment_to_run[i]

        output_df_name = inputs['segment'][segment]['output_file'].format(
            start_date, segment, version)
        output_df_name = output_df_name.replace('.CSV', '_processed.csv')
        input_df_name = inputs['analytics_table'].format(segment, start_date, version)

        output_df = pd.read_csv(output_df_name)
        output_df.set_index('GIGYA_ID', inplace=True)
        prediction_df = output_df[['PREDICTION', 'Label']]

        input_df = pd.read_csv(input_df_name)
        input_df.set_index('GIGYA_ID', inplace=True)

        complete_df = pd.concat([input_df, prediction_df], axis=1)
        complete_df.reset_index(inplace=True)

        del output_df
        del input_df

        features = list(set(complete_df.columns) - {'GIGYA_ID', 'PREDICTION', 'Label'})
        pivoted_df = complete_df.melt(id_vars=['GIGYA_ID', 'PREDICTION', 'Label'], value_vars=features)
        pivoted_df['Run_Date'] = start_date
        """
        chunksize = 1000000

        k = 0
        for i in range(0, pivoted_df.shape[0], chunksize):
            chunk = pivoted_df[i:i + chunksize]
            success, _, numrows, _ = write_pandas(ctx, chunk,
                                                  table_name=f'{segment}_CLUSTERING_ANALYSIS',
                                                  quote_identifiers=False)
            k += numrows
            print(f'uploaded {k} rows')
        """
        del pivoted_df

        prediction_df.drop(columns=['PREDICTION'], inplace=True)
        prediction_df.columns = [f'{segment}']


        if i == 0:
            comp_df = prediction_df
        else:
            comp_df = pd.concat([comp_df, prediction_df], axis = 1)

    comp_df['DATE'] = start_date
    comp_df.reset_index(inplace = True)
    comp_df = comp_df[['GIGYA_ID'] + segment_to_run + ['DATE']]
    success, _, numrows, _ = write_pandas(ctx, comp_df,
                                          table_name='DS_GIGYA_ID_LIST_DEV',
                                          quote_identifiers=False)

    print('table backup complete...')


if __name__ == "__main__":
    main()




