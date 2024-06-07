import pandas as pd
import mparticle
import numpy as np
import logging
import time
import sf_creds
import random
from datetime import datetime
import warnings
import os

logging.getLogger().setLevel(logging.CRITICAL)


def my_callback(response):
    if type(response) is mparticle.rest.ApiException:
        print('An error occured: ' + str(response))
    else:
        # successful uploads will yield an HTTP 202 response and nobody
        print(f'RESPONSE {response}')


sleep_times = [0.3, 0.5, 0.7, 1.0, 1.2]  # sleep time to avoid rate limits


def upload_data_single_attribute(processed_file_name, segment_code):
    seg = segment_code
    df = pd.read_csv(processed_file_name)
    df.rename(columns={'GIGYA_ID': 'CUSTOMER_ID', 'Label': seg}, inplace=True)
    df.drop(columns=list(set(df.columns.to_list()) - {'CUSTOMER_ID', seg}))

    # Adding configuration for mparticle API
    configuration = mparticle.Configuration()
    configuration.api_key = sf_creds.mp_api_key
    configuration.api_secret = sf_creds.mp_api_secret
    configuration.debug = True
    configuration.host = 'https://s2s.eu1.mparticle.com/v2'
    configuration.connection_pool_size = 15

    # initiating API instance
    api_instance = mparticle.EventsApi(configuration)

    # calculating chunk
    num_chunks = int(len(df) / 100)

    chunks = np.array_split(df, num_chunks + 1)
    for idx, chunk in enumerate(chunks, 1):
        chunked_df = chunk.copy()
        chunked_df.reset_index(inplace=True, drop=True)
        bulk_batch = []
        print(f'processing chunk {idx}')
        for i in list(chunked_df.index):
            batch = mparticle.Batch()
            batch.environment = 'production'  # production or development
            identities = mparticle.UserIdentities()
            identities.customerid = chunked_df.loc[i]['CUSTOMER_ID']
            batch.user_identities = identities

            user_attributes = {seg: int(chunked_df.iloc[i][seg])}
            batch.user_attributes = user_attributes
            bulk_batch.append(batch)

        try:
            response = api_instance.bulk_upload_events(bulk_batch, callback=my_callback)
            timer = random.choice(sleep_times)
            time.sleep(timer)
            print(f'response : \n {response}')
            if idx == 1:
                print('retrying chunk 1 again \n')
                response = api_instance.bulk_upload_events(bulk_batch, callback=my_callback)
                time.sleep(timer)
        except mparticle.rest.ApiException as e:
            print(f'Exception while calling mParticle: %{e}\n')

    print('all IDs uploaded')


def upload_data_multiple_attributes(processed_file_name):
    df = pd.read_csv(processed_file_name)

    # Adding configuration for mparticle API
    configuration = mparticle.Configuration()
    configuration.api_key = sf_creds.mp_api_key
    configuration.api_secret = sf_creds.mp_api_secret
    configuration.debug = True
    configuration.host = 'https://s2s.eu1.mparticle.com/v2'
    configuration.connection_pool_size = 15

    # initiating API instance
    api_instance = mparticle.EventsApi(configuration)

    # calculating chunk
    num_chunks = int(len(df) / 100)

    chunks = np.array_split(df, num_chunks + 1)
    for idx, chunk in enumerate(chunks, 1):
        chunked_df = chunk.copy()
        chunked_df.reset_index(inplace=True, drop=True)
        bulk_batch = []
        print(f'processing chunk {idx}')
        for i in list(chunked_df.index):
            batch = mparticle.Batch()
            batch.environment = 'production'  # production or development
            identities = mparticle.UserIdentities()
            identities.customerid = chunked_df.loc[i]['CUSTOMER_ID']
            batch.user_identities = identities

            user_attributes = {}
            for col in list(set(chunked_df.columns.tolist()) - {'CUSTOMER_ID', 'DATE_ADDED'}):
                user_attributes[col] = int(chunked_df.iloc[i][col])
            batch.user_attributes = user_attributes
            bulk_batch.append(batch)

        try:
            response = api_instance.bulk_upload_events(bulk_batch, callback=my_callback)
            timer = random.choice(sleep_times)
            time.sleep(timer)
            print(f'response : \n {response}')
            if idx == 1:
                print('retrying chunk 1 again \n')
                response = api_instance.bulk_upload_events(bulk_batch, callback=my_callback)
                time.sleep(timer)
        except mparticle.rest.ApiException as e:
            print(f'Exception while calling mParticle: %{e}\n')

    print('all IDs uploaded')


def main():

    # Pick one delete other
    #upload_data_single_attribute('mparticle_demo-eventless.csv')
    # or
    upload_data_multiple_attributes('complete_df_2024-05-22.csv')


if __name__ == '__main__':
    main()
