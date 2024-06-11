import DAXBuildDataset as build_ds
import DAXBuildMasterTables as build_master_table
import pandas as pd
import gc
from datetime import datetime, timedelta
import time
import boto3
import aws_cred
from s3functions import download_json, upload_json

pd.options.mode.chained_assignment = None

# Initialising s3 client

session = boto3.Session(
    aws_access_key_id=aws_cred.aws_access_key_id,
    aws_secret_access_key=aws_cred.aws_secret_access_key
)

client = session.client('s3')
bucket_name = 'data-project-meta'


def main():
    t0 = time.time()

    input_file = download_json(client, 'control_files/DAX_ControlFile.json',
                               bucket_name)

    live_segments = [dmp for dmp in input_file['segment'] if input_file['segment'][dmp]['is_main']]
    print(live_segments)

    template = download_json(client, 'control_files/DAX_ControlFile_template.json',
                             bucket_name)

    updated_input_file = input_file

    updated_input_file['start_date'] = (datetime.strptime(input_file["start_date"], '%Y-%m-%d') +
                                        timedelta(weeks=1)).strftime('%Y-%m-%d')

    upload_json(input_file, client, 'control_files/DAX_ControlFile.json',
                bucket_name)
    print(updated_input_file['start_date'], updated_input_file['segment_to_run'])

    print('Building master table')

    build_master_table.main()

    if len(live_segments) > 0:

        # Updating for live segments
        updated_input_file['segment_to_run'] = live_segments

        for segment in live_segments:
            updated_input_file['segment'][segment] = template['segment'][segment]

        upload_json(input_file, client, 'control_files/DAX_ControlFile.json',
                    bucket_name)
        print(updated_input_file['start_date'], updated_input_file['segment_to_run'])

        build_ds.main()
        t1 = time.time()
        print(f'Dataset generated in {(t1 - t0) / 60} minutes')

    for name in list(globals().keys()):
        if not name.startswith('_'):  # Don't delete system variables.
            value = globals()[name]
            if isinstance(value, pd.DataFrame):  # Only delete DataFrame objects.
                del globals()[name]

    gc.collect()


if __name__ == '__main__':
    main()
