import snowflake.connector
import pandas as pd
from datasetgenfunctions import group_categories, build_grouped_df, remove_outliers, scale_sparse_features, \
    reduce_df
import numpy as np
import sf_creds
from datetime import datetime, timedelta
from sklearn import preprocessing
import gc
from functools import reduce
import boto3
import aws_cred
from s3functions import get_data_from_s3, upload_to_s3, download_json, upload_json

pd.options.mode.chained_assignment = None

session = boto3.Session(
    aws_access_key_id=aws_cred.aws_access_key_id,
    aws_secret_access_key=aws_cred.aws_secret_access_key
)

client = boto3.client('s3')
bucket_name = 'data-project-meta'


def main():
    """
    NOTE: All parameters are read from DAX_ControlFile.json
    :param start_date (str) : Starting date of the dataset
    :param end_date (str) : End Date of the dataset
    :param segment (str) : DMP segment to build the datset for
    :return: Snowflake Table
    """

    # Load arguments from control file
    inputs = download_json(client, 'control_files/DAX_ControlFile.json',
                           bucket_name)

    end_date = inputs['start_date']
    start_date = (datetime.strptime(inputs["start_date"], '%Y-%m-%d') -
                  timedelta(weeks=4)).strftime('%Y-%m-%d')
    version = inputs['version']

    # Snowflake connector
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

    # Looping through all segments
    for segment in segment_to_run:

        column_template = download_json(client, f'mapping_files/{segment}/{segment}_template.json',
                                        bucket_name)
        weight_dict = inputs['segment'][segment]['weight_dict']

        tables_to_join = []

        # -----------------------------------------------------------------------------------------------------------------------
        # Total Duration table
        input_file_key = f'meta_stage_2_files/total_duration_{start_date}_{version}.csv'
        total_duration_df = get_data_from_s3(client, input_file_key, bucket_name)
        total_duration_df.set_index('GIGYA_ID', inplace=True, drop=True)

        total_duration_df['TOTAL_LISTEN_TIME'] = total_duration_df['TOTAL_LISTEN_TIME'].astype('float32')

        tables_to_join.append(total_duration_df.copy())
        del total_duration_df

        print('total duration table complete...')

        # -----------------------------------------------------------------------------------------------------------------------
        # Building Itunes Categories Dataset : Stage 1

        input_file_key = f'meta_stage_2_files/podcast_duration_{start_date}_{version}.csv'
        itunes_cat_raw = get_data_from_s3(client, input_file_key, bucket_name)
        itunes_cat_raw['LISTEN_DURATION'] = itunes_cat_raw['LISTEN_DURATION'].astype('float32')

        # Further Preprocessing
        itunes_cat_df = itunes_cat_raw.pivot_table(index='GIGYA_ID', columns='CATEGORY',
                                                   values='LISTEN_DURATION', aggfunc='sum')
        itunes_cat_df.columns.name = None
        itunes_cat_df.rename(columns={'': 'Unknown'}, inplace=True)
        if np.nan in itunes_cat_df.columns:
            itunes_cat_df.drop(columns=[np.nan], inplace=True)

        itunes_cat_df.fillna(0, inplace=True)

        # Category Group map
        cat_map = f'mapping_files/{segment}/podcast_category_map_{segment}.json'
        category_map = download_json(client, cat_map, bucket_name)

        # Assigning columns to the category groups
        col_names = list(itunes_cat_df.columns)
        complete_category_map = group_categories(category_map, col_names)

        # FINAL ITUNES_CATEGORY DATAFRAME
        grouped_itunes_cat_df = build_grouped_df(itunes_cat_df, complete_category_map)

        del itunes_cat_df

        # Storing features to normalize in the final dataset from this stage

        # checking if any new column outside the template was added
        stage1_columns = set(column_template['stage_1'])
        ic_columns = set(list(grouped_itunes_cat_df.columns))
        added_ic_columns = list(ic_columns - stage1_columns)

        print(f'\n {added_ic_columns} was added in stage 1 \n')

        tables_to_join.append(grouped_itunes_cat_df.copy())

        del grouped_itunes_cat_df

        print('podcast category table built...')

        # -----------------------------------------------------------------------------------------------------------------------
        # Building Playlist Dataset: stage 2

        print('Building playlist table....')

        playlist_map_file = f'mapping_files/{segment}/playlist_group_map_{segment}.json'
        playlist_dict = download_json(client, playlist_map_file, bucket_name)

        input_file_key = f'meta_stage_2_files/playlist_duration_{start_date}_{version}.csv'
        playlist_raw = get_data_from_s3(client, input_file_key, bucket_name)
        playlist_raw['TOTAL_DURATION'] = playlist_raw['TOTAL_DURATION'].astype('float32')
        playlist_df = playlist_raw.pivot_table(index='GIGYA_ID', columns='PLAYLIST',
                                               values='TOTAL_DURATION', aggfunc='sum')

        del playlist_raw

        playlist_df.columns.name = None

        if np.nan in playlist_df.columns:
            playlist_df.drop(columns=[np.nan], inplace=True)

        playlist_df.fillna(0, inplace=True)

        # Recording the playlists generated to compare in next run
        new_playlists = list(playlist_df.columns)
        print(f'\n playlists :\n {new_playlists} \n')
        previous_playlists = playlist_dict['prev_playlists']
        columns_added = list(set(new_playlists) - set(previous_playlists))
        print(f'\n{columns_added} are new playlists added to this dataset...\n')

        if inputs['segment'][segment]['is_main']:
            playlist_dict['prev_playlists'] = new_playlists

        upload_json(playlist_dict, client, playlist_map_file, bucket_name)

        playlist_map = playlist_dict['map']

        live_streams = playlist_dict['live']

        """
        # Adding channel level listen type to the table
        channel_playlist = []
        for i in list(playlist_df.columns):
            if '-M-' in str(i):
                channel_playlist.append(i.split('-M-', 1)[0])
        channel_playlist_set = list(set(channel_playlist))

        for channels in channel_playlist_set:
            playlist_df[channels + '_playlist'] = 0
            for playlists in previous_playlists:
                channel = playlists.split('-M-', 1)[0]
                if channel == channels:
                    try:
                        playlist_df[channels + '_playlist'] += playlist_df[playlists]
                    except:
                        continue
        """

        all_playlist_groups = list(playlist_map.keys())
        playlist_groups = list(set(all_playlist_groups) - set(live_streams))
        playlist_df_grouped = build_grouped_df(playlist_df, playlist_map)

        # adding Live and Playlist Listen Types to the dataframe

        playlist_df_grouped['Live'] = 0
        for columns in live_streams:
            try:
                playlist_df_grouped['Live'] += playlist_df_grouped[columns]
            except:
                continue

        playlist_df_grouped['Playlist'] = 0
        for columns in playlist_groups:
            try:
                playlist_df_grouped['Playlist'] += playlist_df_grouped[columns]
            except:
                continue

        if np.nan in playlist_df_grouped.columns:
            playlist_df_grouped.drop(columns=[np.nan], axis=1, inplace=True)

        stage2_columns = set(column_template['stage_2'])
        pl_columns = set(list(playlist_df_grouped.columns))
        added_columns = list(pl_columns - stage2_columns)

        for cls in added_columns:
            if '-M-' in cls:
                print(f'{cls} added to playlist')
                playlist_dict['map']['Specials'].append(cls)
                playlist_df_grouped['Specials'] += playlist_df_grouped[cls]
                playlist_df_grouped.drop(columns=[cls], inplace=True)

            else:
                if (cls.split('_')[0] in column_template['stage_4']) or \
                        (cls.split('_')[0].replace('News', '') in column_template['stage_4']):
                    print(f'col {cls}')
                    playlist_df_grouped[cls.split('_')[0].replace('News', '') + '_Live_Stream'] += playlist_df_grouped[
                        cls]
                    if '_Live_Stream' not in cls:
                        playlist_df_grouped.drop(columns=[cls], inplace=True)
                else:
                    print(f'\n {cls} COLUMN FOUND \n')

        upload_json(playlist_dict, client, playlist_map_file, bucket_name)

        tables_to_join.append(playlist_df_grouped.copy())

        del playlist_df_grouped

        print('playlist table built...')

        # -----------------------------------------------------------------------------------------------------------------------
        # Building channel listen type dataset (NOT IN USE)
        """
        print('building channel listen type table...')

        channel_listen_type_query = f'''with GID_TABLE AS (

    SELECT GIGYA_ID, LISTENER_ID, DURATION FROM  DS_DAX_AUDIO.CURATED.DAX_AUDIO_STREAMS where gigya_id not LIKE ''
            AND REQUEST_START_TIME BETWEEN '{start_date}' AND '{end_date}'
    ),

    AOD_TABLE AS (select listener_id, AOD_TYPE, channel_id from DS_AOD.CURATED.AOD_LISTEN_EVENTS
        where P_LISTEN_DATE BETWEEN '{start_date}' AND '{end_date}')

    SELECT GIGYA_ID, 
    CASE
    when NOT (NAME LIKE ANY('%Radio X%','%XTRA%','%70s%','%80s%','%00s%', '%DANCE%', '%Dance%')) THEN CONCAT_WS('_', regexp_Replace(NAME, ' .*', ''), AOD_TYPE)
    WHEN NAME LIKE '%Radio X%' THEN CONCAT_WS('_', 'Radio X', AOD_TYPE)
    WHEN NAME LIKE '%XTRA%' THEN CONCAT_WS('_', 'Capital XTRA', AOD_TYPE)
    else CONCAT_WS('_', NAME, AOD_TYPE)
    END AS CHANNEL_LISTEN_TYPE,

    SUM(DURATION) AS TOTAL_DURATION

    FROM

    GID_TABLE LEFT JOIN

    AOD_TABLE ON GID_TABLE.LISTENER_ID = AOD_TABLE.LISTENER_ID

    LEFT JOIN INTEGRATION.PROCESSED.DIM_DAX_MEDIA_MANAGER_CHANNELS C

    ON AOD_TABLE.CHANNEL_ID = C.ID


    GROUP BY GIGYA_ID, CHANNEL_LISTEN_TYPE'''

        chanel_listen_type_df = pd.read_sql(channel_listen_type_query, ctx)
        chanel_listen_type_df = chanel_listen_type_df.pivot(index='GIGYA_ID', columns='CHANNEL_LISTEN_TYPE',
                                                        values='TOTAL_DURATION')
        chanel_listen_type_df.columns.name = None
        chanel_listen_type_df.fillna(0, inplace=True)

        if np.nan in chanel_listen_type_df.columns:
            chanel_listen_type_df.drop(columns=[np.nan], inplace=True)

        # removing test channels
        columns_to_drop = []
        for columns in list(chanel_listen_type_df.columns):
            if 'Test' in columns:
                columns_to_drop.append(columns)
        chanel_listen_type_df.drop(columns=columns_to_drop, axis=1, inplace=True)

        stage3_columns = set(column_template['stage_3'])
        channel_type_columns = set(list(chanel_listen_type_df.columns))
        added_columns = list(channel_type_columns - stage3_columns)

        for cls in added_columns:
            print(f'\n {cls} LISTEN TYPE found')
            chanel_listen_type_df['Global_podcast'] += chanel_listen_type_df[cls]
            chanel_listen_type_df.drop(columns=[cls], axis=1, inplace=True)

        tables_to_join.append(chanel_listen_type_df.copy())

        del chanel_listen_type_df

        print('chanel listen type table built...')
        """

        # -----------------------------------------------------------------------------------------------------------------------
        # Building Listen Type Dataset

        print('building listen type table....')

        input_file_key = f'meta_stage_2_files/listentype_duration_{start_date}_{version}.csv'
        listen_type_df = get_data_from_s3(client, input_file_key, bucket_name)
        listen_type_df['TOTAL_LISTEN_TIME'] = listen_type_df['TOTAL_LISTEN_TIME'].astype('float32')
        listen_type_df = listen_type_df.pivot_table(index='GIGYA_ID', columns='AOD_TYPE',
                                                    values='TOTAL_LISTEN_TIME', aggfunc='sum')
        listen_type_df.columns.name = None
        listen_type_df.fillna(0, inplace=True)

        if np.nan in listen_type_df.columns:
            listen_type_df.drop(columns=[np.nan], inplace=True)

        tables_to_join.append(listen_type_df.copy())

        del listen_type_df

        print('listen type table built....')

        # -----------------------------------------------------------------------------------------------------------------------
        # Channel Table (NOT IN USE)
        """
        print('building channel table....')

        channel_query = f'''SELECT GIGYA_ID, CHANNEL, sum(DURATION) as LISTEN_TIME from 

    (select gigya_id, DURATION, stream_name from 
    DS_DAX_AUDIO.CURATED.DAX_AUDIO_STREAMS
    where gigya_id not LIKE '' AND REQUEST_START_TIME BETWEEN '{start_date}' AND '{end_date}') a 

    left join

    RD_RM.SANDBOX.SEGMENTATION_AUDIO_STREAM_MAPPING_TABLE b

    on a.STREAM_NAME = b.STREAM_NAME
    group by GIGYA_ID, CHANNEL'''

        channel_df = pd.read_sql(channel_query, ctx)
        channel_df = channel_df.pivot(index='GIGYA_ID', columns='CHANNEL', values='LISTEN_TIME')
        channel_df.columns.name = None
        channel_df.fillna(0, inplace=True)

        if np.nan in channel_df.columns:
            channel_df.drop(columns=[np.nan], inplace=True)

        tables_to_join.append(channel_df.copy())

        del channel_df

        print('channel table built....')
        """
        # -----------------------------------------------------------------------------------------------------------------------
        # Platform Table

        print('building platform table....')

        input_file_key = f'meta_stage_2_files/platform_duration_{start_date}_{version}.csv'
        platform_df = get_data_from_s3(client, input_file_key, bucket_name)
        platform_df['PLAYER_COUNT'] = platform_df['PLAYER_COUNT'].astype('float32')
        platform_df = platform_df.pivot_table(index='GIGYA_ID', columns='DAX_PLATFORM', values='PLAYER_COUNT',
                                              aggfunc='sum')

        if np.nan in platform_df.columns:
            platform_df.drop(columns=[np.nan], inplace=True)

        platform_df.columns.name = None
        platform_df.fillna(0, inplace=True)

        stage7_columns = set(column_template['stage_5'])
        platform_columns = set(list(platform_df.columns))
        added_columns = list(platform_columns - stage7_columns)

        for cls in added_columns:
            print(f'\n {cls} PLATFORM found')
            platform_df['Web'] += platform_df[cls]
            platform_df.drop(columns=[cls], axis=1, inplace=True)

        tables_to_join.append(platform_df.copy())

        del platform_df

        print('platform table built....')

        # -----------------------------------------------------------------------------------------------------------------------
        # Daypart Table

        print('building daypart table....')

        dp_map = f'mapping_files/{segment}/daypart_{segment}.json'
        daypart_map = download_json(client, dp_map, bucket_name)

        daypart_init_query = '''SELECT GIGYA_ID, KEY_, SUM(DURATION) AS LISTEN_NUMBER_TIME FROM
    (SELECT GIGYA_ID, DURATION,
    DAYNAME(REQUEST_START_TIME) as DOW, DATE_PART(HOUR, REQUEST_START_TIME) as HOUR,
    case '''

        for dayparts in list(daypart_map.keys()):
            daypart_init_query += f''' when HOUR between {daypart_map[dayparts][0]} AND {daypart_map[dayparts][1]} THEN '{dayparts}' \n'''

        daypart_final_query = f''' end as DAYPART,
    CASE
    WHEN DOW LIKE '%Sat%' or DOW LIKE '%Sun%' THEN 'WEEKEND'
    ELSE 'WEEKDAY'
    end as WEEKPART,
    CONCAT_WS('_',DAYPART, WEEKPART) AS KEY_

    FROM DS_DAX_AUDIO.CURATED.DAX_AUDIO_STREAMS
    WHERE REQUEST_START_TIME BETWEEN '{start_date}' AND '{end_date}'
    AND GIGYA_ID NOT LIKE ''
    AND GIGYA_ID IS NOT NULL)
    GROUP BY GIGYA_ID, KEY_ '''

        daypart_query = daypart_init_query + daypart_final_query

        daypart_df = pd.read_sql(daypart_query, ctx)
        daypart_df['LISTEN_NUMBER_TIME'] = daypart_df['LISTEN_NUMBER_TIME'].astype('float32')
        daypart_df = daypart_df.pivot_table(index='GIGYA_ID', columns='KEY_',
                                            values='LISTEN_NUMBER_TIME', aggfunc='sum')
        daypart_df.columns.name = None
        daypart_df.fillna(0, inplace=True)

        tables_to_join.append(daypart_df.copy())

        del daypart_df

        print('daypart table built....')

        # -----------------------------------------------------------------------------------------------------------------------
        # Age Table

        print('building age table....')

        input_file_key = f'meta_stage_2_files/age_duration_{start_date}_{version}.csv'
        age_df = get_data_from_s3(client, input_file_key, bucket_name)
        age_pivot = age_df.pivot_table(index='GIGYA_ID', columns='AGE_BAND', values='AGE_BOOL',
                                       aggfunc='first')
        age_pivot.columns.name = None
        if 'u' in age_pivot.columns:
            age_pivot.drop(columns=['u'], inplace=True)
        age_pivot.fillna(0, inplace=True)

        #-----------------------------------------------------------------------------------------------------
        # Gender Table

        print('building gender table')

        input_file_key = f'meta_stage_2_files/gender_duration_{start_date}_{version}.csv'
        gender_df = get_data_from_s3(client, input_file_key, bucket_name)
        gender_pivot = gender_df.pivot_table(index='GIGYA_ID', columns='GENDER', values='GENDER_BOOL',
                                             aggfunc='first')
        gender_pivot.columns.name = None

        if 'u' in gender_pivot.columns:
            gender_pivot.drop(columns=['u'], inplace=True)

        gender_pivot.fillna(0, inplace=True)

        #-----------------------------------------------------------------------------------------------------

        print('joining all tables....')
        # Substituting pd.concat with merge
        #all_df = pd.concat(tables_to_join, axis=1)
        all_df = reduce(lambda left, right: pd.merge(left, right,
                                                     left_index=True,
                                                     right_index=True, how='left'), tables_to_join)

        complete_df_age = all_df.join(age_pivot, how='left')
        complete_df_age.fillna(0, inplace=True)

        complete_df = complete_df_age.join(gender_pivot, how='left')
        complete_df.fillna(0, inplace=True)

        print(f'size of dataset {len(complete_df)}')

        if 'newsflash' in complete_df.columns:
            complete_df['Live'] = complete_df['Live'] + complete_df['newsflash']
            complete_df.drop(columns=['newsflash'], axis=1, inplace=True)

        df_outlier_removed = remove_outliers(complete_df)
        df_outlier_removed.drop(['TOTAL_LISTEN_TIME'], axis=1, inplace=True)

        df_outlier_removed.fillna(0, inplace=True)

        print(f'size of dataset with outlier removed: {len(df_outlier_removed)}')

        feat_group = list(weight_dict.keys())
        df_outlier_removed = scale_sparse_features(feat_group, df_outlier_removed)

        minmax_scale = preprocessing.MinMaxScaler(feature_range=(0, 100))
        norm_df = minmax_scale.fit_transform(df_outlier_removed)
        norm_df = pd.DataFrame(norm_df, columns=df_outlier_removed.columns)

        norm_df.index = df_outlier_removed.index

        podcast_index = [i for i in feat_group if 'Group' in i]
        print(f'podcast index : {podcast_index}')

        #--------uncomment after test---------------------------------------------------------------------------------

        playlist_index = [i for i in feat_group if 'playlist' in i]
        print(f'playlist index : {playlist_index}')

        if norm_df[podcast_index[0]].sum() == 0:
            inputs['segment'].pop(segment, None)
            segment_to_run = list(set(segment_to_run) - {segment})
            inputs['segment_to_run'] = segment_to_run
            print(f'podcast signal for {segment} not found')
            upload_json(inputs, client, 'control_files/DAX_ControlFile.json',
                        bucket_name)
            continue
        else:
            norm_df[podcast_index[0]] = norm_df[podcast_index[0]]

        if len(set(playlist_index).intersection(set(norm_df.columns.to_list()))) != 0:
            if norm_df[playlist_index[0]].sum() == 0:
                inputs['segment'][segment]['weight_dict'].pop(playlist_index[0], None)
                print(f'playlist signal for {segment} not found')
                upload_json(inputs, client, 'control_files/DAX_ControlFile.json',
                            bucket_name)
            else:
                norm_df[playlist_index[0]] = norm_df[playlist_index[0]]

#--------------------------------------------------------------------------------------------------------------


        # norm_df[podcast_index[0]] = norm_df[podcast_index[0]]*10
        column = norm_df.columns.to_list()

        if '""""""' in column and 'U' in column:
            norm_df.drop(columns=['""""""', 'U'], inplace=True)

        weak_df = norm_df[list(set(norm_df.columns.tolist()) - set(feat_group))]
        strong_df = norm_df[list(set(feat_group).intersection(set(norm_df.columns.to_list())))]

        reduced_df = reduce_df(weak_df)

        transformed_df = pd.concat([reduced_df, strong_df], axis=1)
        transformed_df.fillna(0, inplace=True)

        output_file_key = f'meta_stage_3_files/DS_{start_date}_{segment}_INPUT_{version}.CSV'
        status = upload_to_s3(transformed_df, output_file_key, client, bucket_name)

        print(status)

        analytics_file_key = f'meta_analytics_files/{segment}_{start_date}.CSV'
        status = upload_to_s3(norm_df, analytics_file_key, client, bucket_name)

        del tables_to_join

        for name in list(globals().keys()):
            if not name.startswith('_'):  # Don't delete system variables.
                value = globals()[name]
                if isinstance(value, pd.DataFrame):  # Only delete DataFrame objects.
                    del globals()[name]

        gc.collect()

    print('dataset building complete....')


if __name__ == "__main__":
    main()
