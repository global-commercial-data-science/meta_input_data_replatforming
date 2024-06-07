import snowflake.connector
import pandas as pd
from cdswritereadfunctions import load_input_file as rjf, write_json_to_file as wjf
import sf_creds
from datetime import datetime, timedelta
import gc
import boto3
from io import StringIO
pd.options.mode.chained_assignment = None


# Initialising s3 client
client = boto3.client('s3')
bucket_name = 'data-project-meta'


def upload_to_s3(csv_df, file_key, s3_client):

    csv_buffer = StringIO()
    csv_df.to_csv(csv_buffer, index=False)

    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
        status = f"DataFrame uploaded to {bucket_name}/{file_key} successfully."
    except Exception as e:
        status = "Error uploading DataFrame to S3: {e}"

    return status



def main():

    # Load arguments from control file
    inputs = rjf('DAX_ControlFile.json')

    end_date = inputs['start_date']
    start_date = (datetime.strptime(inputs["start_date"], '%Y-%m-%d') -
                timedelta(weeks=4)).strftime('%Y-%m-%d')
    version = inputs['version']

    # Snowflake connector

    ctx = snowflake.connector.connect(
        user=sf_creds.user,
        password=sf_creds.password,
        account=sf_creds.account,
        role=sf_creds.role,
        warehouse=sf_creds.warehouse,
        database=sf_creds.database,
        schema=sf_creds.schema
    )

    # Total duration table

    print('Building total duration Master table...')

    consented_gid_query = f'''SELECT a.GIGYA_ID, SUM(b.DURATION) AS TOTAL_LISTEN_TIME FROM RD_RM.SANDBOX.GIGYA_ID_CONSENT_MAP a
            left join DS_DAX_AUDIO.CURATED.DAX_AUDIO_STREAMS b on a.GIGYA_ID = b.GIGYA_ID WHERE 
            b.REQUEST_START_TIME BETWEEN '{start_date}' AND '{end_date}'
            and a.CONSENT_STATUS like '%consent_given%' GROUP BY a.GIGYA_ID
            '''

    total_duration_df = pd.read_sql(consented_gid_query, ctx)

    # Uploading to S3
    file_id = f'meta_stage_2_files/total_duration_{start_date}_{version}.csv'

    status = upload_to_s3(total_duration_df, file_id, client)

    print(status)

    del total_duration_df



#-----------------------------------------------------------------------------------------------------------------------
    # Itunes Category Table

    print('Building podcast category Master table...')

    # SQL query to get Itunes Categories of podcasts

    IC_query = f'''with category_table as (
            with b as (select listener_id, episode_id from 
            DS_AOD.CURATED.AOD_LISTEN_EVENTS where P_LISTEN_DATE BETWEEN '{start_date}' AND '{end_date}')

            select listener_id, category, count(ifnull(category,'')) as listen_freq from b

          left join 

          RD_RM.SANDBOX.PODCAST_CATEGORY_EXPANDED_MAP c

          on b.episode_id = c.episodeid
          group by listener_id, category),

        gid_table as (select gigya_id, listener_id
            FROM DS_DAX_AUDIO.CURATED.DAX_AUDIO_STREAMS
            where gigya_id not LIKE ''
            AND REQUEST_START_TIME BETWEEN '{start_date}' AND '{end_date}')

        SELECT GIGYA_ID, CATEGORY, SUM(listen_freq) AS LISTEN_DURATION FROM

        GID_TABLE LEFT JOIN 

        category_table ON GID_TABLE.LISTENER_ID = CATEGORY_TABLE.LISTENER_ID
        GROUP BY GIGYA_ID, CATEGORY

        '''

    itunes_cat_raw = pd.read_sql(IC_query, ctx)
    itunes_cat_raw['LISTEN_DURATION'] = itunes_cat_raw['LISTEN_DURATION'].astype('float32')

    # Uploading to S3
    file_id = f'meta_stage_2_files/podcast_duration_{start_date}_{version}.csv'

    status = upload_to_s3(itunes_cat_raw, file_id, client)

    print(status)

    del itunes_cat_raw



#-----------------------------------------------------------------------------------------------------------------------

    # Playlist

    print('Building playlist category Master table...')

    cs = ctx.cursor()

    init_query = f'''

        create or replace table rd_rm.sandbox.temp_gid_table as

    (select GIGYA_ID, stream_id, stream_name, duration,
      TO_TIMESTAMP(REQUEST_START_TIME) as request_start_time,
      TO_TIMESTAMP(REQUEST_END_TIME) as actual_end_time FROM DS_DAX_AUDIO.CURATED.DAX_AUDIO_STREAMS
      where REQUEST_START_TIME BETWEEN '{start_date}' and '{end_date}' AND GIGYA_ID IS NOT NULL)

    '''

    cs.execute(init_query)

    playlist_query = f'''
                        select GIGYA_ID, CASE 
    WHEN TAG LIKE 0 THEN PLAYLISTS
    ELSE  CONCAT_WS('_', SPLIT_PART(PLAYLISTS, '_', 1), SHOW_LONG_NAME) END AS PLAYLIST,

    SUM(TOTAL_DURATION) AS TOTAL_DURATION FROM

    (with schedule_table as 

            (

            SELECT DISTINCT SHOW_ID, FQSN,

              CASE WHEN not (SPLIT_PART(FQSN,':', 1) LIKE 'Communicorp') and SPLIT_PART(FQSN,':', 3) LIKE ANY('00s', '90s', '70s', '80s', 'Country','Dance','Chill','')  THEN
            CONCAT(REPLACE(SPLIT_PART(FQSN,':', 2),'_',''), SPLIT_PART(FQSN,':', 3), '_Live')
            WHEN not (SPLIT_PART(FQSN,':', 1) LIKE 'Communicorp') AND NOT (SPLIT_PART(FQSN,':', 3) LIKE ANY('00s', '90s', '70s', '80s', 'Country','Dance','Chill','', 'UK', 'National', 'LBC', 'News', 'Reloaded')) THEN
            CONCAT(REPLACE(SPLIT_PART(FQSN,':', 2),'_',''), '_', SPLIT_PART(FQSN, ':', 3))
            WHEN not (SPLIT_PART(FQSN,':', 1) LIKE 'Communicorp') AND SPLIT_PART(FQSN,':', 3) LIKE ANY('UK', 'National', '') THEN
            CONCAT(REPLACE(SPLIT_PART(FQSN,':', 2),'_',''), '_Live')
            WHEN SPLIT_PART(FQSN,':', 1) LIKE 'Communicorp' and SPLIT_PART(FQSN,':', 2) like '%Heart%' then 'Heart_regional'
            WHEN SPLIT_PART(FQSN,':', 1) LIKE 'Communicorp' and SPLIT_PART(FQSN,':', 2) like '%Radio_X%' then 'RadioX_Live'
            WHEN SPLIT_PART(FQSN, ':', 2) LIKE 'LBC' AND SPLIT_PART(FQSN, ':', 3) Like 'News' then 'LBCNews'
            WHEN SPLIT_PART(FQSN, ':', 2) LIKE 'LBC' AND SPLIT_PART(FQSN, ':', 3) Like ANY('UK','LBC') then 'LBC'
            WHEN SPLIT_PART(FQSN, ':', 3) LIKE 'Reloaded' then CONCAT(REPLACE(SPLIT_PART(FQSN,':', 2),'_',''), 'Reloaded')
            ELSE CONCAT(SPLIT_PART(FQSN,':', 2), '_', SPLIT_PART(FQSN, ':', 3))
            END AS BRAND,


              TO_TIMESTAMP(SHOW_START_SCHEDULED) AS START_TIME,
              TO_TIMESTAMP(SHOW_END_SCHEDULED) AS END_TIME,
              SHOW_LONG_NAME FROM
            INTEGRATION.PROCESSED.EPG_SCHEDULE_EVENTS WHERE SHOW_START_SCHEDULED BETWEEN '{start_date}' and '{end_date}'

            ),

    gid_table as

    (
        /** DISTINCT REMOVED FROM HERE **/
      select A.GIGYA_ID, 
      CASE WHEN B.PLAYLIST_PRUNED LIKE '%Live%' then PLAYLIST_PRUNED
      WHEN B.PLAYLIST_PRUNED LIKE '%regional%' then CONCAT(B.CHANNEL, '_', B.PLAYLIST)
      ELSE PLAYLIST_PRUNED END AS BRAND_NAME,

      B.PLAYLIST_PRUNED as PLAYLISTS, request_start_time, request_end_time, Sum(A.CALCULATED_DURATION) as TOTAL_DURATION FROM


      (SELECT GIGYA_ID, stream_id, STREAM_NAME, DURATION, START_INTERVAL AS REQUEST_START_TIME,POTENTIAL_END_INTERVAL,

    CASE WHEN (POTENTIAL_END_INTERVAL < actual_end_time) THEN POTENTIAL_END_INTERVAL
    ELSE actual_end_time END AS REQUEST_END_TIME,

    CASE WHEN TIMEDIFF(SECOND, REQUEST_START_TIME::TIME, REQUEST_END_TIME::TIME) < 0 THEN TIMEDIFF(SECOND, REQUEST_START_TIME::TIME, REQUEST_END_TIME::TIME) + 86400
       ELSE TIMEDIFF(SECOND, REQUEST_START_TIME::TIME, REQUEST_END_TIME::TIME)
    END as CALCULATED_DURATION

    FROM

    (with 

    numbers AS (
        SELECT ROW_NUMBER() OVER(ORDER BY SEQ4()) - 1 AS n
        FROM TABLE(GENERATOR(ROWCOUNT => 500))
    )

    , intervals AS (
        SELECT 
            t.GIGYA_ID, t.stream_id, t.stream_name, t.duration,
            DATEADD(SECOND, 600 * n, t.request_start_time) AS start_interval,
            DATEADD(SECOND, 600 * (n + 1), t.request_start_time) AS potential_end_interval,
            t.actual_end_time
        FROM rd_rm.sandbox.temp_gid_table t
      JOIN numbers num
        ON 600 * num.n < (t.duration + 600)
    )

    SELECT 
        p.GIGYA_ID, p.stream_id, p.stream_name, p.duration, i.start_interval,p.actual_end_time, i.potential_end_interval
    FROM rd_rm.sandbox.temp_gid_table p INNER join intervals i on p.stream_id = i.stream_id 
    and DATEADD(SECOND, 600, p.actual_end_time) > i.potential_end_interval
    ORDER BY stream_name, start_interval) order by stream_id, REQUEST_START_TIME) A LEFT JOIN 

      RD_RM.SANDBOX.SEGMENTATION_AUDIO_STREAM_MAPPING_TABLE B on A.STREAM_NAME = B.STREAM_NAME
                group by A.GIGYA_ID, A.STREAM_NAME, B.PLAYLIST_PRUNED, B.PLAYLIST, B.CHANNEL, request_start_time, request_end_time


    )

    SELECT GIGYA_ID, PLAYLISTS, TOTAL_DURATION, BRAND, SHOW_LONG_NAME,
    CASE WHEN SHOW_LONG_NAME IS NULL THEN 0 ELSE 1 END AS TAG FROM
    GID_TABLE LEFT JOIN SCHEDULE_TABLE ON GID_TABLE.BRAND_NAME = schedule_table.BRAND AND REQUEST_START_TIME >= START_TIME
    AND REQUEST_START_TIME <= END_TIME and 
    LOWER(DAYNAME(REQUEST_START_TIME)) = LOWER(DAYNAME(START_TIME))) GROUP BY GIGYA_ID, PLAYLIST

        '''

    playlist_raw = pd.read_sql(playlist_query, ctx)
    playlist_raw['TOTAL_DURATION'] = playlist_raw['TOTAL_DURATION'].astype('float32')

    # Uploading to S3
    file_id = f'meta_stage_2_files/playlist_duration_{start_date}_{version}.csv'

    status = upload_to_s3(playlist_raw, file_id, client)

    print(status)

    del playlist_raw

    drop_query = '''drop table if exists rd_rm.sandbox.temp_gid_table'''

    cs.execute(drop_query)

#-----------------------------------------------------------------------------------------------------------------------

    # Building Listen Type Dataset

    print('building listen type table....')
    listen_type_query = f'''  with AOD_TABLE as 
            (

                select gigya_id, AOD_TYPE, LISTEN_FREQ from
                (select gigya_id, listener_id from 
                DS_DAX_AUDIO.CURATED.DAX_AUDIO_STREAMS where gigya_id not LIKE ''
                AND REQUEST_START_TIME BETWEEN '{start_date}' AND '{end_date}') a

                left join 

                (select listener_id, AOD_TYPE, COUNT(AOD_TYPE) as LISTEN_FREQ from DS_AOD.CURATED.AOD_LISTEN_EVENTS
                where P_LISTEN_DATE BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY listener_id, AOD_TYPE) b

                on a.listener_id = b.listener_id

                )

                select gigya_id, AOD_TYPE, sum(LISTEN_FREQ) as total_listen_time from aod_table
                group by gigya_id, aod_type

            '''

    listen_type_df = pd.read_sql(listen_type_query, ctx)
    listen_type_df.to_csv(f'Input_Data_Files/listentype_duration_{start_date}_{version}.csv', index=False)
    del listen_type_df

#-----------------------------------------------------------------------------------------------------------------------
    # Platform Table

    print('building platform table....')

    platform_query = f'''SELECT GIGYA_ID, DAX_PLATFORM, 
        SUM(DURATION) AS PLAYER_COUNT
        FROM DS_DAX_AUDIO.CURATED.DAX_AUDIO_STREAMS
        where gigya_id not LIKE ''
        AND REQUEST_START_TIME BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY GIGYA_ID, DAX_PLATFORM'''

    platform_df = pd.read_sql(platform_query, ctx)

    # Uploading to S3
    file_id = f'meta_stage_2_files/platform_duration_{start_date}_{version}.csv'

    status = upload_to_s3(platform_df, file_id, client)

    print(status)

    del platform_df

# -----------------------------------------------------------------------------------------------------------------------
    # Daypart Table

    # This part is executed within DAXBuildDataset.py script as daypart requires segment specific mapping

# -----------------------------------------------------------------------------------------------------------------------
    # Age Table

    print('building age table....')

    age_query = f"""

            SELECT distinct PERS_ID AS GIGYA_ID,
            case when PERS_AGE >= 25 and PERS_AGE <= 34 THEN '25-34'
            when PERS_AGE >= 35 and PERS_AGE <= 44 THEN '35-44'
            when PERS_AGE >= 45 and PERS_AGE <= 54 THEN '45-54'
            when PERS_AGE >= 15 and PERS_AGE <= 19 then '15-19'
            when PERS_AGE >= 20 and PERS_AGE <= 24 then '20-24'
            when PERS_AGE >= 55 and PERS_AGE <= 59 THEN '55-59'
            when PERS_AGE > 59 THEN '60+'
            else 'u' END AS AGE_BAND,
            1 AS AGE_BOOL FROM RD_AUDIENCEONE.ANALYTICS.V_AUDIO_STREAMS_DEMOGRAPHICS
            WHERE REQUEST_START_TIME BETWEEN '{start_date}' AND '{end_date}'

            """
    age_df = pd.read_sql(age_query, ctx)

    # Uploading to S3
    file_id = f'meta_stage_2_files/age_duration_{start_date}_{version}.csv'

    status = upload_to_s3(age_df, file_id, client)

    print(status)

    del age_df


#-----------------------------------------------------------------------------------------------------

    # Gender Table

    print('building gender table')

    gender_query = f'''
            select DISTINCT PERS_ID as GIGYA_ID,
            NVL(PERS_GENDER, 'u') as GENDER,
            1 AS GENDER_BOOL
            FROM RD_AUDIENCEONE.ANALYTICS.V_AUDIO_STREAMS_DEMOGRAPHICS
            WHERE REQUEST_START_TIME BETWEEN '{start_date}' AND '{end_date}'
        '''

    gender_df = pd.read_sql(gender_query, ctx)

    # Uploading to S3
    file_id = f'meta_stage_2_files/ender_duration_{start_date}_{version}.csv'

    status = upload_to_s3(gender_df, file_id, client)

    print(status)

    del gender_df

    for name in list(globals().keys()):
        if not name.startswith('_'):  # Don't delete system variables.
            value = globals()[name]
            if isinstance(value, pd.DataFrame):  # Only delete DataFrame objects.
                del globals()[name]

    gc.collect()


if __name__ == "__main__":
    main()







