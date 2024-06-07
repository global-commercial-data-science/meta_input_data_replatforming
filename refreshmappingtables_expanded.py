import snowflake.connector
import sf_creds
from cdswritereadfunctions import load_input_file as rjf
from datetime import datetime, timedelta


def main():
    input_file = rjf('DAX_ControlFile.json')

    start_date = (datetime.strptime(input_file["start_date"], '%Y-%m-%d') -
                  timedelta(weeks=12)).strftime('%Y-%m-%d')

    ctx = snowflake.connector.connect(
        user=sf_creds.user,
        password=sf_creds.password,
        account=sf_creds.account,
        role=sf_creds.role,
        warehouse=sf_creds.warehouse,
        database=sf_creds.database,
        schema=sf_creds.schema
    )

    podcast_refresh_query = '''
        
                              create or replace table RD_RM.SANDBOX.PODCAST_CATEGORY_EXPANDED_MAP as 
                (
                
                select distinct episodeid, showid, category from
                
                (SELECT DISTINCT EPISODEID, SHOWID, COMBINED_CATS AS CATEGORY FROM
                
                        (select EPISODEID, SHOWID, 
                       array_to_string(ALL_CATS, '-') AS PARENT_CATEGORY, array_to_string(ALL_CHS, '-') AS CHILD_CATEGORY,
                       array_to_string(COMBINED_CATEGORIES, '-')AS COMBINED_CATS
                        from
                        (SELECT DISTINCT EPISODEID, SHOWID, ARRAY_AGG(DISTINCT ITUNESCAT1_PARENT) WITHIN GROUP (ORDER BY ITUNESCAT1_PARENT DESC) AS CAT1,
                        ARRAY_AGG(DISTINCT ITUNESCAT2_PARENT) WITHIN GROUP (ORDER BY ITUNESCAT2_PARENT DESC) AS CAT2,
                        ARRAY_AGG(DISTINCT ITUNESCAT3_PARENT) WITHIN GROUP (ORDER BY ITUNESCAT3_PARENT DESC) AS CAT3, 
                        RD_RM.SANDBOX.ARRAY_DCAT(CAT1, CAT2) AS CAT_1_2,
                        RD_RM.SANDBOX.ARRAY_DCAT(CAT_1_2, CAT3) AS ALL_CATS,
                        ARRAY_AGG(DISTINCT ITUNESCAT1_CHILD) WITHIN GROUP (ORDER BY ITUNESCAT1_CHILD DESC) AS CH1,
                        ARRAY_AGG(DISTINCT ITUNESCAT2_CHILD) WITHIN GROUP (ORDER BY ITUNESCAT2_CHILD DESC) AS CH2,
                        ARRAY_AGG(DISTINCT ITUNESCAT3_CHILD) WITHIN GROUP (ORDER BY ITUNESCAT3_CHILD DESC) AS CH3, 
                        RD_RM.SANDBOX.ARRAY_DCAT(CH1, CH2) AS CH_1_2,
                        RD_RM.SANDBOX.ARRAY_DCAT(CH_1_2, CH3) AS ALL_CHS,
                        RD_RM.SANDBOX.ARRAY_DCAT(ALL_CATS, ALL_CHS) AS COMBINED_CATEGORIES 
                
                        FROM DS_IOW_EVENTS.CURATED.VW_ORACLE_PODCAST_SHOW_EPISODES GROUP BY EPISODEID, SHOWID)
                
                        group by EPISODEID, SHOWID, CHILD_CATEGORY, PARENT_CATEGORY, COMBINED_CATEGORIES)
                UNION (
                
                with all_pd_table as (
                
                  select * from 
                (WITH SHOW_TABLE AS (
                
                WITH EPISODE_TABLE AS (
                
                  SELECT SHOW_ID, MM_V2_ID AS EPISODE_ID, EPISODE_TITLE FROM  INTEGRATION.PROCESSED.AOD_EPISODE_DIM WHERE MM_V2_ID NOT LIKE ''
                  UNION
                  SELECT SHOW_ID, TO_VARCHAR(SPREAKER_ID::INTEGER) AS EPISODE_ID, EPISODE_TITLE FROM  INTEGRATION.PROCESSED.AOD_EPISODE_DIM WHERE SPREAKER_ID NOT LIKE ''
                  UNION
                  SELECT SHOW_ID, ART19_ID AS EPISODE_ID, EPISODE_TITLE FROM  INTEGRATION.PROCESSED.AOD_EPISODE_DIM WHERE ART19_ID NOT LIKE ''
                  UNION
                  SELECT SHOW_ID, BLOGTALK_ID AS EPISODE_ID, EPISODE_TITLE FROM  INTEGRATION.PROCESSED.AOD_EPISODE_DIM WHERE BLOGTALK_ID NOT LIKE ''
                  UNION
                  SELECT SHOW_ID, CAPTIVATE_ID AS EPISODE_ID, EPISODE_TITLE FROM  INTEGRATION.PROCESSED.AOD_EPISODE_DIM WHERE CAPTIVATE_ID NOT LIKE ''
                )
                
                SELECT A.SHOW_ID, EPISODE_ID, EPISODE_TITLE, SHOW_TITLE FROM EPISODE_TABLE A
                LEFT JOIN INTEGRATION.PROCESSED.AOD_SHOW_DIM B ON A.SHOW_ID = B.SHOW_ID
                )
                
                SELECT DISTINCT EPISODE_ID, EPISODE_TITLE, SHOW_ID, SHOW_TITLE, ITUNESCAT1_CHILD, ITUNESCAT2_PARENT, ITUNESCAT2_CHILD,
                ITUNESCAT3_PARENT, ITUNESCAT3_CHILD,
                 case
                WHEN LOWER(SHOW_TITLE) LIKE ANY('%auto%', '%motor%', '%cars%', '%car%', '%drive%') then 'Auto'
                when LOWER(SHOW_TITLE) like any('%beach%', '%journey%', '%travel%', '%holiday%') then 'Travel'
                WHEN LOWER(SHOW_TITLE) LIKE ANY('%it pro%', '%charged%', '%digital%', '%geek%', '%tech%', 
                '%space%', '%future%', '%smart%', '%cyber%') then 'Tech'
                WHEN LOWER(SHOW_TITLE) LIKE ANY('%business%', '%holiday%', '%auto%', '%shop%', '%credit%', 
                '%profession%', '%money%', '%finance%') then 'Credit'
                WHEN LOWER(SHOW_TITLE) LIKE ANY('%nutrition%', '%run%', '%mental%', 
                '%feel %', '%mind%', '%huberman%', '%head%', '%dr.%') then 'Health'
                WHEN LOWER(SHOW_TITLE) LIKE ANY('%garden%', '%grow%', '%beautiful mess%', '%home%') then 'Home'
                WHEN LOWER(SHOW_TITLE) LIKE ANY('%cricket%', '%football%', '%play%', '%sidelines%', 
                '%champion%', '%huddle%', '%ashes%') then 'Sports'
                WHEN LOWER(SHOW_TITLE) LIKE ANY('%hbo%', '%recommend%', '%drama%', '%reality%', '%tv%') then 'Film'
                WHEN LOWER(SHOW_TITLE) LIKE ANY('%game%', '%computer game%') then 'Video Games'
                WHEN LOWER(SHOW_TITLE) LIKE ANY('%dress%', '%wardrobe%', '%lipstick%', '%style%', '%clothing%') then 'Fashion'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%mum%', '%dad%', '%cbeeies%', '%growing%') then 'Family'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%commerce%', '%bread%', '%indoors%', '%shelf%', '%retail%', '%market%', '%shop%') then 'Shopper'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%climate%', '%environment%', '%nature%', '%geographic%', '%nature%', '%sustainable%') then 'Environment'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%medical%') then 'Medical'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%celeb%', '%gossip%', '%pop%', '%drama%') then 'Pop'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%career%', '%profession%', '%management%', '%expert%', '%work%', '%goals%') then 'Career'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%education%', '%numberphile%', '%future%', '%ted talks%') then 'Education'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%paws%', '%animal%', '%flea%', '%dog%', '%pet%', '%dog%') then 'Pets'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%life%', '%events%', '%birthday%', '%party%', '%anniversary%', '%birth%') then 'Life'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%consumer%', '%brand%', '%shop%', '%party%') then 'Consumer'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%food%', '%eat%', '%cook%', '%feast%', '%dairy%', '%drink%', '%wine%') then 'Food'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%money%', '%finance%', '%invest%', '%stock%') then 'Finance_kw'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%birthday%', '%party%', '%anniversary%', '%day%', '%activity%') then 'Holiday_and_Gifts'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%film%', '%movie%', '%backstage%') then 'Movie'
                 WHEN LOWER(SHOW_TITLE) LIKE ANY('%tv%', '%reality%', '%recap%', '%succession%') then 'Television'
                 
                else c.ITUNESCAT1_PARENT
                 end AS ITUNESCAT1_PARENT
                
                FROM SHOW_TABLE LEFT JOIN 
                DS_IOW_EVENTS.CURATED.VW_ORACLE_PODCAST_SHOW_EPISODES C ON SHOW_TABLE.EPISODE_ID = C.EPISODEID)
                
                )
                
                
                SELECT DISTINCT EPISODE_ID as EPISODEID, SHOW_ID as SHOWID, COMBINED_CATS AS CATEGORY FROM
                
                        (select EPISODE_ID, SHOW_ID, 
                       array_to_string(ALL_CATS, '-') AS PARENT_CATEGORY, array_to_string(ALL_CHS, '-') AS CHILD_CATEGORY,
                       array_to_string(COMBINED_CATEGORIES, '-')AS COMBINED_CATS
                        from
                        (SELECT DISTINCT EPISODE_ID, SHOW_ID, ARRAY_AGG(DISTINCT ITUNESCAT1_PARENT) WITHIN GROUP (ORDER BY ITUNESCAT1_PARENT DESC) AS CAT1,
                        ARRAY_AGG(DISTINCT ITUNESCAT2_PARENT) WITHIN GROUP (ORDER BY ITUNESCAT2_PARENT DESC) AS CAT2,
                        ARRAY_AGG(DISTINCT ITUNESCAT3_PARENT) WITHIN GROUP (ORDER BY ITUNESCAT3_PARENT DESC) AS CAT3, 
                        RD_RM.SANDBOX.ARRAY_DCAT(CAT1, CAT2) AS CAT_1_2,
                        RD_RM.SANDBOX.ARRAY_DCAT(CAT_1_2, CAT3) AS ALL_CATS,
                        ARRAY_AGG(DISTINCT ITUNESCAT1_CHILD) WITHIN GROUP (ORDER BY ITUNESCAT1_CHILD DESC) AS CH1,
                        ARRAY_AGG(DISTINCT ITUNESCAT2_CHILD) WITHIN GROUP (ORDER BY ITUNESCAT2_CHILD DESC) AS CH2,
                        ARRAY_AGG(DISTINCT ITUNESCAT3_CHILD) WITHIN GROUP (ORDER BY ITUNESCAT3_CHILD DESC) AS CH3, 
                        RD_RM.SANDBOX.ARRAY_DCAT(CH1, CH2) AS CH_1_2,
                        RD_RM.SANDBOX.ARRAY_DCAT(CH_1_2, CH3) AS ALL_CHS,
                        RD_RM.SANDBOX.ARRAY_DCAT(ALL_CATS, ALL_CHS) AS COMBINED_CATEGORIES 
                
                        FROM all_pd_table GROUP BY EPISODE_ID, SHOW_ID)
                
                        group by EPISODE_ID, SHOW_ID, CHILD_CATEGORY, PARENT_CATEGORY, COMBINED_CATEGORIES)
                
                )))
                        
                        
                        
        '''

    playlist_refresh_query = f'''CREATE OR REPLACE TABLE 
        
        RD_RM.SANDBOX.SEGMENTATION_AUDIO_STREAM_MAPPING_TABLE AS (with channel_table as (

  select stream_name, channel, regexp_Replace(playlist, 'MP3|HD|Adw|Low','') as Playlist FROM

(SELECT distinct STREAM_NAME, 
case
when STREAM_NAME LIKE '%XTRA%' or STREAM_NAME LIKE '%Xtra%' THEN 'CapitalXTRA'
when STREAM_NAME LIKE '%CapitalDance%' or STREAM_NAME LIKE '%HeartDance%' THEN regexp_substr(STREAM_NAME, '^CapitalDance|^HeartDance')
when STREAM_NAME LIKE '%LBCNews%' THEN regexp_substr(STREAM_NAME, '^LBCNews')
when STREAM_NAME LIKE '%ClassicFM%' THEN 'ClassicFM'
when STREAM_NAME LIKE '%Heart70s%' OR STREAM_NAME LIKE '%Heart80s%' OR STREAM_NAME LIKE '%Heart90s%' OR STREAM_NAME LIKE '%Heart00s%' THEN 
regexp_substr(STREAM_NAME, '^Heart70s|^Heart80s|^Heart90s|^Heart00s')
when STREAM_NAME LIKE '%SmoothChill%' OR STREAM_NAME LIKE '%SmoothCountry%' THEN 
regexp_substr(STREAM_NAME, '^SmoothCountry|^SmoothChill')
when STREAM_NAME LIKE '%RadioXClassicRock%' THEN 'RadioXClassicRock'
ELSE regexp_substr(STREAM_NAME, '^Capital|^Heart|^Smooth|^Classic|^Gold|^RadioX|^LBC|^Global')
end as channel,
case
when STREAM_NAME LIKE '%XTRA%' or STREAM_NAME LIKE '%Xtra%' THEN regexp_Replace(STREAM_NAME, 'CapitalXTRA+|CapitalXtra+', '')
when STREAM_NAME LIKE '%ClassicFM%' THEN regexp_Replace(STREAM_NAME, 'ClassicFM+', '')
when STREAM_NAME LIKE '%CapitalDance%' or STREAM_NAME LIKE '%HeartDance%' THEN regexp_Replace(STREAM_NAME, 'CapitalDance+|HeartDance+', '')
when STREAM_NAME LIKE '%Heart70s%' OR STREAM_NAME LIKE '%Heart80s%' OR STREAM_NAME LIKE '%Heart90s%' OR STREAM_NAME LIKE '%Heart00s%' 
THEN regexp_Replace(STREAM_NAME, 'Heart70s+|Heart80s+|Heart90s+|Heart00s+', '') 
when STREAM_NAME LIKE '%SmoothChill%' OR STREAM_NAME LIKE '%SmoothCountry%' THEN 
regexp_Replace(STREAM_NAME, 'SmoothCountry+|SmoothChill+', '')
when STREAM_NAME LIKE '%LBCNews%' THEN regexp_Replace(STREAM_NAME, 'LBCNews+','')
ELSE regexp_Replace(STREAM_NAME, 'Capital+|Heart+|Smooth+|Classic+|Gold+|RadioX+|LBC+|Global+','')
end as playlist
from DS_DAX_AUDIO.CURATED.DAX_AUDIO_STREAMS
 where REQUEST_START_TIME >= '{start_date}'

group by STREAM_NAME, CHANNEL, Playlist)
  
  where channel is not null)

SELECT STREAM_NAME, CHANNEL, PLAYLIST,
        CASE 
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'Capital' then 'Capital_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'CapitalDance' then 'CapitalDance_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'CapitalXTRA' then 'CapitalXTRA_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'ClassicFM' then 'ClassicFM_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'Global' then 'Global_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'Gold' then 'Gold_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'Heart' then 'Heart_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'RadioX' then 'RadioX_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'Smooth' then 'Smooth_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'SmoothCountry' then 'SmoothCountry_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'SmoothChill' then 'SmoothChill_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'HeartDance' then 'HeartDance_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'Heart90s' then 'Heart90s_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'Heart80s' then 'Heart80s_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like 'Heart70s' then 'Heart70s_regional'
        when NOT (PLAYLIST LIKE ANY('%Xmas%','%UK%','%Reloaded%','%P3%','%973%', '%1152%', '%Live%', '%National%', '%-M-%', '')) and CHANNEL like '%Heart00s%' then 'Heart00s_regional'
        when CHANNEL like 'LBC' then 'LBC'
        when CHANNEL like 'LBCNews' then 'LBCNews'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'Capital' then 'Capital_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'CapitalDance' then 'CapitalDance_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'CapitalXTRA' then 'CapitalXTRA_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'ClassicFM' then 'ClassicFM_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'Global' then 'Global_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'Gold' then 'Gold_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'Heart' then 'Heart_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'RadioX' then 'RadioX_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'Smooth' then 'Smooth_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'SmoothCountry' then 'SmoothCountry_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'SmoothChill' then 'SmoothChill_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'HeartDance' then 'HeartDance_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'Heart90s' then 'Heart90s_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'Heart80s' then 'Heart80s_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like 'Heart70s' then 'Heart70s_Live'
        when playlist like any('%UK%', '%Live%', '%National%', '') and CHANNEL like '%Heart00s%' then 'Heart00s_Live'
        else concat(CHANNEL,PLAYLIST)
        end as PLAYLIST_PRUNED
        from channel_table )'''

    user_consent_map_query = f'''
                CREATE OR REPLACE TABLE RD_RM.SANDBOX.GIGYA_ID_CONSENT_MAP as SELECT *
                FROM (
                SELECT
                DISTINCT GIGYA_ID, GDPR_PLATFORM_CONSENT_STATUS AS CONSENT_STATUS, AD_REQUEST_TIMESTAMP_UK as DATE,
                ROW_NUMBER() OVER (PARTITION BY GIGYA_ID ORDER BY DATE DESC) AS row_num
                FROM
                INTEGRATION.PROCESSED.IOW_GOLD_AD_REQUESTS WHERE AD_REQUEST_TIMESTAMP_UK >= '{start_date}' AND GIGYA_ID IS 
                NOT NULL) AS subquery
                WHERE row_num = 1
                '''

    cs = ctx.cursor()
    for query in [podcast_refresh_query, playlist_refresh_query, user_consent_map_query]:
        cs.execute(query)

    cs.close()
    ctx.close()


if __name__ == '__main__':
    main()
