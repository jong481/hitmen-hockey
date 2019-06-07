#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import numpy as np
import sys
sys.path.append('/home/do-user/projects/hitmen_hockey/')

import settings as st
import datetime


# In[ ]:


sys.path.append('/home/do-user/projects/hitmen_hockey/production/data-scraping/utilities')

import web_utilities as wu
import database_utilities as du
import encoder_utilities as eu
import pd_utilities as pu


# In[ ]:


import pandas as pd


# In[ ]:


db_host = st.database['local']['host']
db_port = st.database['local']['port']
db_user = st.database['local']['user']
db_pass = st.database['local']['pass']
db_sys = st.database['local']['system']
db_db = st.database['local']['db']


# In[ ]:


print("START PROCESS: " + str(datetime.datetime.now()))


# In[ ]:


key = "41b145a848f4bd67"


# In[ ]:


season_id = None


# In[ ]:


if season_id == None:
    sql = "SELECT DISTINCT id FROM whl_schedule_by_season order by id asc"
else:
    sql = "SELECT DISTINCT id FROM whl_schedule_by_season where season_id = {0} order by id asc".format(season_id)
res = du.query_database_to_list(db_sys, db_user, db_pass, db_host, db_port, db_db, sql)


# In[ ]:


du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_meta')


# In[ ]:


master_meta_df = pu.empty_df()


# In[ ]:


i = 0

for r in res:
    game_id = r['id']
    
    print(i, game_id)
    
    url = "http://cluster.leaguestat.com/feed/index.php?feed=gc&key={0}&client_code=whl&game_id={1}&lang_code=en&fmt=json&tab=gamesummary".format(key, game_id)
    
    json_data = wu.return_json(url)
    
    ### Metadata
    df_meta = pd.DataFrame(json_data['GC']['Gamesummary']['meta'], index=[0])
    df_meta['venue'] = json_data['GC']['Gamesummary']['venue']
    df_meta['home_shootout'] = json_data['GC']['Gamesummary']['homeShootout']
    df_meta['visiting_shootout'] = json_data['GC']['Gamesummary']['visitorShootout']
    df_meta['home_total_shots'] = json_data['GC']['Gamesummary']['totalShots']['home']
    df_meta['visiting_total_shots'] = json_data['GC']['Gamesummary']['totalShots']['visitor']
    df_meta['home_total_shots_on'] = json_data['GC']['Gamesummary']['totalShotsOn']['home']
    df_meta['visiting_total_shots_on'] = json_data['GC']['Gamesummary']['totalShotsOn']['visitor']
    df_meta['home_powerplay_goals'] = json_data['GC']['Gamesummary']['powerPlayGoals']['home']
    df_meta['visiting_powerplay_goals'] = json_data['GC']['Gamesummary']['powerPlayGoals']['visitor']
    df_meta['home_powerplay_count'] = json_data['GC']['Gamesummary']['powerPlayCount']['home']
    df_meta['visiting_powerplay_count'] = json_data['GC']['Gamesummary']['powerPlayCount']['visitor']
    df_meta['home_total_hits'] = json_data['GC']['Gamesummary']['totalHits']['home']
    df_meta['visiting_total_hits'] = json_data['GC']['Gamesummary']['totalHits']['visitor']
    df_meta['home_assist_count'] = json_data['GC']['Gamesummary']['assistCount']['home']
    df_meta['visiting_assist_count'] = json_data['GC']['Gamesummary']['assistCount']['visitor']
    df_meta['home_points_count'] = json_data['GC']['Gamesummary']['pointsCount']['home']
    df_meta['visiting_points_count'] = json_data['GC']['Gamesummary']['pointsCount']['visitor']
    df_meta['home_goal_count'] = json_data['GC']['Gamesummary']['goalCount']['home']
    df_meta['visiting_goal_count'] = json_data['GC']['Gamesummary']['goalCount']['visitor']
    df_meta['home_goal_total'] = json_data['GC']['Gamesummary']['totalGoals']['home']
    df_meta['visiting_goal_total'] = json_data['GC']['Gamesummary']['totalGoals']['visitor']
    df_meta['home_faceoffs_att'] = json_data['GC']['Gamesummary']['totalFaceoffs']['home']['att']
    df_meta['home_faceoffs_won'] = json_data['GC']['Gamesummary']['totalFaceoffs']['home']['won']
    df_meta['visiting_faceoffs_att'] = json_data['GC']['Gamesummary']['totalFaceoffs']['visitor']['att']
    df_meta['visiting_faceoffs_won'] = json_data['GC']['Gamesummary']['totalFaceoffs']['visitor']['won']
    df_meta['home_pim_total'] = json_data['GC']['Gamesummary']['pimTotal']['home']
    df_meta['visiting_pim_total'] = json_data['GC']['Gamesummary']['pimTotal']['visitor']
    df_meta['home_inf_count'] = json_data['GC']['Gamesummary']['infCount']['home']
    df_meta['visiting_inf_count'] = json_data['GC']['Gamesummary']['infCount']['visitor']

    master_meta_df = master_meta_df.append(df_meta)
    
    i += 1
    
    if (i%5000)==0:
        du.write_df_to_database(master_meta_df, 'stg_whl_game_summary_meta', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
    
        master_meta_df = pu.empty_df()


# In[ ]:


du.write_df_to_database(master_meta_df, 'stg_whl_game_summary_meta', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)


# In[ ]:


print("END PROCESS: " + str(datetime.datetime.now()))

