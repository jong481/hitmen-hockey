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


du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_shootout')


# In[ ]:


master_shootout_df = pu.empty_df()


# In[ ]:


i = 0

for r in res:
    game_id = r['id']
    
    print(i, game_id)
    
    url = "http://cluster.leaguestat.com/feed/index.php?feed=gc&key={0}&client_code=whl&game_id={1}&lang_code=en&fmt=json&tab=gamesummary".format(key, game_id)
    
    json_data = wu.return_json(url)
    
    ### Shootout
    df_shootout = pd.DataFrame(json_data['GC']['Gamesummary']['shootoutDetail'])
    df_shootout['game_id'] = game_id
    master_shootout_df = master_shootout_df.append(df_shootout)
    
    i += 1
    
    if (i%10000)==0:
        du.write_df_to_database(master_shootout_df, 'stg_whl_game_summary_shootout', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)

        master_shootout_df = pu.empty_df()


# In[ ]:


du.write_df_to_database(master_shootout_df, 'stg_whl_game_summary_shootout', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)


# In[ ]:


print("END PROCESS: " + str(datetime.datetime.now()))

