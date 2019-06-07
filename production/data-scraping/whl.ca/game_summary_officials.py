#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import sys
sys.path.append('/home/do-user/projects/hitmen_hockey/')

import settings as st
import datetime


# In[2]:


sys.path.append('/home/do-user/projects/hitmen_hockey/production/data-scraping/utilities')

import web_utilities as wu
import database_utilities as du
import encoder_utilities as eu
import pd_utilities as pu


# In[3]:


import pandas as pd


# In[4]:


db_host = st.database['local']['host']
db_port = st.database['local']['port']
db_user = st.database['local']['user']
db_pass = st.database['local']['pass']
db_sys = st.database['local']['system']
db_db = st.database['local']['db']


# In[5]:


print("START PROCESS: " + str(datetime.datetime.now()))


# In[6]:


key = "41b145a848f4bd67"


# In[7]:


season_id = None


# In[8]:


if season_id == None:
    sql = "SELECT DISTINCT id FROM whl_schedule_by_season order by id asc"
else:
    sql = "SELECT DISTINCT id FROM whl_schedule_by_season where season_id = {0} order by id asc".format(season_id)
res = du.query_database_to_list(db_sys, db_user, db_pass, db_host, db_port, db_db, sql)


# In[9]:


du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_officials')


# In[10]:


master_official_df = pu.empty_df()


# In[11]:


i = 0

for r in res:
    game_id = r['id']
    
    print(i, game_id)
    
    url = "http://cluster.leaguestat.com/feed/index.php?feed=gc&key={0}&client_code=whl&game_id={1}&lang_code=en&fmt=json&tab=gamesummary".format(key, game_id)
    
    json_data = wu.return_json(url)
    
    ### Officials
    df_officials = pd.DataFrame(json_data['GC']['Gamesummary']['officialsOnIce'])
    df_officials['game_id'] = game_id
    master_official_df = master_official_df.append(df_officials)
    
    i += 1
    
    if (i%5000)==0:
        du.write_df_to_database(master_official_df, 'stg_whl_game_summary_officials', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        master_official_df = pu.empty_df()


# In[ ]:


du.write_df_to_database(master_official_df, 'stg_whl_game_summary_officials', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)


# In[ ]:


print("END PROCESS: " + str(datetime.datetime.now()))

