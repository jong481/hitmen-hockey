#!/usr/bin/env python
# coding: utf-8

# In[1]:


write_mode = 'replace'
target_table = 'whl_schedule_by_season'


# In[2]:


import pandas as pd
import numpy as np
import sys
sys.path.append('/home/do-user/projects/hitmen_hockey/')

import settings as st


# In[3]:


sys.path.append('/home/do-user/projects/hitmen_hockey/production/data-scraping/utilities')

import web_utilities as wu
import database_utilities as du
import encoder_utilities as eu
import pd_utilities as pu


# In[4]:


import pandas as pd


# In[5]:


db_host = st.database['local']['host']
db_port = st.database['local']['port']
db_user = st.database['local']['user']
db_pass = st.database['local']['pass']
db_sys = st.database['local']['system']
db_db = st.database['local']['db']


# In[6]:


key = "41b145a848f4bd67"


# In[7]:


sql = "SELECT * FROM whl_season_list"
res = du.query_database_to_list(db_sys, db_user, db_pass, db_host, db_port, db_db, sql)


# In[8]:


master_df = pd.DataFrame()


# In[10]:


for r in res:
    season_id = r['season_id']
    
    print(season_id)
    
    url = "http://lscluster.hockeytech.com/feed/?feed=modulekit&view=schedule&key={0}&fmt=json&client_code=whl&lang=en&season_id={1}&team_id=&league_code=&fmt=json".format(key, season_id)
    
    json_data = wu.return_json(url)
    
    df = pd.DataFrame(json_data['SiteKit']['Schedule'])
    
    df["date_time_played"] = pd.to_datetime(df["date_time_played"])
    df["date_played"] = pd.to_datetime(df["date_played"])
    df["last_modified"] = pd.to_datetime(df["last_modified"])

    df["visiting_goal_count"] = pd.to_numeric(df["visiting_goal_count"])
    df["visiting_team"] = pd.to_numeric(df["visiting_team"])

    df["home_goal_count"] = pd.to_numeric(df["home_goal_count"])
    df["home_team"] = pd.to_numeric(df["home_team"])

    df["id"] = pd.to_numeric(df["id"])
    df["game_number"] = pd.to_numeric(df["game_number"])
    df["attendance"] = pd.to_numeric(df["attendance"])
    df["final"] = pd.to_numeric(df["final"])
    df["if_necessary"] = pd.to_numeric(df["if_necessary"])
    df["intermission"] = pd.to_numeric(df["intermission"])

    df["location"] = pd.to_numeric(df["location"])

    df["overtime"] = pd.to_numeric(df["overtime"])
    df["period"] = pd.to_numeric(df["period"])
    df["quick_score"] = pd.to_numeric(df["quick_score"])
    df["season_id"] = pd.to_numeric(df["season_id"])
    df["shootout"] = pd.to_numeric(df["shootout"])
    df["started"] = pd.to_numeric(df["started"])
    df["status"] = pd.to_numeric(df["status"])
    df["use_shootouts"] = pd.to_numeric(df["use_shootouts"])
    
    master_df = master_df.append(df)


# In[11]:


master_df = master_df.reset_index(drop=True)


# In[12]:


if write_mode == 'append':
    index = du.get_table_new_id(db_sys, db_user, db_pass, db_host, db_port, db_db, target_table, 'index')
    master_df.insert(0, 'index', master_df.index + index)
else:
    master_df.insert(0, 'index', master_df.index)


# In[13]:


du.write_df_to_database(master_df, target_table, db_sys, db_user, db_pass, db_host, db_port, db_db, write_mode, False)

