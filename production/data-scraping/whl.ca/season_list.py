#!/usr/bin/env python
# coding: utf-8

# In[1]:


write_mode = 'append'
target_table = 'whl_season_list'


# In[2]:


import pandas as pd
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


db_host = st.database['local']['host']
db_port = st.database['local']['port']
db_user = st.database['local']['user']
db_pass = st.database['local']['pass']
db_sys = st.database['local']['system']
db_db = st.database['local']['db']


# In[5]:


key = "41b145a848f4bd67"

url = "http://lscluster.hockeytech.com/feed/?feed=modulekit&view=seasons&key={0}&fmt=json&client_code=whl&lang=en&league_code=&fmt=json".format(key)


# In[6]:


json_data = wu.return_json(url)


# In[7]:


df = pd.DataFrame(json_data['SiteKit']['Seasons'])


# In[8]:


df["career"] = pd.to_numeric(df["career"])
df["playoff"] = pd.to_numeric(df["playoff"])
df["season_id"] = pd.to_numeric(df["season_id"])
df["start_date"] = pd.to_datetime(df["start_date"])
df["end_date"] = pd.to_datetime(df["end_date"])


# In[9]:


if write_mode == 'append':
    index = du.get_table_new_id(db_sys, db_user, db_pass, db_host, db_port, db_db, target_table, 'index')
    df.insert(0, 'index', df.index + index)
else:
    df.insert(0, 'index', df.index)


# In[10]:


du.write_df_to_database(df, target_table, db_sys, db_user, db_pass, db_host, db_port, db_db, write_mode, False)

