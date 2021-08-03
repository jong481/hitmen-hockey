#!/usr/bin/env python
# coding: utf-8

# In[1]:


write_mode = 'replace'
target_table = 'whl_team_list_by_season'


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


# In[6]:


sql = "SELECT * FROM whl_season_list"


# In[7]:


res = du.query_database_to_list(db_sys, db_user, db_pass, db_host, db_port, db_db, sql)


# In[11]:


master_df = pd.DataFrame()


# In[12]:


for r in res:
    season_id = r['season_id']
    
    print(season_id)
    
    url = "http://lscluster.hockeytech.com/feed/?feed=modulekit&view=teamsbyseason&key={0}&fmt=json&client_code=whl&lang=en&season_id={1}&league_code=&fmt=json".format(key, season_id)
    
    json_data = wu.return_json(url)
    
    df = pd.DataFrame(json_data['SiteKit']['Teamsbyseason'])
    
    df['season_id'] = season_id
    df["division_id"] = pd.to_numeric(df["division_id"])
    df["id"] = pd.to_numeric(df["id"])
    
    master_df = master_df.append(df)


# In[13]:


master_df = master_df.reset_index(drop=True)


# In[14]:


if write_mode == 'append':
    index = du.get_table_new_id(db_sys, db_user, db_pass, db_host, db_port, db_db, target_table, 'index')
    master_df.insert(0, 'index', master_df.index + index)
else:
    master_df.insert(0, 'index', master_df.index)


# In[15]:


du.write_df_to_database(master_df, target_table, db_sys, db_user, db_pass, db_host, db_port, db_db, write_mode, False)

