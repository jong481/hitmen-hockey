#!/usr/bin/env python
# coding: utf-8

# In[1]:


write_mode = 'replace'
target_table = 'whl_team_schedule_by_season'


# In[2]:


import pandas as pd
import numpy as np
import sys
sys.path.append('/home/do-user/projects/hitmen_hockey/')

import settings as st
import datetime


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


print("START PROCESS: " + str(datetime.datetime.now()))


# In[7]:


key = "41b145a848f4bd67"


# In[8]:


sql = "SELECT * FROM whl_team_list_by_season"
res = du.query_database_to_list(db_sys, db_user, db_pass, db_host, db_port, db_db, sql)


# In[9]:


du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, target_table)


# In[33]:


master_df = pu.empty_df()


# In[34]:


def getOpponentTeam(row):
    team_id = row['team_id']
    home_team = row['home_team']
    visiting_team = row['visiting_team']
    if int(team_id) == int(home_team):
        return visiting_team
    else: return home_team


# In[35]:


i = 0

for r in res:
    season_id = r['season_id']
    team_id = r['id']
    
    print(i, season_id, team_id)
    
    url = 'http://lscluster.hockeytech.com/feed/?feed=modulekit&view=schedule&key={0}&fmt=json&client_code=whl&lang=en&season_id={1}&team_id={2}&league_code=&fmt=json'.format(key, season_id, team_id)
    
    json_data = wu.return_json(url)
    
    if len(json_data['SiteKit']['Schedule']) != 0:
    
        df = pd.DataFrame(json_data['SiteKit']['Schedule'])
        df['team_id'] = team_id
        df['opponent_id'] = df[['team_id', 'home_team', 'visiting_team']].apply(getOpponentTeam, axis=1)

        df = df[['date_played', 'date_time_played', 'game_id', 'game_number', 'season_id', 'team_id', 'opponent_id']]

        master_df = master_df.append(df)

    i+=1

    if (i%1000)==0:
        du.write_df_to_database(master_df, target_table, db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        master_df = pu.empty_df()


# In[16]:


du.write_df_to_database(master_df, target_table, db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)


# In[17]:


print("END PROCESS: " + str(datetime.datetime.now()))

