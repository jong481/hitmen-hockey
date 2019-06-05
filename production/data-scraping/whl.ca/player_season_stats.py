#!/usr/bin/env python
# coding: utf-8

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


# In[8]:


sql = "SELECT DISTINCT player_id FROM whl_player_profile WHERE player_id IS NOT NULL ORDER BY player_id ASC"
res = du.query_database_to_list(db_sys, db_user, db_pass, db_host, db_port, db_db, sql)


# In[18]:


master_df = pd.DataFrame()


# In[19]:


i = 1
for r in res:
    player_id = str(r['player_id']).replace(".0", "")
    
    print(i, player_id)
    
    url = "http://lscluster.hockeytech.com/feed/?feed=modulekit&view=player&key={0}&fmt=json&client_code=whl&lang=en&player_id={1}&category=seasonstats".format(key, player_id)
    
    json_data = wu.return_json(url)
    
    player_df = pd.DataFrame()
    
    try:
        playoff_df = pd.DataFrame(json_data['SiteKit']['Player']['playoff'])
        playoff_df = playoff_df[playoff_df.shortname != "total"]
        playoff_df['season_type'] = 'Playoff'
        playoff_df['player_id'] = player_id
        
        player_df = player_df.append(playoff_df)
    except:
        pass
    
    try:
        regular_df = pd.DataFrame(json_data['SiteKit']['Player']['regular'])
        regular_df = regular_df[regular_df.shortname != "total"]
        regular_df['season_type'] = 'Regular'
        regular_df['player_id'] = player_id

        player_df = player_df.append(regular_df)
    except:
        pass
    
    master_df = master_df.append(player_df)
    
    i+=1


# In[20]:


"""
Clean up Data
"""
master_df['max_start_date'] = master_df.max_start_date.apply(lambda x: pu.clearDate(x))

"""
Fix Data Types
"""
master_df["active"] = pd.to_numeric(master_df["active"])
master_df["assists"] = pd.to_numeric(master_df["assists"])
master_df["career"] = pd.to_numeric(master_df["career"])
master_df["empty_net_goals"] = pd.to_numeric(master_df["empty_net_goals"])
master_df["faceoff_attempts"] = pd.to_numeric(master_df["faceoff_attempts"])
master_df["faceoff_pct"] = pd.to_numeric(master_df["faceoff_pct"])
master_df["faceoff_wins"] = pd.to_numeric(master_df["faceoff_wins"])
master_df["first_goals"] = pd.to_numeric(master_df["first_goals"])
master_df["game_tieing_goals"] = pd.to_numeric(master_df["game_tieing_goals"])
master_df["game_winning_goals"] = pd.to_numeric(master_df["game_winning_goals"])
master_df["games_played"] = pd.to_numeric(master_df["games_played"])
master_df["goals"] = pd.to_numeric(master_df["goals"])
master_df["hits"] = pd.to_numeric(master_df["hits"])
master_df["insurance_goals"] = pd.to_numeric(master_df["insurance_goals"])
master_df["jersey_number"] = pd.to_numeric(master_df["jersey_number"])
master_df["overtime_goals"] = pd.to_numeric(master_df["overtime_goals"])
master_df["penalty_minutes"] = pd.to_numeric(master_df["penalty_minutes"])
master_df["penalty_minutes_per_game"] = pd.to_numeric(master_df["penalty_minutes_per_game"])
master_df["playoff"] = pd.to_numeric(master_df["playoff"])
master_df["plus_minus"] = pd.to_numeric(master_df["plus_minus"])
master_df["points"] = pd.to_numeric(master_df["points"])
master_df["points_per_game"] = pd.to_numeric(master_df["points_per_game"])
master_df["power_play_assists"] = pd.to_numeric(master_df["power_play_assists"])
master_df["power_play_goals"] = pd.to_numeric(master_df["power_play_goals"])
master_df["season_id"] = pd.to_numeric(master_df["season_id"])
master_df["shooting_percentage"] = pd.to_numeric(master_df["shooting_percentage"])
master_df["shootout_attempts"] = pd.to_numeric(master_df["shootout_attempts"])
master_df["shootout_goals"] = pd.to_numeric(master_df["shootout_goals"])
master_df["shootout_percentage"] = pd.to_numeric(master_df["shootout_percentage"])
master_df["shootout_winning_goals"] = pd.to_numeric(master_df["shootout_winning_goals"])
master_df["short_handed_assists"] = pd.to_numeric(master_df["short_handed_assists"])
master_df["short_handed_goals"] = pd.to_numeric(master_df["short_handed_goals"])
master_df["shots"] = pd.to_numeric(master_df["shots"])
master_df["team_id"] = pd.to_numeric(master_df["team_id"])
master_df["unassisted_goals"] = pd.to_numeric(master_df["unassisted_goals"])

master_df["max_start_date"] = pd.to_datetime(master_df["max_start_date"])


# In[22]:


du.write_df_to_database(master_df, 'whl_player_season_stats', db_sys, db_user, db_pass, db_host, db_port, db_db, 'replace', False)

