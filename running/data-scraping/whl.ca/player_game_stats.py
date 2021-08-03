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


stg_table = 'stg_whl_player_game_stats'
target_table = 'whl_player_game_stats'


# In[ ]:


du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, stg_table)


# In[ ]:


key = "41b145a848f4bd67"


# In[ ]:


print("START PROCESS: " + str(datetime.datetime.now()))


# In[ ]:


sql = "SELECT DISTINCT player_id FROM whl_player_profile WHERE player_id IS NOT NULL and cast(player_id as int) > 27884 ORDER BY player_id ASC"
res = du.query_database_to_list(db_sys, db_user, db_pass, db_host, db_port, db_db, sql)


# In[ ]:


master_df = pu.empty_df()


# In[ ]:


i = 1
for r in res:
    player_id = str(r['player_id']).replace(".0", "")
    
    print(i, player_id)
        
    url = "http://lscluster.hockeytech.com/feed/?feed=modulekit&view=player&key={0}&fmt=json&client_code=whl&lang=en&player_id={1}&category=gamebygame".format(key, player_id)

    json_data = wu.return_json(url)
    
    for season in json_data['SiteKit']['Player']['seasons_played']:
        season_id = season['season_id']
        
        print(season_id)
        
        season_url = 'http://lscluster.hockeytech.com/feed/?feed=modulekit&view=player&key={0}&fmt=json&client_code=whl&lang=en&player_id={1}&category=gamebygame&season_id={2}'.format(key, player_id, season_id)
    
        season_data = wu.return_json(season_url)

        df = pd.DataFrame(season_data['SiteKit']['Player']['games'])
        
        df['player_id'] = player_id
        df['season_id'] = season_id
        
        master_df = master_df.append(df)
        
    if (i%10)==0:
        du.write_df_to_database(master_df, stg_table, db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        master_df = pu.empty_df()
        
    i += 1


# In[ ]:


du.write_df_to_database(master_df, stg_table, db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)


# In[ ]:


"""
Fix Data Types From Staging Table
"""

sql = "SELECT * FROM {0}".format(stg_table)
df = du.query_database_to_df(db_sys, db_user, db_pass, db_host, db_port, db_db, sql)

"""
Clean up Data
"""
df['date_played'] = df.date_played.apply(lambda x: pu.clearDate(x))
df['points'] = df.points.apply(lambda x: pu.clearNA(x))

"""
Fix Data Types
"""
df["assists"] = pd.to_numeric(df["assists"])
df["empty_net_goals"] = pd.to_numeric(df["empty_net_goals"])
df["faceoffs_taken"] = pd.to_numeric(df["faceoffs_taken"])
df["faceoffs_won"] = pd.to_numeric(df["faceoffs_won"])
df["first_goals_scored"] = pd.to_numeric(df["first_goals_scored"])
df["game_tieing_goals"] = pd.to_numeric(df["game_tieing_goals"])
df["game_winning_goals"] = pd.to_numeric(df["game_winning_goals"])
df["goalie"] = pd.to_numeric(df["goalie"])
df["goals"] = pd.to_numeric(df["goals"])
df["home"] = pd.to_numeric(df["home"])
df["home_team"] = pd.to_numeric(df["home_team"])
df["id"] = pd.to_numeric(df["id"])
df["insurange_goals"] = pd.to_numeric(df["insurange_goals"])
df["penalty_minutes"] = pd.to_numeric(df["penalty_minutes"])
df["player_id"] = pd.to_numeric(df["player_id"])
df["player_team"] = pd.to_numeric(df["player_team"])
df["plus_minus"] = pd.to_numeric(df["plus_minus"])
df["plusminus"] = pd.to_numeric(df["plusminus"])
df["points"] = pd.to_numeric(df["points"])
df["power_play_goals"] = pd.to_numeric(df["power_play_goals"])
df["season_id"] = pd.to_numeric(df["season_id"])
df["shooting_percentage"] = pd.to_numeric(df["shooting_percentage"])
df["shootout_attempts"] = pd.to_numeric(df["shootout_attempts"])
df["shootout_goals"] = pd.to_numeric(df["shootout_goals"])
df["shootout_goals_win"] = pd.to_numeric(df["shootout_goals_win"])
df["shootout_shots"] = pd.to_numeric(df["shootout_shots"])
df["shootout_shots_percentage"] = pd.to_numeric(df["shootout_shots_percentage"])
df["short_handed_goals"] = pd.to_numeric(df["short_handed_goals"])
df["shots"] = pd.to_numeric(df["shots"])
df["visiting_team"] = pd.to_numeric(df["visiting_team"])

df["date_played"] = pd.to_datetime(df["date_played"])


# In[ ]:


du.write_df_to_database(df, target_table, db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)


# In[ ]:


print("END PROCESS: " + str(datetime.datetime.now()))

