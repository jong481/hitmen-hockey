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


du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_lineup')


# In[ ]:


master_lineup_df = pu.empty_df()


# In[ ]:


i = 0

for r in res:
    game_id = r['id']
    
    print(i, game_id)
    
    url = "http://cluster.leaguestat.com/feed/index.php?feed=gc&key={0}&client_code=whl&game_id={1}&lang_code=en&fmt=json&tab=gamesummary".format(key, game_id)
    
    json_data = wu.return_json(url)
       
    ### Lineup
    
    df_lineup = pu.empty_df()

    home_goalies = pd.DataFrame(json_data['GC']['Gamesummary']['home_team_lineup']['goalies'])
    home_goalies['team'] = 'home'
    home_goalies['type'] = 'goalie'
    home_players = pd.DataFrame(json_data['GC']['Gamesummary']['home_team_lineup']['players'])
    home_players['team'] = 'home'
    home_players['type'] = 'player'
    away_goalies = pd.DataFrame(json_data['GC']['Gamesummary']['visitor_team_lineup']['goalies'])
    away_goalies['team'] = 'away'
    away_goalies['type'] = 'goalie'
    away_players = pd.DataFrame(json_data['GC']['Gamesummary']['visitor_team_lineup']['players'])
    away_players['team'] = 'away'
    away_players['type'] = 'player'

    df_lineup = df_lineup.append(home_goalies)
    df_lineup = df_lineup.append(home_players)
    df_lineup = df_lineup.append(away_goalies)
    df_lineup = df_lineup.append(away_players)

    df_lineup['game_id'] = game_id
    master_lineup_df = master_lineup_df.append(df_lineup)
    
    i += 1
    
    if (i%5000)==0:
        du.write_df_to_database(master_lineup_df, 'stg_whl_game_summary_lineup', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)

        master_lineup_df = pu.empty_df()


# In[ ]:


du.write_df_to_database(master_lineup_df, 'stg_whl_game_summary_lineup', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)


# In[ ]:


print("END PROCESS: " + str(datetime.datetime.now()))

