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


du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_goals')
du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_goals_plus')
du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_goals_minus')


# In[ ]:


master_goals_df = pu.empty_df()
master_goals_plus_df = pu.empty_df()
master_goals_minus_df = pu.empty_df()


# In[ ]:


i = 0

for r in res:
    game_id = r['id']
    
    print(i, game_id)
    
    url = "http://cluster.leaguestat.com/feed/index.php?feed=gc&key={0}&client_code=whl&game_id={1}&lang_code=en&fmt=json&tab=gamesummary".format(key, game_id)
    
    json_data = wu.return_json(url)
    
    ### Goals
    df_goals = pd.DataFrame(json_data['GC']['Gamesummary']['goals'])

    try:
        col_list = []
        for c in pd.DataFrame(df_goals.assist1_player.values.tolist()).columns:
            col_list.append('assist1_player_' + c)
        df_goals[col_list] = pd.DataFrame(df_goals.assist1_player.values.tolist())
        df_goals = df_goals.drop(['assist1_player'], axis=1)
    except Exception as e: pass
        
    try:
        col_list = []
        for c in pd.DataFrame(df_goals.assist2_player.values.tolist()).columns:
            col_list.append('assist2_player_' + c)
        df_goals[col_list] = pd.DataFrame(df_goals.assist2_player.values.tolist())
        df_goals = df_goals.drop(['assist2_player'], axis=1)
    except Exception as e: pass
        
    try:
        col_list = []
        for c in pd.DataFrame(df_goals.goal_scorer.values.tolist()).columns:
            col_list.append('goal_scorer_' + c)
        df_goals[col_list] = pd.DataFrame(df_goals.goal_scorer.values.tolist())
        df_goals = df_goals.drop(['goal_scorer'], axis=1)
    except Exception as e: pass
        
    df_goals['game_id'] = game_id

    df_goals = df_goals.reset_index()
    df_goals.rename(columns={'index':'goal_id'}, inplace=True)
    
    ### Minus

    try:
        df_minus = df_goals[['minus', 'game_id', 'goal_id']].copy()
        df_minus = df_minus.reset_index()

        df_minus_x = df_minus['minus'].apply(pd.Series).reset_index().melt(id_vars='index').dropna()[['index', 'value']].set_index('index')
        df_minus_a = pd.merge(pd.merge(df_minus_x, df_minus[['game_id']], left_index=True, right_index=True), df_minus[['goal_id']], left_index=True, right_index=True)
        df_minus_a = df_minus_a.reset_index()

        col_list = []
        for c in pd.DataFrame(df_minus_a.value.values.tolist()).columns:
            col_list.append(c)

        df_minus_a[col_list] = pd.DataFrame(df_minus_a.value.values.tolist())
        df_minus_a = df_minus_a.drop(['value'], axis=1)
        df_minus_a = df_minus_a.drop(['index'], axis=1)
        master_goals_minus_df = master_goals_minus_df.append(df_minus_a)
    except Exception as e: pass
    
    ### Plus 

    try:
        df_plus = df_goals[['plus', 'game_id', 'goal_id']].copy()
        df_plus = df_plus.reset_index()

        df_plus_x = df_plus['plus'].apply(pd.Series).reset_index().melt(id_vars='index').dropna()[['index', 'value']].set_index('index')
        df_plus_a = pd.merge(pd.merge(df_plus_x, df_plus[['game_id']], left_index=True, right_index=True), df_plus[['goal_id']], left_index=True, right_index=True)
        df_plus_a = df_plus_a.reset_index()

        col_list = []
        for c in pd.DataFrame(df_plus_a.value.values.tolist()).columns:
            col_list.append(c)

        df_plus_a[col_list] = pd.DataFrame(df_plus_a.value.values.tolist())
        df_plus_a = df_plus_a.drop(['value'], axis=1)
        df_plus_a = df_plus_a.drop(['index'], axis=1)
        
        master_goals_plus_df = master_goals_plus_df.append(df_plus_a)
    except Exception as e: pass

    try:
        df_goals = df_goals.drop(['plus', 'minus'], axis=1)
    except Exception as e: pass
    
    master_goals_df = master_goals_df.append(df_goals)
    
    i += 1
    
    if (i%5000)==0:
        du.write_df_to_database(master_goals_df, 'stg_whl_game_summary_goals', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        du.write_df_to_database(master_goals_plus_df, 'stg_whl_game_summary_goals_plus', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        du.write_df_to_database(master_goals_minus_df, 'stg_whl_game_summary_goals_minus', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        
        master_goals_df = pu.empty_df()
        master_goals_plus_df = pu.empty_df()
        master_goals_minus_df = pu.empty_df()


# In[ ]:


du.write_df_to_database(master_goals_df, 'stg_whl_game_summary_goals', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
du.write_df_to_database(master_goals_plus_df, 'stg_whl_game_summary_goals_plus', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
du.write_df_to_database(master_goals_minus_df, 'stg_whl_game_summary_goals_minus', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)


# In[ ]:


print("END PROCESS: " + str(datetime.datetime.now()))

