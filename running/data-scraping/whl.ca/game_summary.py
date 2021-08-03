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


du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_meta')
du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_periods')
du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_team')
du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_officials')
du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_shootout')
du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_penalties')
du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_goalies')
du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_lineup')
du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_coaches')
du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_goals')
du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_goals_plus')
du.truncate_table(db_sys, db_user, db_pass, db_host, db_port, db_db, 'stg_whl_game_summary_goals_minus')


# In[ ]:


master_meta_df = pu.empty_df()
master_period_df = pu.empty_df()
master_team_df = pu.empty_df()
master_official_df = pu.empty_df()
master_shootout_df = pu.empty_df()
master_penalties_df = pu.empty_df()
master_goalies_df = pu.empty_df()
master_lineup_df = pu.empty_df()
master_coach_df = pu.empty_df()
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
    
    ### Metadata
    df_meta = pd.DataFrame(json_data['GC']['Gamesummary']['meta'], index=[0])
    df_meta['venue'] = json_data['GC']['Gamesummary']['venue']
    df_meta['home_shootout'] = json_data['GC']['Gamesummary']['homeShootout']
    df_meta['visiting_shootout'] = json_data['GC']['Gamesummary']['visitorShootout']
    df_meta['home_total_shots'] = json_data['GC']['Gamesummary']['totalShots']['home']
    df_meta['visiting_total_shots'] = json_data['GC']['Gamesummary']['totalShots']['visitor']
    df_meta['home_total_shots_on'] = json_data['GC']['Gamesummary']['totalShotsOn']['home']
    df_meta['visiting_total_shots_on'] = json_data['GC']['Gamesummary']['totalShotsOn']['visitor']
    df_meta['home_powerplay_goals'] = json_data['GC']['Gamesummary']['powerPlayGoals']['home']
    df_meta['visiting_powerplay_goals'] = json_data['GC']['Gamesummary']['powerPlayGoals']['visitor']
    df_meta['home_powerplay_count'] = json_data['GC']['Gamesummary']['powerPlayCount']['home']
    df_meta['visiting_powerplay_count'] = json_data['GC']['Gamesummary']['powerPlayCount']['visitor']
    df_meta['home_total_hits'] = json_data['GC']['Gamesummary']['totalHits']['home']
    df_meta['visiting_total_hits'] = json_data['GC']['Gamesummary']['totalHits']['visitor']
    df_meta['home_assist_count'] = json_data['GC']['Gamesummary']['assistCount']['home']
    df_meta['visiting_assist_count'] = json_data['GC']['Gamesummary']['assistCount']['visitor']
    df_meta['home_points_count'] = json_data['GC']['Gamesummary']['pointsCount']['home']
    df_meta['visiting_points_count'] = json_data['GC']['Gamesummary']['pointsCount']['visitor']
    df_meta['home_goal_count'] = json_data['GC']['Gamesummary']['goalCount']['home']
    df_meta['visiting_goal_count'] = json_data['GC']['Gamesummary']['goalCount']['visitor']
    df_meta['home_goal_total'] = json_data['GC']['Gamesummary']['totalGoals']['home']
    df_meta['visiting_goal_total'] = json_data['GC']['Gamesummary']['totalGoals']['visitor']
    df_meta['home_faceoffs_att'] = json_data['GC']['Gamesummary']['totalFaceoffs']['home']['att']
    df_meta['home_faceoffs_won'] = json_data['GC']['Gamesummary']['totalFaceoffs']['home']['won']
    df_meta['visiting_faceoffs_att'] = json_data['GC']['Gamesummary']['totalFaceoffs']['visitor']['att']
    df_meta['visiting_faceoffs_won'] = json_data['GC']['Gamesummary']['totalFaceoffs']['visitor']['won']
    df_meta['home_pim_total'] = json_data['GC']['Gamesummary']['pimTotal']['home']
    df_meta['visiting_pim_total'] = json_data['GC']['Gamesummary']['pimTotal']['visitor']
    df_meta['home_inf_count'] = json_data['GC']['Gamesummary']['infCount']['home']
    df_meta['visiting_inf_count'] = json_data['GC']['Gamesummary']['infCount']['visitor']

    master_meta_df = master_meta_df.append(df_meta)
    
    ### Period
    period_df = pu.empty_df()
    for x in json_data['GC']['Gamesummary']['periods']:
        df = pd.DataFrame(json_data['GC']['Gamesummary']['periods'][x], index=[0])
        period_df = period_df.append(df)

    period_df['game_id'] = game_id
    master_period_df = master_period_df.append(period_df)
    
    ### Teams
    df_away = pd.DataFrame(json_data['GC']['Gamesummary']['visitor'], index=[0])
    df_away.columns = ['away_' + str(col) for col in df_away.columns]

    df_home = pd.DataFrame(json_data['GC']['Gamesummary']['home'], index=[0])
    df_home.columns = ['home_' + str(col) for col in df_home.columns]

    df_teams = pd.concat([df_home, df_away], axis=1)
    df_teams['game_id'] = game_id
    master_team_df = master_team_df.append(df_teams)
    
    ### MVP
    # json_data['GC']['Gamesummary']['mvps']
    
    ### Officials
    df_officials = pd.DataFrame(json_data['GC']['Gamesummary']['officialsOnIce'])
    df_officials['game_id'] = game_id
    master_official_df = master_official_df.append(df_officials)
    
    ### Shootout
    df_shootout = pd.DataFrame(json_data['GC']['Gamesummary']['shootoutDetail'])
    df_shootout['game_id'] = game_id
    master_shootout_df = master_shootout_df.append(df_shootout)
    
    ### Penalties
    df_penalties = pd.DataFrame(json_data['GC']['Gamesummary']['penalties'])

    try:
        col_list = []
        for c in pd.DataFrame(df_penalties.player_penalized_info.values.tolist()).columns:
            col_list.append('player_penalized_' + c)

        df_penalties[col_list] = pd.DataFrame(df_penalties.player_penalized_info.values.tolist())
        df_penalties = df_penalties.drop(['player_penalized_info'], axis=1)
        
        col_list = []
        for c in pd.DataFrame(df_penalties.player_served_info.values.tolist()).columns:
            col_list.append('player_served_' + c)

        df_penalties[col_list] = pd.DataFrame(df_penalties.player_served_info.values.tolist())
        df_penalties = df_penalties.drop(['player_served_info'], axis=1)

        df_penalties['game_id'] = game_id
        master_penalties_df = master_penalties_df.append(df_penalties)
    except Exception as e: pass
        
    ### Goalies
    
    df_goalies = pu.empty_df()

    df_goalies_visitor = pd.DataFrame(json_data['GC']['Gamesummary']['goalies']['visitor'])
    df_goalies_visitor['team'] = 'away'
    df_goalies_home = pd.DataFrame(json_data['GC']['Gamesummary']['goalies']['home'])
    df_goalies_home['team'] = 'home'

    df_goalies = df_goalies.append(df_goalies_visitor)
    df_goalies = df_goalies.append(df_goalies_home)
    df_goalies['game_id'] = game_id
    master_goalies_df = master_goalies_df.append(df_goalies)
    
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
    
    ### Coach
    
    df_coach = pu.empty_df()

    df_coach_visitor = pd.DataFrame(json_data['GC']['Gamesummary']['coaches']['visitor'])
    df_coach_visitor['team'] = 'away'
    df_coach_home = pd.DataFrame(json_data['GC']['Gamesummary']['coaches']['home'])
    df_coach_home['team'] = 'home'

    df_coach = df_coach.append(df_coach_visitor)
    df_coach = df_coach.append(df_coach_home)
    df_coach['game_id'] = game_id
    master_coach_df = master_coach_df.append(df_coach)
    
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
        du.write_df_to_database(master_meta_df, 'stg_whl_game_summary_meta', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        du.write_df_to_database(master_period_df, 'stg_whl_game_summary_periods', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        du.write_df_to_database(master_team_df, 'stg_whl_game_summary_team', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        du.write_df_to_database(master_official_df, 'stg_whl_game_summary_officials', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        du.write_df_to_database(master_shootout_df, 'stg_whl_game_summary_shootout', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        du.write_df_to_database(master_penalties_df, 'stg_whl_game_summary_penalties', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        du.write_df_to_database(master_goalies_df, 'stg_whl_game_summary_goalies', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        du.write_df_to_database(master_lineup_df, 'stg_whl_game_summary_lineup', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        du.write_df_to_database(master_coach_df, 'stg_whl_game_summary_coaches', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        du.write_df_to_database(master_goals_df, 'stg_whl_game_summary_goals', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        du.write_df_to_database(master_goals_plus_df, 'stg_whl_game_summary_goals_plus', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        du.write_df_to_database(master_goals_minus_df, 'stg_whl_game_summary_goals_minus', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
        
        master_meta_df = pu.empty_df()
        master_period_df = pu.empty_df()
        master_team_df = pu.empty_df()
        master_official_df = pu.empty_df()
        master_shootout_df = pu.empty_df()
        master_penalties_df = pu.empty_df()
        master_goalies_df = pu.empty_df()
        master_lineup_df = pu.empty_df()
        master_coach_df = pu.empty_df()
        master_goals_df = pu.empty_df()
        master_goals_plus_df = pu.empty_df()
        master_goals_minus_df = pu.empty_df()


# In[ ]:


du.write_df_to_database(master_meta_df, 'stg_whl_game_summary_meta', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
du.write_df_to_database(master_period_df, 'stg_whl_game_summary_periods', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
du.write_df_to_database(master_team_df, 'stg_whl_game_summary_team', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
du.write_df_to_database(master_official_df, 'stg_whl_game_summary_officials', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
du.write_df_to_database(master_shootout_df, 'stg_whl_game_summary_shootout', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
du.write_df_to_database(master_penalties_df, 'stg_whl_game_summary_penalties', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
du.write_df_to_database(master_goalies_df, 'stg_whl_game_summary_goalies', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
du.write_df_to_database(master_lineup_df, 'stg_whl_game_summary_lineup', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
du.write_df_to_database(master_coach_df, 'stg_whl_game_summary_coaches', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
du.write_df_to_database(master_goals_df, 'stg_whl_game_summary_goals', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
du.write_df_to_database(master_goals_plus_df, 'stg_whl_game_summary_goals_plus', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
du.write_df_to_database(master_goals_minus_df, 'stg_whl_game_summary_goals_minus', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)


# In[ ]:


print("END PROCESS: " + str(datetime.datetime.now()))


# In[ ]:


"""
'pimBench': {'visitor': 0, 'home': 0},
 'shotsByPeriod': {'visitor': {'1': 16, '2': 5, '3': 10, '4': 3},
  'home': {'1': 11, '2': 15, '3': 6, '4': 1}},
 'penaltyshots': {'visitor': [], 'home': []},
 'goalsByPeriod': {'visitor': {'1': 2, '2': 1, '3': 0, '4': 0},
  'home': {'1': 0, '2': 2, '3': 1, '4': 0}}}
"""

