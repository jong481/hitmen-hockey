#!/usr/bin/env python
# coding: utf-8

# In[ ]:


write_mode = 'replace'
target_table = 'whl_team_roster_by_season'


# In[ ]:


import pandas as pd
import numpy as np
import sys
sys.path.append('/home/do-user/projects/hitmen_hockey/')

import settings as st


# In[ ]:


sys.path.append('/home/do-user/projects/hitmen_hockey/production/data-scraping/utilities')

import web_utilities as wu
import database_utilities as du
import encoder_utilities as eu
import pd_utilities as pu


# In[ ]:


db_host = st.database['local']['host']
db_port = st.database['local']['port']
db_user = st.database['local']['user']
db_pass = st.database['local']['pass']
db_sys = st.database['local']['system']
db_db = st.database['local']['db']


# In[ ]:


key = "41b145a848f4bd67"


# In[ ]:


sql = "SELECT * FROM whl_season_list"
res = du.query_database_to_list(db_sys, db_user, db_pass, db_host, db_port, db_db, sql)


# In[ ]:


master_player_df = pd.DataFrame()


# In[ ]:


"""
Loop through each season
"""
for r in res:
    season_id = r['season_id']
    
    sql = "SELECT * FROM whl_team_list_by_season where season_id = {0}".format(season_id)
    team_list = du.query_database_to_list(db_sys, db_user, db_pass, db_host, db_port, db_db, sql)
    
    """
    Loop through each team
    """
    for team in team_list:
                                
        team_id = team['id']
        
        print("SEASON: " + str(season_id), "TEAM: " + str(team_id))
    
        url = 'http://lscluster.hockeytech.com/feed/?feed=modulekit&view=roster&key={0}&fmt=json&client_code=whl&lang=en&season_id={1}&team_id={2}&fmt=json'.format(key, season_id, team_id)
        json_data = wu.return_json(url)
    
        """
        Split Player and Coach data
        """
        player_data = []
        coaches_data = []

        data_len = len(json_data['SiteKit']['Roster'])

        i = 1
        for data in json_data['SiteKit']['Roster']:

            if i < data_len:
                player_data.append(data)
            else:
                coaches_data.append(data)

            i+=1
            
        if len(player_data) > 0:
            
            player_df = pd.DataFrame(player_data)
            coaches_df = pd.DataFrame(coaches_data)

            """
            Append Team and Season ID
            """
            player_df['team_id'] = team_id
            player_df['season_id'] = season_id

            """
            Extract Draft Info Data
            """
            try:
                player_df['temp'] = pd.DataFrame(player_df['draftinfo'].apply(pd.Series)[0])

                col_list = []
                for c in pd.DataFrame(player_df.temp.apply(lambda x: pu.clearNan(x)).values.tolist()).columns:
                    col_list.append(c)

                player_df[col_list] = pd.DataFrame(player_df.temp.apply(lambda x: pu.clearNan(x)).values.tolist())
                player_df = player_df.drop(['temp'], axis=1)

                player_df = player_df.drop(['draftinfo'], axis=1)

                player_df['draft_date'] = player_df.draft_date.apply(lambda x: pu.clearDate(x))
                player_df["draft_year"] = pd.to_numeric(player_df["draft_year"])
                player_df["draft_date"] = pd.to_datetime(player_df["draft_date"])
            
            except Exception as e:
                player_df = player_df.drop(['draftinfo'], axis=1)

            """
            Clean up Data using Lambda Functions
            """
            player_df['isRookie'] = player_df.isRookie.apply(lambda x: pu.clearDate(x))
            player_df['isRookie'] = player_df.isRookie.apply(lambda x: pu.clearNbsp(x))
            player_df['birthplace'] = player_df.birthplace.apply(lambda x: pu.clearDate(x))
            player_df['birthplace'] = player_df.birthplace.apply(lambda x: pu.clearComma(x))
            player_df['rawbirthdate'] = player_df.rawbirthdate.apply(lambda x: pu.clearDate(x))

            """
            Fix Data Types
            """
            player_df["active"] = pd.to_numeric(player_df["active"])
            player_df["position_id"] = pd.to_numeric(player_df["position_id"])
            player_df["rookie"] = pd.to_numeric(player_df["rookie"])
            player_df["tp_jersey_number"] = pd.to_numeric(player_df["tp_jersey_number"])
            try: player_df["weight"] = pd.to_numeric(player_df["weight"])
            except Exception as e: print(e)
            player_df["player_id"] = pd.to_numeric(player_df["player_id"])
            player_df["person_id"] = pd.to_numeric(player_df["person_id"])
            player_df["id"] = pd.to_numeric(player_df["id"])
            player_df["hidden"] = pd.to_numeric(player_df["hidden"])
            player_df["latest_team_id"] = pd.to_numeric(player_df["latest_team_id"])
            player_df["playerId"] = pd.to_numeric(player_df["playerId"])
            try: player_df["w"] = pd.to_numeric(player_df["w"])
            except Exception as e: print(e)
            player_df["birthdate"] = pd.to_datetime(player_df["birthdate"])
            player_df["rawbirthdate"] = pd.to_datetime(player_df["rawbirthdate"])

            master_player_df = master_player_df.append(player_df)


# In[ ]:


master_player_df = master_player_df.reset_index(drop=True)


# In[ ]:


if write_mode == 'append':
    index = du.get_table_new_id(db_sys, db_user, db_pass, db_host, db_port, db_db, target_table, 'index')
    master_player_df.insert(0, 'index', master_player_df.index + index)
else:
    master_player_df.insert(0, 'index', master_player_df.index)


# In[ ]:


du.write_df_to_database(master_player_df, target_table, db_sys, db_user, db_pass, db_host, db_port, db_db, write_mode, False)

