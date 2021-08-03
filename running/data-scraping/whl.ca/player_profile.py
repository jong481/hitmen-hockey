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


key = "41b145a848f4bd67"


# In[ ]:


print("START PROCCESS {0}".format(str(datetime.datetime.now())))


# In[ ]:


sql = "SELECT DISTINCT player_id FROM whl_team_roster_by_season WHERE player_id IS NOT NULL ORDER BY player_id ASC"
res = du.query_database_to_list(db_sys, db_user, db_pass, db_host, db_port, db_db, sql)


# In[ ]:


def draft_df_cleanup(df):
    df=df.rename(columns = {'id':'draft_id'})
    df["draft_id"] = pd.to_numeric(df["draft_id"])
    df['draft_date'] = df.draft_date.apply(lambda x: pu.clearDate(x))
    df["draft_mode"] = pd.to_numeric(df["draft_mode"])
    df["draft_rank"] = pd.to_numeric(df["draft_rank"])
    df["draft_round"] = pd.to_numeric(df["draft_round"])
    df["draft_type_id"] = pd.to_numeric(df["draft_type_id"])
    
    return df

def master_df_cleanup(df):   
    """
    Clean up Data
    """
    df['birthdate'] = df.birthdate.apply(lambda x: pu.clearDate(x))
    df['rookie'] = df.rookie.apply(lambda x: pu.clearNanToNone(x))
    df['weight'] = df.weight.apply(lambda x: pu.ifNumeric(x))

    """
    Fix Data Types
    """
    df["active"] = pd.to_numeric(df["active"])
    df["jersey_number"] = pd.to_numeric(df["jersey_number"])
    df["most_recent_team_id"] = pd.to_numeric(df["most_recent_team_id"])
    df["rookie"] = pd.to_numeric(df["rookie"])
    df["weight"] = pd.to_numeric(df["weight"])
    df["birthdate"] = pd.to_datetime(df["birthdate"])
    
    return df


# In[ ]:


master_df = pu.empty_df()
master_draft_df = pu.empty_df()
# master_bio_df = empty_df


# In[ ]:


i = 1
for r in res:
    player_id = str(r['player_id']).replace(".0", "")
        
    print(i, player_id)
        
    url = "http://lscluster.hockeytech.com/feed/?feed=modulekit&view=player&key={0}&fmt=json&client_code=whl&lang=en&player_id={1}&category=profile".format(key, player_id)
        
    json_data = wu.return_json(url)
    json_obj = json_data['SiteKit']['Player']
    
    """
    DRAFT DATA
    """
    json_draft = json_obj['draft']
    for x in range(0, len(json_draft)):
        draft_df = pd.DataFrame(json_draft[x], index=[0])
        draft_df['player_id'] = player_id
    
        master_draft_df = master_draft_df.append(draft_df)
    
    # BIO DATA - CURRENTLY DROPPING
    # json_bio = json_obj['bio']
    # print(len(json_bio))
    # bio_df = pd.DataFrame(json_bio, index=[0])
    # bio_df['player_id'] = player_id
    
    """
    MAIN DF
    """
    json_obj.pop('bio')
    json_obj.pop('draft')
    try: json_obj.pop('show_on_roster')
    except: pass
    df = pd.DataFrame(json_obj, index=[0])
    df['player_id'] = player_id
    
    master_df = master_df.append(df)
    
    i+=1


# In[ ]:


master_draft_df = draft_df_cleanup(master_draft_df)
master_df = master_df_cleanup(master_df)


# In[ ]:


du.write_df_to_database(master_df, 'whl_player_profile', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)
du.write_df_to_database(master_draft_df, 'whl_player_profile_draft', db_sys, db_user, db_pass, db_host, db_port, db_db, 'append', False)


# In[ ]:


print("END PROCCESS {0}".format(str(datetime.datetime.now())))


# In[ ]:




