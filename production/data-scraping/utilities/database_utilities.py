#!/usr/bin/env python
# coding: utf-8

# In[13]:


import warnings
warnings.filterwarnings('ignore')


# In[2]:


from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import psycopg2.extras

def write_df_to_database(df, tableName, system, user, pw, host, port, database, ifExists='replace', indexBoolean=False):
    engine = create_engine('{0}://{1}:{2}@{3}:{4}/{5}'.format(system, user, pw, host, port, database))
    df.to_sql(tableName, engine, if_exists=ifExists,index=indexBoolean)
    
def query_database_to_list(system, user, pw, host, port, database, query):
    connection = psycopg2.connect(user=user, password=pw, host=host, port=port, database=database)
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    cursor.execute(query)
    res = cursor.fetchall()
    connection.close()
    
    return res

def query_database_to_df(system, user, pw, host, port, database, query):
    engine = create_engine('{0}://{1}:{2}@{3}:{4}/{5}'.format(system, user, pw, host, port, database))
    df = pd.read_sql(query, engine)
    
    return df

def get_table_new_id(system, user, pw, host, port, database, table, id_column):
    connection = psycopg2.connect(user=user, password=pw, host=host, port=port, database=database)
    cursor = connection.cursor()
    
    try:
        cursor.execute("SELECT MAX({0}) as max FROM {1}".format(id_column, table))
        res = cursor.fetchall() 
        connection.close()
    
        max_index = str(res[0][0]).strip()
    except:
        connection.close()
        
        max_index = 'None'
    
    if max_index == 'None':
        max_index = 0
    else:
        max_index = int(max_index) + 1
    
    return max_index

def truncate_table(system, user, pw, host, port, database, table):
    connection = psycopg2.connect(user=user, password=pw, host=host, port=port, database=database)
    cursor = connection.cursor()
    
    try:
        cursor.execute("TRUNCATE TABLE {0}".format(table))
        connection.commit()
        connection.close()
    
        return True
    except:
        connection.close()
        return False


# In[ ]:




