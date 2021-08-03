#!/usr/bin/env python
# coding: utf-8

# In[2]:


def return_json(url):
    import requests
    r = requests.get(url=url)
    return (r.json())


# In[ ]:




