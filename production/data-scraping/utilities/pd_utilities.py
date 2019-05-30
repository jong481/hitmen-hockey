#!/usr/bin/env python
# coding: utf-8

# In[16]:


def clearNan(x):
    if str(x) == 'nan':
        return {}
    else:
        return x

def clearNanToNone(x):
    if (str(x).lower().strip()) == 'nan' or (str(x).strip() == '') or (x == None):
        return None
    else:
        return x
    
def clearDate(x):
    if str(x) == '0000-00-00':
        return None
    else: 
        return x
    
def clearNbsp(x):
    if str(x) == '&nbsp;':
        return None
    else:
        return x
    
def clearComma(x):
    if str(x).strip() == ',':
        return None
    else:
        return x


# In[ ]:




