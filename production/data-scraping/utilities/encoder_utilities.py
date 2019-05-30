#!/usr/bin/env python
# coding: utf-8

# In[1]:


import base64
import sys
sys.path.append('../../../')

import settings as st

def encode(string):
    
    string = string + st.encoder
    
    encoded = base64.b64encode(string.encode('utf-8'))
    return encoded
    
def decode(string):
    
    decoded = base64.b64decode(string).decode('utf-8')[:-len(st.encoder)]
    return str(decoded)

