#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import openpyxl
import pymysql
import datetime,time


# In[2]:


con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()


# In[3]:


def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df


# In[4]:


S1="""
select pre_order_id,channel_id,des,platform,customer_id
from lg_order
where
platform='WISH_ONLINE'
and order_time between '2021-11-21 16:00:00' and '2021-11-22 16:00:00'
and is_deleted='n'
"""


# In[5]:


d1=execude_sql(S1)


# In[6]:


S2="""
select pre_order_id,channel_id,des,platform,customer_id
from lg_order
where
platform='WISH_ONLINE'
and order_time between '2021-11-22 16:00:00' and '2021-11-23 16:00:00'
and is_deleted='n'
"""

S3="""
select pre_order_id,channel_id,des,platform,customer_id
from lg_order
where
platform='WISH_ONLINE'
and order_time between '2021-11-23 16:00:00' and '2021-11-24 16:00:00'
and is_deleted='n'
"""

S4="""
select pre_order_id,channel_id,des,platform,customer_id
from lg_order
where
platform='WISH_ONLINE'
and order_time between '2021-11-24 16:00:00' and '2021-11-25 16:00:00'
and is_deleted='n'
"""

S5="""
select pre_order_id,channel_id,des,platform,customer_id
from lg_order
where
platform='WISH_ONLINE'
and order_time between '2021-11-25 16:00:00' and '2021-11-26 16:00:00'
and is_deleted='n'
"""


# In[7]:


S6="""
select pre_order_id,channel_id,des,platform,customer_id
from lg_order
where
platform='WISH_ONLINE'
and order_time between '2021-11-26 16:00:00' and '2021-11-27 16:00:00'
and is_deleted='n'
"""


# In[8]:


S7="""
select pre_order_id,channel_id,des,platform,customer_id
from lg_order
where
platform='WISH_ONLINE'
and order_time between '2021-11-27 16:00:00' and '2021-11-28 16:00:00'
and is_deleted='n'
"""


# In[9]:


d2=execude_sql(S2)


# In[10]:


d3=execude_sql(S3)


# In[11]:


d4=execude_sql(S4)


# In[12]:


d5=execude_sql(S5)


# In[13]:


d6=execude_sql(S6)


# In[14]:


d7=execude_sql(S7)


# In[15]:


d1.to_csv(r'C:\Users\hp\Desktop\标准事件推送备份\1122-28\正式单\2122.csv',index=False)
d2.to_csv(r'C:\Users\hp\Desktop\标准事件推送备份\1122-28\正式单\2223.csv',index=False)
d3.to_csv(r'C:\Users\hp\Desktop\标准事件推送备份\1122-28\正式单\2324.csv',index=False)
d4.to_csv(r'C:\Users\hp\Desktop\标准事件推送备份\1122-28\正式单\2425.csv',index=False)
d5.to_csv(r'C:\Users\hp\Desktop\标准事件推送备份\1122-28\正式单\2526.csv',index=False)


# In[16]:


d6.to_csv(r'C:\Users\hp\Desktop\标准事件推送备份\1122-28\正式单\2627.csv',index=False)
d7.to_csv(r'C:\Users\hp\Desktop\标准事件推送备份\1122-28\正式单\2728.csv',index=False)


# In[ ]:




