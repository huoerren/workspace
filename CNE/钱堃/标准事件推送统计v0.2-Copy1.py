#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import openpyxl
import pymysql
import datetime,time


# In[2]:


d1=pd.read_csv(r'C:\Users\hp\Desktop\标准事件推送备份\new\事件推送\0708.csv')
d2=pd.read_csv(r'C:\Users\hp\Desktop\标准事件推送备份\new\事件推送\0809.csv')
d3=pd.read_csv(r'C:\Users\hp\Desktop\标准事件推送备份\new\事件推送\0910.csv')
d4=pd.read_csv(r'C:\Users\hp\Desktop\标准事件推送备份\new\事件推送\1011.csv')
d5=pd.read_csv(r'C:\Users\hp\Desktop\标准事件推送备份\new\事件推送\1112.csv')
d6=pd.read_csv(r'C:\Users\hp\Desktop\标准事件推送备份\new\事件推送\1213.csv')
d7=pd.read_csv(r'C:\Users\hp\Desktop\标准事件推送备份\new\事件推送\1314.csv')


# In[3]:


r=pd.concat([d1,d2,d3,d4,d5,d6,d7])


# In[4]:


d11=pd.read_csv(r'C:\Users\hp\Desktop\标准事件推送备份\new\正式单\0708.csv')
d22=pd.read_csv(r'C:\Users\hp\Desktop\标准事件推送备份\new\正式单\0809.csv')
d33=pd.read_csv(r'C:\Users\hp\Desktop\标准事件推送备份\new\正式单\0910.csv')
d44=pd.read_csv(r'C:\Users\hp\Desktop\标准事件推送备份\new\正式单\1011.csv')
d55=pd.read_csv(r'C:\Users\hp\Desktop\标准事件推送备份\new\正式单\1112.csv')
d66=pd.read_csv(r'C:\Users\hp\Desktop\标准事件推送备份\new\正式单\1213.csv')
d77=pd.read_csv(r'C:\Users\hp\Desktop\标准事件推送备份\new\正式单\1314.csv')


# In[5]:


rr=pd.concat([d11,d22,d33,d44,d55,d66,d77])


# In[6]:


total=pd.merge(r,rr,on=['pre_order_id','channel_id','des'],how='inner')


# In[7]:


total['customer_id']=total['customer_id'].apply(lambda x:"4PL" if x in(1151368,1181372,1151370,1181374) else "3PL")


# In[8]:


total=total.rename(columns={'customer_id':'type'})


# In[9]:


ARRIVE_FIRST_MILE=total[total['standard_event_code_list'].str.contains("ARRIVE_FIRST_MILE")].groupby(['type','channel_id','des']).agg({"pre_order_id":"nunique"}).reset_index()
DEPART_FIRST_MILE=total[total['standard_event_code_list'].str.contains("DEPART_FIRST_MILE")].groupby(['type','channel_id','des']).agg({"pre_order_id":"nunique"}).reset_index()
ARRIVE_CARRIER=total[total['standard_event_code_list'].str.contains("ARRIVE_CARRIER")].groupby(['type','channel_id','des']).agg({"pre_order_id":"nunique"}).reset_index()
DEPART_CARRIER=total[total['standard_event_code_list'].str.contains("DEPART_CARRIER")].groupby(['type','channel_id','des']).agg({"pre_order_id":"nunique"}).reset_index()
ARRIVE_AIRPORT=total[total['standard_event_code_list'].str.contains("ARRIVE_AIRPORT")].groupby(['type','channel_id','des']).agg({"pre_order_id":"nunique"}).reset_index()
DEPART_AIRPORT=total[total['standard_event_code_list'].str.contains("DEPART_AIRPORT")].groupby(['type','channel_id','des']).agg({"pre_order_id":"nunique"}).reset_index()
ARRIVE_DEST_AIRPORT=total[total['standard_event_code_list'].str.contains("ARRIVE_DEST_AIRPORT")].groupby(['type','channel_id','des']).agg({"pre_order_id":"nunique"}).reset_index()
ARRIVE_DEST_COUNTRY=total[total['standard_event_code_list'].str.contains("ARRIVE_DEST_COUNTRY")].groupby(['type','channel_id','des']).agg({"pre_order_id":"nunique"}).reset_index()
DEPART_DEST_CUSTOMS=total[total['standard_event_code_list'].str.contains("DEPART_DEST_CUSTOMS")].groupby(['type','channel_id','des']).agg({"pre_order_id":"nunique"}).reset_index()
ARRIVE_TRANSITHUB=total[total['standard_event_code_list'].str.contains("ARRIVE_TRANSITHUB")].groupby(['type','channel_id','des']).agg({"pre_order_id":"nunique"}).reset_index()
DEPART_TRANSITHUB=total[total['standard_event_code_list'].str.contains("DEPART_TRANSITHUB")].groupby(['type','channel_id','des']).agg({"pre_order_id":"nunique"}).reset_index()
DELIVERED=total[total['standard_event_code_list'].str.contains("DELIVERED")].groupby(['type','channel_id','des']).agg({"pre_order_id":"nunique"}).reset_index()
RETURNED_BY_CARRIER=total[total['standard_event_code_list'].str.contains("RETURNED_BY_CARRIER")].groupby(['type','channel_id','des']).agg({"pre_order_id":"nunique"}).reset_index()


# In[10]:


RETURNED_BY_CARRIER


# In[11]:


con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()


# In[12]:


def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df


# In[13]:


S1="""
SELECT 
channel_id,
des,
(case when customer_id in(1151368,1181372,1151370,1181374) then '4PL' else '3PL' end) as type,
count(*) as 票数
from lg_order
where 
platform='WISH_ONLINE'
AND order_time between '2021-11-07 16:00:00' and '2021-11-14 16:00:00'
AND is_deleted='n'
GROUP BY 1,2,3
"""


# In[14]:


d1=execude_sql(S1)


# In[15]:


ARRIVE_FIRST_MILE=ARRIVE_FIRST_MILE.rename(columns={'pre_order_id':'ARRIVE_FIRST_MILE'})
DEPART_FIRST_MILE=DEPART_FIRST_MILE.rename(columns={'pre_order_id':'DEPART_FIRST_MILE'})
ARRIVE_CARRIER=ARRIVE_CARRIER.rename(columns={'pre_order_id':'ARRIVE_CARRIER'})
DEPART_CARRIER=DEPART_CARRIER.rename(columns={'pre_order_id':'DEPART_CARRIER'})
ARRIVE_AIRPORT=ARRIVE_AIRPORT.rename(columns={'pre_order_id':'ARRIVE_AIRPORT'})
DEPART_AIRPORT=DEPART_AIRPORT.rename(columns={'pre_order_id':'DEPART_AIRPORT'})
ARRIVE_DEST_AIRPORT=ARRIVE_DEST_AIRPORT.rename(columns={'pre_order_id':'ARRIVE_DEST_AIRPORT'})
ARRIVE_DEST_COUNTRY=ARRIVE_DEST_COUNTRY.rename(columns={'pre_order_id':'ARRIVE_DEST_COUNTRY'})
DEPART_DEST_CUSTOMS=DEPART_DEST_CUSTOMS.rename(columns={'pre_order_id':'DEPART_DEST_CUSTOMS'})
ARRIVE_TRANSITHUB=ARRIVE_TRANSITHUB.rename(columns={'pre_order_id':'ARRIVE_TRANSITHUB'})
DEPART_TRANSITHUB=DEPART_TRANSITHUB.rename(columns={'pre_order_id':'DEPART_TRANSITHUB'})
DELIVERED=DELIVERED.rename(columns={'pre_order_id':'DELIVERED'})
RETURNED_BY_CARRIER=RETURNED_BY_CARRIER.rename(columns={'pre_order_id':'RETURNED_BY_CARRIER'})


# In[16]:


r1=pd.merge(d1,ARRIVE_FIRST_MILE,on=['type','channel_id','des'],how='left')
r1=pd.merge(r1,DEPART_FIRST_MILE,on=['type','channel_id','des'],how='left')
r1=pd.merge(r1,ARRIVE_CARRIER,on=['type','channel_id','des'],how='left')
r1=pd.merge(r1,DEPART_CARRIER,on=['type','channel_id','des'],how='left')
r1=pd.merge(r1,ARRIVE_AIRPORT,on=['type','channel_id','des'],how='left')
r1=pd.merge(r1,DEPART_AIRPORT,on=['type','channel_id','des'],how='left')
r1=pd.merge(r1,ARRIVE_DEST_AIRPORT,on=['type','channel_id','des'],how='left')
r1=pd.merge(r1,ARRIVE_DEST_COUNTRY,on=['type','channel_id','des'],how='left')
r1=pd.merge(r1,DEPART_DEST_CUSTOMS,on=['type','channel_id','des'],how='left')
r1=pd.merge(r1,ARRIVE_TRANSITHUB,on=['type','channel_id','des'],how='left')
r1=pd.merge(r1,DEPART_TRANSITHUB,on=['type','channel_id','des'],how='left')
r1=pd.merge(r1,DELIVERED,on=['type','channel_id','des'],how='left')
r1=pd.merge(r1,RETURNED_BY_CARRIER,on=['type','channel_id','des'],how='left')


# In[17]:


r1=r1.replace(np.nan,0)


# In[18]:


S2="""
select distinct channel_id,channel_code
from lg_order
where platform='WISH_ONLINE'
AND order_time between '2021-11-07 16:00:00' and '2021-11-14 16:00:00'
AND is_deleted='n'
"""


# In[19]:


d2=execude_sql(S2)


# In[20]:


channel_dict=d2[['channel_id','channel_code']].to_dict(orient ='series')


# In[21]:


channel_dict=d2.set_index('channel_id')['channel_code'].to_dict()


# In[22]:


r1['channel_id']=r1['channel_id'].map(channel_dict)


# In[23]:


code_list=['ARRIVE_FIRST_MILE','DEPART_FIRST_MILE','ARRIVE_CARRIER','DEPART_CARRIER','ARRIVE_AIRPORT','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','ARRIVE_DEST_COUNTRY','DEPART_DEST_CUSTOMS','ARRIVE_TRANSITHUB','DEPART_TRANSITHUB','DELIVERED','RETURNED_BY_CARRIER']


# In[24]:


for code in code_list:
    r1[code]=r1[code]/r1['票数']


# In[25]:


r1=r1.rename(columns={'type':'业务类型'})


# In[26]:


r1.to_excel(r'C:\Users\hp\Desktop\标准事件推送率.xlsx',index=False)


# In[ ]:





# In[ ]:




