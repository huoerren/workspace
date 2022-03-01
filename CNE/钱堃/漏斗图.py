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


days="BETWEEN '2021-11-19 16:00:00' and '2021-11-30 16:00:00'"
qd='CNE全球特惠'
des='US'


# In[5]:


S1="""
select '首扫' as 环节,count(*) 总票量
from lg_order lgo
where des='{}'
and channel_code='{}'
and gmt_create {}
and lgo.platform='WISH_ONLINE' 
AND lgo.customer_id in (1151368,1151370,1181372,1181374) 
and is_deleted='n'
""".format(des,qd,days)


# In[6]:


d1=execude_sql(S1)


# In[7]:


d1


# In[8]:


S2="""
select '装车' as 环节,count(distinct order_no) 总票量
from
lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
where
lgo.des='{}'
and lgo.channel_code='{}'
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE' 
AND lgo.customer_id in (1151368,1151370,1181372,1181374) 
and lgo.is_deleted='n'
and lbor.order_id=lgo.id
and lbor.is_deleted='n'
and lbor.bag_id=tbe.bag_id
and tbe.is_deleted='n'
and tbe.event_code='DEPS' 
""".format(des,qd,days)


# In[9]:


d2=execude_sql(S2)


# In[10]:


d2


# In[11]:


S3="""
SELECT 
'起飞' as 环节,count(distinct order_no) 总票量
FROM
lg_order lgo,track_mawb_event tme
where
lgo.des='{}'
and lgo.channel_code='{}'
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE' 
AND lgo.customer_id in (1151368,1151370,1181372,1181374) 
and lgo.is_deleted='n'
and lgo.mawb_id=tme.mawb_id
and event_code in ("SDFO","DEPC","DEPT","LKJC") 
""".format(des,qd,days)


# In[12]:


d3=execude_sql(S3)


# In[13]:


d3


# In[14]:


S44="""
SELECT 
'落地1' as 环节,count(distinct order_no) 总票量
FROM
lg_order lgo,track_mawb_event tme
where
lgo.des='{}'
and lgo.channel_code='{}'
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE' 
AND lgo.customer_id in (1151368,1151370,1181372,1181374) 
and lgo.is_deleted='n'
and lgo.mawb_id=tme.mawb_id
and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
""".format(des,qd,days)

S444="""
SELECT 
'落地2' as 环节,count(distinct order_no) 总票量
FROM
lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
where
lgo.des='{}'
and lgo.channel_code='{}'
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE' 
AND lgo.customer_id in (1151368,1151370,1181372,1181374) 
and lgo.is_deleted='n'
and lbor.order_id=lgo.id
and lbor.is_deleted='n'
and lbor.bag_id=tbe.bag_id
and tbe.is_deleted='n'
and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
""".format(des,qd,days)

S4444="""
SELECT 
'落地3' as 环节,count(distinct order_no) 总票量
FROM
lg_order lgo,track_order_event toe
where
lgo.des='{}'
and lgo.channel_code='{}'
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE' 
AND lgo.customer_id in (1151368,1151370,1181372,1181374) 
and lgo.is_deleted='n'
and toe.order_id=lgo.id
and toe.is_deleted='n'
and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
""".format(des,qd,days)


# In[15]:


d44=execude_sql(S44)
d444=execude_sql(S444)
d4444=execude_sql(S4444)


# In[16]:


d4=pd.DataFrame()
d4['环节']=None
d4['总票量']=None
d4.loc[0,'环节']='落地'
d4.loc[0,'总票量']=d44.loc[0,'总票量']+d444.loc[0,'总票量']+d4444.loc[0,'总票量']


# In[17]:


d4


# In[18]:


S55="""
SELECT 
'清关' as 环节,count(distinct order_no) 总票量
FROM
lg_order lgo,track_mawb_event tme
where
lgo.des='{}'
and lgo.channel_code='{}'
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE' 
AND lgo.customer_id in (1151368,1151370,1181372,1181374) 
and lgo.is_deleted='n'
and tme.mawb_id=lgo.mawb_id
and tme.is_deleted='n'
and event_code in ("IRCM","PVCS","IRCN","RFIC")
""".format(des,qd,days)

S555="""
SELECT 
'清关2' as 环节,count(distinct order_no) 总票量
FROM
lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
where
lgo.des='{}'
and lgo.channel_code='{}'
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE' 
AND lgo.customer_id in (1151368,1151370,1181372,1181374) 
and lgo.is_deleted='n'
and lbor.order_id=lgo.id
and lbor.is_deleted='n'
and lbor.bag_id=tbe.bag_id
and tbe.is_deleted='n'
and event_code in ("IRCM","PVCS","IRCN","RFIC")
""".format(des,qd,days)

S5555="""
SELECT 
'清关3' as 环节,count(distinct order_no) 总票量
FROM
lg_order lgo,track_order_event toe
where
lgo.des='{}'
and lgo.channel_code='{}'
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE' 
AND lgo.customer_id in (1151368,1151370,1181372,1181374) 
and lgo.is_deleted='n'
and toe.order_id=lgo.id
and toe.is_deleted='n'
and event_code in ("IRCM","PVCS","IRCN","RFIC")
""".format(des,qd,days)


# In[19]:


S55="""
SELECT 
order_no
FROM
lg_order lgo,track_mawb_event tme
where
lgo.des='{}'
and lgo.channel_code='{}'
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE' 
AND lgo.customer_id in (1151368,1151370,1181372,1181374) 
and lgo.is_deleted='n'
and tme.mawb_id=lgo.mawb_id
and tme.is_deleted='n'
and event_code in ("IRCM","PVCS","IRCN","RFIC")
""".format(des,qd,days)

S555="""
SELECT 
order_no
FROM
lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
where
lgo.des='{}'
and lgo.channel_code='{}'
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE' 
AND lgo.customer_id in (1151368,1151370,1181372,1181374) 
and lgo.is_deleted='n'
and lbor.order_id=lgo.id
and lbor.is_deleted='n'
and lbor.bag_id=tbe.bag_id
and tbe.is_deleted='n'
and event_code in ("IRCM","PVCS","IRCN","RFIC")
""".format(des,qd,days)

S5555="""
SELECT 
order_no
FROM
lg_order lgo,track_order_event toe
where
lgo.des='{}'
and lgo.channel_code='{}'
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE' 
AND lgo.customer_id in (1151368,1151370,1181372,1181374) 
and lgo.is_deleted='n'
and toe.order_id=lgo.id
and toe.is_deleted='n'
and event_code in ("IRCM","PVCS","IRCN","RFIC")
""".format(des,qd,days)


# In[20]:


d55=execude_sql(S55)
d555=execude_sql(S555)
d5555=execude_sql(S5555)

d5c=pd.concat([d55,d555,d5555])
d5c=d5c.drop_duplicates(subset='order_no')


# In[21]:


d5=pd.DataFrame()
d5['环节']=None
d5['总票量']=None
d5.loc[0,'环节']='清关'
d5.loc[0,'总票量']=d5c.shape[0]


# In[22]:


# d55=execude_sql(S55)
# d555=execude_sql(S555)
# d5555=execude_sql(S5555)
# d5=pd.DataFrame()
# d5['环节']=None
# d5['总票量']=None
# d5.loc[0,'环节']='清关'
# d5.loc[0,'总票量']=d55.loc[0,'总票量']+d555.loc[0,'总票量']+d5555.loc[0,'总票量']


# In[23]:


# d5=execude_sql(S55)


# In[24]:


S6="""
SELECT 
'交付' as 环节,count(distinct order_no) 总票量
FROM
lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
where
lgo.des='{}'
and lgo.channel_code='{}'
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE' 
AND lgo.customer_id in (1151368,1151370,1181372,1181374) 
and lgo.is_deleted='n'
and lbor.order_id=lgo.id
and lbor.is_deleted='n'
and lbor.bag_id=tbe.bag_id
and tbe.is_deleted='n'
and event_code in ("JFMD")
""".format(des,qd,days)


# In[25]:


d6=execude_sql(S6)


# In[26]:


d6


# In[27]:


S7="""
SELECT 
'妥投' as 环节,count(distinct order_no) 总票量
FROM
lg_order lgo
where
lgo.des='{}'
and lgo.channel_code='{}'
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE' 
AND lgo.customer_id in (1151368,1151370,1181372,1181374)
and order_status=3
and lgo.is_deleted='n'
""".format(des,qd,days)


# In[28]:


d7=execude_sql(S7)


# In[29]:


d7


# In[30]:


result=pd.concat([d1,d2,d3,d4,d5,d6,d7])


# In[31]:


result


# In[32]:


# result.loc[result['环节']=='装车','总票量']=4657


# In[33]:


result['百分比']=result['总票量']/d1.loc[0,'总票量']


# In[34]:


result['百分比']=result['百分比']*100


# In[35]:


result['百分比']=result['百分比'].apply(lambda x:round(x,0))


# In[36]:


# result['百分比']=result['百分比'].astype('str')+'%'


# In[37]:


from pyecharts import Funnel


# In[38]:


funnel = Funnel("{}-{}".format(qd,des), width=350, height=400, title_pos='center')
# funnel = Funnel("漏斗图")
funnel.add("", result['环节'].tolist(),
           result['百分比'].tolist(),
           is_label_show=True,
           label_formatter='{b}{c}%',
           label_pos="outside",
           legend_orient='vertical',
           legend_pos='left')
funnel.render("funnel.html")


# In[39]:


funnel


# In[ ]:




