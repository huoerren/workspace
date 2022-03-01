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


# 按月统计渠道路向各节点的推送数
S1="""
select 
month(DATE_ADD(lgo.gmt_create,interval 8 hour)) 揽收周期,
wtd.des,
wtd.channel_id,
-- 客户的节点
wtd.tracking_status,
count(distinct wtd.pre_order_id) 推送数
from platform_track_data wtd inner join lg_order lgo on lgo.pre_order_id=wtd.pre_order_id
where wtd.is_deleted='n'
and wtd.order_time>'2021-09-30 16:00:00'
and wtd.platform='TIKTOK'
and wtd.has_push=1
group by 1,2,3,4
"""


# In[5]:


d1=execude_sql(S1)


# In[6]:


d1


# In[7]:


# 按周统计渠道路向各节点的推送数
S2="""
select 
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 揽收周期,
wtd.des,
wtd.channel_id,
wtd.tracking_status,
count(distinct wtd.pre_order_id) 推送数
from platform_track_data wtd inner join lg_order lgo on lgo.pre_order_id=wtd.pre_order_id
where wtd.is_deleted='n'
and wtd.order_time>'2021-09-30 16:00:00'
and wtd.platform='TIKTOK'
and wtd.has_push=1
group by 1,2,3,4
"""


# In[8]:


d2=execude_sql(S2)


# In[9]:


d2


# In[10]:


# channel_id和channel_code映射表
S3="""
select distinct channel_id,channel_code
from lg_order
where order_time >= '2021-09-30 16:00:00'
and platform='TIKTOK'
AND is_deleted='n'
"""


# In[11]:


d3=execude_sql(S3)


# In[12]:


d3


# In[13]:


channel_dict=d3.set_index('channel_id')['channel_code'].to_dict()


# In[14]:


# S4="""
# select
# 揽收周期,
# des,
# channel_id,
# "ALL" as tracking_status,
# count(*) 推送数
# from
# (
# select 
# month(DATE_ADD(lgo.gmt_create,interval 8 hour)) 揽收周期,
# wtd.des des,
# wtd.channel_id channel_id,
# wtd.pre_order_id pre_order_id
# from platform_track_data wtd force index(Index_preorderid),lg_order lgo
# where wtd.is_deleted='n'
# and wtd.order_time>'2021-10-30 16:00:00'
# and wtd.platform='TIKTOK'
# and wtd.has_push=1
# and wtd.tracking_status in ('cb_trans_inbound','cb_trans_outbound','cb_transport_originalport','cb_transport_departed','cb_transport_arrived','cb_imcustoms_finished','station_inbound','signed_personally')
# and lgo.pre_order_id=wtd.pre_order_id
# group by 1,2,3,4
# HAVING (count(distinct tracking_status)=8)
# ) as t
# group by 1,2,3
# """


# In[15]:


# d4=execude_sql(S4)


# 计算物流完整率

# In[16]:


# 取非退件的pre_order_id
SK="""
Select
pre_order_id
from lg_order
where order_time>'2021-09-30 16:00:00'
and platform='TIKTOK'
and order_status !=8
"""


# In[17]:


dk=execude_sql(SK)


# In[18]:


# pre_order_id组成元组
str2=tuple(dk['pre_order_id'].tolist())


# In[19]:


# 计算非退件票件物流完整率
S5="""
select
揽收周期,
des,
channel_id,
"ALL" as tracking_status,
count(*) 推送数
from
(
select 
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 揽收周期,
wtd.des des,
wtd.channel_id channel_id,
wtd.pre_order_id pre_order_id
from platform_track_data wtd force index(Index_preorderid),lg_order lgo
where 
wtd.is_deleted='n'
and wtd.pre_order_id in {}
and wtd.has_push=1
and wtd.tracking_status in ('cb_trans_inbound','cb_trans_outbound','cb_transport_originalport','cb_transport_departed','cb_transport_arrived','cb_imcustoms_finished','station_inbound','signed_personally')
and lgo.pre_order_id=wtd.pre_order_id
group by 1,2,3,4
HAVING (count(distinct tracking_status)=8)
) as t
group by 1,2,3
""".format(str2)


# In[20]:


d5=execude_sql(S5)


# In[21]:


S511="""
select
揽收周期,
des,
channel_id,
"ALL" as tracking_status,
count(*) 推送数
from
(
select 
MONTH(DATE_ADD(lgo.gmt_create,interval 8 hour)) 揽收周期,
wtd.des des,
wtd.channel_id channel_id,
wtd.pre_order_id pre_order_id
from platform_track_data wtd force index(Index_preorderid),lg_order lgo
where 
wtd.is_deleted='n'
and wtd.pre_order_id in {}
and wtd.has_push=1
and wtd.tracking_status in ('cb_trans_inbound','cb_trans_outbound','cb_transport_originalport','cb_transport_departed','cb_transport_arrived','cb_imcustoms_finished','station_inbound','signed_personally')
and lgo.pre_order_id=wtd.pre_order_id
group by 1,2,3,4
HAVING (count(distinct tracking_status)=8)
) as t
group by 1,2,3
""".format(str2)


# In[22]:


d511=execude_sql(S511)


# In[23]:


# 取退件的pre_order_id
S51="""
Select
pre_order_id
from lg_order
where order_time>'2021-09-30 16:00:00'
and platform='TIKTOK'
and order_status=8
"""


# In[24]:


d51=execude_sql(S51)


# In[25]:


str1=tuple(d51['pre_order_id'].tolist())


# In[26]:


# 退件票件物流完整率
S52="""
select
揽收周期,
des,
channel_id,
"ALL" as tracking_status,
count(*) 推送数
from
(
select 
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 揽收周期,
wtd.des des,
wtd.channel_id channel_id,
wtd.pre_order_id pre_order_id
from platform_track_data wtd force index(Index_preorderid),lg_order lgo
where 
wtd.is_deleted='n'
and wtd.pre_order_id in {}
and wtd.has_push=1
and wtd.tracking_status in ('cb_trans_inbound','cb_trans_return')
and lgo.pre_order_id=wtd.pre_order_id
group by 1,2,3,4
HAVING (count(distinct tracking_status)=2)
) as t
group by 1,2,3
""".format(str1)


# In[27]:


d52=execude_sql(S52)


# In[28]:


S53="""
select
揽收周期,
des,
channel_id,
"ALL" as tracking_status,
count(*) 推送数
from
(
select 
month(DATE_ADD(lgo.gmt_create,interval 8 hour)) 揽收周期,
wtd.des des,
wtd.channel_id channel_id,
wtd.pre_order_id pre_order_id
from platform_track_data wtd force index(Index_preorderid),lg_order lgo
where 
wtd.is_deleted='n'
and wtd.pre_order_id in {}
and wtd.has_push=1
and wtd.tracking_status in ('cb_trans_inbound','cb_trans_return')
and lgo.pre_order_id=wtd.pre_order_id
group by 1,2,3,4
HAVING (count(distinct tracking_status)=2)
) as t
group by 1,2,3
""".format(str1)


# In[29]:


d53=execude_sql(S53)


# In[30]:


# 月周数据合并
d5=pd.concat([d5,d52])


# In[31]:


d5=pd.concat([d5,d53])


# In[32]:


d5=pd.concat([d5,d511])


# In[33]:


# 合并退件及非退件的物流完整票数
d5=d5.groupby(by=['揽收周期','des','channel_id','tracking_status'])['推送数'].sum().reset_index()


# In[34]:


# 合并所有数据
d_t=pd.concat([d1,d2,d5])


# In[35]:


# 映射channel_id
d_t['channel_id']=d_t['channel_id'].map(channel_dict)


# In[36]:


# 查询得到的节点去重后做成列表
list1=list(set(d_t.tracking_status.tolist()))


# In[37]:


# 浅拷贝，不影响list1的值，list2做表头
list2=list1.copy()
list2.append('channel_id')
list2.append('des')
list2.append('揽收周期')


# In[38]:


df1=pd.DataFrame(columns=list2)


# In[39]:


# 重置索引
d_t=d_t.reset_index().drop(['index'],axis=1)


# In[40]:


# 如不存在次二节点,默认置零
df1['cb_trans_return']=0
df1['unreachable_returning']=0


# In[41]:


# 转置数据
count=0
for i in d_t.groupby(['揽收周期','des','channel_id']):
    df1.loc[count,'揽收周期']=i[0][0]
    df1.loc[count,'des']=i[0][1]
    df1.loc[count,'channel_id']=i[0][2]
    for j in i[1].index:
        df1.loc[count,i[1].loc[j,'tracking_status']]=i[1].loc[j,'推送数']
    count+=1


# In[42]:


df1=df1.replace(np.nan,0)


# In[43]:


# 总票数
S6="""
select 
month(DATE_ADD(lgo.gmt_create,interval 8 hour)) 揽收周期,
lgo.des,
lgo.channel_id,
count(*) 含揽收节点票数
from lg_order lgo
where lgo.is_deleted='n'
and lgo.order_time>'2021-09-30 16:00:00'
and lgo.platform='TIKTOK'
group by 1,2,3
"""


# In[44]:


S7="""
select 
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 揽收周期,
lgo.des,
lgo.channel_id,
count(*) 含揽收节点票数
from lg_order lgo
where lgo.is_deleted='n'
and lgo.order_time>'2021-09-30 16:00:00'
and lgo.platform='TIKTOK'
group by 1,2,3
"""


# In[45]:


d6=execude_sql(S6)


# In[46]:


d7=execude_sql(S7)


# In[47]:


d8=pd.concat([d6,d7])


# In[48]:


d8['channel_id']=d8['channel_id'].map(channel_dict)


# In[49]:


r=pd.merge(d8,df1,on=['揽收周期','des','channel_id'],how='left')


# In[50]:


# 计算推送率
for i in list1:
    r[i]=r[i]/r['含揽收节点票数']


# In[51]:


r=r.replace(np.nan,0)


# In[52]:


# r.loc[r['揽收周期']=='202201','揽收周期']='202154'
# r.loc[r['揽收周期']=='202202','揽收周期']='202155'


# In[53]:


# r=r.groupby(by=[''])


# In[54]:


# 添加周期
def start(x):
    if(int(x)<2000):
        return np.NAN
    if(int(x)<202200):
        years = int(x[0:4])
        mon = x[4:6:1]
        days = (int(mon)-1) * 7
        fir_day = datetime.date(years, 1, 4)
        zone = datetime.timedelta(days-1)
        JS = fir_day + zone
        zones = datetime.timedelta(days-7)
        KS = fir_day + zones
    if(int(x)>=202200):
        years = int(x[0:4])
        mon = x[4:6:1]
        days = (int(mon)-1) * 7
        fir_day = datetime.date(years, 1, 4)
        zone = datetime.timedelta(days-2)
        JS = fir_day + zone
        zones = datetime.timedelta(days-8)
        KS = fir_day + zones
    return KS.strftime('%m-%d')+'至'+JS.strftime('%m-%d')


# In[55]:


r['揽收周期']=r['揽收周期'].astype('int')
r['揽收周期']=r['揽收周期'].astype('str')


# In[56]:


r['周期']=r['揽收周期'].apply(lambda x:start(x))


# In[57]:


r.to_excel(r'C:\Users\hp\Desktop\tiktok节点\tiktok推送.xlsx',index=False)


# In[ ]:




