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


#数据库转df
def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df


# In[4]:


days="BETWEEN '2020-10-31 16:00:00' and '2021-05-31 16:00:00'"


# In[5]:


S1 ="""
SELECT 
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') 业务日期, 
lgo.channel_code 渠道,
lgo.des 国家, 
lgo.supply_channel_code 尾端配送商,
(case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
count(1) 票数,
ceil(TIMESTAMPDIFF(hour,gmt_create,delivery_date)/24) 妥投时间间隔
FROM
lg_order lgo
where
lgo.customer_id in(1151368,1181372,1151370,1181374)
and lgo.platform ='WISH_ONLINE'
and lgo.gmt_create {}
and lgo.is_deleted='n'
and lgo.channel_code in('CNE全球经济','CNE全球特惠','CNE全球优先','CNE全球通挂号')
group by
1,2,3,4,5,7
""".format(days)


# In[6]:


d1=execude_sql(S1)


# In[7]:


d1['业务日期']=pd.to_datetime(d1['业务日期'])


# In[8]:


d1['月']=d1['业务日期'].dt.month.astype('str')
d1['年']=d1['业务日期'].dt.year.astype('str')


# In[9]:


def judge(a,b):
    if a=='2020':
        return 20201
    elif b in ['1','9','10','11','12']:
        return 20211
    else:
        return 20210


# In[10]:


d1['淡旺季'] = d1.apply(lambda x:judge(x['年'],x['月']),axis=1)


# In[11]:


d1['业务年月'] = d1['年'] + '-' + d1['月']


# 导入KPI

# In[12]:


KPI = pd.read_excel('/Users/dingmengnan/downloads/Wish-4PL-SLA-1134.xlsx',sheet_name='KPI调整表',dtype=dict(zip(['TTD（TDD)','年月'],['int','str',])))
KPI=KPI.rename(columns={'国家简码':'国家','国家':'国家1','年月':'淡旺季','物流渠道':'渠道','TTD（TDD)':'KPI','妥投率':'KPI妥投率'})
KPI['淡旺季']=KPI['淡旺季'].astype('int')


# In[13]:


KPI.head(3)


# 总妥投率判断

# In[14]:


d2 = d1.groupby(['业务年月','渠道','国家','淡旺季','妥投时间间隔','是否妥投'])['票数'].sum().reset_index()


# In[15]:


d2.head(2)


# In[16]:


d2p=pd.merge(d2,KPI,on=['渠道','国家','淡旺季'],how='left')


# In[17]:


d3p = d2p[(d2p['妥投时间间隔']>d2p['KPI'])&(d2p['是否妥投']=='已妥投')].groupby(['业务年月','渠道','国家','淡旺季','KPI','KPI妥投率'])['票数'].sum().reset_index()


# In[18]:


d3p=d3p.rename(columns={'票数':'超额票数'})


# In[19]:


d2p=pd.merge(d2p[d2p['是否妥投']=='已妥投'].groupby(['业务年月','渠道','国家','淡旺季','KPI','KPI妥投率'])['票数'].sum().reset_index(),d3p[['业务年月','渠道','国家','淡旺季','超额票数']],on=['业务年月','渠道','国家','淡旺季'],how='left')


# In[20]:


d2p['超额票数'].replace(np.nan,0)


# In[21]:


d2p


# In[22]:


d2t = d1.groupby(['业务年月','渠道','国家','淡旺季'])['票数'].sum().reset_index()


# In[23]:


d2t=d2t.rename(columns={'票数':'总票数'})


# In[24]:


d3t=pd.merge(d2[d2['是否妥投']=='已妥投'].groupby(['业务年月','渠道','国家','淡旺季'])['票数'].sum().reset_index(),d2t,on=['业务年月','渠道','国家','淡旺季'])


# In[25]:


d3t['总妥投率'] = round((d3t['票数']/d3t['总票数']),3)


# In[26]:


d3t['未妥投票数']=d3t['总票数']-d3t['票数']


# In[27]:


d3t.head(3)


# In[28]:


d2p


# In[29]:


d4=pd.merge(d3t,d2p,on=['业务年月','渠道','国家','淡旺季'],how='left')


# In[30]:


d4.head(3)


# In[31]:


d4['妥投率达标']=0
d4['豁免订单']=0
d4['可扣减票数']=0


# In[32]:


d4


# In[33]:


for i in d4.index:
    n1 = d4.loc[i,'总妥投率']
    n2 = d4.loc[i,'KPI妥投率']
    if n1>=n2:
        d4.loc[i,'妥投率达标']=int(1)
    k1=round(((1-d4.loc[i,'KPI妥投率'])*d4.loc[i,'总票数']),0)
    k2=round(((d4.loc[i,'总妥投率']-d4.loc[i,'KPI妥投率'])*d4.loc[i,'总票数']),0)
    if k1>=0:
        d4.loc[i,'豁免订单']=round(((1-d4.loc[i,'KPI妥投率'])*d4.loc[i,'总票数']),0)
    if k2>=0:
        d4.loc[i,'可扣减票数']=round(((d4.loc[i,'总妥投率']-d4.loc[i,'KPI妥投率'])*d4.loc[i,'总票数']),0)


# In[34]:


d4.head(2)


# In[35]:


# d4['未妥投票数']=(d4['总票数']*(1-d4['总妥投率'])).astype(int)


# In[36]:


# d4.head(2)


# #时效判断

# In[37]:


d1t = d1[d1['是否妥投']=='已妥投'].groupby(['业务年月','渠道','国家','淡旺季','妥投时间间隔'])['票数'].sum().reset_index().sort_values(['业务年月','渠道', '国家','妥投时间间隔'])


# In[38]:


d1t1 = d1[d1['是否妥投']=='已妥投'].groupby(['业务年月','渠道','国家','淡旺季'])['票数'].sum().reset_index()


# In[39]:


d1t1=d1t1.rename(columns={'票数':'总票数'})


# In[40]:


d1t['累计票数'] = d1t.groupby(['业务年月','渠道','国家','淡旺季'])['票数'].cumsum()


# In[41]:


p1=pd.merge(d1t,d1t1,on=['业务年月','渠道','国家','淡旺季'])


# In[42]:


p1.head(2)


# In[43]:


p1['累计妥投率']=p1['累计票数']/p1['总票数']


# In[44]:


p2=pd.merge(p1,KPI,on=['渠道','国家','淡旺季'])


# In[45]:


p2['时效达标']=0


# In[46]:


for i in p2.index:
    k1 = p2.loc[i,'累计妥投率']
    k2 = 0.9
    if k1>=k2:
        p2.loc[i,'时效达标']=1


# In[47]:


p3=p2[p2['时效达标']==1]
p3=p3.drop_duplicates(['业务年月', '渠道', '国家', '淡旺季'],keep='first' )
p4=pd.merge(d4,p3[['业务年月', '渠道', '国家', '淡旺季','妥投时间间隔']],on=['业务年月', '渠道', '国家', '淡旺季'])


# In[48]:


p4['时效达标']=0
p4.head(4)


# In[49]:


p4['单票罚金']=0
for i in p4.index:
    k1 = p4.loc[i,'妥投时间间隔']
    k2 = p4.loc[i,'KPI']
    if int(k1)<=k2:
        p4.loc[i,'时效达标']=1
    elif int(k1)<=1.2*k2:p4.loc[i,'单票罚金']=10
    elif int(k1)<=1.5*k2:p4.loc[i,'单票罚金']=15
    else:p4.loc[i,'单票罚金']=20


# In[50]:


r1 = pd.merge(d4,p4[['业务年月','渠道','国家','淡旺季','时效达标','单票罚金']],on=['业务年月','渠道','国家','淡旺季'])


# In[57]:


r1['超额票数']=r1['超额票数'].replace(np.nan,0)
r1['未妥投票数']=r1['未妥投票数'].replace(np.nan,0)


# In[58]:


r1['时效罚金']=0
r1['妥投罚金']=0


# In[59]:


for i in r1.index:
    res1=r1.loc[i,'妥投率达标']
    res2=r1.loc[i,'时效达标']
#     print(res1,res2)
    if int(res1)==1 and int(res2)==0:
#         print(111)
        r1.loc[i,'时效罚金']=(r1.loc[i,'超额票数']-r1.loc[i,'可扣减票数'])*r1.loc[i,'单票罚金']
    elif int(res1)==0 and int(res2)==1:
        r1.loc[i,'妥投罚金']=(r1.loc[i,'未妥投票数']-r1.loc[i,'豁免订单'])*20
    elif int(res1)==0 and int(res2)==0:
        r1.loc[i,'时效罚金']=(r1.loc[i,'超额票数']-r1.loc[i,'可扣减票数'])*r1.loc[i,'单票罚金']
        r1.loc[i,'妥投罚金']=(r1.loc[i,'未妥投票数']-r1.loc[i,'豁免订单'])*20
    else:pass


# In[60]:


r1['总罚金']=r1['时效罚金']+r1['妥投罚金']


# In[61]:


r1=r1.sort_values(by=['总罚金'],ascending=False)


# In[62]:


r1.to_excel('/Users/dingmengnan/downloads/202011-202105Wish渠道罚金.xlsx')


# In[ ]:




