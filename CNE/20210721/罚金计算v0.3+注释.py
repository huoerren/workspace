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

# 確定查詢範圍變量
days="BETWEEN '2021-4-30 16:00:00' and '2021-05-31 16:00:00'"


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

# 导入往期数据，合并
dyw = pd.read_excel(u"F:\\其他部门\\罚金计算\\WISH罚金202011-202104.xlsx",dtype={'业务年月': 'str','淡旺季': 'int','渠道': 'str','国家': 'str'})
d1 = pd.concat([d1, dyw])
# 导入KPI

# In[12]:

# 讀取KPI表格
KPI = pd.read_excel(r'D:\PBI\BI\Wish-4PL-SLA-1134.xlsx',sheet_name='KPI调整表',dtype=dict(zip(['TTD（TDD)','年月'],['int','str',])))
KPI=KPI.rename(columns={'国家简码':'国家','国家':'国家1','年月':'淡旺季','物流渠道':'渠道','TTD（TDD)':'KPI','妥投率':'KPI妥投率'})
KPI['淡旺季']=KPI['淡旺季'].astype('int')


# In[13]:


KPI.head(3)


# 总妥投率判断

# In[14]:


#聚合求票数
d2 = d1.groupby(['业务年月','渠道','国家','淡旺季','妥投时间间隔','是否妥投'])['票数'].sum().reset_index()


# In[15]:


#合并KPI
d2=pd.merge(d2,KPI,on=['渠道','国家','淡旺季'],how='left')


# In[16]:


#筛选出超时妥投订单
dcs = d2[(d2['妥投时间间隔']>d2['KPI'])&(d2['是否妥投']=='已妥投')].groupby(['业务年月','渠道','国家','淡旺季','KPI','KPI妥投率'])['票数'].sum().reset_index()


# In[17]:


#统计超时
dcs=dcs.rename(columns={'票数':'超时票数'})


# In[18]:


#聚合所有妥投数据后左连接超時票数统计
d2=pd.merge(d2[d2['是否妥投']=='已妥投'].groupby(['业务年月','渠道','国家','淡旺季','KPI','KPI妥投率'])['票数'].sum().reset_index(),dcs[['业务年月','渠道','国家','淡旺季','超时票数']],on=['业务年月','渠道','国家','淡旺季'],how='left')


# In[19]:


#超時票数空值补零
d2['超时票数']=d2['超时票数'].replace(np.nan,0)
print(d2)

# In[20]:


#聚合求总票数 妥投率 未妥投票数
dt = d1.groupby(['业务年月','渠道','国家','淡旺季'])['票数'].sum().reset_index()


# In[21]:


dt=dt.rename(columns={'票数':'总票数'})


# In[22]:

# 计算妥投票数并与总票数匹配
djs=pd.merge(d2,dt,on=['业务年月','渠道','国家','淡旺季'])


# In[23]:


# s=d2[d2['是否妥投']=='已妥投'].groupby(['业务年月','渠道','国家','淡旺季'])['票数'].sum().reset_index()


# In[24]:

# 计算妥投率，保留3位有效
djs['总妥投率'] = round((djs['票数']/djs['总票数']),3)


# In[25]:


djs['未妥投票数']=djs['总票数']-djs['票数']
print(djs)

# In[26]:


# d3t[d3t['国家']=='US']


# In[27]:

# 匹配超时票数
# djs=pd.merge(djs,dcs[['业务年月','渠道','国家','淡旺季','超额票数','KPI妥投率','KPI']],on=['业务年月','渠道','国家','淡旺季'],how='left')


# In[28]:


djs['妥投率达标']=0
djs['豁免订单']=0
djs['可扣减票数']=0


# In[29]:


for i in djs.index:
    n1 = djs.loc[i,'总妥投率']
    n2 = djs.loc[i,'KPI妥投率']
    if n1>=n2:
        djs.loc[i,'妥投率达标']=int(1)
    k1=round(((1-djs.loc[i,'KPI妥投率'])*djs.loc[i,'总票数']),0)
    k2=round(((djs.loc[i,'总妥投率']-djs.loc[i,'KPI妥投率'])*djs.loc[i,'总票数']),0)
    if k1>=0:
        djs.loc[i,'豁免订单']=k1
    if k2>=0:
        djs.loc[i,'可扣减票数']=k2


# #时效判断

# In[30]:


#已妥投订单按妥投时间间隔升序
dsx = d1[d1['是否妥投']=='已妥投'].groupby(['业务年月','渠道','国家','淡旺季','妥投时间间隔'])['票数'].sum().reset_index().sort_values(['业务年月','渠道', '国家','妥投时间间隔'])


# In[31]:

# 计算总妥投票数
dtt = d1[d1['是否妥投']=='已妥投'].groupby(['业务年月','渠道','国家','淡旺季'])['票数'].sum().reset_index()


# In[32]:


dtt=dtt.rename(columns={'票数':'总妥投票数'})


# In[33]:


#计算累计票数
dsx['累计票数'] = dsx.groupby(['业务年月','渠道','国家','淡旺季'])['票数'].cumsum()


# In[34]:

# 累计票数匹配总妥投票数
dsx=pd.merge(dsx,dtt,on=['业务年月','渠道','国家','淡旺季'])


# In[35]:


dsx['票数占比']=dsx['累计票数']/dsx['总妥投票数']


# In[36]:


# dsx=pd.merge(dsx,KPI,on=['渠道','国家','淡旺季'])


# In[37]:


dsx['90分位']=0


# In[38]:


for i in dsx.index:
    k1 = dsx.loc[i,'票数占比']
    k2 = 0.9
    if k1>=k2:
        dsx.loc[i,'90分位']=1


# In[39]:


#筛选90%分位以上数据，保留第一条
d90=dsx[dsx['90分位']==1]
d90=d90.drop_duplicates(['业务年月', '渠道', '国家', '淡旺季'],keep='first' )
d90=pd.merge(djs,d90[['业务年月', '渠道', '国家', '淡旺季','妥投时间间隔']],on=['业务年月', '渠道', '国家', '淡旺季'])


# In[40]:


d90['时效达标']=0
d90.head(4)


# In[41]:


#根据实际妥投区间分配每单罚金
d90['单票罚金']=0
for i in d90.index:
    k1 = d90.loc[i,'妥投时间间隔']
    k2 = d90.loc[i,'KPI']
    if int(k1)<=k2:
        d90.loc[i,'时效达标']=1
    elif int(k1)<=1.2*k2:d90.loc[i,'单票罚金']=10
    elif int(k1)<=1.5*k2:d90.loc[i,'单票罚金']=15
    else:d90.loc[i,'单票罚金']=20

print(d90)
# In[42]:


#合并妥投表和实际表
df = pd.merge(djs,d90[['业务年月','渠道','国家','淡旺季','时效达标','单票罚金','妥投时间间隔']],on=['业务年月','渠道','国家','淡旺季'])


# In[43]:


#0替空值
df['超时票数']=df['超时票数'].replace(np.nan,0)
df['未妥投票数']=df['未妥投票数'].replace(np.nan,0)


# In[44]:


#添加列，默认为0
df['时效罚金']=0
df['妥投罚金']=0


# In[45]:


#计算时效罚金和妥投罚金
for i in df.index:
    res1=df.loc[i,'妥投率达标']
    res2=df.loc[i,'时效达标']
#     print(res1,res2)
    if int(res1)==1 and int(res2)==0:
#         print(111)
        df.loc[i,'时效罚金']=(df.loc[i,'超时票数']-df.loc[i,'可扣减票数'])*df.loc[i,'单票罚金']
    elif int(res1)==0 and int(res2)==1:
        df.loc[i,'妥投罚金']=(df.loc[i,'未妥投票数']-df.loc[i,'豁免订单'])*20
    elif int(res1)==0 and int(res2)==0:
        df.loc[i,'时效罚金']=(df.loc[i,'超时票数']-df.loc[i,'可扣减票数'])*df.loc[i,'单票罚金']
        df.loc[i,'妥投罚金']=(df.loc[i,'未妥投票数']-df.loc[i,'豁免订单'])*20
    else:pass


# In[46]:


df['总罚金']=df['时效罚金']+df['妥投罚金']


# In[47]:


df=df.sort_values(by=['总罚金'],ascending=False)
df = df[(df['总罚金']>0) & (df['渠道'] != 'CNE全球通平邮')]
print(df)

# In[161]:


# df.to_excel('/Users/dingmengnan/downloads/20212-20215Wish渠道罚金.xlsx')
file=r'F:\其他部门\罚金计算'
bf = r'{}\WISH罚金.xlsx'.format(file)
writer = pd.ExcelWriter(bf)
d1.to_excel(writer, '基础数据')
d2.to_excel(writer,'聚合数据')
df.to_excel(writer, '罚金')
writer.save()

# In[48]:


# df.to_excel('/Users/dingmengnan/downloads/202011-202105Wish渠道罚金.xlsx')


# In[ ]:




