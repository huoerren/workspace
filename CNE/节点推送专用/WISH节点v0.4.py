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


days="BETWEEN '2021-09-05 16:00:00' and '2021-10-18 16:00:00'"
days1="BETWEEN '2021-07-31 16:00:00' and '2021-10-18 16:00:00'"


# In[5]:


SA1="""
SELECT 
DATE_FORMAT(DATE_ADD(order_time,interval 8 hour),'%Y%u') weeks,
(case when customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
channel_code,
des,
track_status,
count(distinct order_id) 订单数
from push_track_log ptl
where track_status in('ARRIVE_FIRST_MILE','DEPART_FIRST_MILE','ARRIVE_CARRIER')
and ptl.push_status='S'
and ptl.is_deleted='n'
and order_time {}
group by 1,2,3,4,5
""".format(days)

dA1=execude_sql(SA1)

SA2="""
SELECT 
DATE_FORMAT(DATE_ADD(order_time,interval 8 hour),'%Y%u') weeks,
(case when customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
channel_code,
des,
track_status,
count(distinct order_id) 订单数
from push_track_log ptl
where track_status in('DEPART_CARRIER','ARRIVE_AIRPORT','DEPART_AIRPORT')
and ptl.push_status='S'
and ptl.is_deleted='n'
and order_time {}
group by 1,2,3,4,5
""".format(days)

dA2=execude_sql(SA2)

SA3="""
SELECT 
DATE_FORMAT(DATE_ADD(order_time,interval 8 hour),'%Y%u') weeks,
(case when customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
channel_code,
des,
track_status,
count(distinct order_id) 订单数
from push_track_log ptl
where track_status in('ARRIVE_DEST_AIRPORT','ARRIVE_DEST_COUNTRY','DEPART_DEST_CUSTOMS')
and ptl.push_status='S'
and ptl.is_deleted='n'
and order_time {}
group by 1,2,3,4,5
""".format(days)

dA3=execude_sql(SA3)

SA4="""
SELECT 
DATE_FORMAT(DATE_ADD(order_time,interval 8 hour),'%Y%u') weeks,
(case when customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
channel_code,
des,
track_status,
count(distinct order_id) 订单数
from push_track_log ptl
where track_status in('ARRIVE_TRANSITHUB','DEPART_TRANSITHUB','ARRIVE_TRANSITHUB')
and ptl.push_status='S'
and ptl.is_deleted='n'
and order_time {}
group by 1,2,3,4,5
""".format(days)

dA4=execude_sql(SA4)


SA5="""
SELECT 
DATE_FORMAT(DATE_ADD(order_time,interval 8 hour),'%Y%u') weeks,
(case when customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
channel_code,
des,
track_status,
count(distinct order_id) 订单数
from push_track_log ptl
where track_status in('DELIVERED','RETURNED_BY_CARRIER')
and ptl.push_status='S'
and ptl.is_deleted='n'
and order_time {}
group by 1,2,3,4,5
""".format(days)


dA5=execude_sql(SA5)

d1=pd.concat([dA1,dA2,dA3,dA4,dA5])

d1['weeks']=(d1['weeks'].astype('int')+1).astype('str')

SB1="""
SELECT
Month(DATE_ADD(order_time,interval 8 hour)) weeks,
(case when customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
channel_code,
des,
track_status,
count(DISTINCT order_id) 订单数
FROM
push_track_log ptl
WHERE
order_time {}
AND is_deleted='n'
and ptl.track_status in('ARRIVE_FIRST_MILE','DEPART_FIRST_MILE','ARRIVE_CARRIER')
and ptl.push_status='S'
and ptl.is_deleted='n'
GROUP BY 1,2,3,4,5
""".format(days1)

dB1=execude_sql(SB1)


SB2="""
SELECT
Month(DATE_ADD(order_time,interval 8 hour)) weeks,
(case when customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
channel_code,
des,
track_status,
count(DISTINCT order_id) 订单数
FROM
push_track_log ptl
WHERE
order_time {}
AND is_deleted='n'
and ptl.track_status in('DEPART_CARRIER','ARRIVE_AIRPORT','DEPART_AIRPORT')
and ptl.push_status='S'
and ptl.is_deleted='n'
GROUP BY 1,2,3,4,5
""".format(days1)

dB2=execude_sql(SB2)


SB3="""
SELECT
Month(DATE_ADD(order_time,interval 8 hour)) weeks,
(case when customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
channel_code,
des,
track_status,
count(DISTINCT order_id) 订单数
FROM
push_track_log ptl
WHERE
order_time {}
AND is_deleted='n'
and ptl.track_status in('ARRIVE_DEST_AIRPORT','ARRIVE_DEST_COUNTRY','DEPART_DEST_CUSTOMS')
and ptl.push_status='S'
and ptl.is_deleted='n'
GROUP BY 1,2,3,4,5
""".format(days1)


dB3=execude_sql(SB3)


SB4="""
SELECT
Month(DATE_ADD(order_time,interval 8 hour)) weeks,
(case when customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
channel_code,
des,
track_status,
count(DISTINCT order_id) 订单数
FROM
push_track_log ptl
WHERE
order_time {}
AND is_deleted='n'
and ptl.track_status in('ARRIVE_TRANSITHUB','DEPART_TRANSITHUB','ARRIVE_TRANSITHUB')
and ptl.push_status='S'
and ptl.is_deleted='n'
GROUP BY 1,2,3,4,5
""".format(days1)


dB4=execude_sql(SB4)


SB5="""
SELECT
Month(DATE_ADD(order_time,interval 8 hour)) weeks,
(case when customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
channel_code,
des,
track_status,
count(DISTINCT order_id) 订单数
FROM
push_track_log ptl
WHERE
order_time {}
AND is_deleted='n'
and ptl.track_status in('DELIVERED','RETURNED_BY_CARRIER')
and ptl.push_status='S'
and ptl.is_deleted='n'
GROUP BY 1,2,3,4,5
""".format(days1)


# In[27]:


dB5=execude_sql(SB5)


# In[28]:


d11=pd.concat([dB1,dB2,dB3,dB4,dB5])

# list1 ：获得d1中所有的 track_status
list1=list(set(d1.track_status.tolist()))

# list2 ：数据扁平化
list2=list1.copy()
list2.append('channel_code')
list2.append('des')
list2.append('weeks')
list2.append('业务类型')


df1=pd.DataFrame(index=[i for i in range(0,200)],columns=list2)

print (df1.shape)
d1=d1.reset_index().drop(['index'],axis=1)
print(d1)
print('----------------------------------------------------')
count=0
for i in d1.groupby(['weeks','业务类型','channel_code','des']):
    df1.loc[count,'weeks']=i[0][0]
    df1.loc[count,'业务类型']=i[0][1]
    df1.loc[count,'channel_code']=i[0][2]
    df1.loc[count,'des']=i[0][3]
    for j in i[1].index:
        df1.loc[count,i[1].loc[j,'track_status']]=i[1].loc[j,'订单数']
    count+=1

print(df1.shape)
print(df1.head())

d11=d11.reset_index().drop(['index'],axis=1)

for i in d11.groupby(['weeks','业务类型','channel_code','des']):
    df1.loc[count,'weeks']=i[0][0]
    df1.loc[count,'业务类型']=i[0][1]
    df1.loc[count,'channel_code']=i[0][2]
    df1.loc[count,'des']=i[0][3]
    for j in i[1].index:
        df1.loc[count,i[1].loc[j,'track_status']]=i[1].loc[j,'订单数']
    count+=1

print(df1.shape)
print(df1.tail())

S2="""
SELECT DISTINCT
DATE_FORMAT(DATE_ADD(lgo.order_time,interval 8 hour),'%Y%u') weeks,
(case when lgo.customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
lgo.channel_code,
lgo.des,
count(*) 总量
FROM
lg_order lgo
WHERE
lgo.platform = 'WISH_ONLINE' 
AND lgo.order_time {}
AND lgo.is_deleted='n'
GROUP BY 1,2,3,4
""".format(days)


S22="""
SELECT DISTINCT
Month(DATE_ADD(lgo.order_time,interval 8 hour)) weeks,
(case when lgo.customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
lgo.channel_code,
lgo.des,
count(*) 总量
FROM
lg_order lgo
WHERE
lgo.platform = 'WISH_ONLINE' 
AND lgo.order_time {}
AND lgo.is_deleted='n'
GROUP BY 1,2,3,4
""".format(days1)


d2=execude_sql(S2)
d2['weeks']=(d2['weeks'].astype('int')+1).astype('str')

d22=execude_sql(S22)

d2=pd.concat([d2,d22])

print(d2.shape)
print(d2.head())

df1['weeks']=df1['weeks'].astype('str')
d2['weeks']=d2['weeks'].astype('str')

r1=pd.merge(df1,d2,on=['weeks','业务类型','channel_code','des'],how='left')

print(r1.shape)
print(r1.columns.to_list())


for i in list1:
    r1[i]=r1[i].astype('float')
    r1[i]=r1[i]/r1['总量']


#系统妥投率
S3="""
SELECT DISTINCT
DATE_FORMAT(DATE_ADD(lgo.order_time,interval 8 hour),'%Y%u') weeks,
(case when lgo.customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
lgo.channel_code,
lgo.des,
SUM(CASE WHEN ORDER_STATUS=3 THEN 1 ELSE 0 END)/count(*) 系统妥投率
FROM
lg_order lgo
WHERE
lgo.platform = 'WISH_ONLINE' 
AND lgo.order_time {}
AND lgo.is_deleted='n'
GROUP BY 1,2,3,4
""".format(days)


S33="""
SELECT DISTINCT
Month(DATE_ADD(lgo.order_time,interval 8 hour)) weeks,
(case when lgo.customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
lgo.channel_code,
lgo.des,
SUM(CASE WHEN ORDER_STATUS=3 THEN 1 ELSE 0 END)/count(*) 系统妥投率
FROM
lg_order lgo
WHERE
lgo.platform = 'WISH_ONLINE' 
AND lgo.order_time {} 
AND lgo.is_deleted='n'
GROUP BY 1,2,3,4
""".format(days1)


d3=execude_sql(S3)
d3['weeks']=(d3['weeks'].astype('int')+1).astype('str')

d33=execude_sql(S33)

d3=pd.concat([d3,d33])
d3['weeks']=d3['weeks'].astype('str')


r1=pd.merge(r1,d3,on=['weeks','业务类型','channel_code','des'],how='left')


r1=r1.replace(np.nan,0)


# In[56]:


#国内段退件
S4="""
select
DATE_FORMAT(DATE_ADD(lgo.order_time,interval 8 hour),'%Y%u') weeks,
(case when lgo.customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
lgo.channel_code,
lgo.des,
count(DISTINCT lgo.logistics_no) 国内段退件数
from lg_order lgo
where 
lgo.order_time {}
and order_status not in (3,10)
and lgo.is_deleted='n'
and platform='WISH_ONLINE'
and exists(
select 1
from track_order_event toe
where toe.order_id=lgo.id
and toe.event_code in  (     'GNTJ',     'GYST',     'TKDZ',        'JCTJ',       'CFDG',     'CTBY',     'ATIN',     'CZLL',     'CHIC',     'YCJJ',     'LJIE',     'DIBJ',     'BGPS',   'SIRC',   'GNTJ','CSHD',
'CSIN','HGCY','HJFX','GNCY','HGYC','CKCY','HGXH')
and toe.is_deleted='n'
)
GROUP BY 1,2,3,4
""".format(days)


S44="""
select
Month(DATE_ADD(lgo.order_time,interval 8 hour)) weeks,
(case when lgo.customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
lgo.channel_code,
lgo.des,
count(DISTINCT lgo.logistics_no) 国内段退件数
from lg_order lgo
where 
lgo.order_time {}
and order_status not in (3,10)
and lgo.is_deleted='n'
and platform='WISH_ONLINE'
and exists(
select 1
from track_order_event toe
where toe.order_id=lgo.id
and toe.event_code in  (     'GNTJ',     'GYST',     'TKDZ',        'JCTJ',       'CFDG',     'CTBY',     'ATIN',     'CZLL',     'CHIC',     'YCJJ',     'LJIE',     'DIBJ',     'BGPS',   'SIRC',   'GNTJ','CSHD',
'CSIN','HGCY','HJFX','GNCY','HGYC','CKCY','HGXH')
and toe.is_deleted='n'
)
GROUP BY 1,2,3,4
""".format(days1)


S444="""
select
DATE_FORMAT(DATE_ADD(lgo.order_time,interval 8 hour),'%Y%u') weeks,
(case when lgo.customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
lgo.channel_code,
lgo.des,
count(DISTINCT lgo.logistics_no) 未发送数
from lg_order lgo
where 
lgo.order_time {}
and order_status = 0
and lgo.is_deleted='n'
and platform='WISH_ONLINE'
GROUP BY 1,2,3,4
""".format(days1)


S45="""
select
Month(DATE_ADD(lgo.order_time,interval 8 hour)) weeks,
(case when lgo.customer_id in (1151368,1181372,1151370,1181374) then '4PL' ELSE '3PL' END) 业务类型,
lgo.channel_code,
lgo.des,
count(DISTINCT lgo.logistics_no) 未发送数
from lg_order lgo
where 
lgo.order_time {}
and order_status = 0
and lgo.is_deleted='n'
and platform='WISH_ONLINE'
GROUP BY 1,2,3,4
""".format(days1)

d4=execude_sql(S4)
d444=execude_sql(S444)

d4=pd.merge(d4,d444,on=['weeks','业务类型','channel_code','des'],how='outer')
d4['weeks']=(d4['weeks'].astype('int')+1).astype('str')


print(d4.shape)
print(d4.head())



d44=execude_sql(S44)
d45=execude_sql(S45)

d44=pd.merge(d44,d45,on=['weeks','业务类型','channel_code','des'],how='outer')
d44['未发送数']=d44['未发送数'].replace(np.nan,0)

print(d44.shape)
print(d44.head())

d4=pd.concat([d4,d44])

d4['weeks']=d4['weeks'].astype('str')

print(d4.shape)
print(d4.head())


r1=pd.merge(r1,d4,on=['weeks','业务类型','channel_code','des'],how='left')

r1=r1.replace(np.nan,0)
print(r1.shape)
print(r1.head())

r1['推送率上限']=(r1['总量']-r1['国内段退件数']-r1['未发送数'])/r1['总量']
r1=r1.replace(np.nan,0)
r1.loc[r1['DEPART_CARRIER']>1,['DEPART_CARRIER']]=1
r1['系统退件率(国内)']=r1['国内段退件数']/r1['总量']
r1['退件考核']=None
# r1['退件考核']=r1['RETURNED_BY_CARRIER']/(1-r1['DEPART_CARRIER'])
r1['returned节点']=(r1['RETURNED_BY_CARRIER']*r1['总量'])/r1['国内段退件数']



for i in r1.index:
    if r1.loc[i,'DEPART_CARRIER']==1:
        r1.loc[i,'退件考核']==1
    else:
        r1.loc[i,'退件考核']=r1.loc[i,'RETURNED_BY_CARRIER']/(1-r1.loc[i,'DEPART_CARRIER'])


# In[77]:


r2=r1[['weeks','业务类型','channel_code','des','总量','国内段退件数','推送率上限','ARRIVE_FIRST_MILE','DEPART_FIRST_MILE','ARRIVE_CARRIER','DEPART_CARRIER','ARRIVE_AIRPORT','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','ARRIVE_DEST_COUNTRY','DEPART_DEST_CUSTOMS','ARRIVE_TRANSITHUB','DEPART_TRANSITHUB','DELIVERED','系统妥投率','系统退件率(国内)','退件考核','returned节点']]
r2['退件考核']=r2['退件考核'].replace(np.nan,1)
r2['returned节点']=r2['returned节点'].replace(np.nan,1)

print(r2.shape)
print(r2.head())

r3=pd.read_excel(r'C:\Users\hp\Desktop\节点监控\月合并\wish3-7月节点监控.xlsx')
result=pd.concat([r2,r3])

result=result.replace(float('inf'),1)
result['weeks']=result['weeks'].astype('str')

print(result.shape)
print(result.head())

# In[83]:


import datetime,time


# In[84]:


def start(x):
    if(int(x)<2000):
        return np.NAN
    years = int(x[0:4])
    mon = x[4:6:1]
    days = (int(mon)-1) * 7
    fir_day = datetime.date(years, 1, 4)
    zone = datetime.timedelta(days-1)
    JS = fir_day + zone
    zones = datetime.timedelta(days-7)
    KS = fir_day + zones
    return KS.strftime('%m-%d')+'至'+JS.strftime('%m-%d')



result['周期']=result['weeks'].apply(lambda x:start(x))


result.to_excel(r'D:\PBI\BI\节点推送\wish节点监控全v0.2.xlsx',index=False)





