#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import openpyxl
import pymysql
import datetime,time
nows=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')


print(datetime.date.today())
import numpy as np
# #数仓连接
con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()

# days="BETWEEN '2021-03-31 16:00:00' and '"+time_yes+"'"1
# days="BETWEEN '2021-03-31 16:00:00' and "+"'"+nows+"'"
days="BETWEEN '2021-11-30 16:00:00' and "+"'"+nows+"'"


end_day = datetime.date.today()
start_day = datetime.date.today()+ datetime.timedelta(-60)
print(end_day)
print(start_day)
print(days)
# days="BETWEEN '2020-12-17 16:00:00' and '2021-02-15 15:59:59'"


# In[2]:


# list

s1="""SELECT 
lgo.channel_code "渠道名称",
lgo.des "目的地",
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') as "业务日期" ,
round(timestampdiff(hour,lgo.gmt_create,lgo.delivery_date)/24,1) as delivery_interval,
lgo.order_status "追踪状态" ,
ISNULL(lgo.mawb_id ),
(case when lgo.customer_id=4691294 then "TT UK" else "Perceiver Limited" end) 账号名,
(case when (isnull(lgo.volumetric_weight)=1 or lgo.volumetric_weight<=lgo.weight ) then "不计泡" else "计泡" end) 计泡,
(case when isnull(lgo.volumetric_weight)=1  then "空" else "非空" end) 判定,
count(distinct lgo.order_no) c ,
sum(lgo.weight) 总重,
sum(lgo.volumetric_weight) 总泡重,
avg(lgo.weight) 单重
from 
lg_order lgo  
where 
lgo.gmt_create {}
and lgo.platform='TIKTOK'
and lgo.is_deleted='n'   
group by
1,2,3,4,5,6,7,8,9
""".format(days)


# In[ ]:





# In[3]:


# 预录单
s2="""
SELECT 
lpo.channel_code,
lpo.status,  
lpo.des,
date_format(date_add(lpo.gmt_create,interval 8 hour),'%Y-%m-%d') as datetime,  
(case when lpo.customer_id=4691294 then "TT UK" else "Perceiver Limited" end) 账号名,
count( 1 ) c ,
sum(lpo.weight) 总重,
avg(lpo.weight) 单重
FROM  
lg_pre_order lpo  
WHERE  
lpo.gmt_create {}
and lpo.platform='TIKTOK'
and lpo.is_deleted= 'n'   
GROUP BY  
1,2,3,4,5
""".format(days)


# In[4]:


# 退件单AND lgo.platform = 'TIKTOK' 
s3="""
SELECT 
lgo.order_no,
lgo.id order_id,
date_add(lgo.gmt_create,interval 8 hour) 业务日期,
lgo.channel_code,
lgo.des,
(case when lgo.customer_id=4691294 then "TT UK" else "Perceiver Limited" end) 账号名
FROM  
lg_order lgo
WHERE  
lgo.gmt_create {}
and lgo.platform='TIKTOK'
and lgo.order_status=8
and lgo.is_deleted= 'n'   
""".format(days)


# In[5]:


def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df
# 主表


# In[6]:


d1=execude_sql(s1)
# print(d1['业务日期'].dtypes)
d1['当日']=datetime.date.today()
d1['业务日期']=pd.to_datetime(d1['业务日期'])
# print()
d1['距离当天']=(pd.to_datetime(d1['当日'])- d1['业务日期']).dt.days

# 追踪状态 的中文解释
o_s={0:'未发送',1 :'已发送',2 :'转运中', 3:'送达',4:'超时',5:'扣关',6:'地址错误',7:'快件丢失', 8:'退件',9:'其他异常'}
d1['状态']=d1['追踪状态'].map(o_s)
d1['状态']=d1['状态'].fillna('销毁')

# 间隔日
# d1.info()
d1['间隔日']=pd.cut(d1['距离当天'],[-999,-2,3, 5, 7, 12, 20, 30, 100000],
                 labels=['已发送','03天未发出', '05天未发出', '07天未发出', '12天未发出', '20天未发出','30天未发出','30天以上未发出',
                         ])
d1.loc[d1['状态']!='未发送',['间隔日']]='已发送'


# In[7]:


d1


# In[8]:


# 预录单
d2=execude_sql(s2)
print(d2)


# In[9]:


d3=execude_sql(s3)
print(d3)


# In[10]:


dt=d3['order_id']
str1=tuple(dt)
print(str1)


# In[11]:


# 查询退件节点时间
s4="""
SELECT 
toe.order_id,
toe.event_code,
date_add(toe.event_time,interval 8 hour) tjdate,
tec1.track_status, 
tec1.event_cn_desc 事件描述,  
tec1.event_en_desc
from
track_order_event toe 
INNER JOIN track_event_code tec1 on toe.event_code = tec1.event_code
WHERE  
 toe.is_deleted= 'n'
and tec1.is_deleted= 'n'
and tec1.event_en_desc!='-1' 
AND toe.event_code IN 
(     'GNTJ',     'GYST',     'TKDZ',        'JCTJ',       'CFDG',     'CTBY',     'ATIN',     'CZLL',     'CHIC',     'YCJJ',     'LJIE',     'DIBJ',     'BGPS',   'SIRC',   'GNTJ','CSHD',
'CSIN','HGCY','HJFX','GNCY','HGYC','CKCY','HGXH')
and toe.order_id in {} 
""".format(str1)


# In[12]:


d4=execude_sql(s4)
print(d4)


# In[13]:


d4['tjdate']=pd.to_datetime(d4['tjdate'])


# In[14]:


dtt=d4.sort_values('tjdate',ascending=True)


# In[15]:


dtt=dtt.drop_duplicates(['order_id','event_code','track_status','事件描述','event_en_desc'],keep='first' )


# In[16]:


print(dtt)


# In[17]:



dtj=pd.merge(d3,dtt,on=['order_id'],how='left')
dtj['duration']=(pd.to_datetime(dtj['tjdate'])-pd.to_datetime(dtj['业务日期'])).astype('timedelta64[h]')


# In[18]:


print(dtj)


# In[19]:


# 滚动两个月总票数
s5="""
SELECT 
lgo.channel_code "渠道名称",
lgo.des "目的地",
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') as "业务日期" ,
lgo.order_status "追踪状态" ,
lgo.mawb_id,
lgo.standard_track_event_id code_id,
(case when lgo.customer_id=4691294 then "TT UK" else "Perceiver Limited" end) 账号名,
count(1) "总票数"
from 
lg_order lgo 
where  
lgo.gmt_create {}
and lgo.platform='TIKTOK'
and lgo.is_deleted='n'   
group by
1,2,3,4,5,6,7
""".format(days)
d5=execude_sql(s5)
print(d5)


# In[20]:


dm=d5['mawb_id']
dm.dropna(axis=0,how='any',inplace=True)
str2=tuple(dm)
print(str2)


# In[21]:


s6="""
SELECT 
tme.mawb_id,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') as "起飞日期" ,
(case when tme.event_code in ("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF') then "全部起飞" else  "部分起飞" end) 部分与否
from 
track_mawb_event tme
where 
tme.mawb_id in {}
AND tme.event_code in("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF','PMWC')
and tme.is_deleted='n'
""".format(str2)
d6=execude_sql(s6)
print(d6)


# In[22]:


d6['起飞日期']=pd.to_datetime(d6['起飞日期'])
d6=d6.sort_values('起飞日期',ascending=False)
d6=d6.drop_duplicates(['mawb_id'],keep='first')
print(d6)


# In[23]:



d7=pd.merge(d6,d5,on=['mawb_id'],how='right')
print(d7)


# In[24]:



s8="""
select
lgm.mawb_no,lgm.id mawb_id,lgm.destination_airport
from
lg_mawb lgm
where
lgm.id in {}
and lgm.is_deleted='n'
""".format(str2)
d8=execude_sql(s8)
print(d8)


# In[25]:



dqf=pd.merge(d7,d8,on=['mawb_id'],how='left')

dqf['当日']=datetime.date.today()
dqf['业务日期']=pd.to_datetime(dqf['业务日期'])
# print()
dqf['距离当天']=(pd.to_datetime(dqf['当日'])- dqf['业务日期']).dt.days

# 追踪状态 的中文解释
o_s={0:'未发送',1 :'已发送',2 :'转运中', 3:'送达',4:'超时',5:'扣关',6:'地址错误',7:'快件丢失', 8:'退件',9:'其他异常'}
dqf['状态']=dqf['追踪状态'].map(o_s)
dqf['状态']=dqf['状态'].fillna('销毁')
print(dqf)


# In[26]:


# 未转换正式单订单
s9="""
select
lpo.order_no,
lpo.id,
lpo.channel_code,
lpo.des
from 
lg_pre_order lpo
where
lpo.platform='TIKTOK'
and lpo.status=0
and lpo.is_deleted='n'
and lpo.gmt_create {}
""".format(days)
d9=execude_sql(s9)
print(d9)


# In[27]:


dw=d9['id']
str3=tuple(dw)
print(str3)


# In[28]:


# 查询订单揽收日期
s10="""
select
tpoe.pre_order_id,
date_add(tpoe.event_time,interval 8 hour) 揽收日期,
elt(interval(ROUND((UNIX_TIMESTAMP(now())-UNIX_TIMESTAMP(tpoe.event_time))/60/60/24,1),-10,1,2,3,5,9), '1天内','2天内','3天内','4_5天', '6_10天', '10天以上') as 揽收至今
from
track_pre_order_event tpoe
where
tpoe.pre_order_id in {}
and tpoe.event_code='PICK'
and tpoe.is_deleted='n'
""".format(str3)
d10=execude_sql(s10)
print(d10)


# In[29]:


dj=pd.merge(d10,d9,left_on=['pre_order_id'],right_on=['id'],how='left')
print(dj)


# In[30]:


# 查询预录单中所有有揽收日期的订单的转换情况
s11="""
select
lpo.channel_code,
lpo.des,
date_add(tpoe.event_time,interval 8 hour) 揽收日期,
count(1) c,
(case when lpo.status=0 then "未处理" else "已处理" end) 是否操作
from
lg_pre_order lpo left join track_pre_order_event tpoe on lpo.id=tpoe.pre_order_id
where
lpo.platform='TIKTOK'
and tpoe.event_code='PICK'
and lpo.is_deleted='n'
and lpo.gmt_create {}
and tpoe.is_deleted='n'
group by
1,2,3,5
""".format(days)
d11=execude_sql(s11)
print(d11)


# In[31]:


# 查询标准事件id
s12="""
select
id code_id,
zh_name
from
standard_track_event
where 
is_deleted='n'
"""
d12=execude_sql(s12)


# In[32]:



print(d12)


# In[33]:


dqf=pd.merge(dqf,d12,on=['code_id'],how='left')
print(dqf)


# In[34]:


# name_l=[['总list',d1],['预录单',d2],['退件',dtj],['起飞计算',dqf],['积压',dj],['揽收预录单转换',d11]]


# In[35]:



name_l=[['总list',d1],['预录单',d2],['起飞计算',dqf],['积压',dj],['揽收预录单转换',d11],['退件',dtj],['标注事件',d12]]

print(name_l)

def file_xlsx(name,df):
    bf =r'F:\PBI临时文件\TIKTOK总表监控\{}.xlsx'.format(name)
    writer = pd.ExcelWriter(bf)
    df.to_excel(writer,'sheet1',index=False)
    writer.save()
for n in name_l:
    file_xlsx(n[0],n[1])


# In[ ]:





# In[ ]:




