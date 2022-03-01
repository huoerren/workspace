#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import openpyxl
import pymysql
import datetime,time
nows=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')


print(datetime.date.today())
import numpy as np
#数仓连接
con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()

# days="BETWEEN '2021-03-31 16:00:00' and '"+time_yes+"'"1
# days="BETWEEN '2021-03-31 16:00:00' and "+"'"+nows+"'"
days="BETWEEN '2021-09-30 16:00:00' and "+"'"+nows+"'"


end_day = datetime.date.today()
start_day = datetime.date.today()+ datetime.timedelta(-60)
print(end_day)
print(start_day)
print(days)
# days="BETWEEN '2020-12-17 16:00:00' and '2021-02-15 15:59:59'"

# list
s1="""select
channel_code "渠道名称",
des "目的地",
date_format(date_add(gmt_create,interval 8 hour),'%Y-%m-%d') as "业务日期" ,
 customer_id,
 round(timestampdiff(hour,lgo.gmt_create,lgo.delivery_date)/24,1) as delivery_interval,
 (case when isnull(lgo.volumetric_weight)=1 then "泡重情况1" when lgo.volumetric_weight<=lgo.weight then "泡重情况2" else "泡重情况3" end) 计泡,
 order_status "追踪状态" ,
 ISNULL( mawb_id ),
 count(1) c ,
 sum(volumetric_weight) 总泡重,
 sum(weight) 总重,
 avg(weight) 单重
from lg_order lgo 
where gmt_create {}
and platform='WISH_ONLINE' 
AND customer_id in (1151368,1151370,1181372,1181374) 
and is_deleted='n'   
group by
1,2,3,4,5,6,7,8
order by c desc
""".format(days)

# 预录单
s2="""
SELECT  
channel_code,
status,  
des,
date_format(date_add(gmt_create,interval 8 hour),'%Y-%m-%d') as datetime,  
count( 1 ) c ,
customer_id ,
sum(weight) 总重,
avg(weight) 单重
FROM  lg_pre_order lpo  
WHERE  gmt_create {}
AND platform = 'WISH_ONLINE'   AND customer_id IN ( 1151368, 1151370, 1181372, 1181374 )  and is_deleted= 'n'   
GROUP BY  channel_code,  des ,datetime,status,customer_id 
ORDER BY  c DESC 
""".format(days)

# 3PL妥投订单
s3="""SELECT 
lgo.channel_code,
lgo.des,
date_format(date_add(lgo.order_time,interval 8 hour),'%Y-%m-%d') orderdate,
date_format(date_add(lgo.delivery_date,interval 8 hour),'%y-%m-%d')ttdate, 
count(1) c 
from lg_order lgo 
where  lgo.order_time {}
and platform='WISH_ONLINE' 
AND customer_id not in (1151368,1151370,1181372,1181374) 
and lgo.is_deleted='n'
and lgo.order_status=3 
group by lgo.channel_code, lgo.des, orderdate, ttdate
""".format(days)

s4="""SELECT order_no,date_add(gmt_create,interval 8 hour) datetime 
from lg_order lgo
where 
gmt_create>  '2021-09-30 16:00:00'
and platform='WISH_ONLINE'
AND  customer_id IN ( 1151368, 1151370, 1181372, 1181374 ) and lgo.is_deleted='n'"""



def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df
# 主表
# d1=execude_sql(s1)
# # print(d1['业务日期'].dtypes)
# d1['当日']=datetime.date.today()
# d1['业务日期']=pd.to_datetime(d1['业务日期'])
# # print()
# d1['距离当天']=(pd.to_datetime(d1['当日'])- d1['业务日期']).dt.days

# # 追踪状态 的中文解释
# o_s={0:'未发送',1 :'已发送',2 :'转运中', 3:'送达',4:'超时',5:'扣关',6:'地址错误',7:'快件丢失', 8:'退件',9:'其他异常'}
# d1['状态']=d1['追踪状态'].map(o_s)
# d1['状态']=d1['状态'].fillna('销毁')

# # 间隔日
# # d1.info()
# d1['间隔日']=pd.cut(d1['距离当天'],[-999,-2,3, 5, 7, 12, 20, 30, 100000],
#                  labels=['已发送','03天未发出', '05天未发出', '07天未发出', '12天未发出', '20天未发出','30天未发出','30天以上未发出',
#                          ])
# d1.loc[d1['状态']!='未发送',['间隔日']]='已发送'

# # 淡旺季
# d1['淡旺季']='20210'#先默认淡季
# # print(d1['业务日期'].dt.month)
# # print(d1.loc[1,'业务日期'].year)
# for i in d1.index:
#     if d1.loc[i,'业务日期'].year==2020:
#         d1.loc[i,'淡旺季']='20201'
#     elif d1.loc[i,'业务日期'].month in [1,9,10,11,12]:
#         d1.loc[i, '淡旺季'] = '20211'
# print(d1)



# In[2]:


d1=execude_sql(s1)
# print(d1['业务日期'].dtypes)
d1['当日']=datetime.date.today()
d1['业务日期']=pd.to_datetime(d1['业务日期'])
# print()
d1['距离当天']=(pd.to_datetime(d1['当日'])- d1['业务日期']).dt.days


# In[3]:


o_s={0:'未发送',1 :'已发送',2 :'转运中', 3:'送达',4:'超时',5:'扣关',6:'地址错误',7:'快件丢失', 8:'退件',9:'其他异常'}
d1['状态']=d1['追踪状态'].map(o_s)
d1['状态']=d1['状态'].fillna('销毁')

# 间隔日
# d1.info()
d1['间隔日']=pd.cut(d1['距离当天'],[-999,-2,3, 5, 7, 12, 20, 30, 100000],
                 labels=['已发送','03天未发出', '05天未发出', '07天未发出', '12天未发出', '20天未发出','30天未发出','30天以上未发出',
                         ])
d1.loc[d1['状态']!='未发送',['间隔日']]='已发送'


# In[4]:


# 淡旺季
# d1['淡旺季']='20210'#先默认淡季
# print(d1['业务日期'].dt.month)
# print(d1.loc[1,'业务日期'].year)
def cal(a,b):
    if(a==2020):
        return '20201'
    elif b in [1,9,10,11,12]:
        return '20211'
    else:
        return '20210'

# for i in d1.index:
#     if d1.loc[i,'业务日期'].year==2020:
#         d1.loc[i,'淡旺季']='20201'
#     elif d1.loc[i,'业务日期'].month in [1,9,10,11,12]:
#         d1.loc[i, '淡旺季'] = '20211'
d1['淡旺季']=d1.apply(lambda x:cal(x['业务日期'].year,x['业务日期'].month),axis=1)


# In[5]:


s6="""
SELECT 
lgo.channel_code "渠道名称",
lgo.des "目的地",
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') as "业务日期" ,
lgo.customer_id,
lgo.order_status "追踪状态" ,
lgo.mawb_id,
count(1)   "总票数"
from 
lg_order lgo 
where  
lgo.gmt_create between '{} 16:00:00' and '{} 15:59:59'
and platform='WISH_ONLINE' 
AND customer_id in (1151368,1151370,1181372,1181374) 
and is_deleted='n'   
group by
1,2,3,4,5,6
""".format(start_day,end_day)


# In[6]:


# 滚动2个月总票数
d6=execude_sql(s6)
dm=d6['mawb_id']
dm.dropna(axis=0,how='any',inplace=True)
str1=tuple(dm)
# print(str1)
#滚动2个月起飞时间


# In[7]:


s5="""
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
""".format(str1)
d5=execude_sql(s5)
print(d5)

d5=d5.sort_values(by=['mawb_id','起飞日期'],ascending=False).drop_duplicates(subset=['mawb_id'],keep='first')

d7=pd.merge(d6,d5,on=['mawb_id'],how='left')


# In[8]:


# d5=d5.sort_values(by=['mawb_id','起飞日期'],ascending=False).drop_duplicates(subset=['mawb_id'],keep='first')


# In[9]:


# d7=d7.sort_values(by=['mawb_id','起飞日期'],ascending=False).drop_duplicates(subset=['mawb_id'],keep='first')


# In[10]:


s8="""
select
lgm.mawb_no,lgm.id mawb_id,lgm.destination_airport
from
lg_mawb lgm
where
lgm.id in {}
and lgm.is_deleted='n'
""".format(str1)
d8=execude_sql(s8)
print(d8)


# In[11]:


dtt=pd.merge(d7,d8,on=['mawb_id'],how='left')

dtt['当日']=datetime.date.today()
dtt['业务日期']=pd.to_datetime(dtt['业务日期'])
# print()
dtt['距离当天']=(pd.to_datetime(dtt['当日'])- dtt['业务日期']).dt.days

# 追踪状态 的中文解释
o_s={0:'未发送',1 :'已发送',2 :'转运中', 3:'送达',4:'超时',5:'扣关',6:'地址错误',7:'快件丢失', 8:'退件',9:'其他异常'}
dtt['状态']=dtt['追踪状态'].map(o_s)
dtt['状态']=dtt['状态'].fillna('销毁')
name_l=[['总list',d1],['预录单',execude_sql(s2)],['3PL妥投订单',execude_sql(s3)],['起飞计算',dtt]]

print(name_l)

def file_xlsx(name,df):
    bf =r'F:\PBI临时文件\wish总表监控\{}.xlsx'.format(name)
    writer = pd.ExcelWriter(bf)
    df.to_excel(writer,'sheet1',index=False)
    writer.save()
for n in name_l:
    file_xlsx(n[0],n[1])

d4=execude_sql(s4)
d4.to_csv(r'F:\PBI临时文件\wish总表监控\正式单.csv',index=False)


# In[ ]:





# In[ ]:





# In[ ]:




