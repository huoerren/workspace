#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import openpyxl
import pymysql
import datetime,time


from dateutil.parser import parse
from business_duration import businessDuration
from dateutil import rrule


# In[4]:


def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df

con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()


# In[5]:


nows=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')


# In[6]:


# days="BETWEEN '2021-12-31 16:00:00' and "+"'"+nows+"'"


# In[7]:


bag_no=pd.read_excel(r'C:\Users\hp\Desktop\ebay大包号.xlsx')


# In[8]:


tp_bag=tuple(bag_no['bag_no'].tolist())


# In[9]:


# 根据大包号查询定位订单并查询出包裹id，主单id，order_id，妥投时间

S0="""
SELECT 
lgo.order_no,
lgb.bag_no,
lgb.id bag_id,
lgo.mawb_id,
lgo.id order_id,
lgo.weight,
date_add(delivery_date,interval 8 hour) 妥投时间
from 
lg_order lgo,
lg_bag_order_relation lbor,
lg_bag lgb
where 
lgo.id=lbor.order_id
and lbor.bag_id=lgb.id
and lbor.is_deleted='n'
and lgb.is_deleted='n'
and lgb.bag_no in{}
""".format(tp_bag)


# In[10]:


d0=execude_sql(S0)


# In[11]:


d0


# In[12]:


tp_order=tuple(d0['order_no'].tolist())
tp_bagid=tuple(d0['bag_id'].tolist())
tp_orderid=tuple(d0['order_id'].tolist())

dm=d0['mawb_id']
dm.dropna(axis=0,how='any',inplace=True)
tp_mawbid=tuple(dm)


# In[13]:


# 揽收时间，目前节点没弄好，未确认表来源

S1 = """

"""


# In[14]:


# 称重封袋时间,根据大包id查询封袋时间，但是前提是不换大包号
S2="""
SELECT 
lgb.id bag_id,
date_add(lgb.sealing_bag_time,interval 8 hour) 称重封袋时间
from 
lg_bag lgb
where 
lgb.bag_no in{}
and lgb.is_deleted='n'
""".format(tp_bag)


# In[15]:


d2=execude_sql(S2)


# In[16]:


d2


# In[17]:


# 根据大包ID来查询装车时间
S3="""
SELECT 
tbe.bag_id ,
date_add(tbe.event_time,interval 8 hour) 装车时间
FROM
track_bag_event tbe
where
tbe.bag_id in {}
and tbe.is_deleted='n'
and tbe.event_code='DEPS'
""".format(tp_bagid)


# In[18]:


d3=execude_sql(S3)


# In[19]:


# 根据主单id查询航班起飞时间，因为存在部分起飞与后续全部起飞，导致一个id有可能出现两个起飞时间，根据业务场景，默认最后一个时间为主单起飞时间
S4="""
SELECT 
tme.mawb_id,
date_add(tme.event_time,interval 8 hour) 起飞时间
FROM
track_mawb_event tme
where
tme.mawb_id in {}
and tme.is_deleted='n'
and event_code in ("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF','PMWC')
""".format(tp_mawbid)


# In[20]:


d4=execude_sql(S4)

# 求每个主单最后的起飞时间
d4['起飞时间']=pd.to_datetime(d4['起飞时间'])
d4=d4.sort_values('起飞时间',ascending=False)
d4=d4.drop_duplicates(['mawb_id'],keep='first')
print(d4)


# In[21]:


# 根据主单id查询主单落地时间，目前只在tme查询
S5="""
SELECT 
tme.mawb_id,
date_add(tme.event_time,interval 8 hour) 落地时间
FROM
track_mawb_event tme
where
tme.mawb_id in {}
and tme.is_deleted='n'
and tme.event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
""".format(tp_mawbid)


# In[22]:


# 落地时间
d5=execude_sql(S5)


# In[23]:


# 根据主单id查询提货时间，目前只在tme查询
S6="""
SELECT
tme.mawb_id,
date_add(tme.event_time,interval 8 hour) 提货时间
FROM
track_mawb_event tme
WHERE
tme.mawb_id in {}
and tme.event_code in ("RPPU","MWPU","RPPU","MWPU")
and tme.is_deleted='n'
""".format(tp_mawbid)


# In[24]:


# 提货时间
d6=execude_sql(S6)


# In[25]:


# 根据主单id查询进入海关监管仓时间，从落地至此是redpack提供服务，6个工作日
S7="""
SELECT 
tme.mawb_id ,
date_add(tme.event_time,interval 8 hour) 进入海关监管仓时间
from
track_mawb_event tme
where 
tme.mawb_id in {}
and tme.is_deleted='n'
and tme.event_code in ("ABDW","ENCI")
""".format(tp_mawbid)


# In[26]:


# 进入海关监管仓
d7=execude_sql(S7)


# In[27]:


d7


# In[28]:


# 依次根据主单id，内单id，大包id查询主单的清关时间聚合，再求出按照主单id最大的清关时间作为主单id唯一的清关时间
S8="""
select
qg.mawb_id,
max(qg.qgdate) qgdate
from
((select  
tme.mawb_id,
date_add(tme.event_time,interval 8 hour) qgdate
from 
track_mawb_event tme
where  
 tme.event_code in("IRCM","PVCS","IRCN","RFIC", "BGRK") 
AND tme.mawb_id in {}  
and tme.is_deleted='n' )

union all

(select  
distinct
lgo.mawb_id ,
date_add(toe.event_time,interval 8 hour) qgdate
from 
lg_order lgo,
track_order_event toe
where  
 toe.event_code in("IRCM","PVCS","IRCN","RFIC", "BGRK") 
and toe.order_id in {}
and toe.order_id=lgo.id
AND lgo.is_deleted='n'   
and toe.is_deleted='n'
group by 1,2)

union all

(select
distinct
lgo.mawb_id,
date_add(tbe.event_time,interval 8 hour) qgdate
from 
track_bag_event tbe, 
lg_order lgo, 
lg_bag_order_relation lbor 
where 
tbe.event_code in("IRCM","PVCS","IRCN","RFIC", "BGRK")  
and tbe.bag_id in {} 
and lgo.id=lbor.order_id 
and tbe.bag_id=lbor.bag_id
AND lgo.is_deleted='n'    
and tbe.is_deleted='n' 
and lbor.is_deleted='n' 
group by 1,2)
) qg
group by
1
""".format(tp_mawbid,tp_orderid,tp_bagid)


# In[29]:


# 清关时间
d8=execude_sql(S8)


# In[30]:


d8


# In[ ]:





# In[ ]:





# In[31]:


# 根据大包id查询抵达转运中心时间
S9="""
SELECT 
tbe.bag_id ,
date_add(event_time,interval 8 hour) 抵达转运中心时间
FROM
track_bag_event tbe
where
tbe.bag_id in {}
and tbe.is_deleted='n'
and tbe.event_code in ("JFMD")
""".format(tp_bagid)


# In[32]:


# 包裹抵达转运中心
d9=execude_sql(S9)


# In[33]:


d9


# In[34]:


# 根据order_id来查询tsee中的离开转运中心时间
S10="""
SELECT 
tsee.order_id ,
date_add(event_time,interval 8 hour) 离开转运中心时间
FROM
track_supplier_en_event tsee
where
tsee.order_id in {}
and tsee.is_deleted='n'
and tsee.standard_event_id=90042
""".format(tp_orderid)


# In[35]:


# 离开转运中心时间
d10=execude_sql(S10)


# In[36]:


d10


# In[37]:


# 根据主单id查询主单号
S11="""
SELECT 
id mawb_id,
mawb_no 主单号
from 
lg_mawb 
WHERE
id in {}
and is_deleted='n'
""".format(tp_mawbid)


# In[38]:


# 查询主单详情
d11=execude_sql(S11)


# In[39]:


d11


# In[40]:


# 合并以上所有表
total=pd.merge(d0,d2,on=['bag_id'],how='left')
total=pd.merge(total,d3,on=['bag_id'],how='left')
total=pd.merge(total,d4,on=['mawb_id'],how='left')
total=pd.merge(total,d5,on=['mawb_id'],how='left')
total=pd.merge(total,d6,on=['mawb_id'],how='left')
total=pd.merge(total,d7,on=['mawb_id'],how='left')
total=pd.merge(total,d8,on=['mawb_id'],how='left')
total=pd.merge(total,d9,on=['bag_id'],how='left')
total=pd.merge(total,d10,on=['order_id'],how='left')
total=pd.merge(total,d11,on=['mawb_id'],how='left')


# In[41]:


total


# In[42]:


db=d0.groupby(['bag_no']).agg({'order_no':'count','weight':'sum'})


# In[43]:


db


# In[ ]:





# In[ ]:





# In[ ]:





# In[44]:


# 定义大包重量段分布函数
def tj(a):
    if  0<a<=5:
        return"0-5kg"
    elif  a<=10:
        return"5-10kg"
    elif  a<=15:
        return"10-15kg"
    else:
        return"15kg以上"


# In[45]:


db['重量段']=db.apply(lambda x:tj(x['weight']),axis=1)


# In[46]:


db


# In[47]:


def forMX(received_time,complete_time): # 周一到周六
    if(pd.isnull(received_time) or pd.isnull(complete_time)):
        return np.nan
    workdays = [x for x in range(7) if x not in [6]]
    time_period = rrule.rrule(rrule.MINUTELY, dtstart=received_time, until=complete_time, byweekday=workdays).count()
    return round(time_period / (60 * 24), 2)

def forMX_02(received_time,complete_time): # 周一到周五及周六上午半天
    if(pd.isnull(received_time) or pd.isnull(complete_time)):
        return np.nan
    else:
        period = businessDuration(received_time, complete_time, unit='min')
        period = round(period / (60), 2)  # 将‘min’ 转为 ‘hour’，得到周一到周五 时常（小时）

        # case01: 开始时间和结束时间是在同一天
        if received_time.strftime('%Y-%m-%d') == complete_time.strftime('%Y-%m-%d'):
            if (received_time.weekday()!= 5) :
                period = round((complete_time - received_time).total_seconds()/3600 ,2 )
                period = round(period/24 ,2 )
                return period
            else:
                period1 = round((complete_time - received_time).total_seconds()/3600 ,2 )
                date_compare = received_time.strftime('%Y-%m-%d')  + " 12:00:00"
                period2 = round((parse(date_compare) - received_time).total_seconds()/3600 ,2 )
                if period2>=period1:
                    period=period1
                else:
                    period=period2
                period = round(period/24 ,2 )
                return period

        # case02:开始时间和结束时间不是在同一天
        else:

            # 判断起始时间是否在周六且在周六12点前，如果是 则 需要考虑起始时间 至 周六 12点前的时间
            if (received_time.weekday()== 5) : # 周六
                date_compare = received_time.strftime('%Y-%m-%d')  + " 12:00:00"
                sub_time_01_seconds = (parse(date_compare) - received_time).total_seconds()
                sub_time_01_hours = round(sub_time_01_seconds/(3600),2)
                if (sub_time_01_hours>0):
                    period = period + sub_time_01_hours

            # 判断结束时间是否在周六且在周六12点前，如果是 则 需要考虑周六 00:00:00 至 周六的时间
            # 结束时间 只有两种情况，case01: 在 周一至周五 ； case02:周六 12点前。
            if (complete_time.weekday()== 5) : # 周六
                date_compare = complete_time.strftime('%Y-%m-%d')  + " 00:00:00"
                sub_time_02_seconds = ( complete_time - parse(date_compare)).total_seconds()
                sub_time_02_hours = round(sub_time_02_seconds/(3600),2)
                if(sub_time_02_hours>12):
                    sub_time_02_hours=12
                period = period + sub_time_02_hours
            # 判断在 (开始时间, 结束时间) 之间有几个周六, 有x个周六，则需要在总时间之上再加上 x个半天
            days = complete_time - received_time
            count=0
            received_time=received_time + datetime.timedelta(days=1)
            while(received_time.strftime('%Y-%m-%d') <= (complete_time+datetime.timedelta(days=-1)).strftime('%Y-%m-%d')):
                if (received_time).weekday() == 5:
                    count+=1
                received_time=received_time + datetime.timedelta(days=1)
            period = period + 12 * count
        period = round(period/24 ,2 )
        return  period  


# In[48]:


total['装车用时']=total.apply(lambda x:round(((x["装车时间"]-x["称重封袋时间"]).total_seconds())/60/60/24,2),axis=1)
#装车时间--起飞时间

total['起飞用时']=total.apply(lambda x:round(((x["起飞时间"]-x["装车时间"]).total_seconds())/60/60/24,2),axis=1)

#起飞时间--落地时间
total['飞行用时']=total.apply(lambda x:round((((x["落地时间"]-x["起飞时间"]).total_seconds())/60/60+14)/24,2),axis=1)

#到达机场-进口”---“进入海关监管仓
#落地-提货
total['提货时间用时']=total.apply(lambda x:forMX(x['落地时间'],x['提货时间']),axis=1)

#提货--进入海关监管仓用时
total['进入海关监管仓用时']=total.apply(lambda x:forMX(x['提货时间'],x['进入海关监管仓时间']),axis=1)


#进入海关监管仓”---“海关放行-进口

total['清关用时']=total.apply(lambda x:forMX_02(x['进入海关监管仓时间'],x['qgdate']),axis=1)


#进入海关监管仓--抵达转运中心
total['抵达转运中心用时']=total.apply(lambda x:forMX(x['qgdate'],x['抵达转运中心时间']),axis=1)

#进入海关监管仓--抵达转运中心
total['离开转运中心用时']=total.apply(lambda x:forMX(x['抵达转运中心时间'],x['离开转运中心时间']),axis=1)

#海关放行-进口”---“妥投
total['妥投用时']=total.apply(lambda x:forMX(x['离开转运中心时间'],x['妥投时间']),axis=1)

# 全段用时
total['全段用时']=total['装车用时']+total['起飞用时']+total['飞行用时']+total['提货时间用时']+total['进入海关监管仓用时']+total['清关用时']+total['抵达转运中心用时']+total['离开转运中心用时']+total['妥投用时']
total.to_excel("./测试.xlsx")


# In[49]:


db.to_excel('./大包号重量.xlsx')


# In[ ]:




