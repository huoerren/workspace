#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import numpy as np
import openpyxl
import pymysql
import datetime,time


from dateutil.parser import parse
from business_duration import businessDuration
from dateutil import rrule



def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df

con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()




nows=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

# days="BETWEEN '2021-12-31 16:00:00' and "+"'"+nows+"'"
days = """ BETWEEN '2022-02-06 16:00:00' and '2022-02-13 16:00:00'   """
bag_no=pd.read_excel(r'C:\Users\hp\Desktop\ebay大包号.xlsx')
tp_bag=tuple(bag_no['bag_no'].tolist())

S0="""
SELECT order_no
from lg_order lgo,lg_bag_order_relation lbor,lg_bag lgb
where 
lgo.id=lbor.order_id
and lbor.bag_id=lgb.id
and lbor.is_deleted='n'
and lgb.is_deleted='n'
and lgb.bag_no in{}
""".format(tp_bag)




d0=execude_sql(S0)




tp_order=tuple(d0['order_no'].tolist())


# 揽收时间
S1 = """

"""


# 称重封袋时间
S2="""
SELECT 
order_no 内单号,
date_add(lgb.sealing_bag_time,interval 8 hour) 称重封袋时间
from 
lg_order lgo,
lg_bag_order_relation lbor,
lg_bag lgb
where 
order_no in{}
and lgo.id=lbor.order_id
and lgb.id=lbor.bag_id
and lbor.is_deleted='n'
and lgb.is_deleted='n'
and isnull(lgb.sealing_bag_time)=0
""".format(tp_order)



d2=execude_sql(S2)



S3="""
SELECT 
order_no 内单号,
date_add(event_time,interval 8 hour) 装车时间
FROM
lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
where
lgo.order_no in {}
and lgo.is_deleted='n'
and lbor.order_id=lgo.id
and lbor.is_deleted='n'
and lbor.bag_id=tbe.bag_id
and tbe.is_deleted='n'
and tbe.event_code='DEPS'
""".format(tp_order)




d3=execude_sql(S3)



S4="""
SELECT 
order_no 内单号,
date_add(event_time,interval 8 hour) 起飞时间
FROM
lg_order lgo,track_mawb_event tme
where
lgo.order_no in {}
and lgo.is_deleted='n'
and lgo.mawb_id=tme.mawb_id
and tme.is_deleted='n'
and event_code in ("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF','PMWC')
""".format(tp_order)

d4=execude_sql(S4)



S5="""
SELECT 
order_no 内单号,
date_add(event_time,interval 8 hour) 落地时间
FROM
lg_order lgo,track_mawb_event tme
where
lgo.order_no in {}
and lgo.is_deleted='n'
and lgo.mawb_id=tme.mawb_id
and tme.is_deleted='n'
and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
""".format(tp_order)



d5=execude_sql(S5)



S6="""
SELECT
order_no 内单号,
date_add(tme.event_time,interval 8 hour)  提货时间
FROM
lg_order lgo,
track_mawb_event tme
WHERE
lgo.order_no in {}
and lgo.is_deleted='n'
and lgo.mawb_id=tme.mawb_id
and tme.event_code in ("RPPU","MWPU","DDTH","ZDTH")
and tme.is_deleted='n'
""".format(tp_order)






d6=execude_sql(S6)



S7="""
SELECT 
order_no 内单号,
date_add(tme1.event_time,interval 8 hour) 进入海关监管仓时间
from
lg_order lgo,
track_mawb_event tme1
where 
lgo.order_no in {}
and lgo.is_deleted='n'
and lgo.mawb_id=tme1.mawb_id
and tme1.event_code in ("ABDW","ENCI")
""".format(tp_order)


d7=execude_sql(S7)




S81="""
SELECT 
order_no 内单号,
date_add(tbe.event_time,interval 8 hour) 清关时间
from 
lg_order lgo,
lg_bag_order_relation lbor,
track_bag_event tbe
where 
lgo.order_no in {}
and lgo.is_deleted='n'
and lgo.id=lbor.order_id
and tbe.bag_id=lbor.bag_id 
and tbe.event_code in ("IRCM","PVCS","IRCN","RFIC","BGRK")
""".format(tp_order)


d81=execude_sql(S81)


S82="""
SELECT 
order_no 内单号,
date_add(tme.event_time,interval 8 hour) 清关时间
from 
lg_order lgo,
track_mawb_event tme
where 
lgo.order_no in {}
and lgo.is_deleted='n'
and lgo.mawb_id=tme.mawb_id
and tme.event_code in ("IRCM","PVCS","IRCN","RFIC","BGRK")
""".format(tp_order)



d82=execude_sql(S82)


d8=pd.concat([d81,d82])


S9="""
SELECT 
order_no 内单号,
date_add(event_time,interval 8 hour) 抵达转运中心时间
FROM
lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
where
lgo.order_no in {}
and lgo.is_deleted='n'
and lbor.order_id=lgo.id
and lbor.is_deleted='n'
and lbor.bag_id=tbe.bag_id
and tbe.is_deleted='n'
and event_code in ("JFMD")
""".format(tp_order)



d9=execude_sql(S9)



S10="""
SELECT 
order_no 内单号,
date_add(event_time,interval 8 hour) 离开转运中心时间
FROM
lg_order lgo,track_supplier_en_event tsee
where
lgo.order_no in {}
and lgo.is_deleted='n'
and tsee.order_id=lgo.id
and tsee.is_deleted='n'
and tsee.standard_event_id=90042
""".format(tp_order)



d10=execude_sql(S10)



S11="""
SELECT 
order_no 内单号,
date_add(delivery_date,interval 8 hour) 妥投时间
from 
lg_order lgo
WHERE
lgo.order_no in {}
and lgo.is_deleted='n'
""".format(tp_order)


d11=execude_sql(S11)

total=pd.merge(d2,d3,on=['内单号'],how='left')
total=pd.merge(total,d4,on=['内单号'],how='left')
total=pd.merge(total,d5,on=['内单号'],how='left')
total=pd.merge(total,d6,on=['内单号'],how='left')
total=pd.merge(total,d7,on=['内单号'],how='left')
total=pd.merge(total,d8,on=['内单号'],how='left')
total=pd.merge(total,d9,on=['内单号'],how='left')
total=pd.merge(total,d10,on=['内单号'],how='left')
total=pd.merge(total,d11,on=['内单号'],how='left')

print(total)



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
                period = round((complete_time - received_time).total_seconds()/3600 ,2 )
                if period>12:
                    period=12
                period = round(period/24 ,2 )
                return period

        # case02:开始时间和结束时间不是在同一天
        else:

            # 判断起始时间是否在周六且在周六12点前，如果是 则 需要考虑起始时间 至 周六 12点前的时间
            if (received_time.weekday()== 5) : # 周六
                date_compare = received_time.strftime('%Y-%m-%d')  + " 12:00:00"
                sub_time_01_seconds = (parse(date_compare) - received_time).total_seconds()  #计算周六起始时间用时
                sub_time_01_hours = round(sub_time_01_seconds/(3600),2)
                period = period + sub_time_01_hours

            # 判断结束时间是否在周六且在周六12点前，如果是 则 需要考虑周六 00:00:00 至 周六的时间
            # 结束时间 只有两种情况，case01: 在 周一至周五 ； case02:周六 12点前。
            if (complete_time.weekday()== 5) : # 周六
                date_compare = complete_time.strftime('%Y-%m-%d')  + " 00:00:00"
                sub_time_02_seconds = ( complete_time - parse(date_compare)).total_seconds()  #计算结束时间完成用时
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

total['装车用时']=total.apply(lambda x:round(((x["装车时间"]-x["称重封袋时间"]).total_seconds())/60/60/24,2),axis=1)
#装车时间--起飞时间

total['起飞用时']=total.apply(lambda x:round(((x["起飞时间"]-x["装车时间"]).total_seconds())/60/60/24,2),axis=1)

#起飞时间--落地时间
total['飞行用时']=total.apply(lambda x:round(((x["落地时间"]-x["起飞时间"]).total_seconds())/60/60/24,2),axis=1)

#到达机场-进口”---“进入海关监管仓
#落地-提货
total['提货时间用时']=total.apply(lambda x:forMX(x['落地时间'],x['提货时间']),axis=1)

#提货--进入海关监管仓用时
total['进入海关监管仓用时']=total.apply(lambda x:forMX(x['提货时间'],x['进入海关监管仓时间']),axis=1)


#进入海关监管仓”---“海关放行-进口

total['清关用时']=total.apply(lambda x:forMX_02(x['进入海关监管仓时间'],x['清关时间']),axis=1)


#进入海关监管仓--抵达转运中心
total['抵达转运中心用时']=total.apply(lambda x:forMX(x['清关时间'],x['抵达转运中心时间']),axis=1)

#进入海关监管仓--抵达转运中心
total['离开转运中心用时']=total.apply(lambda x:forMX(x['抵达转运中心时间'],x['离开转运中心时间']),axis=1)

#海关放行-进口”---“妥投
total['妥投用时']=total.apply(lambda x:forMX(x['离开转运中心时间'],x['妥投时间']),axis=1)
total.to_excel("./测试.xlsx")



