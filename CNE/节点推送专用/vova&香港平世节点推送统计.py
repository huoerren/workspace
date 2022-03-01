#coding=utf-8

import pandas as pd
pd.set_option('display.max_rows', 10)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)
pd.set_option('expand_frame_repr', False)
import openpyxl
import pymysql
import datetime,time
import pyecharts.options as opts
from pyecharts.charts import Line,Grid,Page
from pyecharts.globals import ThemeType
nows=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
# 目的文件存放地址
file=r'F:\其他部门\vova&平世节点推送统计'
print(datetime.date.today())
import numpy as np
# #数仓连接
con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()

# days="BETWEEN '2021-03-31 16:00:00' and '"+time_yes+"'"1
# days="BETWEEN '2021-05-31 16:00:00' and '2021-06-30 16:00:00' "
# days="BETWEEN '2021-04-30 16:00:00' and "+"'"+nows+"'"
days1="BETWEEN '2021-02-28 16:00:00' and '2021-03-31 16:00:00'"
days2="BETWEEN '2021-03-31 16:00:00' and '2021-04-30 16:00:00'"
days3="BETWEEN '2021-04-30 16:00:00' and '2021-05-31 16:00:00'"
days4="BETWEEN '2021-05-31 16:00:00' and '2021-06-30 16:00:00'"
days5="BETWEEN '2021-06-30 16:00:00' and "+"'"+nows+"'"

#统计-主单-状态-数量
S1 ="""
SELECT                                                                                                
date_format(date_add(lgo.order_time,interval 8 hour),'%Y-%m-%d') 下单日期, 
lgo.channel_code 渠道,
lgo.des 国家, 
lgo.customer_id 客户id,
lgm.mawb_no 主单号,
lgo.order_status 物流状态,
# (case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
count(1) 票数
FROM 
lg_order lgo,
lg_mawb lgm
where
lgo.customer_id in(3041600, 1211327 )
and lgo.mawb_id=lgm.id
# and lgo.platform ='WISH_ONLINE'
and lgo.order_time >= '2021-02-28 16:00:00'
and lgo.is_deleted='n'
# and lgo.channel_code in('CNE全球经济','CNE全球特惠','CNE全球优先','CNE全球通挂号','CNE全球通平邮')
group by
1,2,3,4,5,6
"""

# 统计单个节点推送率
S2="""
SELECT
date_format(date_add(vptl.order_time,interval 8 hour),'%Y-%m-%d') 下单日期,
vptl.channel_code 渠道,
vptl.des 国家,
vptl.customer_id 客户id,
vptl.tracking_status,
lgm.mawb_no 主单号,
lgo.order_status 物流状态,
count(distinct vptl.pre_order_id)  as "推送量"
FROM
vova_push_track_log vptl,lg_order lgo ,lg_mawb lgm
where
vptl.customer_id in(3041600,1211327 )
and vptl.pre_order_id=lgo.pre_order_id
and lgo.mawb_id=lgm.id
and vptl.order_time {}
and vptl.is_deleted='n'
and vptl.tracking_status in ('LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER','DELIVERED','RETURNED_FROM_OVERSEA')
group by
1,2,3,4,5,6,7
""".format(days1)

S3="""
SELECT
date_format(date_add(vptl.order_time,interval 8 hour),'%Y-%m-%d') 下单日期,
vptl.channel_code 渠道,
vptl.des 国家,
vptl.customer_id 客户id,
vptl.tracking_status,
lgm.mawb_no 主单号,
lgo.order_status 物流状态,
count(distinct vptl.pre_order_id)  as "推送量"
FROM
vova_push_track_log vptl,lg_order lgo,lg_mawb lgm
where
vptl.customer_id in(3041600,1211327 )
and vptl.pre_order_id=lgo.pre_order_id
and lgo.mawb_id=lgm.id
and vptl.order_time {}
and vptl.is_deleted='n'
and vptl.tracking_status in ('LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER','DELIVERED','RETURNED_FROM_OVERSEA')
group by
1,2,3,4,5,6,7
""".format(days2)


S4="""
SELECT
date_format(date_add(vptl.order_time,interval 8 hour),'%Y-%m-%d') 下单日期,
vptl.channel_code 渠道,
vptl.des 国家,
vptl.customer_id 客户id,
vptl.tracking_status,
lgm.mawb_no 主单号,
lgo.order_status 物流状态,
count(distinct vptl.pre_order_id)  as "推送量"
FROM
vova_push_track_log vptl,lg_order lgo,lg_mawb lgm
where
vptl.customer_id in(3041600,1211327 )
and vptl.pre_order_id =lgo.pre_order_id
and lgo.mawb_id=lgm.id
and vptl.order_time {}
and vptl.is_deleted='n'
and vptl.tracking_status in ('LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER','DELIVERED','RETURNED_FROM_OVERSEA')
group by
1,2,3,4,5,6,7
""".format(days3)

S5="""
SELECT
date_format(date_add(vptl.order_time,interval 8 hour),'%Y-%m-%d') 下单日期,
vptl.channel_code 渠道,
vptl.des 国家,
vptl.customer_id 客户id,
vptl.tracking_status,
lgm.mawb_no 主单号,
lgo.order_status 物流状态,
count(distinct vptl.pre_order_id)  as "推送量"
FROM
vova_push_track_log vptl,lg_order lgo,lg_mawb lgm
where
vptl.customer_id in(3041600,1211327 )
and vptl.pre_order_id=lgo.pre_order_id
and lgo.mawb_id=lgm.id
and vptl.order_time {}
and vptl.is_deleted='n'
and vptl.tracking_status in ('LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER','DELIVERED','RETURNED_FROM_OVERSEA')
group by
1,2,3,4,5,6,7
""".format(days4)

S6="""
SELECT
date_format(date_add(vptl.order_time,interval 8 hour),'%Y-%m-%d') 下单日期, 
vptl.channel_code 渠道,
vptl.des 国家, 
vptl.customer_id 客户id,
vptl.tracking_status,
lgm.mawb_no 主单号,
lgo.order_status 物流状态,
count(distinct vptl.pre_order_id)  as "推送量"
FROM 
vova_push_track_log vptl,lg_order lgo,lg_mawb lgm
where
vptl.customer_id in(3041600,1211327 )
and vptl.pre_order_id=lgo.pre_order_id
and lgo.mawb_id=lgm.id
and vptl.order_time {}
and vptl.is_deleted='n'
and vptl.tracking_status in ('LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER','DELIVERED','RETURNED_FROM_OVERSEA')
group by
1,2,3,4,5,6,7
""".format(days5)


# 统计推送了所有节点的订单量
S7="""
select
下单日期,渠道,国家,客户id,lgm.mawb_no 主单号,lgo.order_status 物流状态,count(*)  all票数
from
(
select
date_format(date_add(vptl.order_time,interval 8 hour),'%Y-%m-%d') 下单日期,
vptl.channel_code 渠道,
vptl.des 国家,
vptl.customer_id 客户id,
pre_order_id
FROM
vova_push_track_log vptl
where
vptl.customer_id in(3041600,1211327)
and vptl.order_time {}
# and lgo.channel_code='CNE全球优先'
# and lgo.des='GR'
and vptl.is_deleted='n'
and vptl.tracking_status in ('LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER','DELIVERED')
group by
1,2,3,4,pre_order_id
having (count(distinct tracking_status)=8 )
) t,lg_order lgo,lg_mawb lgm
where
t.pre_order_id=lgo.pre_order_id
and lgo.mawb_id=lgm.id
GROUP BY
下单日期,渠道,国家,客户id,主单号,物流状态""".format(days1)

S8="""
select t.下单日期,t.渠道,t.国家,t.客户id,lgm.mawb_no 主单号,lgo.order_status 物流状态,count(*) all票数
from
(
select
date_format(date_add(vptl.order_time,interval 8 hour),'%Y-%m-%d') 下单日期,
vptl.channel_code 渠道,
vptl.des 国家,
vptl.customer_id 客户id,
vptl.pre_order_id
FROM
vova_push_track_log vptl
where
vptl.customer_id in(3041600,1211327)
and vptl.order_time {}
# and lgo.channel_code='CNE全球优先'
# and lgo.des='GR'
and vptl.is_deleted='n'
and vptl.tracking_status in ('LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER','DELIVERED')
group by
1,2,3,4,pre_order_id
having (count(distinct tracking_status)=8 )
) t,lg_order lgo,lg_mawb lgm
where
t.pre_order_id=lgo.pre_order_id
and lgo.mawb_id=lgm.id
GROUP BY 1,2,3,4,5,6""".format(days2)

S9="""
select
下单日期,渠道,国家,客户id,lgm.mawb_no 主单号,lgo.order_status 物流状态,count(*)  all票数
from
(
select
date_format(date_add(vptl.order_time,interval 8 hour),'%Y-%m-%d') 下单日期,
vptl.channel_code 渠道,
vptl.des 国家,
vptl.customer_id 客户id,
pre_order_id
FROM
vova_push_track_log vptl
where
vptl.customer_id in(3041600,1211327)
and vptl.order_time {}
# and lgo.channel_code='CNE全球优先'
# and lgo.des='GR'
and vptl.is_deleted='n'
and vptl.tracking_status in ('LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER','DELIVERED')
group by
1,2,3,4,pre_order_id
having (count(distinct tracking_status)=8 )
) t,lg_order lgo,lg_mawb lgm
where
t.pre_order_id=lgo.pre_order_id
and lgo.mawb_id=lgm.id
GROUP BY
下单日期,渠道,国家,客户id,主单号,物流状态""".format(days3)

S10="""
select
下单日期,渠道,国家,客户id,lgm.mawb_no 主单号,lgo.order_status 物流状态,count(*)  all票数
from
(
select
date_format(date_add(vptl.order_time,interval 8 hour),'%Y-%m-%d') 下单日期,
vptl.channel_code 渠道,
vptl.des 国家,
vptl.customer_id 客户id,
pre_order_id
FROM
vova_push_track_log vptl
where
vptl.customer_id in(3041600,1211327)
and vptl.order_time {}
# and lgo.channel_code='CNE全球优先'
# and lgo.des='GR'
and vptl.is_deleted='n'
and vptl.tracking_status in ('LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER','DELIVERED')
group by
1,2,3,4,pre_order_id
having (count(distinct tracking_status)=8 )
) t,lg_order lgo,lg_mawb lgm
where
t.pre_order_id=lgo.pre_order_id
and lgo.mawb_id=lgm.id
GROUP BY
下单日期,渠道,国家,客户id,主单号,物流状态""".format(days4)


S11="""
select 
下单日期,渠道,国家,客户id,lgm.mawb_no 主单号,lgo.order_status 物流状态,count(*)  all票数
from
(
select
date_format(date_add(vptl.order_time,interval 8 hour),'%Y-%m-%d') 下单日期, 
vptl.channel_code 渠道,
vptl.des 国家, 
vptl.customer_id 客户id,
pre_order_id
FROM 
vova_push_track_log vptl
where
vptl.customer_id in(3041600,1211327)
and vptl.order_time {}
# and lgo.channel_code='CNE全球优先'
# and lgo.des='GR'
and vptl.is_deleted='n'
and vptl.tracking_status in ('LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER','DELIVERED')
group by
1,2,3,4,pre_order_id
having (count(distinct tracking_status)=8 )
) t,lg_order lgo,lg_mawb lgm
where
t.pre_order_id=lgo.pre_order_id
and lgo.mawb_id=lgm.id
GROUP BY 
下单日期,渠道,国家,客户id,主单号,物流状态""".format(days5)

def execude_sql(SQL):

    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df
d1=execude_sql(S1)
print(d1)
d2=execude_sql(S2)
print(d2)
d3=execude_sql(S3)
print(d3)
d4=execude_sql(S4)
print(d4)
d5=execude_sql(S5)
print(d5)
d6=execude_sql(S6)
print(d6)
d7=execude_sql(S7)
print(d7)
d8=execude_sql(S8)
print(d8)
d9=execude_sql(S9)
print(d9)
d10=execude_sql(S10)
print(d10)
d11=execude_sql(S11)
print(d11)
dtd=pd.concat([d2,d3,d4,d5,d6])
dta=pd.concat([d7,d8,d9,d10,d11])
print(dta)
dta.fillna(0)
dt=d1.groupby(['下单日期','渠道','国家','客户id','主单号','物流状态'])['票数'].sum().reset_index()
# print(dt)
dt=pd.merge(dt,dtd[dtd['tracking_status']=='LINEHAUL_PICK_UP'],on=['下单日期','渠道','国家','客户id','主单号','物流状态'],how='left').drop('tracking_status',axis=1)
dt=dt.rename(columns={'推送量':'LINEHAUL_PICK_UP'})

dt=pd.merge(dt,dtd[dtd['tracking_status']=='DEPART_AIRPORT'],on=['下单日期','渠道','国家','客户id','主单号','物流状态'],how='left').drop('tracking_status',axis=1)
dt=dt.rename(columns={'推送量':'DEPART_AIRPORT'})
dt=pd.merge(dt,dtd[dtd['tracking_status']=='ARRIVE_DEST_AIRPORT'],on=['下单日期','渠道','国家','客户id','主单号','物流状态'],how='left').drop('tracking_status',axis=1)
dt=dt.rename(columns={'推送量':'ARRIVE_DEST_AIRPORT'})
dt=pd.merge(dt,dtd[dtd['tracking_status']=='DEPART_DEST_CUSTOMS'],on=['下单日期','渠道','国家','客户id','主单号','物流状态'],how='left').drop('tracking_status',axis=1)
dt=dt.rename(columns={'推送量':'DEPART_DEST_CUSTOMS'})
dt=pd.merge(dt,dtd[dtd['tracking_status']=='TRANSFER_LAST_MILE'],on=['下单日期','渠道','国家','客户id','主单号','物流状态'],how='left').drop('tracking_status',axis=1)
dt=dt.rename(columns={'推送量':'TRANSFER_LAST_MILE'})
dt=pd.merge(dt,dtd[dtd['tracking_status']=='ARRIVE_LAST_MILE'],on=['下单日期','渠道','国家','客户id','主单号','物流状态'],how='left').drop('tracking_status',axis=1)
dt=dt.rename(columns={'推送量':'ARRIVE_LAST_MILE'})
dt=pd.merge(dt,dtd[dtd['tracking_status']=='ATTEMPT_DELIVER'],on=['下单日期','渠道','国家','客户id','主单号','物流状态'],how='left').drop('tracking_status',axis=1)
dt=dt.rename(columns={'推送量':'ATTEMPT_DELIVER'})
dt=pd.merge(dt,dtd[dtd['tracking_status']=='DELIVERED'],on=['下单日期','渠道','国家','客户id','主单号','物流状态'],how='left').drop('tracking_status',axis=1)
dt=dt.rename(columns={'推送量':'DELIVERED'})
dt=pd.merge(dt,dtd[dtd['tracking_status']=='RETURNED_FROM_OVERSEA'],on=['下单日期','渠道','国家','客户id','主单号','物流状态'],how='left').drop('tracking_status',axis=1)
dt=dt.rename(columns={'推送量':'RETURNED_FROM_OVERSEA'})
dt=pd.merge(dt,dta,on=['下单日期','渠道','国家','客户id','主单号','物流状态'],how='left')
print(dt)
dt.fillna(0,inplace=True)
dt['下单日期']=pd.to_datetime(dt['下单日期'])
d1['下单日期']=pd.to_datetime(d1['下单日期'])
# d2['下单日期']=pd.to_datetime(d2['下单日期'])
# d3['下单日期']=pd.to_datetime(d3['下单日期'])
# d4['下单日期']=pd.to_datetime(d4['下单日期'])
# d5['下单日期']=pd.to_datetime(d5['下单日期'])
# d6['下单日期']=pd.to_datetime(d6['下单日期'])
# d7['下单日期']=pd.to_datetime(d7['下单日期'])
# d8['下单日期']=pd.to_datetime(d8['下单日期'])
# d9['下单日期']=pd.to_datetime(d9['下单日期'])
# d10['下单日期']=pd.to_datetime(d10['下单日期'])
# 淡旺季
# d1['淡旺季']='20210'#先默认淡季
# # print(d1['下单日期'].dt.month)
# # print(d1.loc[1,'下单日期'].year)
# for i in d1.index:
#     if d1.loc[ i,'下单日期'].year==2020:
#         d1.loc[i,'淡旺季']='20201'
#     elif d1.loc[i,'下单日期'].month in [1,9,10,11,12]:
#         d1.loc[i, '淡旺季'] = '20211'

# 添加周序数
# d1['周序数']=d1['下单日期'].dt.isocalendar().week
#这一天是周中的第几天，Monday=0, Sunday=6
# d1['dayofweek']=d1['下单日期'].dt.dayofweek
# print(d1.columns)
# z汇总周的起止日
dw = pd.DataFrame(pd.date_range(start='20210101',end=nows,periods=None,freq='D'),columns=['下单日期'])
dw['下单日期']=pd.to_datetime(dw['下单日期'])
dw['周序数']=dw['下单日期'].dt.isocalendar().week
dw['moon']=dw['下单日期'].dt.month.astype('str')
dw['day']=dw['下单日期'].dt.day.astype('str')
dw['日期']=dw['moon']+'.'+dw['day']
dw_min=dw[['周序数','日期']].drop_duplicates(['周序数'],keep='first')
dw_max=dw[['周序数','日期']].drop_duplicates(['周序数'],keep='last')
dwm=pd.merge(dw_min,dw_max,on=['周序数'],how='outer')
dwm['周期']=dwm['日期_x']+'-'+dwm['日期_y']
print(dwm)
dw=pd.merge(dw,dwm,on=['周序数'],how='left')
# dw['周期']=dwm['周序数'].map(dict(zip(dwm['周序数'],dwm['周期'])))
# # dw=dw.set_index(['周序数','dayofweek']).stack().unstack(['dayofweek',-1])
print(dw)

dt=pd.merge(dt,dw[['下单日期','周序数','周期','moon']],on=['下单日期'],how='left').sort_values(['周序数','渠道','国家','客户id','主单号'])
d1=pd.merge(d1,dw[['下单日期','周序数','周期','moon']],on=['下单日期'],how='left').sort_values(['周序数','渠道','国家','客户id','主单号'])
# d21=pd.merge(d2,dw[['下单日期','周序数','周期']],on=['下单日期'],how='left')
# d31=pd.merge(d3,dw[['下单日期','周序数','周期']],on=['下单日期'],how='left')
# d41=pd.merge(d4,dw[['下单日期','周序数','周期']],on=['下单日期'],how='left')
# d51=pd.merge(d5,dw[['下单日期','周序数','周期']],on=['下单日期'],how='left')
# d61=pd.merge(d6,dw[['下单日期','周序数','周期']],on=['下单日期'],how='left')
# d71=pd.merge(d7,dw[['下单日期','周序数','周期']],on=['下单日期'],how='left')
# d81=pd.merge(d8,dw[['下单日期','周序数','周期']],on=['下单日期'],how='left')
# d91=pd.merge(d9,dw[['下单日期','周序数','周期']],on=['下单日期'],how='left')
# d101=pd.merge(d10,dw[['下单日期','周序数','周期']],on=['下单日期'],how='left')
print(dt)
# dt1=d1.groupby(['周期','渠道','国家','客户id','物流状态','主单号'])['票数'].sum().reset_index()
# dm1=d1.groupby(['moon','渠道','国家','客户id','物流状态','主单号'])['票数'].sum().reset_index()
# print(dt1)
# print(dm1)
# dt1=pd.DataFrame(dt1,columns=['周期','渠道','国家','客户id','是否妥投','票数'])
# dm1=pd.DataFrame(dm1,columns=['moon','渠道','国家','客户id','是否妥投','票数'])
# print(dt1)
# print(dm1)
# dtt=dt.groupby(['周期','渠道','国家','客户id'],as_index=True)[['票数','LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER','DELIVERED','all票数']].sum()
# dtt=dt.groupby(['周期','渠道','国家','客户id','主单号','物流状态'],as_index=True).agg({'票数':sum,'LINEHAUL_PICK_UP':sum,'DEPART_AIRPORT':sum,'ARRIVE_DEST_AIRPORT':sum,'DEPART_DEST_CUSTOMS':sum,'TRANSFER_LAST_MILE':sum,'ARRIVE_LAST_MILE':sum,'ATTEMPT_DELIVER':sum,'DELIVERED':sum,'all票数':sum,'RETURNED_FROM_OVERSEA':sum,'ATTEMPT_DELIVER':sum}).reset_index()
dtt=dt.groupby(['周期','渠道','国家','客户id','主单号','物流状态'])[['票数','LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER','DELIVERED','RETURNED_FROM_OVERSEA','all票数']].sum().reset_index()
dtt=dtt.rename(columns={'票数':'总票数'})
print(dtt)
dmt=dt.groupby(['moon','渠道','国家','客户id','主单号','物流状态'])[['票数','LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER','DELIVERED','all票数','RETURNED_FROM_OVERSEA']].sum().reset_index()
dmt=dmt.rename(columns={'票数':'总票数'})
print(dmt)
# dt2=dt1[dt1['物流状态']==3]
# dtt=pd.merge(dtt,dt2,on=['周期','渠道','国家','客户id','主单号'],how='left')
# dm2=dm1[dm1['是否妥投']=='已妥投']
# dmt=pd.merge(dmt,dm2,on=['moon','渠道','国家','客户id','主单号'],how='left')
# print(dtt)
# print(dmt)
# dtt['系统妥投率']=dtt['票数']/dtt['总票数']

# dtt['LINEHAUL_PICK_UP比率']=dtt['LINEHAUL_PICK_UP']/dtt['总票数']
# dtt['DEPART_AIRPORT比率']=dtt['DEPART_AIRPORT']/dtt['总票数']
# dtt['ARRIVE_DEST_AIRPORT比率']=dtt['ARRIVE_DEST_AIRPORT']/dtt['总票数']
# dtt['DEPART_DEST_CUSTOMS比率']=dtt['DEPART_DEST_CUSTOMS']/dtt['总票数']
# dtt['TRANSFER_LAST_MILE比率']=dtt['TRANSFER_LAST_MILE']/dtt['总票数']
# dtt['ARRIVE_LAST_MILE比率']=dtt['ARRIVE_LAST_MILE']/dtt['总票数']
# dtt['ATTEMPT_DELIVER比率']=dtt['ATTEMPT_DELIVER']/dtt['总票数']
# dtt['DELIVERED比率']=dtt['DELIVERED']/dtt['总票数']
# dtt['全含票数']=dtt['all票数']/dtt['总票数']
#
# # dmt['系统妥投率']=dmt['票数']/dmt['总票数']
# dmt['LINEHAUL_PICK_UP比率']=dmt['LINEHAUL_PICK_UP']/dmt['总票数']
# dmt['DEPART_AIRPORT比率']=dmt['DEPART_AIRPORT']/dmt['总票数']
# dmt['ARRIVE_DEST_AIRPORT比率']=dmt['ARRIVE_DEST_AIRPORT']/dmt['总票数']
# dmt['DEPART_DEST_CUSTOMS比率']=dmt['DEPART_DEST_CUSTOMS']/dmt['总票数']
# dmt['TRANSFER_LAST_MILE比率'] =dmt['TRANSFER_LAST_MILE']/dmt['总票数']
# dmt['ARRIVE_LAST_MILE比率']=dmt['ARRIVE_LAST_MILE']/dmt['总票数']
# dmt['ATTEMPT_DELIVER比率']=dmt['ATTEMPT_DELIVER']/dmt['总票数']
# dmt['DELIVERED比率']=dmt['DELIVERED']/dmt['总票数']
# dmt['全含票数']=dmt['all票数']/dmt['总票数']
# dmt['系统妥投率']=dmt['票数']/dmt['总票数']
dmt=dmt.rename(columns={'moon':'周期'})
dtt=pd.concat([dtt,dmt])
# dtt=dtt.drop(['LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER','DELIVERED','all票数'],axis=1)
# dyw=pd.read_excel(u"F:\\其他部门\\妥投分位源数据往期.xlsx",dtype={'淡旺季': 'str'})
# dtt=pd.DataFrame(dtt,columns=['周期','渠道','国家','客户id','总票数','系统妥投率','LINEHAUL_PICK_UP','DEPART_AIRPORT','ARRIVE_DEST_AIRPORT','DEPART_DEST_CUSTOMS','TRANSFER_LAST_MILE','ARRIVE_LAST_MILE','ATTEMPT_DELIVER'])
bf = r'{}\vova&平世节点推送统计.xlsx'.format(file)
writer = pd.ExcelWriter(bf)
dtt.to_excel(writer, '节点推送统计')
dt.to_excel(writer, '数据total')
d1.to_excel(writer, '原数据')
d2.to_excel(writer, 'd2')
d3.to_excel(writer, 'd3')
d4.to_excel(writer, 'd4')
d5.to_excel(writer, 'd5')
d6.to_excel(writer, 'd6')
d7.to_excel(writer, 'd7')
d8.to_excel(writer, 'd8')
d9.to_excel(writer, 'd9')
d10.to_excel(writer,'d10')
d11.to_excel(writer,'d11')
dtd.to_excel(writer,'dtd')
dta.to_excel(writer,'dta')
writer.save()



