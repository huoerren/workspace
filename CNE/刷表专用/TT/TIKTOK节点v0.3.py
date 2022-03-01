#!/usr/bin/env python
# coding: utf-8

# %%
import pandas as pd
import numpy as np
import openpyxl
import pymysql
import datetime,time

con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()

def exe(sql):
    return pd.read_sql(sql,con)


# %%
# s1 查询结果：
# 揽收周期, des, channel_id, tracking_status, 推送数
# 11, FR, 2458108, cb_trans_inbound, 4
# 揽收周期按月统计
S1="""
SELECT
    MONTH(DATE_ADD( lgo.gmt_create, INTERVAL 8 HOUR )) 揽收周期,
	wtd.des,
	wtd.channel_id,
	wtd.tracking_status,
	count( DISTINCT wtd.pre_order_id ) 推送数 
FROM
	platform_track_data wtd
	INNER JOIN lg_order lgo ON lgo.pre_order_id = wtd.pre_order_id 
WHERE
	wtd.is_deleted = 'n' 
	AND wtd.order_time > '2021-09-30 16:00:00' 
	AND wtd.platform = 'TIKTOK' 
	AND wtd.has_push = 1 
GROUP BY
	1,2,3,4
"""


# %%
d1=exe(S1)

# %%
# s2 查询结果：
# 揽收周期，des，channel_id, tracking_status, 推送数
# 1, GB, 2398107, signed_personally, 3493
# 揽收周期按周统计
S2="""
SELECT
	DATE_FORMAT( DATE_ADD( lgo.gmt_create, INTERVAL 8 HOUR ), '%Y%u' )+ 1 揽收周期,
	wtd.des,
	wtd.channel_id,
	wtd.tracking_status,
	count( DISTINCT wtd.pre_order_id ) 推送数 
FROM
	platform_track_data wtd
	INNER JOIN lg_order lgo ON lgo.pre_order_id = wtd.pre_order_id 
WHERE
	wtd.is_deleted = 'n' 
	AND wtd.order_time > '2021-09-30 16:00:00' 
	AND wtd.platform = 'TIKTOK' 
	AND wtd.has_push = 1 
GROUP BY 1,2,3,4
"""


# %%
d2=exe(S2)

# %%
# S3查询结果：
# channel_id, channel_code
# 2398107, CNE全球特惠TT
# 确认平台当前发货渠道
S3="""
SELECT
    DISTINCT channel_id,
	channel_code 
FROM
	lg_order 
WHERE
	order_time >= '2021-09-30 16:00:00' 
	AND platform = 'TIKTOK' 
	AND is_deleted = 'n'
"""


# %%
d3=exe(S3)
channel_dict=d3.set_index('channel_id')['channel_code'].to_dict()


# %%
# 境内退件
domstj="""
SELECT
	pre_order_id,
	_580 境内退件 
FROM
	order_event_report 
WHERE
	order_create_time > '2021-09-30 16:00:00' 
	AND platform = 'TIKTOK' 
	AND ISNULL( _580 ) = 0
"""
# %%
# 境外退件
brtj="""
SELECT
	pre_order_id,
	_600 境外退件 
FROM
	order_event_report 
WHERE
	order_create_time > '2021-09-30 16:00:00' 
	AND platform = 'TIKTOK' 
	AND ISNULL( _600) = 0
"""



d5=exe(domstj)
domstj=tuple(d5['pre_order_id'].tolist())



d6=exe(brtj)
brtj=tuple(d6['pre_order_id'].tolist())

sumtj=tuple(pd.concat([d5['pre_order_id'],d6['pre_order_id']]))
# %%
# 正常件
SKU="""
SELECT
	pre_order_id 
FROM
	lg_order 
WHERE
	order_time > '2021-09-30 16:00:00' 
	AND platform = 'TIKTOK' 
	AND pre_order_id not in {}
""".format(sumtj)

# %%
SKU=exe(SKU)

# %%
pkg=tuple(SKU['pre_order_id'].tolist())

# %%
# 正常件推送，按周统计
S4_0="""
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
""".format(pkg)

# %%
# 正常件推送，按月统计
S4_1="""
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
""".format(pkg)

# %%
# 境内退件 按周统计
S5_0="""
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
""".format(domstj)

# %%
# 境内退件 按月统计
S5_1="""
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
""".format(domstj)

# %%
# 境外退件 按周统计
S6_0="""
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
and wtd.tracking_status in ('cb_trans_inbound','cb_trans_outbound','cb_transport_originalport','cb_transport_departed','cb_transport_arrived','cb_imcustoms_finished','unreachable_returned_wh')
and lgo.pre_order_id=wtd.pre_order_id
group by 1,2,3,4
HAVING (count(distinct tracking_status)=2)
) as t
group by 1,2,3
""".format(brtj)

# %%
# 境外退件 按月统计
S6_1="""
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
and wtd.tracking_status in ('cb_trans_inbound','cb_trans_outbound','cb_transport_originalport','cb_transport_departed','cb_transport_arrived','cb_imcustoms_finished','unreachable_returned_wh')
and lgo.pre_order_id=wtd.pre_order_id
group by 1,2,3,4
HAVING (count(distinct tracking_status)=2)
) as t
group by 1,2,3
""".format(brtj)

# %%
d4_0=exe(S4_0)
d4_1=exe(S4_1)
d5_0=exe(S5_0)
d5_1=exe(S5_1)
d6_0=exe(S6_0)
d6_1=exe(S6_1)
dlist=pd.concat([d4_0,d4_1,d5_0,d5_1,d6_0,d6_1])

# %%
dlist=dlist.groupby(by=['揽收周期','des','channel_id','tracking_status'])['推送数'].sum().reset_index()


# %%
d_t=pd.concat([d1,d2,dlist])


# %%
d_t['channel_id']=d_t['channel_id'].map(channel_dict)


# %%
list1=list(set(d_t.tracking_status.tolist()))


# %%
list2=list1.copy()
list2.append('channel_id')
list2.append('des')
list2.append('揽收周期')
# list2.append('业务类型')


# %%
df1=pd.DataFrame(columns=list2)


# %%
d_t=d_t.reset_index().drop(['index'],axis=1)


# %%
df1['cb_trans_return']=0
df1['unreachable_returning']=0


# %%
count=0
for i in d_t.groupby(['揽收周期','des','channel_id']):
    df1.loc[count,'揽收周期']=i[0][0]
    df1.loc[count,'des']=i[0][1]
    df1.loc[count,'channel_id']=i[0][2]
    for j in i[1].index:
        df1.loc[count,i[1].loc[j,'tracking_status']]=i[1].loc[j,'推送数']
    count+=1


# %%
df1=df1.replace(np.nan,0)


# %%
# 计算系统妥投率, 按妥投状态=3筛选出妥投订单.
# 妥投状态可变.导致系统妥投率可能比实际妥投率小
# 系统妥投率 按月统计
S7="""
select 
month(DATE_ADD(lgo.gmt_create,interval 8 hour)) 揽收周期,
lgo.des,
lgo.channel_id,
count(distinct lgo.pre_order_id) 含揽收节点票数,
sum(case when lgo.order_status = 3 then 1 else 0 end)/count(*) 系统妥投率
from lg_order lgo
where lgo.is_deleted='n'
and lgo.order_time>'2021-09-30 16:00:00'
and lgo.platform='TIKTOK'
group by 1,2,3
"""


# %%
# 系统妥投率 按周统计
S8="""
select 
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 揽收周期,
lgo.des,
lgo.channel_id,
count(distinct lgo.pre_order_id) 含揽收节点票数,
sum(case when lgo.order_status = 3 then 1 else 0 end)/count(*) 系统妥投率
from lg_order lgo
where lgo.is_deleted='n'
and lgo.order_time>'2021-09-30 16:00:00'
and lgo.platform='TIKTOK'
group by 1,2,3
"""


# %%
d7=exe(S7)
d8=exe(S8)
d9=pd.concat([d7,d8])
d9['channel_id']=d9['channel_id'].map(channel_dict)


# %%
r=pd.merge(d9,df1,on=['揽收周期','des','channel_id'],how='left')


# %%
for i in list1:
    r[i]=r[i]/r['含揽收节点票数']


# %%
r=r.replace(np.nan,0)


# %%
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


# %%
r['揽收周期']=r['揽收周期'].astype('int')
r['揽收周期']=r['揽收周期'].astype('str')


# %%
r['周期']=r['揽收周期'].apply(lambda x:start(x))


# %%
r.to_excel('TT节点推送.xlsx',index=False)





