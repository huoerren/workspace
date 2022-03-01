# %% [markdown]
# # TT分段监控开发

# %%
import pandas as pd
import openpyxl
import pymysql
import time
from datetime import datetime
import numpy as np

# order_status 物流状态：{0:'未发送',1 :'已发送',2 :'转运中', 3:'送达',4:'超时',5:'扣关',6:'地址错误',7:'快件丢失', 8:'退件',9:'其他异常'}
# SQL执行函数
def exe(sql):
    return pd.read_sql(sql,con)

nows = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
print(nows)

# %%
con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8",
                      autocommit=True, database='logisticscore')
cur = con.cursor()

# days="BETWEEN '2021-03-31 16:00:00' and '"+time_yes+"'"1
# days="BETWEEN '2021-03-31 16:00:00' and "+"'"+nows+"'"
days = "BETWEEN '2021-12-06 16:00:00' and "+"'"+nows+"' "
# days = "BETWEEN '2021-03-31 16:00:00' and '2021-04-1 16:00:00' "
print(days)

# %%
# 未配载监控
# s1 查询结果：
# bag_no, channel_code, des, order_no, order_status, fddate, gmtdate, 封袋时间，未配载时间, 账户名
# 2, 921GB00057366, CNE全球特惠TT, GB, 1, 2022-01-11, 2022-01-11, 0, 1.8, 1.9, TT UK
s1 = """select
count(1) c,
lgb.bag_no,
lgo.channel_code,
lgo.des,
-- lgo.order_no,
lgo.order_status,
DATE_FORMAT(DATE_ADD(sealing_bag_time, interval 8 hour),'%Y-%m-%d') fddate,
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
ROUND(TIMESTAMPDIFF(hour,lgo.gmt_create,lgb.sealing_bag_time)/24,1) 业务到封袋,
ROUND(TIMESTAMPDIFF(hour,lgb.sealing_bag_time,NOW())/24,1) 封袋至今,
ROUND(TIMESTAMPDIFF(hour,lgo.gmt_create,NOW())/24,1) 业务至今,
(case when lgo.customer_id = 4691294 then "TT UK" else "Perceiver Limited" end) 账户名

FROM lg_order lgo,
lg_bag_order_relation lbor,
lg_bag lgb

WHERE lgo.id = lbor.order_id
AND lbor.bag_id = lgb.id
AND lgo.is_deleted = 'n'
AND lbor.is_deleted = 'n'
AND lgb.is_deleted = 'n'
AND lgo.platform = 'TIKTOK'
AND lgo.order_status in (1,2)
AND lgo.gmt_create {}
-- 未配载条件一 没有主单
AND ISNULL(lgo.mawb_id) = 1
-- 未配载条件二 已封袋
AND ISNULL(lgb.sealing_bag_time) = 0
GROUP BY 2,3,4,5,6,7,8,9,10,11
""".format(days)

# dfload = exe(s1)
# PBI 已添加条件列
# dfload.replace({'order_status':{1:'已发送'，2:'转运中'}})


# 出库监控
# s2 查询结果：
# c, mawb_no, channel_code, des, fddate, gmtdate, 封袋时间, 账户名
# 1, 112-61548911, CNE全球优先TT, GB, 2021-12-31, 2021-12-31, 0.6, TT UK
s2 = """select count(1) c,
lgm.mawb_no,
lgo.channel_code,
lgo.des,
DATE_FORMAT(DATE_ADD(lgb.sealing_bag_time, interval 8 hour),'%Y-%m-%d') fddate,
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
ROUND(TIMESTAMPDIFF(hour,lgo.gmt_create,lgb.sealing_bag_time)/24,1) 业务到封袋,
(case when lgo.customer_id = 4691294 then "TT UK" else "Perceiver Limited" end) cusid,
(case when lgo.order_status = 2 then "转运中" else "其他" end) status

FROM lg_order lgo,
lg_bag lgb,
lg_mawb lgm,
lg_bag_order_relation lbor

WHERE lgo.id = lbor.order_id
AND lbor.bag_id = lgb.id
AND lgo.mawb_id = lgm.id
AND lgo.is_deleted = 'n'
AND lgb.is_deleted = 'n'
AND lbor.is_deleted = 'n'
AND lgm.is_deleted = 'n'
AND lgo.platform = 'TIKTOK'
AND lgo.gmt_create {}
-- 未配载条件一 有主单
AND ISNULL(lgo.mawb_id) = 0
-- 未配载条件二 已封袋
AND ISNULL(lgb.sealing_bag_time) = 0
-- 按主单，fddate，gmtdate，channel_code，des， 业务到封袋，cusid, status 聚合
GROUP BY 2,3,4,5,6,7,8,9
""".format(days)

# 装车监控
# s3 查询结果：
# c, mawb_no, channel_code, des, gmtdate, fddate, zcdate, 业务到封袋，封袋到装车，业务到装车，cusid，status
# 1, CNE全球优先TT,	112-61548535, GB, 2021-12-28, 2021-12-28, 2021-12-29, 0.1, 1.3, 1.5, TT UK, 其他
s3 = """SELECT count(1) c,
lgo.channel_code,
lgm.mawb_no,
lgo.des,
DATE_FORMAT(DATE_ADD(lgo.gmt_create, interval 8 hour), '%Y-%m-%d') gmtdate,
DATE_FORMAT(DATE_ADD(lgb.sealing_bag_time, interval 8 hour), '%Y-%m-%d') fddate,
DATE_FORMAT(DATE_ADD(tbe.event_time, interval 8 hour), '%Y-%m-%d') zcdate,
ROUND(TIMESTAMPDIFF(hour,lgo.gmt_create,lgb.sealing_bag_time)/24,1) 业务到封袋,
ROUND(TIMESTAMPDIFF(hour,lgb.sealing_bag_time,tbe.event_time)/24,1) 封袋到装车,
ROUND(TIMESTAMPDIFF(hour,lgo.gmt_create,tbe.event_time)/24,1) 业务到装车,
(case when lgo.customer_id = 4691294 then "TT UK" else "Perceiver Limited" end) cusid,
(case when lgo.order_status = 2 then "转运中" else "其他" end) status

FROM lg_order lgo,
lg_bag lgb,
lg_bag_order_relation lbor,
lg_mawb lgm,
track_bag_event tbe

WHERE lgo.id = lbor.order_id
AND lbor.bag_id = lgb.id
AND lbor.bag_id = tbe.bag_id
AND lgo.mawb_id = lgm.id
AND lgo.is_deleted = 'n'
AND lgb.is_deleted = 'n'
AND lgm.is_deleted = 'n'
AND lbor.is_deleted = 'n'
AND tbe.is_deleted = 'n'
AND lgo.platform = 'TIKTOK'
AND lgo.gmt_create {}
AND tbe.event_code = 'DEPS'
-- 未配载条件一 有主单
AND ISNULL(lgo.mawb_id) = 0
-- 未配载条件二 已封袋
AND ISNULL(lgb.sealing_bag_time) = 0

GROUP BY 2,3,4,5,6,7,8,9,10,11,12
""".format(days)

# 起飞监控
# s4 查询结果：
# c, channel_code, mawb_no, des, gmtdate, qfdate, zcdate, 装车到起飞, 业务到起飞, cusid, STATUS
# 112-61548944, GB, 2022/1/3, 2022/1/4, 2022/1/3, 1, 1.5, TT UK, 转运中
s4 = """SELECT
	count( 1 ) c,
	lgo.channel_code,
	lgm.mawb_no,
	lgo.des,
	DATE_FORMAT( DATE_ADD( lgo.gmt_create, INTERVAL 8 HOUR ), '%Y-%m-%d' ) gmtdate,
	DATE_FORMAT( DATE_ADD( tme.event_time, INTERVAL 8 HOUR ), '%Y-%m-%d' ) qfdate,
	DATE_FORMAT( DATE_ADD( tbe.event_time, INTERVAL 8 HOUR ), '%Y-%m-%d' ) zcdate,
	ROUND( TIMESTAMPDIFF( HOUR, tbe.event_time, tme.event_time )/ 24, 1 ) 装车到起飞,
	ROUND( TIMESTAMPDIFF( HOUR, lgo.gmt_create, tme.event_time )/ 24, 1 ) 业务到起飞,
	( CASE WHEN lgo.customer_id = 4691294 THEN "TT UK" ELSE "Perceiver Limited" END ) cusid,
	( CASE WHEN lgo.order_status = 2 THEN "转运中" ELSE "其他" END ) STATUS 
FROM
	lg_order lgo,
	lg_mawb lgm,
	track_bag_event tbe,
	track_mawb_event tme,
	lg_bag_order_relation lbor 
WHERE
	lgo.id = lbor.order_id 
	AND lbor.bag_id = tbe.bag_id 
	AND lgo.mawb_id = lgm.id
	AND lgo.mawb_id = tme.mawb_id 
	AND lgo.is_deleted = 'n' 
	AND lgm.is_deleted = 'n' 
	AND tbe.is_deleted = 'n' 
	AND tme.is_deleted = 'n' 
	AND lbor.is_deleted = 'n'
	AND lgo.platform = 'TIKTOK' 
	AND lgo.gmt_create BETWEEN '2021-11-30 16:00:00' and '2022-1-31 16:00:00'
	AND '2022-1-31 16:00:00' 
	AND tbe.event_code = 'DEPS' 
	AND tme.event_code IN ( 'SDFO', 'DEPC', 'DEPT', 'LKJC', 'SYFD', 'SYYF', 'PMWC' ) 

GROUP BY 2,3,4,5,6,7,8,9,10,11
""".format(days)

# 落地主单
# s6 查询结果：
# c, mawb_no, bag_no, channel_code, des, lddate, qfdate, ywdate, 起飞到落地，业务到落地，cusid，STATUS
# 10, 112-61548955, 818GB00361932, CNE全球优先TT, GB, 2022/1/4, 2022/1/4, 2022/1/2, 0.1, 1.8, TT UK, 转运中
s6 = """
SELECT
	count( 1 ) c,
	lgm.mawb_no,
	lgb.bag_no,
	lgo.channel_code,
	lgo.des,
	date_format( date_add( tme1.event_time, INTERVAL 8 HOUR ), '%Y-%m-%d' ) lddate,
	date_format( date_add( tme2.event_time, INTERVAL 8 HOUR ), '%Y-%m-%d' ) qfdate,
	date_format( date_add( lgo.gmt_create, INTERVAL 8 HOUR ), '%Y-%m-%d' ) ywdate,
	round( timestampdiff( HOUR, tme2.event_time, tme1.event_time )/ 24, 1 ) 起飞到落地,
	round( timestampdiff( HOUR, lgo.gmt_create, tme1.event_time )/ 24, 1 ) 业务到落地,
	( CASE WHEN lgo.customer_id = 4691294 THEN "TT UK" ELSE "Perceiver Limited" END ) cusid,
	( CASE WHEN lgo.order_status = 2 THEN "转运中" ELSE "其他" END ) STATUS 
FROM
	lg_order lgo,
	track_mawb_event tme1,
	track_mawb_event tme2,
	lg_mawb lgm,
	lg_bag_order_relation lbor,
	lg_bag lgb 
WHERE
	tme1.event_code IN ( "ARIR", "ABCD", "ABAD", "AECD", "ARMA" ) 
	AND tme2.event_code IN ( "SDFO", "DEPC", "DEPT", "LKJC", 'SYFD', 'SYYF', 'PMWC' ) 
	AND lgo.platform = 'TIKTOK' 
	AND lgo.gmt_create {}
	AND tme1.mawb_id = lgo.mawb_id 
	AND tme2.mawb_id = lgo.mawb_id 
	AND lgo.mawb_id = lgm.id 
	AND lgo.id = lbor.order_id 
	AND lbor.bag_id = lgb.id 
	AND lgo.is_deleted = 'n' 
	AND lgm.is_deleted = 'n' 
	AND lgb.is_deleted = 'n' 
	AND lbor.is_deleted = 'n' 
	AND tme1.is_deleted = 'n' 
	AND tme2.is_deleted = 'n' 
GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
""".format(days)

# 落地bag
s13 = """SELECT
count(1) c ,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d') lddate,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate,
round(timestampdiff(hour,tme.event_time,tbe.event_time)/24,1) 落地用时,
round(timestampdiff(hour,lgo.gmt_create,tbe.event_time)/24,1) 业务至落地用时
from
track_bag_event tbe,
lg_bag_order_relation lbor,
track_mawb_event tme,
lg_mawb lgm,
lg_order lgo,
lg_bag lgb
where
lbor.bag_id=tbe.bag_id 
and lbor.order_id=lgo.id 
and lgm.id=lgo.mawb_id 
and tbe.bag_id=lgb.id
and lgo.mawb_id=tme.mawb_id
AND tbe.event_code in("ARIR","ABCD","ABAD","AECD","ARMA")
and tme.event_code in("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF','PMWC') 
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE'
AND lgo.customer_id in (1151368,1151370,1181372,1181374)
and lgo.order_status in(1,2)
and lgo.is_deleted='n'
and tbe.is_deleted='n'
and lbor.is_deleted='n'
and lgm.is_deleted='n'
and tme.is_deleted='n'
group by
2,3,4,5,6,7,8,9,10
""".format(days)

# 落地order
s14 = """select
count(1) c,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des,
date_format(date_add(toe.event_time,interval 8 hour),'%Y-%m-%d') lddate,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate,
round(timestampdiff(hour,tme.event_time,toe.event_time)/24,1) 落地用时,
round(timestampdiff(hour,lgo.gmt_create,toe.event_time)/24,1) 业务至落地用时
from lg_order lgo,
track_order_event toe,
lg_mawb lgm,
lg_bag lgb,
lg_bag_order_relation lbor,
track_mawb_event tme
where  toe.event_code in("ARIR","ABCD","ABAD","AECD","ARMA")
and tme.event_code in("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF','PMWC') 
and lgo.gmt_create {}
and lgo.platform='WISH_ONLINE'
AND lgo.customer_id in  (1151368,1151370,1181372,1181374)
and lgo.order_status in(1,2)
and lgo.id=toe.order_id
and lgo.mawb_id=lgm.id
and lgo.id=lbor.order_id
and lbor.bag_id=lgb.id
and lgo.mawb_id=tme.mawb_id
and lgo.is_deleted='n'
and toe.is_deleted='n'
and lgm.is_deleted='n'
and lgb.is_deleted='n'
and lbor.is_deleted='n'
and tme.is_deleted='n'
group by 2,3,4,5,6,7,8,9,10
""".format(days)



# 主单全
# s12 查询结果：
# mawb_no
# 112-61548966
s12 = """select DISTINCT lgm.mawb_no

FROM lg_order lgo,
lg_mawb lgm

WHERE lgo.mawb_id = lgm.id
AND lgo.is_deleted = 'n'
AND lgm.is_deleted = 'n'
AND lgo.platform = 'TIKTOK'
AND lgo.order_status in (1,2)
AND lgo.gmt_create {}
AND ISNULL(lgo.mawb_id) = 0
""".format(days)

# 清关全
# s15 查询结果：
# c, channel_code, des, mawb_no, bag_no, fxdate, gmtdate, cusid, status, Dimension
# 1, CNE全球特惠TT,	ES,	999-15022615, 369ES00042637, 2021-12-06, 2021-12-01, Perceiver Limited, 其他, 主单
s15 = '''
-- 清关主单
SELECT
	count( 1 ) c,
	lgo.channel_code,
	lgo.des,
	lgm.mawb_no,
	lgb.bag_no,
	date_format( date_add( tme.event_time, INTERVAL 8 HOUR ), '%Y-%m-%d' ) fxdate,
	date_format( date_add( lgo.gmt_create, INTERVAL 8 HOUR ), '%Y-%m-%d' ) gmtdate,
	(case when lgo.customer_id = 4691294 then "TT UK" else "Perceiver Limited" end) cusid,
	(case when lgo.order_status = 2 then "转运中" else "其他" end) status,
	"主单" AS Dimension 
FROM
	track_mawb_event tme,
	lg_order lgo,
	lg_mawb lgm,
	lg_bag lgb,
	lg_bag_order_relation lbor 
WHERE
	tme.event_code IN ( "IRCM", "PVCS", "IRCN", "RFIC", "BGRK" ) 
	AND lgo.gmt_create {}
	AND platform = 'TIKTOK'
	AND tme.mawb_id = lgo.mawb_id 
	AND lgo.mawb_id = lgm.id 
	AND lgo.id = lbor.order_id 
	AND lbor.bag_id = lgb.id 
	AND lgo.is_deleted = 'n' 
	AND tme.is_deleted = 'n' 
	AND lgm.is_deleted = 'n' 
	AND lgb.is_deleted = 'n' 
	AND lbor.is_deleted = 'n'

GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION ALL

-- 清关order
SELECT
	count( 1 ) c,
	lgo.channel_code,
	lgo.des,
	lgm.mawb_no,
	lgb.bag_no,
	date_format( date_add( toe.event_time, INTERVAL 8 HOUR ), '%Y-%m-%d' ) fxdate,
	date_format( date_add( lgo.gmt_create, INTERVAL 8 HOUR ), '%Y-%m-%d' ) gmtdate,
	( CASE WHEN lgo.customer_id = 4691294 THEN "TT UK" ELSE "Perceiver Limited" END ) cusid,
	( CASE WHEN lgo.order_status = 2 THEN "转运中" ELSE "其他" END ) STATUS,
	"order" AS Dimension
FROM
	lg_order lgo,
	lg_mawb lgm,
	track_order_event toe,
	lg_bag lgb,
	lg_bag_order_relation lbor 
WHERE
	lgo.id = toe.order_id 
	AND lgm.id = lgo.mawb_id 
	AND lgo.id = lbor.order_id 
	AND lbor.bag_id = lgb.id 
	AND toe.event_code IN ( "IRCM", "PVCS", "IRCN", "RFIC", "BGRK" ) 
	AND lgo.gmt_create {}
	AND lgo.is_deleted = 'n' 
	AND lgm.is_deleted = 'n' 
	AND toe.is_deleted = 'n' 
	AND lgb.is_deleted = 'n' 
	AND lbor.is_deleted = 'n' 
	AND platform = 'TIKTOK' 
GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10

UNION ALL
-- 清关bag
SELECT   
count( 1 ) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no,
date_format( date_add( tbe.event_time, INTERVAL 8 HOUR ), '%Y-%m-%d' ) fxdate,
date_format( date_add( lgo.gmt_create, INTERVAL 8 HOUR ), '%Y-%m-%d' ) gmtdate,
( CASE WHEN lgo.customer_id = 4691294 THEN "TT UK" ELSE "Perceiver Limited" END ) cusid,
( CASE WHEN lgo.order_status = 2 THEN "转运中" ELSE "其他" END ) STATUS,
"bag" AS Dimension
FROM
	track_bag_event tbe,
	lg_bag lgb,
	lg_mawb lgm,
	lg_order lgo,
	lg_bag_order_relation lbor 
WHERE
	lgb.id = tbe.bag_id 
	AND tbe.bag_id = lbor.bag_id 
	AND lbor.order_id = lgo.id 
	AND lgo.mawb_id = lgm.id 
	AND tbe.event_code IN ( "IRCM", "PVCS", "IRCN", "RFIC", "BGRK" ) 
	AND lgo.gmt_create {}
	AND lgo.platform = 'TIKTOK' 
	AND lgo.is_deleted = 'n' 
	AND tbe.is_deleted = 'n' 
	AND lgb.is_deleted = 'n' 
	AND lgm.is_deleted = 'n' 
	AND lbor.is_deleted = 'n'
GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10'''

# 清关主单
s7="""
-- 清关主单
SELECT
	count( 1 ) c,
	lgo.channel_code,
	lgo.des,
	lgm.mawb_no,
	lgb.bag_no,
	date_format( date_add( tme.event_time, INTERVAL 8 HOUR ), '%Y-%m-%d' ) fxdate,
	date_format( date_add( lgo.gmt_create, INTERVAL 8 HOUR ), '%Y-%m-%d' ) gmtdate,
	(case when lgo.customer_id = 4691294 then "TT UK" else "Perceiver Limited" end) cusid,
	(case when lgo.order_status = 2 then "转运中" else "其他" end) status,
	"主单" AS Dimension 
FROM
	track_mawb_event tme,
	lg_order lgo,
	lg_mawb lgm,
	lg_bag lgb,
	lg_bag_order_relation lbor 
WHERE
	tme.event_code IN ( "IRCM", "PVCS", "IRCN", "RFIC", "BGRK" ) 
	AND lgo.gmt_create {}
	AND platform = 'TIKTOK'
	AND tme.mawb_id = lgo.mawb_id 
	AND lgo.mawb_id = lgm.id 
	AND lgo.id = lbor.order_id 
	AND lbor.bag_id = lgb.id 
	AND lgo.is_deleted = 'n' 
	AND tme.is_deleted = 'n' 
	AND lgm.is_deleted = 'n' 
	AND lgb.is_deleted = 'n' 
	AND lbor.is_deleted = 'n'

GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10
""".format(days)

# 清关bag
s8="""
SELECT   
count( 1 ) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no,
date_format( date_add( tbe.event_time, INTERVAL 8 HOUR ), '%Y-%m-%d' ) fxdate,
date_format( date_add( lgo.gmt_create, INTERVAL 8 HOUR ), '%Y-%m-%d' ) gmtdate,
( CASE WHEN lgo.customer_id = 4691294 THEN "TT UK" ELSE "Perceiver Limited" END ) cusid,
( CASE WHEN lgo.order_status = 2 THEN "转运中" ELSE "其他" END ) STATUS,
"bag" AS Dimension
FROM
	track_bag_event tbe,
	lg_bag lgb,
	lg_mawb lgm,
	lg_order lgo,
	lg_bag_order_relation lbor 
WHERE
	lgb.id = tbe.bag_id 
	AND tbe.bag_id = lbor.bag_id 
	AND lbor.order_id = lgo.id 
	AND lgo.mawb_id = lgm.id 
	AND tbe.event_code IN ( "IRCM", "PVCS", "IRCN", "RFIC", "BGRK" ) 
	AND lgo.gmt_create {}
	AND lgo.platform = 'TIKTOK' 
	AND lgo.is_deleted = 'n' 
	AND tbe.is_deleted = 'n' 
	AND lgb.is_deleted = 'n' 
	AND lgm.is_deleted = 'n' 
	AND lbor.is_deleted = 'n'
GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10
""".format(days)

# 清关order
s9="""
SELECT
	count( 1 ) c,
	lgo.channel_code,
	lgo.des,
	lgm.mawb_no,
	lgb.bag_no,
	date_format( date_add( toe.event_time, INTERVAL 8 HOUR ), '%Y-%m-%d' ) fxdate,
	date_format( date_add( lgo.gmt_create, INTERVAL 8 HOUR ), '%Y-%m-%d' ) gmtdate,
	( CASE WHEN lgo.customer_id = 4691294 THEN "TT UK" ELSE "Perceiver Limited" END ) cusid,
	( CASE WHEN lgo.order_status = 2 THEN "转运中" ELSE "其他" END ) STATUS,
	"order" AS Dimension
FROM
	lg_order lgo,
	lg_mawb lgm,
	track_order_event toe,
	lg_bag lgb,
	lg_bag_order_relation lbor 
WHERE
	lgo.id = toe.order_id 
	AND lgm.id = lgo.mawb_id 
	AND lgo.id = lbor.order_id 
	AND lbor.bag_id = lgb.id 
	AND toe.event_code IN ( "IRCM", "PVCS", "IRCN", "RFIC", "BGRK" ) 
	AND lgo.gmt_create {}
	AND lgo.is_deleted = 'n' 
	AND lgm.is_deleted = 'n' 
	AND toe.is_deleted = 'n' 
	AND lgb.is_deleted = 'n' 
	AND lbor.is_deleted = 'n' 
	AND platform = 'TIKTOK' 
GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10
""".format(days)

# 交付节点
# s10 查询结果：
#
s10="""
SELECT
	count( 1 ) c,
	lgo.channel_code,
	lgo.des,
	lgb.bag_no,
	lgm.mawb_no,
	date_format( date_add( tbe.event_time, INTERVAL 8 HOUR ), '%Y-%m-%d' ) jfdate,
	date_format( date_add( lgo.gmt_create, INTERVAL 8 HOUR ), '%Y-%m-%d' ) ywdate,
	round( timestampdiff( HOUR, lgo.gmt_create, tbe.event_time )/ 24, 1 ) 业务到交付,
	round( timestampdiff( HOUR, tbe.event_time, now())/ 24, 1 ) 交付至今,
	( CASE WHEN lgo.customer_id = 4691294 THEN "TT UK" ELSE "Perceiver Limited" END ) cusid,
	( CASE WHEN lgo.order_status = 2 THEN "转运中" ELSE "其他" END ) STATUS
FROM
	lg_order lgo,
	track_bag_event tbe,
	lg_bag_order_relation lbor,
	lg_bag lgb,
	lg_mawb lgm 
WHERE
	tbe.bag_id = lgb.id 
	AND lgb.id = lbor.bag_id 
	AND lbor.order_id = lgo.id 
	AND lgm.id = lgo.mawb_id 
	AND tbe.event_code IN ( "JFMD", "AAPS" ) 
	AND lgo.platform = 'TIKTOK' 
	AND lgo.gmt_create BETWEEN '2021-11-30 16:00:00' AND '2022-12-3 16:00:00' 
	AND lgo.order_status = 2 
	AND lgo.is_deleted = 'n' 
	AND lgb.is_deleted = 'n' 
	AND tbe.is_deleted = 'n' 
	AND lbor.is_deleted = 'n' 
GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
""".format(days)

# 断更监控
s11="""
SELECT
	date_format( date_add( lgo.gmt_create, INTERVAL 8 HOUR ), '%Y-%m-%d' ) gmtdate,
	lgo.channel_code,
	lgo.des,
	( CASE WHEN lgo.order_status = 2 THEN '转运中' ELSE '其他' END ) STATUS,
	( CASE WHEN lgo.customer_id = 4691294 THEN "TT UK" ELSE "Perceiver Limited" END ) cusid,
	round( timestampdiff( HOUR, olt.event_time, now())/ 24, 1 ) 末条至今,
	round( timestampdiff( HOUR, lgo.gmt_create, olt.event_time )/ 24, 1 ) 业务到末条,
	count( DISTINCT lgo.order_no ) c
	
FROM
	lg_order lgo,
	lg_bag_order_relation lbor,
	order_last_track olt, #末条信息
	lg_bag lgb,
	track_bag_event tbe
	
WHERE
	tbe.event_code IN ( "JFMD", "AAPS" ) #派送公司收货
	AND lbor.order_id = lgo.id
	AND lgb.id = lbor.bag_id
	AND olt.order_id = lgo.id
	AND tbe.bag_id = lgb.id
	AND lgo.gmt_create {}
	AND olt.event_time {}
	AND lgo.platform = 'TIKTOK' 
	AND lgo.is_deleted = 'n' 
	AND lgb.is_deleted = 'n' 
	AND tbe.is_deleted = 'n' 
	AND lbor.is_deleted = 'n' 
	AND olt.is_deleted = 'n'
GROUP BY 1, 2, 3, 4, 5, 6, 7
""".format(days,days)




# %%
dfunload = exe(s1)
# dp['order_status'].unique()
print("未配载", dfunload.head())

dfcust = pd.concat([exe(s7), exe(s8), exe(s9)])
print("清关全", dfcust.head())

# %%
dfl = pd.concat([exe(s6), exe(s13), exe(s14)])
dfl['c']=dfl['c'].astype('int')
dfland = dfl.groupby(['mawb_no','bag_no', 'channel_code', 'des', 'lddate', 'qfdate', 'ywdate', '起飞到落地','业务到落地','cusid','STATUS'])['c'].sum().reset_index()
print("落地监控", dfland.head())

# %%
dftkoff = exe(s4)
print("起飞监控", dftkoff.head())

dfload = exe(s2)
print("出库监控", dfload.head())

dfdeps = exe(s3)
print("装车监控", dfdeps.head())

dfdlvr = exe(s11)
print("断更监控", dfdlvr.head())

# %%
dfd = exe(s10)
dfd['c'] = dfd['c'].astype('int')
dfdist = dfd.groupby(['mawb_no', 'bag_no', 'channel_code', 'des', 'jfdate', 'ywdate', '业务到交付','交付至今','cusid','STATUS'])['c'].sum().reset_index()
print("交付节点", dfd.head())
print("交付监控", dfdist.head())

# %%
name_l = [['未配载监控', dfunload], ['出库监控', dfload], ['装车监控', dfdeps], ['起飞监控', dftkoff], ['落地监控', dfland], ['清关全', dfcust], ['主单全', exe(s12)],
['交付节点', dfd], ['断更监控', dfdlvr], ['交付监控', dfdist], ['落地节点', dfl]]


def file_xlsx(name, df):
    df.to_excel(excel_writer=r"F:\PBI临时文件\TT分段监控\{}.xlsx".format(name),index = False)
for n in name_l:
    file_xlsx(n[0], n[1])


