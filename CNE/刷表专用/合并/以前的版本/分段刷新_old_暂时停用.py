import pandas as pd
import openpyxl
import pymysql
import datetime, time

nows = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
print(nows)
import numpy as np

# #数仓连接
con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8",
                      autocommit=True, database='logisticscore')
cur = con.cursor()

# days="BETWEEN '2021-03-31 16:00:00' and '"+time_yes+"'"1
# days="BETWEEN '2021-03-31 16:00:00' and "+"'"+nows+"'"
days = "BETWEEN '2021-09-15 16:00:00' and "+"'"+nows+"' "
# days = "BETWEEN '2021-03-31 16:00:00' and '2021-04-1 16:00:00' "
print(days)
# days="BETWEEN '2020-12-17 16:00:00' and '202
# 1-02-15 15:59:59'"

# 未配载监控
s1 = """(SELECT 
lgb.bag_no,
lgo.order_no,
lgo.channel_code,
lgo.customer_id,
date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
lgo.des,lgo.order_status,
round(timestampdiff(hour,lgo.gmt_create,lgb.sealing_bag_time)/24,1) 封袋时间,
round(timestampdiff(hour,lgb.sealing_bag_time,now())/24,1) 未配载时间
from
lg_order lgo,
lg_bag_order_relation lbor,
lg_bag lgb
WHERE   lgo.gmt_create  {}
and customer_id in ('3161297','3282094')
and lgo.order_status in(1,2)
and lbor.order_id=lgo.id
and lgb.id=lbor.bag_id
and lgo.is_deleted= 'n'
and isnull(lgo.mawb_id)=1
and isnull(lgb.sealing_bag_time)=0
and lgb.is_deleted= 'n'
and lbor.is_deleted='n')

UNION ALL

(SELECT 
lgb.bag_no,
lgo.order_no,
lgo.channel_code,
lgo.customer_id,
date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
lgo.des,lgo.order_status,
round(timestampdiff(hour,lgo.gmt_create,lgb.sealing_bag_time)/24,1) 封袋时间,
round(timestampdiff(hour,lgb.sealing_bag_time,now())/24,1) 未配载时间
from
lg_order lgo,
lg_bag_order_relation lbor,
lg_bag lgb
WHERE   lgo.gmt_create  {}
and platform='JDW'
and lgo.order_status in(1,2)
and lbor.order_id=lgo.id
and lgb.id=lbor.bag_id
and lgo.is_deleted= 'n'
and isnull(lgo.mawb_id)=1
and isnull(lgb.sealing_bag_time)=0
and lgb.is_deleted= 'n'
and lbor.is_deleted='n')
""".format(days,days)


print('-------------------- s1 : --------------------')
print(s1)

# 出库监控
s2 = """(select 
count(1) c,
lgm.mawb_no,
lgo.channel_code,
lgo.customer_id,
date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
round(TIMESTAMPDIFF(hour,lgo.gmt_create,lgb.sealing_bag_time)/24,1) 出库用时,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
lgo.des
from
lg_order lgo,
lg_mawb lgm,lg_bag_order_relation lbor,lg_bag lgb
where  lgo.gmt_create {}
and customer_id in ('3161297','3282094')
# and lgo.order_status in(1,2)
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
and  lgo.mawb_id=lgm.id 
and lgo.is_deleted='n' 
and lgb.is_deleted='n'
and lgm.is_deleted='n' 
and lbor.is_deleted='n' 
and isnull(lgo.mawb_id)=0 
and isnull(lgb.sealing_bag_time)=0  
group by lgm.mawb_no,lgo.customer_id,fddate, gmtdate, lgo.channel_code, lgo.des,出库用时,是否转运)

UNION ALL
(select 
count(1) c,
lgm.mawb_no,
lgo.channel_code,
lgo.customer_id,
date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
round(TIMESTAMPDIFF(hour,lgo.gmt_create,lgb.sealing_bag_time)/24,1) 出库用时,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
lgo.des
from
lg_order lgo,
lg_mawb lgm,lg_bag_order_relation lbor,lg_bag lgb
where  lgo.gmt_create {}
and platform='JDW'
# and lgo.order_status in(1,2)
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
and  lgo.mawb_id=lgm.id 
and lgo.is_deleted='n' 
and lgb.is_deleted='n'
and lgm.is_deleted='n' 
and lbor.is_deleted='n' 
and isnull(lgo.mawb_id)=0 
and isnull(lgb.sealing_bag_time)=0  
group by lgm.mawb_no,lgo.customer_id,fddate, gmtdate, lgo.channel_code, lgo.des,出库用时,是否转运)

""".format(days,days)



print('-------------------- s2 : --------------------')
print(s2)

# 装车监控
s3 = """(SELECT 
count(1) c, 
lgo.channel_code,
lgo.des, 
lgo.customer_id,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate, 
date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate, 
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d') zcdate, 
round(timestampdiff(hour,lgb.sealing_bag_time,tbe.event_time)/24,1) 装车用时,
round(timestampdiff(hour,lgo.gmt_create,lgb.sealing_bag_time)/24,1) 出库用时,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
lgm.mawb_no 
from lg_order lgo,lg_bag lgb, track_bag_event tbe, lg_bag_order_relation lbor, lg_mawb lgm
where tbe.bag_id=lbor.bag_id
and lbor.order_id=lgo.id  
and lbor.bag_id=lgb.id 
and lgo.mawb_id=lgm.id 
AND tbe.event_code="DEPS"
AND lgo.gmt_create {}
and customer_id in ('3161297','3282094')
# and lgo.order_status in(1,2)
and lgo.is_deleted='n'
and lgb.is_deleted='n' 
and tbe.is_deleted='n'
and lbor.is_deleted='n' 
and lgm.is_deleted='n'
and isnull(lgo.mawb_id)=0 
and isnull(lgb.sealing_bag_time)=0  
group by 2,3,4,5,6,7,8,9,10,11)


UNION ALL


(SELECT 
count(1) c, 
lgo.channel_code,
lgo.des, 
lgo.customer_id,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate, 
date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate, 
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d') zcdate, 
round(timestampdiff(hour,lgb.sealing_bag_time,tbe.event_time)/24,1) 装车用时,
round(timestampdiff(hour,lgo.gmt_create,lgb.sealing_bag_time)/24,1) 出库用时,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
lgm.mawb_no 
from lg_order lgo,lg_bag lgb, track_bag_event tbe, lg_bag_order_relation lbor, lg_mawb lgm
where tbe.bag_id=lbor.bag_id
and lbor.order_id=lgo.id  
and lbor.bag_id=lgb.id 
and lgo.mawb_id=lgm.id 
AND tbe.event_code="DEPS"
AND lgo.gmt_create {}
and platform='JDW'
# and lgo.order_status in(1,2)
and lgo.is_deleted='n'
and lgb.is_deleted='n' 
and tbe.is_deleted='n'
and lbor.is_deleted='n' 
and lgm.is_deleted='n'
and isnull(lgo.mawb_id)=0 
and isnull(lgb.sealing_bag_time)=0  
group by 2,3,4,5,6,7,8,9,10,11)
""".format(days,days)

print('-------------------- s3 : --------------------')
print(s3)

# 起飞监控
s4 = """(select  count(1) c, 
lgm.mawb_no,
lgo.channel_code, 
lgo.customer_id,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d') zcdate,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate, 
date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate,
round(timestampdiff(hour,lgb.sealing_bag_time,tbe.event_time)/24,1) 装车用时,
round(timestampdiff(hour,tbe.event_time,tme.event_time)/24,1) 起飞用时,
round(timestampdiff(hour,lgo.gmt_create,tme.event_time)/24,1) 业务至起飞用时,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
lgo.des 
from 
lg_order lgo, 
track_bag_event tbe, 
lg_bag_order_relation lbor, 
track_mawb_event tme, 
lg_bag lgb, 
lg_mawb lgm 
where  
tbe.event_code="DEPS" 
AND tme.event_code in("SDFO","DEPC","DEPT","LKJC")
and lgo.gmt_create {}
and customer_id in ('3161297','3282094')
# and lgo.order_status in(1,2)
and lgo.id=lbor.order_id 
and lbor.bag_id=tbe.bag_id 
and tme.mawb_id=lgo.mawb_id 
AND lgb.id=lbor.bag_id
and lgo.mawb_id=lgm.id 
and lgo.is_deleted='n' 
and lgb.is_deleted='n' 
and tbe.is_deleted='n' 
and tme.is_deleted='n' 
and lgm.is_deleted='n' 
and lbor.is_deleted='n' 
group by 
lgm.mawb_no,lgo.customer_id, zcdate, qfdate, gmtdate, fddate, lgo.channel_code, lgo.des,装车用时,起飞用时,业务至起飞用时,是否转运)


UNION ALL

(select  count(1) c, 
lgm.mawb_no,
lgo.channel_code, 
lgo.customer_id,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d') zcdate,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate, 
date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate,
round(timestampdiff(hour,lgb.sealing_bag_time,tbe.event_time)/24,1) 装车用时,
round(timestampdiff(hour,tbe.event_time,tme.event_time)/24,1) 起飞用时,
round(timestampdiff(hour,lgo.gmt_create,tme.event_time)/24,1) 业务至起飞用时,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
lgo.des 
from 
lg_order lgo, 
track_bag_event tbe, 
lg_bag_order_relation lbor, 
track_mawb_event tme, 
lg_bag lgb, 
lg_mawb lgm 
where  
tbe.event_code="DEPS" 
AND tme.event_code in("SDFO","DEPC","DEPT","LKJC")
and lgo.gmt_create {}
and platform='JDW'
# and lgo.order_status in(1,2)
and lgo.id=lbor.order_id 
and lbor.bag_id=tbe.bag_id 
and tme.mawb_id=lgo.mawb_id 
AND lgb.id=lbor.bag_id
and lgo.mawb_id=lgm.id 
and lgo.is_deleted='n' 
and lgb.is_deleted='n' 
and tbe.is_deleted='n' 
and tme.is_deleted='n' 
and lgm.is_deleted='n' 
and lbor.is_deleted='n' 
group by 
lgm.mawb_no,lgo.customer_id, zcdate, qfdate, gmtdate, fddate, lgo.channel_code, lgo.des,装车用时,起飞用时,业务至起飞用时,是否转运)
""".format(days,days)

print('-------------------- s4 : --------------------')
print(s4)
# 落地主单
s6 = """
(select  
count(1) c,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des, 
lgo.customer_id,
date_format(date_add(tme1.event_time,interval 8 hour),'%Y-%m-%d') lddate,
date_format(date_add(tme2.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate,
round(timestampdiff(hour,tme2.event_time,tme1.event_time)/24,1) 落地用时,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
round(timestampdiff(hour,lgo.gmt_create,tme1.event_time)/24,1) 业务至落地用时
from 
lg_order lgo,
track_mawb_event tme1, 
track_mawb_event tme2, 
lg_mawb lgm,
lg_bag_order_relation lbor,
lg_bag lgb 
where  
tme1.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
and tme2.event_code in("SDFO","DEPC","DEPT","LKJC") 
and lgo.gmt_create {}
and customer_id in ('3161297','3282094')
# and lgo.order_status in(1,2) 
and tme1.mawb_id=lgo.mawb_id 
and tme2.mawb_id=lgo.mawb_id 
and lgo.mawb_id=lgm.id 
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
AND lgo.is_deleted='n' 
and lgm.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
and tme1.is_deleted='n' 
and tme2.is_deleted='n' 
group by 
2,3,4,5,6,7,8,9,10,11,12)

UNION ALL

(select  
count(1) c,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des, 
lgo.customer_id,
date_format(date_add(tme1.event_time,interval 8 hour),'%Y-%m-%d') lddate,
date_format(date_add(tme2.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate,
round(timestampdiff(hour,tme2.event_time,tme1.event_time)/24,1) 落地用时,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
round(timestampdiff(hour,lgo.gmt_create,tme1.event_time)/24,1) 业务至落地用时
from 
lg_order lgo,
track_mawb_event tme1, 
track_mawb_event tme2, 
lg_mawb lgm,
lg_bag_order_relation lbor,
lg_bag lgb 
where  
tme1.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
and tme2.event_code in("SDFO","DEPC","DEPT","LKJC") 
and lgo.gmt_create {}
and platform='JDW'
# and lgo.order_status in(1,2) 
and tme1.mawb_id=lgo.mawb_id 
and tme2.mawb_id=lgo.mawb_id 
and lgo.mawb_id=lgm.id 
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
AND lgo.is_deleted='n' 
and lgm.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
and tme1.is_deleted='n' 
and tme2.is_deleted='n' 
group by 
2,3,4,5,6,7,8,9,10,11,12)

""".format(days,days)

print('-------------------- s6 : --------------------')
print(s6)

# 清关主单
s7="""(select  count(1) c,
lgo.channel_code, 
lgo.des, 
lgm.mawb_no,
lgb.bag_no,  
lgo.customer_id,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') fxdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
"主单" as Dimension
from 
track_mawb_event tme, 
lg_order lgo, 
lg_mawb lgm,
lg_bag lgb,
lg_bag_order_relation lbor 
where tme.event_code in("IRCM","PVCS","IRCN","RFIC")  
and lgo.gmt_create {}
and customer_id in ('3161297','3282094')
# and lgo.order_status =2 
and tme.mawb_id=lgo.mawb_id  
and lgo.mawb_id=lgm.id 
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
AND lgo.is_deleted='n'  
and tme.is_deleted='n'  
and lgm.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
group by 2,3,4,5,6,7,8,9,10)

UNION ALL
(select  count(1) c,
lgo.channel_code, 
lgo.des, 
lgm.mawb_no,
lgb.bag_no,  
lgo.customer_id,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') fxdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
"主单" as Dimension
from 
track_mawb_event tme, 
lg_order lgo, 
lg_mawb lgm,
lg_bag lgb,
lg_bag_order_relation lbor 
where tme.event_code in("IRCM","PVCS","IRCN","RFIC")  
and lgo.gmt_create {}
and platform='JDW'
# and lgo.order_status =2 
and tme.mawb_id=lgo.mawb_id  
and lgo.mawb_id=lgm.id 
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
AND lgo.is_deleted='n'  
and tme.is_deleted='n'  
and lgm.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
group by 2,3,4,5,6,7,8,9,10)

""".format(days,days)

print('-------------------- s7 : --------------------')
print(s7)


# 清关order
s8="""(SELECT 
count(1) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no, 
lgo.customer_id,
date_format(date_add(toe.event_time ,interval 8 hour),'%Y-%m-%d') fxdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
"order" as Dimension 
from 
lg_order lgo, 
lg_mawb lgm, 
track_order_event toe,
lg_bag lgb,
lg_bag_order_relation lbor 
where 
lgo.id=toe.order_id  
and lgm.id=lgo.mawb_id 
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
AND toe.event_code in("IRCM","PVCS","IRCN","RFIC") 
# AND lgo.order_status =2 
and lgo.gmt_create {} 
and lgo.is_deleted='n' 
and lgm.is_deleted='n' 
and toe.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
and customer_id in ('3161297','3282094')
group by 
2,3,4,5,6,7,8,9,10)

UNION ALL

(SELECT 
count(1) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no, 
lgo.customer_id,
date_format(date_add(toe.event_time ,interval 8 hour),'%Y-%m-%d') fxdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
"order" as Dimension 
from 
lg_order lgo, 
lg_mawb lgm, 
track_order_event toe,
lg_bag lgb,
lg_bag_order_relation lbor 
where 
lgo.id=toe.order_id  
and lgm.id=lgo.mawb_id 
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
AND toe.event_code in("IRCM","PVCS","IRCN","RFIC") 
# AND lgo.order_status =2 
and lgo.gmt_create {} 
and lgo.is_deleted='n' 
and lgm.is_deleted='n' 
and toe.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
and platform='JDW'
group by 
2,3,4,5,6,7,8,9,10)


""".format(days,days)


print('-------------------- s8 : --------------------')
print(s8)

# 清关bag
s9="""(SELECT   
count(1) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no,
lgo.customer_id,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d')fxdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
"bag" as Dimension
from 
track_bag_event tbe, 
lg_bag lgb, 
lg_mawb lgm, 
lg_order lgo,
lg_bag_order_relation lbor 
where   
lgb.id=tbe.bag_id 
and tbe.bag_id=lbor.bag_id 
and lbor.order_id=lgo.id 
and lgo.mawb_id=lgm.id 
AND tbe.event_code in("IRCM","PVCS","IRCN","RFIC") 
and lgo.gmt_create {} 
and customer_id in ('3161297','3282094')
# and lgo.order_status =2 
and lgo.is_deleted='n' 
and tbe.is_deleted='n' 
and lgb.is_deleted='n' 

and lgm.is_deleted='n' 
and lbor.is_deleted='n' 
group by 2,3,4,5,6,7,8,9,10)

UNION ALL

(SELECT   
count(1) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no,
lgo.customer_id,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d')fxdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
"bag" as Dimension
from 
track_bag_event tbe, 
lg_bag lgb, 
lg_mawb lgm, 
lg_order lgo,
lg_bag_order_relation lbor 
where   
lgb.id=tbe.bag_id 
and tbe.bag_id=lbor.bag_id 
and lbor.order_id=lgo.id 
and lgo.mawb_id=lgm.id 
AND tbe.event_code in("IRCM","PVCS","IRCN","RFIC") 
and lgo.gmt_create {} 
and platform='JDW'
# and lgo.order_status =2 
and lgo.is_deleted='n' 
and tbe.is_deleted='n' 
and lgb.is_deleted='n' 

and lgm.is_deleted='n' 
and lbor.is_deleted='n' 
group by 2,3,4,5,6,7,8,9,10)

""".format(days,days)

print('-------------------- s9 : --------------------')
print(s9)

# 交付节点
s10="""(SELECT 
count(1) c ,
lgo.channel_code,
lgo.des,
lgo.customer_id,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d')jfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate, 
round(timestampdiff(hour,tbe.event_time,lgo.delivery_date)/24,1) 妥投用时,
round(timestampdiff(hour,lgo.gmt_create,tbe.event_time)/24,1) 业务至交付用时,
round(timestampdiff(hour,tbe.event_time,now())/24,1) 交付至今,
(case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
lgb.bag_no,
lgm.mawb_no 
from lg_order lgo, 
track_bag_event tbe,
lg_bag_order_relation lbor, 
lg_bag lgb,
lg_mawb lgm 
where 
tbe.bag_id=lgb.id 
and lgb.id=lbor.bag_id 
and lbor.order_id=lgo.id 
and lgm.id=lgo.mawb_id   
AND tbe.event_code="JFMD" 
and lgo.gmt_create {} 
and customer_id in ('3161297','3282094')
# and lgo.order_status =2  
and lgo.is_deleted='n'  
and lgb.is_deleted='n' 
and tbe.is_deleted='n' 
and lbor.is_deleted='n' 
group by 
2,3,4,5,6,7,8,9,10,11,12)
UNION ALL

(SELECT 
count(1) c ,
lgo.channel_code,
lgo.des,
lgo.customer_id,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d')jfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate, 
round(timestampdiff(hour,tbe.event_time,lgo.delivery_date)/24,1) 妥投用时,
round(timestampdiff(hour,lgo.gmt_create,tbe.event_time)/24,1) 业务至交付用时,
round(timestampdiff(hour,tbe.event_time,now())/24,1) 交付至今,
(case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
lgb.bag_no,
lgm.mawb_no 
from lg_order lgo, 
track_bag_event tbe,
lg_bag_order_relation lbor, 
lg_bag lgb,
lg_mawb lgm 
where 
tbe.bag_id=lgb.id 
and lgb.id=lbor.bag_id 
and lbor.order_id=lgo.id 
and lgm.id=lgo.mawb_id   
AND tbe.event_code="JFMD" 
and lgo.gmt_create {} 
and platform='JDW'
# and lgo.order_status =2  
and lgo.is_deleted='n'  
and lgb.is_deleted='n' 
and tbe.is_deleted='n' 
and lbor.is_deleted='n' 
group by 
2,3,4,5,6,7,8,9,10,11,12)


""".format(days,days)
print('-------------------- s10 : --------------------')
print(s10)




# 断更监控
s11="""(SELECT 
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate, 
lgo.channel_code,
lgo.des, 
lgo.customer_id,
(case when lgo.order_status=2 then '未妥投' else '其它' end ) 是否妥投, 
round(timestampdiff(hour,olt.event_time,now())/24,1) 追踪信息日期差, 
# DATEDIFF(CURDATE(),date_format(date_add(olt.event_time,interval 8 hour),'%Y-%m-%d')) 追踪信息日期差, 
round(timestampdiff(hour,lgo.gmt_create,olt.event_time)/24,1) 业务至末条总用时,
count(DISTINCT lgo.order_no) c 
from 
lg_order lgo inner join lg_bag_order_relation lbor on lbor.order_id=lgo.id 
inner join order_last_track olt on olt.order_id=lgo.id #末条信息 
inner join lg_bag lgb on lgb.id=lbor.bag_id 
inner join track_bag_event tbe on tbe.bag_id=lgb.id  
where  tbe.event_code="JFMD" #派送公司收货 
and lgo.gmt_create {}
and olt.event_time {} 
and customer_id in ('3161297','3282094')
and lgo.is_deleted='n' 
and lgb.is_deleted='n'  
and tbe.is_deleted='n' 
and lbor.is_deleted='n' 
and olt.is_deleted='n' 
GROUP BY 1,2,3,4,5,6,7)

UNION ALL
(SELECT 
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate, 
lgo.channel_code,
lgo.des, 
lgo.customer_id,
(case when lgo.order_status=2 then '未妥投' else '其它' end ) 是否妥投, 
round(timestampdiff(hour,olt.event_time,now())/24,1) 追踪信息日期差, 
# DATEDIFF(CURDATE(),date_format(date_add(olt.event_time,interval 8 hour),'%Y-%m-%d')) 追踪信息日期差, 
round(timestampdiff(hour,lgo.gmt_create,olt.event_time)/24,1) 业务至末条总用时,
count(DISTINCT lgo.order_no) c 
from 
lg_order lgo inner join lg_bag_order_relation lbor on lbor.order_id=lgo.id 
inner join order_last_track olt on olt.order_id=lgo.id #末条信息 
inner join lg_bag lgb on lgb.id=lbor.bag_id 
inner join track_bag_event tbe on tbe.bag_id=lgb.id  
where  tbe.event_code="JFMD" #派送公司收货 
and lgo.gmt_create {}
and olt.event_time {} 
and platform='JDW'
and lgo.is_deleted='n' 
and lgb.is_deleted='n'  
and tbe.is_deleted='n' 
and lbor.is_deleted='n' 
and olt.is_deleted='n' 
GROUP BY 1,2,3,4,5,6,7)

""".format(days,days,days,days)

print('-------------------- s11 : --------------------')
print(s11)


# 主单全
s12 = """(select  
distinct lgm.mawb_no ,
lgo.customer_id
from 
lg_order lgo,
lg_mawb lgm 
where  
lgo.mawb_id=lgm.id 
and lgo.gmt_create {} 
and customer_id in ('3161297','3282094')
and lgo.is_deleted='n' 
and lgm.is_deleted='n'  
and isnull(lgo.mawb_id)=0)

UNION ALL

(select  
distinct lgm.mawb_no ,
lgo.customer_id
from 
lg_order lgo,
lg_mawb lgm 
where  
lgo.mawb_id=lgm.id 
and lgo.gmt_create {} 
and platform='JDW'
and lgo.is_deleted='n' 
and lgm.is_deleted='n'  
and isnull(lgo.mawb_id)=0)

""".format(days,days)
print('-------------------- s12 : --------------------')
print(s12)

# 落地bag
s13 = """(SELECT
count(1) c ,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des,
lgo.customer_id,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d') lddate,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate,
round(timestampdiff(hour,tme.event_time,tbe.event_time)/24,1) 落地用时,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
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
and tme.event_code in("SDFO","DEPC","DEPT","LKJC") 
and lgo.gmt_create {}
and customer_id in ('3161297','3282094')
# and lgo.order_status in(1,2)
and lgo.is_deleted='n'
and tbe.is_deleted='n'
and lbor.is_deleted='n'
and lgm.is_deleted='n'
and tme.is_deleted='n'
group by
2,3,4,5,6,7,8,9,10,11,12)


UNION ALL

(SELECT
count(1) c ,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des,
lgo.customer_id,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d') lddate,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate,
round(timestampdiff(hour,tme.event_time,tbe.event_time)/24,1) 落地用时,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
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
and tme.event_code in("SDFO","DEPC","DEPT","LKJC") 
and lgo.gmt_create {}
and platform='JDW'
# and lgo.order_status in(1,2)
and lgo.is_deleted='n'
and tbe.is_deleted='n'
and lbor.is_deleted='n'
and lgm.is_deleted='n'
and tme.is_deleted='n'
group by
2,3,4,5,6,7,8,9,10,11,12)

""".format(days,days)


print('-------------------- s13 : --------------------')
print(s13)


# 落地order
s14 = """(select
count(1) c,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des,
lgo.customer_id,
date_format(date_add(toe.event_time,interval 8 hour),'%Y-%m-%d') lddate,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate,
round(timestampdiff(hour,tme.event_time,toe.event_time)/24,1) 落地用时,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
round(timestampdiff(hour,lgo.gmt_create,toe.event_time)/24,1) 业务至落地用时
from lg_order lgo,
track_order_event toe,
lg_mawb lgm,
lg_bag lgb,
lg_bag_order_relation lbor,
track_mawb_event tme
where  toe.event_code in("ARIR","ABCD","ABAD","AECD","ARMA")
and tme.event_code in("SDFO","DEPC","DEPT","LKJC") 
and lgo.gmt_create {}
and customer_id in ('3161297','3282094')
# and lgo.order_status in(1,2)
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
group by 2,3,4,5,6,7,8,9,10,11,12)


UNION ALL

(select
count(1) c,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des,
lgo.customer_id,
date_format(date_add(toe.event_time,interval 8 hour),'%Y-%m-%d') lddate,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate,
round(timestampdiff(hour,tme.event_time,toe.event_time)/24,1) 落地用时,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
round(timestampdiff(hour,lgo.gmt_create,toe.event_time)/24,1) 业务至落地用时
from lg_order lgo,
track_order_event toe,
lg_mawb lgm,
lg_bag lgb,
lg_bag_order_relation lbor,
track_mawb_event tme
where  toe.event_code in("ARIR","ABCD","ABAD","AECD","ARMA")
and tme.event_code in("SDFO","DEPC","DEPT","LKJC") 
and lgo.gmt_create {}
and platform='JDW'
# and lgo.order_status in(1,2)
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
group by 2,3,4,5,6,7,8,9,10,11,12)

""".format(days,days)

print('-------------------- s14 : --------------------')
print(s14)


# 头程延误
s15 = """(select  count(1) c, 
lgm.mawb_no,
lgo.channel_code, 
lgo.customer_id,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d') zcdate,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate, 
date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate,
round(timestampdiff(hour,lgb.sealing_bag_time,tbe.event_time)/24,1) 装车用时,
round(timestampdiff(hour,tbe.event_time,now())/24,1) 装车至今,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
lgo.des 
from 
lg_order lgo, 
lg_bag lgb, 
track_bag_event tbe, 
lg_bag_order_relation lbor, 
track_mawb_event tme, 
lg_mawb lgm 
where  
tbe.event_code="DEPS" 
and lgo.gmt_create {}
and customer_id in ('3161297','3282094')
and lgo.id=lbor.order_id 
and lbor.bag_id=tbe.bag_id
AND lgb.id=lbor.bag_id 
and tme.mawb_id=lgo.mawb_id 
AND lgb.id=lbor.bag_id
and lgo.mawb_id=lgm.id 
and lgo.is_deleted='n' 
and tbe.is_deleted='n' 
and tme.is_deleted='n' 
and lgm.is_deleted='n' 
and lbor.is_deleted='n'  
and not exists
(
select 1
from  
track_mawb_event tme 
where  
tme.mawb_id=lgo.mawb_id 
AND tme.event_code in("SDFO","DEPC","DEPT","LKJC")
and tme.is_deleted='n' 
)
group by 
lgm.mawb_no, lgo.customer_id,zcdate, qfdate,  gmtdate, fddate, lgo.channel_code, lgo.des,装车用时,装车至今,是否转运)


UNION ALL

(select  count(1) c, 
lgm.mawb_no,
lgo.channel_code, 
lgo.customer_id,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d') zcdate,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate, 
date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate,
round(timestampdiff(hour,lgb.sealing_bag_time,tbe.event_time)/24,1) 装车用时,
round(timestampdiff(hour,tbe.event_time,now())/24,1) 装车至今,
(case when lgo.order_status in(1,2) then '转运中' else '其它' end ) 是否转运,
lgo.des 
from 
lg_order lgo, 
lg_bag lgb, 
track_bag_event tbe, 
lg_bag_order_relation lbor, 
track_mawb_event tme, 
lg_mawb lgm 
where  
tbe.event_code="DEPS" 
and lgo.gmt_create {}
and platform='JDW'
and lgo.id=lbor.order_id 
and lbor.bag_id=tbe.bag_id
AND lgb.id=lbor.bag_id 
and tme.mawb_id=lgo.mawb_id 
AND lgb.id=lbor.bag_id
and lgo.mawb_id=lgm.id 
and lgo.is_deleted='n' 
and tbe.is_deleted='n' 
and tme.is_deleted='n' 
and lgm.is_deleted='n' 
and lbor.is_deleted='n'  
and not exists
(
select 1
from  
track_mawb_event tme 
where  
tme.mawb_id=lgo.mawb_id 
AND tme.event_code in("SDFO","DEPC","DEPT","LKJC")
and tme.is_deleted='n' 
)
group by 
lgm.mawb_no, lgo.customer_id,zcdate, qfdate, gmtdate, fddate, lgo.channel_code, lgo.des,装车用时,装车至今,是否转运)



""".format(days,days)

print('-------------------- s15 : --------------------')
print(s15)






# 交付监控
# s15 = """SELECT
# count(1) c ,
# lgo.channel_code,
# lgo.des,
# date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d')jfdate,
# date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate,
# lgb.bag_no,
# lgm.mawb_no
# from lg_order lgo,
# track_bag_event tbe,
# lg_bag_order_relation lbor,
# lg_bag lgb,
# lg_mawb lgm
# where
# tbe.bag_id=lgb.id
# and lgb.id=lbor.bag_id
# and lbor.order_id=lgo.id
# and lgm.id=lgo.mawb_id
# AND tbe.event_code="JFMD"
# and lgo.gmt_create {}
# and lgo.platform='WISH_ONLINE'
# AND lgo.customer_id in (1151368,1151370,1181372,1181374)
# and lgo.order_status =2
# and lgo.is_deleted='n'
# and lgb.is_deleted='n'
# and tbe.is_deleted='n'
# and lbor.is_deleted='n'
# group by
# 2,3,4,5,6,7
# """.format(days)

def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df


dp = execude_sql(s1)
dp.loc[dp['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
dp.loc[dp['customer_id'] == 3282094, 'customer_id'] = '促佳'
dp['项目名称'] = dp.apply(lambda row: "兰亭集势" if row['customer_id'] =='兰亭集势' else( '促佳' if row['customer_id'] =='促佳'  else "京东"), axis=1)
dp.loc[dp['order_status'] == 1, 'order_status'] = '已发送'
dp.loc[dp['order_status'] == 2, 'order_status'] = '转运中'
dp['order_status'].unique()

dq = pd.concat([execude_sql(s7), execude_sql(s8), execude_sql(s9)])
dq.loc[dq['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
dq.loc[dq['customer_id'] == 3282094, 'customer_id'] = '促佳'

dq['项目名称'] = dq.apply(lambda row: "兰亭集势" if row['customer_id'] =='兰亭集势' else( '促佳' if row['customer_id'] =='促佳'  else "京东"), axis=1)

dq['当日']=datetime.date.today()
dq['fxdate']=pd.to_datetime(dq['fxdate'])
# print()
dq['清关距离当天']=(pd.to_datetime(dq['当日'])- dq['fxdate']).dt.days

dl = pd.concat([execude_sql(s6), execude_sql(s13), execude_sql(s14)])
dl.loc[dl['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
dl.loc[dl['customer_id'] == 3282094, 'customer_id'] = '促佳'
dl['项目名称'] = dl.apply(lambda row: "兰亭集势" if row['customer_id'] =='兰亭集势' else( '促佳' if row['customer_id'] =='促佳'  else "京东"), axis=1)


dqf = execude_sql(s4)
dqf.loc[dqf['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
dqf.loc[dqf['customer_id'] == 3282094, 'customer_id'] = '促佳'
dqf['项目名称'] = dqf.apply(lambda row: "兰亭集势" if row['customer_id'] =='兰亭集势' else( '促佳' if row['customer_id'] =='促佳'  else "京东"), axis=1)


dck = execude_sql(s2)
dck.loc[dck['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
dck.loc[dck['customer_id'] == 3282094, 'customer_id'] = '促佳'
dck['项目名称'] = dck.apply(lambda row: "兰亭集势" if row['customer_id'] =='兰亭集势' else( '促佳' if row['customer_id'] =='促佳'  else "京东"), axis=1)


dzc = execude_sql(s3)
dzc.loc[dzc['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
dzc.loc[dzc['customer_id'] == 3282094, 'customer_id'] = '促佳'
dzc['项目名称'] = dzc.apply(lambda row: "兰亭集势" if row['customer_id'] =='兰亭集势' else( '促佳' if row['customer_id'] =='促佳'  else "京东"), axis=1)


djf = execude_sql(s10)
djf.loc[djf['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
djf.loc[djf['customer_id'] == 3282094, 'customer_id'] = '促佳'
djf['项目名称'] = djf.apply(lambda row: "兰亭集势" if row['customer_id'] =='兰亭集势' else( '促佳' if row['customer_id'] =='促佳'  else "京东"), axis=1)


ddg = execude_sql(s11)
ddg.loc[ddg['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
ddg.loc[ddg['customer_id'] == 3282094, 'customer_id'] = '促佳'
ddg['项目名称'] = ddg.apply(lambda row: "兰亭集势" if row['customer_id'] =='兰亭集势' else( '促佳' if row['customer_id'] =='促佳'  else "京东"), axis=1)


dtc = execude_sql(s15)
dtc.loc[dtc['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
dtc.loc[dtc['customer_id'] == 3282094, 'customer_id'] = '促佳'
dtc['项目名称'] = dtc.apply(lambda row: "兰亭集势" if row['customer_id'] =='兰亭集势' else( '促佳' if row['customer_id'] =='促佳'  else "京东"), axis=1)


djf['c'] = djf['c'].astype('int')
dj = djf.groupby(['mawb_no', 'bag_no', 'channel_code', 'des', 'jfdate', 'ywdate', 'customer_id', '项目名称','是否妥投'])['c'].sum().reset_index()


dl['c']=dl['c'].astype('int')
dll = dl.groupby(['mawb_no', 'bag_no', 'channel_code', 'des', 'lddate', 'ywdate', 'customer_id', '项目名称','是否转运'])['c'].sum().reset_index()

dzd = execude_sql(s12)
dzd.loc[dzd['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
dzd.loc[dzd['customer_id'] == 3282094, 'customer_id'] = '促佳'
dzd['项目名称'] = dzd.apply(lambda row: "兰亭集势" if row['customer_id'] =='兰亭集势' else( '促佳' if row['customer_id'] =='促佳'  else "京东"), axis=1)

name_l = [['未配载监控', dp], ['出库监控', dck], ['装车监控', dzc], ['起飞监控', dqf], ['落地监控', dl], ['清关全', dq], ['主单全', dzd],
['交付节点', djf], ['断更监控', ddg], ['交付监控', dj], ['落地节点', dll],['头程延误', dtc]]


def file_xlsx(name, df):
    bf = r'F:\PBI临时文件\合并分段监控\{}.xlsx'.format(name)
    writer = pd.ExcelWriter(bf)
    df.to_excel(writer, 'sheet1', index=False)
    writer.save()
for n in name_l:
    file_xlsx(n[0], n[1])
# file_xlsx('断更监控', ddg)
# ---------------------
# print(dl.columns)
# dl['c']=dl['c'].astype('int')
# dll=dl.groupby(['mawb_no', 'bag_no', 'channel_code', 'des', 'lddate', 'ywdate'])['c'].sum().reset_index()
# print(dll)#落地 目标文件

# dl.info()
# bf =r'F:\PBI临时文件\wish分段监控\断更监控.xlsx'
# writer = pd.ExcelWriter(bf)
# ddg.to_excel(writer,'sheet1',index=False)

# writer.save()
# 将落地监控那张表按照mawb_no,bag_no,channel_code,des,lddate,ywdate 这几个联合字段来聚合计算c列,新生成的表命名为落地节点
# data = pd.read_excel(u"F:\\PBI临时文件\\wish分段监控\\落地监控.xlsx")
# print(data)
# new = pd.DataFrame
# data['合并'] = data['mawb_no'] + "-" + data['bag_no'] + "-" + data['channel_code'] + "-" + data['des'] + "-" + data['lddate'].map(str) + "-" + data['ywdate'].map(str)
# print(data.head(9))
# data.info()
# data['c'] = data['c'].astype('float')
# grouped = data.groupby(data['合并'])['c'].su
# print(grouped)
# dfnew = grouped.sum()
# new['c']= dfnew
# new = new.reset_index()
# data = data.drop(['qfdate', '落地用时', '业务至落地用时'],axis=1)
# print(data)
# bf =r'F:\PBI临时文件\wish分段监控\落地节点.xlsx'
# writer = pd.ExcelWriter(bf)
# data.to_excel(writer,'sheet1',index=False)
# writer.save()


