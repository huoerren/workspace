import datetime

import pandas as pd
import pymysql

nows = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
print(nows)

# #数仓连接
con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8",
                      autocommit=True, database='logisticscore')
cur = con.cursor()

# days="BETWEEN '2021-03-31 16:00:00' and '"+time_yes+"'"1
# days="BETWEEN '2021-03-31 16:00:00' and "+"'"+nows+"'"
days = "BETWEEN '2021-12-25 16:00:00' and " + "'" + nows + "' "
# days = "BETWEEN '2021-03-31 16:00:00' and '2021-04-1 16:00:00' "
# days="BETWEEN '2020-12-17 16:00:00' and '202
# 1-02-15 15:59:59'"

# 未配载监控
s1 = """ SELECT
	lgb.bag_no, lgo.order_no, lgo.channel_code, lgo.customer_id,lgo.des,lgo.order_status,
    date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate,
    date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
    round(timestampdiff(hour,lgo.gmt_create,lgb.sealing_bag_time)/24,1) 封袋时间,
    round(timestampdiff(hour,lgb.sealing_bag_time,now())/24,1) 未配载时间,
    platform
from
    lg_order lgo left join lg_bag_order_relation lbor on lgo.id = lbor.order_id 
										left join lg_bag lgb on lbor.bag_id= lgb.id 
    
WHERE   lgo.gmt_create  {}
    and ((customer_id in ('3161297','3282094')) or  (platform='JDW') or  (platform='DHLINK') )
    and lgo.order_status in(1,2)
    and lgo.mawb_id is null 
    and lgb.sealing_bag_time is not null
    and lgo.is_deleted= 'n'
    and lgb.is_deleted= 'n'
    and lbor.is_deleted='n' 
""".format(days)

print('------ s1 : ------')
print(s1)


print('-------------------------')


# 出库监控
s2 = """ 
select 
    count(distinct order_no) c,
    lgm.mawb_no,
    lgo.channel_code,
    lgo.customer_id,
    date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate,
    date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
    round(TIMESTAMPDIFF(hour,lgo.gmt_create,lgb.sealing_bag_time)/24,1) 出库用时,
    (case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
    lgo.des,
    platform
from
	lg_order lgo left join lg_bag_order_relation lbor on lgo.id=lbor.order_id 
									left join lg_bag lgb on  lbor.bag_id=lgb.id  
										left join lg_mawb lgm on lgo.mawb_id=lgm.id
where  lgo.gmt_create  {} 
       and ((customer_id in ('3161297','3282094')) or (platform='JDW') or (platform='DHLINK') ) 
	    
       and lgo.is_deleted='n' 
       and lgb.is_deleted='n'
       and lgm.is_deleted='n' 
       and lbor.is_deleted='n' 
         
	group by lgm.mawb_no,lgo.customer_id,fddate, gmtdate, lgo.channel_code, lgo.des,出库用时,是否妥投,platform
""".format(days )
print('------ s2 : ------')
print(s2)
print('------------------')

# 装车监控
s3 = """ 
SELECT 
    count(distinct order_no) c, 
    lgo.channel_code,
    lgo.des, 
    lgo.customer_id,
    date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate, 
    date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate, 
    date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d') zcdate, 
    round(timestampdiff(hour,lgb.sealing_bag_time,tbe.event_time)/24,1) 装车用时,
    round(timestampdiff(hour,lgo.gmt_create,lgb.sealing_bag_time)/24,1) 出库用时,
    (case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
    lgm.mawb_no,
    platform 
from lg_order lgo left join lg_bag_order_relation lbor on lgo.id = lbor.order_id 
										left join lg_bag lgb on lbor.bag_id=lgb.id 
											left join track_bag_event tbe on lbor.bag_id = tbe.bag_id 
												left join lg_mawb lgm on  lgo.mawb_id=lgm.id 
where
    lgo.gmt_create {}  
    and tbe.event_code="DEPS"
    and ((customer_id in ('3161297','3282094')) or (platform='JDW') or (platform='DHLINK'))
    
    and lgo.is_deleted='n'
    and lgb.is_deleted='n' 
    and tbe.is_deleted='n'
    and lbor.is_deleted='n' 
    and lgm.is_deleted='n'
     
    group by 2,3,4,5,6,7,8,9,10,11,12
""".format(days )
print('------ s3 : ------')
print(s3)
print('------------------')

# 起飞监控
# s4 = """
#  select  count(distinct order_no) c,
# 	lgm.mawb_no,
# 	lgo.channel_code,
# 	lgo.customer_id,
# 	date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
# 	date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d') zcdate,
# 	date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
# 	date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate,
# 	round(timestampdiff(hour,lgb.sealing_bag_time,tbe.event_time)/24,1) 装车用时,
# 	round(timestampdiff(hour,tbe.event_time,tme.event_time)/24,1) 起飞用时,
# 	round(timestampdiff(hour,lgo.gmt_create,tme.event_time)/24,1) 业务至起飞用时,
# 	(case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
# 	(case when tme.event_code in ("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF','PMWC') then "全部起飞" else  "部分起飞" end) 部分与否,
# 	lgo.des
# from
# 	lg_order lgo left join lg_bag_order_relation lbor on lgo.id=lbor.order_id
# 		left join lg_bag lgb on lgb.id=lbor.bag_id
# 			left join	lg_mawb lgm on  lgo.mawb_id=lgm.id
# 				left join  track_bag_event tbe on lbor.bag_id=tbe.bag_id
# 					left join  track_mawb_event tme on tme.mawb_id=lgo.mawb_id
# where
#     lgo.gmt_create  {}
# and tbe.event_code="DEPS"
# and tme.event_code in("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF','PMWC' )
# and ( (customer_id in ('3161297','3282094')) or (platform='JDW') )
# and lgo.is_deleted='n'
# and lgb.is_deleted='n'
# and tbe.is_deleted='n'
#
# and tme.is_deleted='n'
# and lgm.is_deleted='n'
# and lbor.is_deleted='n'
# group by
#     lgm.mawb_no,lgo.customer_id, zcdate, qfdate, gmtdate, fddate, lgo.channel_code,
# 			lgo.des,装车用时,起飞用时,业务至起飞用时,是否妥投,部分与否
#
# """.format(days)

s4 = """
(select  count(distinct order_no) c,
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
    (case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
    (case when tme.event_code in ("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF' ) then "全部起飞" else  "部分起飞" end) 部分与否,
lgo.des,
platform
from
    lg_order lgo,
    track_bag_event tbe,
    lg_bag_order_relation lbor,
    track_mawb_event tme,
    lg_bag lgb,
    lg_mawb lgm
where
    tbe.event_code="DEPS"
    AND tme.event_code in("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF','PMWC' )
    and lgo.gmt_create {}
    and ((customer_id in ('3161297','3282094')) or (platform='JDW') or (platform='DHLINK') )
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
    lgm.mawb_no,lgo.customer_id, zcdate, qfdate, gmtdate, fddate, lgo.channel_code, lgo.des,装车用时,起飞用时,业务至起飞用时,是否妥投,部分与否,platform)

""".format(days, days)

print('------ s4 : ------')
print(s4)
print('------------------')

# 落地主单
s6 = """
 select  
    count(distinct order_no) c,
    lgm.mawb_no,
    lgb.bag_no,
    lgo.channel_code,
    lgo.des, 
    lgo.customer_id,
    date_format(date_add(tme1.event_time,interval 8 hour),'%Y-%m-%d') lddate,
    date_format(date_add(tme2.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
    date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate,
    round(timestampdiff(hour,tme2.event_time,tme1.event_time)/24,1) 落地用时,
    round(timestampdiff(hour,lgo.gmt_create,tme1.event_time)/24,1) 业务至落地用时,
		(case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
		platform
from 
    lg_order lgo left join lg_bag_order_relation lbor on lgo.id=lbor.order_id  
								left join lg_mawb lgm on lgo.mawb_id=lgm.id  
									left join lg_bag lgb on lbor.bag_id=lgb.id 
										left join  track_mawb_event tme1 on tme1.mawb_id=lgo.mawb_id 
											left join track_mawb_event tme2 on tme2.mawb_id=lgo.mawb_id 
where 
    lgo.gmt_create {}  
    and tme1.event_code in("ARIR","ABCD","ABAD","AECD","ARMA")
    and tme2.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
    and ( (customer_id in ('3161297','3282094')) or (platform='JDW') or (platform='DHLINK') )
	 	 
    and lgo.is_deleted='n' 
    and lgm.is_deleted='n' 
    and lgb.is_deleted='n' 
    and lbor.is_deleted='n' 
    and tme1.is_deleted='n' 
    and tme2.is_deleted='n'
group by  2,3,4,5,6,7,8,9,10,11,12,13

""".format(days )
print('------ s6 : ------')
print(s6)
print('------------------')


# 清关主单
s7 = """ 
select  count(distinct order_no) c,
    lgo.channel_code, 
    lgo.des, 
    lgm.mawb_no,
    lgb.bag_no,  
    lgo.customer_id,
    date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') fxdate,
    date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
    (case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
    "主单" as Dimension,
    platform
from 
    lg_order lgo left join lg_bag_order_relation lbor on lgo.id=lbor.order_id 
										left join lg_bag lgb on lbor.bag_id=lgb.id 
											left join lg_mawb lgm on lgo.mawb_id=lgm.id  
												left join track_mawb_event tme on tme.mawb_id=lgo.mawb_id    
where 
    lgo.gmt_create  {}
and tme.event_code in("IRCM","PVCS","IRCN","RFIC","BGRK")
and ( (customer_id in ('3161297','3282094')) or (platform='JDW') or (platform='DHLINK'))

 
and lgo.is_deleted='n'  
and tme.is_deleted='n'  
and lgm.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
group by 2,3,4,5,6,7,8,9,10,11 

""".format(days )
print('------ s7 : ------')
print(s7)
print('------------------')


# 清关order
s8 = """ 
SELECT 
    count(distinct order_no) c,
    lgo.channel_code,
    lgo.des,
    lgm.mawb_no,
    lgb.bag_no, 
    lgo.customer_id,
    date_format(date_add(toe.event_time ,interval 8 hour),'%Y-%m-%d') fxdate,
    date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
    (case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
    "order" as Dimension,
    platform 
from 
    lg_order lgo left join lg_bag_order_relation lbor on lgo.id=lbor.order_id 
										left join lg_bag lgb on lbor.bag_id=lgb.id  
											left join  lg_mawb lgm on lgm.id=lgo.mawb_id 
												left join track_order_event toe on lgo.id=toe.order_id   
where 
  lgo.gmt_create {}  
and ( (customer_id in ('3161297','3282094')) or (platform='JDW') or (platform='DHLINK') )
and toe.event_code in("IRCM","PVCS","IRCN","RFIC","BGRK")

and lgo.is_deleted='n' 
and lgm.is_deleted='n' 
and toe.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 

group by 
2,3,4,5,6,7,8,9,10,11

""".format(days)
print('------ s8 : ------')
print(s8)
print('------------------')


# 清关bag
s9 = """ 
 SELECT   
    count(distinct order_no) c,
    lgo.channel_code,
    lgo.des,
	lgo.customer_id,
    lgm.mawb_no,
    lgb.bag_no,
    
    date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d')fxdate,
    date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
    (case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
    "bag" as Dimension,
    platform
from 
    lg_order lgo left join lg_bag_order_relation lbor on lbor.order_id=lgo.id  
										left join track_bag_event tbe on tbe.bag_id=lbor.bag_id  left join lg_bag lgb on lgb.id=tbe.bag_id
											left join lg_mawb lgm on lgo.mawb_id=lgm.id  
where
   lgo.gmt_create {}  
and tbe.event_code in("IRCM","PVCS","IRCN","RFIC","BGRK") 
and ( (customer_id in ('3161297','3282094')) or (platform='JDW') or (platform='DHLINK') )  

and lgo.is_deleted='n' 
and tbe.is_deleted='n' 
and lgb.is_deleted='n' 
and lgm.is_deleted='n' 
and lbor.is_deleted='n'

group by 2,3,4,5,6,7,8,9,10,11

""".format(days )
print('------ s9 : ------')
print(s9)
print('------------------')

# 交付节点
s10 = """ 
 SELECT 
    count(distinct order_no) c ,
    lgo.channel_code,
    lgo.des,
    lgo.customer_id,
    date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d')jfdate,
    date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate, 
    round(timestampdiff(hour,lgo.gmt_create,tbe.event_time)/24,1) 业务至交付用时,
    round(timestampdiff(hour,tbe.event_time,now())/24,1) 交付至今,
    (case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
    round(timestampdiff(hour,tbe.event_time,lgo.delivery_date)/24,1) 妥投用时,
    lgb.bag_no,
    lgm.mawb_no,
    platform 
from 
    lg_order lgo left join lg_bag_order_relation lbor on lbor.order_id=lgo.id  
										left join lg_bag lgb on lgb.id=lbor.bag_id  left join track_bag_event tbe on tbe.bag_id=lgb.id 
    										left join lg_mawb lgm on lgm.id=lgo.mawb_id     
where 
     lgo.gmt_create {}  
    and tbe.event_code in ("JFMD","AAPS") 
    and (customer_id in ('3161297','3282094') or (platform='JDW') or (platform='DHLINK') )
    
    and lgo.is_deleted='n'  
    and lgb.is_deleted='n' 
    and tbe.is_deleted='n' 
    and lbor.is_deleted='n' 
    group by 
    2,3,4,5,6,7,8,9,10,11,12,13
""".format(days )
print('------ s10 : ------')
print(s10)
print('------------------')


# 断更监控
s11 = """ 
SELECT 
    date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate, 
    lgo.channel_code,
    lgo.des, 
    lgo.customer_id,
    (case when lgo.order_status=2 then '未妥投' else '其它' end ) 是否妥投, 
    round(timestampdiff(hour,olt.event_time,now())/24,1) 追踪信息日期差, 
    round(timestampdiff(hour,lgo.gmt_create,olt.event_time)/24,1) 业务至末条总用时,
    platform ,
    count(DISTINCT lgo.order_no) c 
    
from 
lg_order lgo left join lg_bag_order_relation lbor on lbor.order_id=lgo.id 
    left join order_last_track olt on olt.order_id=lgo.id #末条信息 
        left join lg_bag lgb on lgb.id=lbor.bag_id 
            left join track_bag_event tbe on tbe.bag_id=lgb.id  
where  
  lgo.gmt_create {}
and olt.event_time {}  
and ((customer_id in ('3161297','3282094')) or (platform='JDW') or (platform='DHLINK')) 

and tbe.event_code in("JFMD","AAPS") #派送公司收货 

and lgo.is_deleted='n' 
and lgb.is_deleted='n'  
and tbe.is_deleted='n' 
and lbor.is_deleted='n' 
and olt.is_deleted='n' 

GROUP BY 1,2,3,4,5,6,7,8

""".format(days, days )
print('------ s11 : ------')
print(s11)
print('------------------')


# 主单全
s12 = """ 
select  
	distinct lgm.mawb_no ,
	lgo.customer_id,
	platform
from 
	lg_order lgo left join lg_mawb lgm on lgo.mawb_id=lgm.id
where
  lgo.gmt_create {}  
and ( (customer_id in ('3161297','3282094'))  or  (platform='JDW' ) )
and lgo.is_deleted='n' 
and lgm.is_deleted='n'  
and  lgo.mawb_id is not null  

""".format(days )

print('------ s12 : ------')
print(s12)
print('------------------')

# 落地bag
s13 = """ 
 SELECT
    count(distinct order_no) c ,
    lgm.mawb_no,
    lgb.bag_no,
    lgo.channel_code,
    lgo.des,
    lgo.customer_id,
    date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d') lddate,
    date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
    date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate,
    round(timestampdiff(hour,tme.event_time,tbe.event_time)/24,1) 落地用时,
    (case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
    round(timestampdiff(hour,lgo.gmt_create,tbe.event_time)/24,1) 业务至落地用时,
    platform
from
    lg_order lgo left join lg_bag_order_relation lbor on lbor.order_id=lgo.id 
										left join track_bag_event tbe on lbor.bag_id=tbe.bag_id 
											left join lg_bag lgb on tbe.bag_id=lgb.id 
												left join track_mawb_event tme on lgo.mawb_id=tme.mawb_id 
													left join lg_mawb lgm on lgm.id=lgo.mawb_id 							
where
      lgo.gmt_create {} 
    and ( customer_id in ('3161297','3282094') or (platform='JDW') or (platform='DHLINK') ) 
		and tbe.event_code in("ARIR","ABCD","ABAD","AECD","ARMA")
				
    and lgo.is_deleted='n'
    and tbe.is_deleted='n'
    and lbor.is_deleted='n'
    and lgm.is_deleted='n'
    and tme.is_deleted='n'
    group by
    2,3,4,5,6,7,8,9,10,11,12,13
""".format(days, days)

print('------ s13 : ------')
print(s13)
print('------------------')


# 落地order
s14 = """ 
 select
    count(distinct order_no) c,
    lgm.mawb_no,
    lgb.bag_no,
    lgo.channel_code,
    lgo.des,
    lgo.customer_id,
    date_format(date_add(toe.event_time,interval 8 hour),'%Y-%m-%d') lddate,
    date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
    date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') ywdate,
    round(timestampdiff(hour,tme.event_time,toe.event_time)/24,1) 落地用时,
    round(timestampdiff(hour,lgo.gmt_create,toe.event_time)/24,1) 业务至落地用时,
		(case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
		platform
from 
    lg_order lgo left join lg_bag_order_relation lbor on lgo.id=lbor.order_id 
										left join lg_bag lgb on lbor.bag_id=lgb.id 
											left join lg_mawb lgm on lgo.mawb_id=lgm.id 
												left join track_order_event toe on lgo.id=toe.order_id 
													left join track_mawb_event tme on lgo.mawb_id=tme.mawb_id 
where  
     lgo.gmt_create {} 
    and toe.event_code in("ARIR","ABCD","ABAD","AECD","ARMA")
		
    and ((customer_id in ('3161297','3282094')) or (platform='JDW') or (platform='DHLINK') )
	
    and lgo.is_deleted='n'
    and toe.is_deleted='n'
    and lgm.is_deleted='n'
    and lgb.is_deleted='n'
    and lbor.is_deleted='n'
    and tme.is_deleted='n'
    group by 2,3,4,5,6,7,8,9,10,11,12,13
    
""".format(days )
print('------ s14 : ------')
print(s14)
print('------------------')


# 头程延误
s15 = """ 
select  
    count(distinct order_no) c, 
    lgm.mawb_no,
    lgo.channel_code, 
    lgo.customer_id,
    date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
    date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d') zcdate,
    date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate, 
    date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate,
    round(timestampdiff(hour,lgb.sealing_bag_time,tbe.event_time)/24,1) 装车用时,
    round(timestampdiff(hour,tbe.event_time,now())/24,1) 装车至今,
    (case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
    lgo.des,
    platform 
from 
    lg_order lgo, 
    lg_bag lgb, 
    track_bag_event tbe, 
    lg_bag_order_relation lbor, 
    track_mawb_event tme, 
    lg_mawb lgm 
where 
    lgo.gmt_create {} 
and tbe.event_code="DEPS" 
and ((customer_id in ('3161297','3282094')) or  (platform='JDW') or  (platform='DHLINK') )
and lgo.id=lbor.order_id 
and lbor.bag_id=tbe.bag_id
and lgb.id=lbor.bag_id 
and tme.mawb_id=lgo.mawb_id 
and lgb.id=lbor.bag_id
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
and tme.event_code in("SDFO","DEPC","DEPT","LKJC")
and tme.is_deleted='n' 
)
group by 
lgm.mawb_no, lgo.customer_id,zcdate, qfdate, gmtdate, fddate, lgo.channel_code, lgo.des,装车用时,装车至今,是否妥投,platform
""".format(days)
print('------ s15 : ------')
print(s15)
print('------------------')

# 头程延误 - by panxin 20220106
    # 装车数据 左关联 起飞数据 找出 起飞-装车间隔大于阈值（优先渠道 阈值为 1天 ，其他渠道阈值为3天 ）

s15_new = """

select count(distinct a.order_no) c ,b.mawb_no,a.channel_code ,a.customer_id,a.gmtdate , zcdate, qfdate,  
		   a.fddate ,a.装车用时,a.装车至今 ,a.是否妥投, a.des ,a.platform
from  (
  SELECT 
	 order_no  ,
	 date_add(event_time,interval 8 hour)  as zcdate,
	 des,
	 channel_code,
	 customer_id,
	 date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
	 date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate,
	 round(timestampdiff(hour,lgb.sealing_bag_time,tbe.event_time)/24,1) 装车用时,
	 round(timestampdiff(hour,tbe.event_time,now())/24,1) 装车至今,
	 (case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
	 platform
	  
   FROM
	lg_order lgo left join  lg_bag_order_relation lbor on  lgo.id = lbor.order_id 
			           left join track_bag_event tbe  on  lbor. bag_id = tbe.bag_id
									left join lg_bag lgb on lgb.id= lbor.bag_id 
	where
		    lgo.gmt_create {}
	    and tbe.event_code='DEPS'
	    and ((lgo.customer_id in ('3161297','3282094')) or  (lgo.platform='JDW') or  (platform='DHLINK') )
        and lgo.is_deleted='n'
        and lbor.is_deleted='n'
        and tbe.is_deleted='n'
)a left join  ( 
SELECT  order_no  ,
         date_add(event_time,interval 8 hour) qfdate,
				lgm.mawb_no	
    FROM
            lg_order lgo left join track_mawb_event tme on lgo.mawb_id=tme.mawb_id left join lg_mawb lgm on lgo.mawb_id = lgm.id
        
		where	lgo.gmt_create {}
			and event_code in ("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF','PMWC')
			and lgo.is_deleted='n'
			and tme.is_deleted='n'
)b 
on a.order_no = b.order_no 
where  
	case when a.channel_code ='CNE全球优先' then  round(timestampdiff(hour,zcDate,qfdate )/24,1) > 1 
										else round(timestampdiff(hour,zcDate ,qfdate)/24,1) > 3 end 
group by  b.mawb_no, a.channel_code,a.customer_id, a.gmtdate ,zcDate, qfdate,  a.fddate ,a.装车用时,a.装车至今 ,a.是否妥投, a.des, a.platform  
""".format(days,days)

print('--------------- s15_new :----------------')
print(s15_new)


def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df


dp = execude_sql(s1)
dp.loc[dp['platform'] == 'MYSHOP', 'customer_id'] = '兰亭集势'
dp.loc[dp['platform'] == 'OTHER', 'customer_id'] = '促佳'
dp.loc[dp['platform'] == 'DHLINK', 'customer_id'] = '敦煌'
dp['项目名称'] = dp.apply(
    lambda row: "兰亭集势" if row['customer_id'] == '兰亭集势' else (
        '促佳' if row['customer_id'] == '促佳' else ('敦煌' if row['customer_id'] == '敦煌' else "京东")), axis=1)

print('================================')
print(dp.head())

dp.loc[dp['order_status'] == 1, 'order_status'] = '已发送'
dp.loc[dp['order_status'] == 2, 'order_status'] = '转运中'
dp['order_status'].unique()

# print(dp)

dq = pd.concat([execude_sql(s7), execude_sql(s8), execude_sql(s9)])
dq.loc[dq['platform'] == 'MYSHOP', 'customer_id'] = '兰亭集势'
dq.loc[dq['platform'] == 'OTHER', 'customer_id'] = '促佳'
dq.loc[dq['platform'] == 'DHLINK', 'customer_id'] = '敦煌'
dq['项目名称'] = dq.apply(
    lambda row: "兰亭集势" if row['customer_id'] == '兰亭集势' else (
        '促佳' if row['customer_id'] == '促佳' else ('敦煌' if row['customer_id'] == '敦煌' else "京东")), axis=1)
print(dq.columns.tolist())
# add by panxin 20220310
dq = dq[
    ['c', 'channel_code', 'des', 'mawb_no', 'bag_no', 'customer_id', 'fxdate', 'gmtdate', '是否妥投', 'Dimension', '项目名称']]

dq['当日'] = datetime.date.today()
dq['fxdate'] = pd.to_datetime(dq['fxdate'])
# print()
dq['清关距离当天'] = (pd.to_datetime(dq['当日']) - dq['fxdate']).dt.days

dl = pd.concat([execude_sql(s6), execude_sql(s13), execude_sql(s14)])

print(dl.columns.tolist())

dl.loc[dl['platform'] == 'MYSHOP', 'customer_id'] = '兰亭集势'
dl.loc[dl['platform'] == 'OTHER', 'customer_id'] = '促佳'
dl.loc[dl['platform'] == 'DHLINK', 'customer_id'] = '敦煌'
dl['项目名称'] = dl.apply(
    lambda row: "兰亭集势" if row['customer_id'] == '兰亭集势' else (
        '促佳' if row['customer_id'] == '促佳' else ('敦煌' if row['customer_id'] == '敦煌' else "京东")), axis=1)

dqf = execude_sql(s4)

dqf.loc[dqf['platform'] == 'MYSHOP', 'customer_id'] = '兰亭集势'
dqf.loc[dqf['platform'] == 'OTHER', 'customer_id'] = '促佳'
dqf.loc[dqf['platform'] == 'DHLINK', 'customer_id'] = '敦煌'
dqf['项目名称'] = dqf.apply(
    lambda row: "兰亭集势" if row['customer_id'] == '兰亭集势' else (
        '促佳' if row['customer_id'] == '促佳' else ('敦煌' if row['customer_id'] == '敦煌' else "京东")), axis=1)

mawb_list = list(set(dqf[dqf['部分与否'] == '全部起飞']['mawb_no'].tolist()))
dqf = dqf[~((dqf['部分与否'] == '部分起飞') & (dqf['mawb_no'].isin(mawb_list)))]
print(dqf)

dck = execude_sql(s2)
dck.loc[dck['platform'] == 'MYSHOP', 'customer_id'] = '兰亭集势'
dck.loc[dck['platform'] == 'OTHER', 'customer_id'] = '促佳'
dck.loc[dck['platform'] == 'DHLINK', 'customer_id'] = '敦煌'
dck['项目名称'] = dck.apply(
    lambda row: "兰亭集势" if row['customer_id'] == '兰亭集势' else (
        '促佳' if row['customer_id'] == '促佳' else ('敦煌' if row['customer_id'] == '敦煌' else "京东")), axis=1)

# print(dck)
dzc = execude_sql(s3)
dzc.loc[dzc['platform'] == 'MYSHOP', 'customer_id'] = '兰亭集势'
dzc.loc[dzc['platform'] == 'OTHER', 'customer_id'] = '促佳'
dzc.loc[dzc['platform'] == 'DHLINK', 'customer_id'] = '敦煌'
dzc['项目名称'] = dzc.apply(
    lambda row: "兰亭集势" if row['customer_id'] == '兰亭集势' else (
        '促佳' if row['customer_id'] == '促佳' else ('敦煌' if row['customer_id'] == '敦煌' else "京东")), axis=1)
# print(dzc)

djf = execude_sql(s10)
djf.loc[djf['platform'] == 'MYSHOP', 'customer_id'] = '兰亭集势'
djf.loc[djf['platform'] == 'OTHER', 'customer_id'] = '促佳'
djf.loc[djf['platform'] == 'DHLINK', 'customer_id'] = '敦煌'
djf['项目名称'] = djf.apply(
    lambda row: "兰亭集势" if row['customer_id'] == '兰亭集势' else (
        '促佳' if row['customer_id'] == '促佳' else ('敦煌' if row['customer_id'] == '敦煌' else "京东")), axis=1)
# print(djf)
ddg = execude_sql(s11)
ddg.loc[ddg['platform'] == 'MYSHOP', 'customer_id'] = '兰亭集势'
ddg.loc[ddg['platform'] == 'OTHER', 'customer_id'] = '促佳'
ddg.loc[ddg['platform'] == 'DHLINK', 'customer_id'] = '敦煌'
ddg['项目名称'] = ddg.apply(
    lambda row: "兰亭集势" if row['customer_id'] == '兰亭集势' else (
        '促佳' if row['customer_id'] == '促佳' else ('敦煌' if row['customer_id'] == '敦煌' else "京东")), axis=1)
# print(ddg)

ddg = ddg[['gmtdate', 'channel_code', 'des', 'customer_id', '是否妥投', '追踪信息日期差', '业务至末条总用时', 'c', '项目名称']]

# dtc = execude_sql(s15)
dtc = execude_sql(s15_new)
dtc.loc[dtc['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
dtc.loc[dtc['customer_id'] == 3282094, 'customer_id'] = '促佳'
dtc['项目名称'] = dtc.apply(
    lambda row: "兰亭集势" if row['customer_id'] == '兰亭集势' else ('促佳' if row['customer_id'] == '促佳' else "京东"), axis=1)
# print(dtc)

djf['c'] = djf['c'].astype('int')
dj = djf.groupby(['mawb_no', 'bag_no', 'channel_code', 'des', 'jfdate', 'ywdate', 'customer_id', '项目名称', '是否妥投'])[
    'c'].sum().reset_index()
# print(dj)
dl['c'] = dl['c'].astype('int')
dll = dl.groupby(['mawb_no', 'bag_no', 'channel_code', 'des', 'lddate', 'ywdate', 'customer_id', '项目名称', '是否妥投'])[
    'c'].sum().reset_index()
# print(dll)

dzd = execude_sql(s12)
dzd.loc[dzd['platform'] == 'MYSHOP', 'customer_id'] = '兰亭集势'
dzd.loc[dzd['platform'] == 'OTHER', 'customer_id'] = '促佳'
dzd.loc[dzd['platform'] == 'DHLINK', 'customer_id'] = '敦煌'
dzd['项目名称'] = dzd.apply(
    lambda row: "兰亭集势" if row['customer_id'] == '兰亭集势' else (
        '促佳' if row['customer_id'] == '促佳' else ('敦煌' if row['customer_id'] == '敦煌' else "京东")), axis=1)

dzd = dzd[['mawb_no', 'customer_id', '项目名称']]

print('----------- 清关全 表结构 --------')
print(dq.columns.tolist())

print('----------- 落地节点 表结构 --------')
print(dll.columns.tolist())

print('----------- 主单全 表结构 --------')
print(dzd.columns.tolist())

print('----------- 断更监控 表结构 --------')
print(ddg.columns.tolist())

name_l = [['未配载监控', dp], ['出库监控', dck], ['装车监控', dzc], ['起飞监控', dqf], ['落地监控', dl], ['清关全', dq], ['主单全', dzd],
          ['交付节点', djf], ['断更监控', ddg], ['交付监控', dj], ['落地节点', dll], ['头程延误', dtc]]


def file_xlsx(name, df):
    bf = r'F:\PBI临时文件\合并分段监控\{}.xlsx'.format(name)
    writer = pd.ExcelWriter(bf)
    df.to_excel(writer, 'sheet1', index=False)
    writer.save()


for n in name_l:
    file_xlsx(n[0], n[1])

