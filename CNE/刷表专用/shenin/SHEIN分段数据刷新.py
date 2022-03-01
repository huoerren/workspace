import pandas as pd
import openpyxl
import pymysql
import datetime,time
nows=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
print(nows)
import numpy as np
# #数仓连接
con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()

# days="BETWEEN '2021-03-31 16:00:00' and '"+time_yes+"'"1
days="BETWEEN '2021-04-11 16:00:00' and "+"'"+nows+"'"
days="BETWEEN '2021-05-31 16:00:00' and "+"'"+nows+"'"
# days="BETWEEN '2021-04-21 16:00:00' and "+"'"+nows+"'"

print(days)
# days="BETWEEN '2020-12-17 16:00:00' and '2021-02-15 15:59:59'"

# 未配载监控
s1="""SELECT lgb.bag_no,lgo.order_no,lgo.channel_code,date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%y-%m-%d') fddate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%y-%m-%d') gmtdate,
lgo.des,lgo.order_status 
from
lg_order lgo,
lg_bag_order_relation lbor,
lg_bag lgb
WHERE   lgo.gmt_create  {}  
AND lgo.customer_id=441331
and lgo.order_status in(1,2)
and lbor.order_id=lgo.id
and lgb.id=lbor.bag_id
and lgo.is_deleted= 'n'
and isnull(lgo.mawb_id)=1
and isnull(lgb.sealing_bag_time)=0
and lgb.is_deleted= 'n'
and lbor.is_deleted='n'
""".format(days)


# 出库监控
# s2="""select
# count(1) c,
# lgm.mawb_no,
# lgo.channel_code,
# date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%y-%m-%d') fddate,
# date_format(date_add(lgo.gmt_create,interval 8 hour),'%y-%m-%d') gmtdate,
# lgo.des
# from
# lg_order lgo,
# lg_mawb lgm,lg_bag_order_relation lbor,lg_bag lgb
# where  lgo.gmt_create {}
# and lgo.platform='WISH_ONLINE'
# AND lgo.customer_id in  (1151368,1151370,1181372,1181374)
# and lgo.order_status in (1,2,3)
# and lgo.id=lbor.order_id
# and lbor.bag_id=lgb.id
# and  lgo.mawb_id=lgm.id
# and lgo.is_deleted='n'
# and lgb.is_deleted='n'
# and lgm.is_deleted='n'
# and lbor.is_deleted='n'
# and isnull(lgo.mawb_id)=0
# and isnull(lgb.sealing_bag_time)=0
# group by lgm.mawb_no, fddate, gmtdate, lgo.channel_code, lgo.des
# """.format(days)

# 装车监控
s3="""SELECT count(1) c, 
lgo.channel_code,
lgo.des, 
date_format(date_add(lgo.gmt_create,interval 8 hour),'%y-%m-%d') gmtdate,  
date_format(date_add(tbe.event_time,interval 8 hour),'%y-%m-%d')zcdate, 
lgm.mawb_no 
from lg_order lgo,lg_bag lgb, track_bag_event tbe, lg_bag_order_relation lbor, lg_mawb lgm
where tbe.bag_id=lbor.bag_id
and lbor.order_id=lgo.id  
and lbor.bag_id=lgb.id 
and lgo.mawb_id=lgm.id 
AND tbe.event_code="DEPS"
AND lgo.gmt_create {} 
AND lgo.customer_id=441331 
and lgo.order_status=2
and lgo.is_deleted='n'
and lgb.is_deleted='n' 
and tbe.is_deleted='n'
and lbor.is_deleted='n' 
and lgm.is_deleted='n'
and isnull(lgo.mawb_id)=0   
group by lgm.mawb_no, lgo.channel_code, lgo.des, gmtdate, zcdate 
""".format(days)

# 起飞监控
s4="""select  count(1) c, 
lgm.mawb_no,
lgo.channel_code, 
date_format(date_add(lgo.gmt_create,interval 8 hour),'%y-%m-%d') gmtdate,
date_format(date_add(tbe.event_time,interval 8 hour),'%y-%m-%d') zcdate,
date_format(date_add(tme.event_time,interval 8 hour),'%y-%m-%d') qfdate, 
lgo.des 
from 
lg_order lgo, 
track_bag_event tbe, 
lg_bag_order_relation lbor, 
track_mawb_event tme,  
lg_mawb lgm 
where  
tbe.event_code="DEPS"  
AND tme.event_code="SDFO" 
and lgo.gmt_create {}
AND customer_id=441331
and lgo.order_status=2 
and lgo.id=lbor.order_id 
and lbor.bag_id=tbe.bag_id 
and tme.mawb_id=lgo.mawb_id  
and lgo.mawb_id=lgm.id 
and lgo.is_deleted='n'  
and tbe.is_deleted='n' 
and tme.is_deleted='n' 
and lgm.is_deleted='n' 
and lbor.is_deleted='n' 
group by 
lgm.mawb_no, zcdate, qfdate, gmtdate,lgo.channel_code, lgo.des
""".format(days)

# 全航班起飞
s5="""select  
count(1) c,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des, 
date_format(date_add(tme.event_time,interval 8 hour),'%y-%m-%d') qfdate 
from 
track_mawb_event tme, 
lg_order lgo,
lg_mawb lgm,
lg_bag_order_relation lbor,
lg_bag lgb 
where  
tme.event_code in("SDFO","DEPC","DEPT","LKJC") 
and lgo.gmt_create {}
AND customer_id =441331 
and lgo.order_status=2  
and tme.mawb_id=lgo.mawb_id 
and lgo.mawb_id=lgm.id 
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
AND lgo.is_deleted='n' 
and lgm.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
and tme.is_deleted='n' 
group by lgm.mawb_no,lgb.bag_no,lgo.channel_code,lgo.des, qfdate
""".format(days)

# 落地
s6="""
select  
count(1) c,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des, 
date_format(date_add(tme.event_time,interval 8 hour),'%y-%m-%d') lddate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%y-%m-%d') ywdate 
from 
track_mawb_event tme, 
lg_order lgo,
lg_mawb lgm,
lg_bag_order_relation lbor,
lg_bag lgb 
where  
tme.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
and lgo.gmt_create {} 
AND customer_id =441331
and lgo.order_status=2 
and tme.mawb_id=lgo.mawb_id 
and lgo.mawb_id=lgm.id 
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
AND lgo.is_deleted='n' 
and lgm.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
and tme.is_deleted='n' 
group by 
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des, 
lddate,
ywdate
""".format(days)

# 清关主单
s7="""select  count(1) c,
lgo.channel_code, 
lgo.des, 
lgm.mawb_no,
lgb.bag_no,  
date_format(date_add(tme.event_time,interval 8 hour),'%y-%m-%d') fxdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%y-%m-%d') ywdate,
"主单" as Dimension
from 
track_mawb_event tme, 
lg_order lgo, 
lg_mawb lgm,
lg_bag lgb,
lg_bag_order_relation lbor 
where tme.event_code in("IRCM","PVCS") 
and lgo.gmt_create {}
AND customer_id =441331
and lgo.order_status =2
and tme.mawb_id=lgo.mawb_id  
and lgo.mawb_id=lgm.id 
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
AND lgo.is_deleted='n'  
and tme.is_deleted='n'  
and lgm.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
group by lgo.channel_code, lgo.des, lgm.mawb_no,lgb.bag_no,fxdate,ywdate,Dimension
""".format(days)

# 清关order
s8="""SELECT 
count(1) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no, 
date_format(date_add(toe.event_time ,interval 8 hour),'%y-%m-%d') fxdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%y-%m-%d') ywdate,
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
AND toe.event_code="IRCN" 
AND lgo.order_status =2
and lgo.gmt_create {} 
and lgo.is_deleted='n' 
and lgm.is_deleted='n' 
and toe.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n'  
AND customer_id =441331
group by 
lgo.channel_code,
lgo.des, 
lgm.mawb_no,
lgb.bag_no, 
fxdate,
ywdate,
Dimension 
""".format(days)

# 清关bag
s9="""SELECT   
count(1) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no,
date_format(date_add(tbe.event_time,interval 8 hour),'%y-%m-%d')fxdate, 
date_format(date_add(lgo.gmt_create,interval 8 hour),'%y-%m-%d') ywdate,
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
AND tbe.event_code in ("RFIC","IRCN") 
and lgo.gmt_create {}  
AND lgo.customer_id =441331
and lgo.order_status =2
and lgo.is_deleted='n' 
and tbe.is_deleted='n' 
and lgb.is_deleted='n' 
and lgm.is_deleted='n' 
and lbor.is_deleted='n' 
group by lgo.channel_code,lgo.des, lgm.mawb_no,lgb.bag_no, fxdate,ywdate
""".format(days)

# 交付节点
s10="""SELECT 
count(1) c ,
lgo.channel_code,
lgo.des,
date_format(date_add(tbe.event_time,interval 8 hour),'%y-%m-%d')jfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%y-%m-%d') ywdate, 
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
AND lgo.customer_id =441331 
and lgo.order_status =2  
and lgo.is_deleted='n'  
and lgb.is_deleted='n' 
and tbe.is_deleted='n' 
and lbor.is_deleted='n' 
group by 
lgb.bag_no,
lgm.mawb_no,
lgo.channel_code,
lgo.des, 
jfdate,
ywdate
""".format(days)

# 主单全
s11="""select  
distinct lgm.mawb_no 
from 
lg_order lgo,
lg_mawb lgm 
where  
lgo.mawb_id=lgm.id 
and lgo.gmt_create {} 
AND lgo.customer_id =441331
and lgo.is_deleted='n' 
and lgm.is_deleted='n'  
and isnull(lgo.mawb_id)=0
""".format(days)

# 断更监控
s12="""SELECT 
date_format(date_add(lgo.gmt_create,interval 8 hour),'%y-%m-%d') gmtdate, 
lgo.channel_code,
lgo.des, 
(case when lgo.order_status=2 then '未妥投' else '其它' end ) 是否妥投, 
DATEDIFF(CURDATE(),date_format(date_add(olt.event_time,interval 8 hour),'%y-%m-%d')) 追踪信息日期差, 
count(DISTINCT lgo.order_no) c 
from 
lg_order lgo inner join lg_bag_order_relation lbor on lbor.order_id=lgo.id 
inner join order_last_track olt on olt.order_id=lgo.id #末条信息 
inner join lg_bag lgb on lgb.id=lbor.bag_id 
inner join track_bag_event tbe on tbe.bag_id=lgb.id  
where  tbe.event_code="JFMD" #派送公司收货 
and lgo.gmt_create {}
and olt.event_time {} 
AND lgo.customer_id =441331   
and lgo.is_deleted='n' 
and lgb.is_deleted='n'  
and tbe.is_deleted='n' 
and lbor.is_deleted='n' 
and olt.is_deleted='n' 
GROUP BY 1,2,3,4,5
""".format(days,days)

# 落地bag
s13="""SELECT
count(1) c ,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des,
date_format(date_add(tbe.event_time,interval 8 hour),'%y-%m-%d') lddate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%y-%m-%d') ywdate
from
track_bag_event tbe,
lg_bag_order_relation lbor,
lg_mawb lgm,
lg_order lgo,
lg_bag lgb
where
lbor.bag_id=tbe.bag_id 
and lbor.order_id=lgo.id 
and lgm.id=lgo.mawb_id 
and tbe.bag_id=lgb.id
AND tbe.event_code in("ARIR","ABCD","ABAD","AECD","ARMA")
and lgo.gmt_create {}
AND lgo.customer_id =441331
and lgo.order_status =2
and lgo.is_deleted='n'
and tbe.is_deleted='n'
and lbor.is_deleted='n'
and lgm.is_deleted='n'
group by
lgm.mawb_no,lgb.bag_no,lgo.channel_code,lgo.des,lddate,ywdate
""".format(days)

# 落地order
s14="""select  
count(1) c, 
lgm.mawb_no,
lgb.bag_no, 
lgo.channel_code, 
lgo.des,
date_format(date_add(toe.event_time,interval 8 hour),'%y-%m-%d') lddate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%y-%m-%d') ywdate
 
from lg_order lgo, 
track_order_event toe,
lg_mawb lgm,
lg_bag lgb,
lg_bag_order_relation lbor 
where  toe.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
and lgo.gmt_create {}  
AND lgo.customer_id =441331
and lgo.order_status =2 
and lgo.id=toe.order_id 
and lgo.mawb_id=lgm.id 
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
and lgo.is_deleted='n' 
and toe.is_deleted='n' 
and lgm.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n'   
group by lgm.mawb_no,lgb.bag_no, lddate, lgo.channel_code, lgo.des,ywdate
""".format(days)

def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df

dp=execude_sql(s1)
dp.loc[dp['order_status']==1,'order_status']='已发送'
dp.loc[dp['order_status']==2,'order_status']='转运中'
# d1['order_status'].unique()

dq=pd.concat([execude_sql(s7),execude_sql(s8),execude_sql(s9)])
print(dq)

dl=pd.concat([execude_sql(s6),execude_sql(s13),execude_sql(s14)])
print(dl)
name_l=[['未配载监控',dp],['装车监控',execude_sql(s3)],['起飞监控',execude_sql(s4)],
        ['全航班起飞',execude_sql(s5)],['落地',dl],['清关监控',dq],['交付节点',execude_sql(s10)],
        ['主单全',execude_sql(s11)],['断更监控',execude_sql(s12)]]

def file_xlsx(name,df):
    bf =r'F:\PBI临时文件\shein分段监控\{}.xlsx'.format(name)
    writer = pd.ExcelWriter(bf)
    df.to_excel(writer,'sheet1',index=False)
    writer.save()
for n in name_l:
    file_xlsx(n[0],n[1])

# bf =r'F:\PBI临时文件\shein分段监控\清关监控.xlsx'
# writer = pd.ExcelWriter(bf)
# dq.to_excel(writer,'sheet1',index=False)
# writer.save()

