import pandas as pd
import pymysql
import datetime

nows = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
print(nows)

# #数仓连接
con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8",
                      autocommit=True, database='logisticscore')
cur = con.cursor()
days = "BETWEEN '2021-07-04 16:00:00' and "+"'"+nows+"' "


# 落地主单
s6 = """select  
count(1) c,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des, 
date_format(date_add(tme1.event_time,interval 8 hour),'%Y-%m-%d') lddate,
date_format(date_add(tme2.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
round(timestampdiff(hour,tme2.event_time,tme1.event_time)/24,1) 落地用时
from
lg_order lgo,
track_mawb_event tme1, 
track_mawb_event tme2, 
lg_mawb lgm,
lg_bag_order_relation lbor,
lg_bag lgb 
where  
tme1.event_code in("ARIR","ABCD","ABAD","AECD","ARMA")    #到达
and tme2.event_code in("SDFO","DEPC","DEPT","LKJC")    #起飞
AND lgo.customer_id='3041600'
and tme2.event_time {}
and lgo.order_status=2
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
2,3,4,5,6,7,8
""".format(days)

#落地order
s14 = """select
count(1) c,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des,
date_format(date_add(toe.event_time,interval 8 hour),'%Y-%m-%d') lddate,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
round(timestampdiff(hour,tme.event_time,toe.event_time)/24,1) 落地用时
from lg_order lgo,
track_order_event toe,
lg_mawb lgm,
lg_bag lgb,
lg_bag_order_relation lbor,
track_mawb_event tme
where  toe.event_code in("ARIR","ABCD","ABAD","AECD","ARMA")
and tme.event_code in("SDFO","DEPC","DEPT","LKJC") 
AND lgo.customer_id='3041600'
and tme.event_time {}
and lgo.order_status=2
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
group by 2,3,4,5,6,7,8
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
round(timestampdiff(hour,tme.event_time,tbe.event_time)/24,1) 落地用时
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
and tme.event_time {}
AND lgo.customer_id='3041600'
and lgo.order_status=2
and lgo.is_deleted='n'
and tbe.is_deleted='n'
and lbor.is_deleted='n'
and lgm.is_deleted='n'
and tme.is_deleted='n'
group by
2,3,4,5,6,7,8
""".format(days)


# 清关主单
s7="""select  count(1) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d')fxdate,
date_format(date_add(tme2.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
"主单" as Dimension
from 
track_mawb_event tme, 
lg_order lgo, 
lg_mawb lgm,
lg_bag lgb,
track_mawb_event tme2, 
lg_bag_order_relation lbor 
where tme.event_code in("IRCM","PVCS","IRCN","RFIC")  
and tme2.event_code in("SDFO","DEPC","DEPT","LKJC")
and tme2.event_time {}
and lgo.order_status=2
AND lgo.customer_id='3041600'
AND tme2.mawb_id = lgo.mawb_id
and tme.mawb_id=lgo.mawb_id
and lgo.mawb_id=lgm.id 
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
AND lgo.is_deleted='n'  
and tme.is_deleted='n'  
and lgm.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n'
group by 2,3,4,5,6,7,8
and tme2.is_deleted='n'
""".format(days)

# 清关order
s8="""SELECT 
count(1) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no,
date_format(date_add(toe.event_time,interval 8 hour),'%Y-%m-%d')fxdate,
date_format(date_add(tme2.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
"order" as Dimension 
from 
lg_order lgo, 
lg_mawb lgm, 
track_order_event toe,
lg_bag lgb,
track_mawb_event tme2, 
lg_bag_order_relation lbor 
where 
lgo.id=toe.order_id  
and lgm.id=lgo.mawb_id 
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
AND toe.event_code in("IRCM","PVCS","IRCN","RFIC") 
and tme2.event_code in("SDFO","DEPC","DEPT","LKJC") 
and tme2.mawb_id=lgo.mawb_id 
AND lgo.customer_id='3041600'
and tme2.event_time {} 
and lgo.order_status=2
and lgo.is_deleted='n' 
and lgm.is_deleted='n' 
and toe.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
and tme2.is_deleted='n'
group by 
2,3,4,5,6,7
""".format(days)


# 清关bag
s9="""SELECT   
count(1) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d')fxdate,
date_format(date_add(tme2.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
"bag" as Dimension
from 
track_bag_event tbe, 
lg_bag lgb, 
lg_mawb lgm, 
lg_order lgo,
track_mawb_event tme2, 
lg_bag_order_relation lbor 
where   
lgb.id=tbe.bag_id 
and tbe.bag_id=lbor.bag_id 
and lbor.order_id=lgo.id 
and lgo.mawb_id=lgm.id 
AND tbe.event_code in("IRCM","PVCS","IRCN","RFIC") 
and tme2.event_code in("SDFO","DEPC","DEPT","LKJC") 
and tme2.mawb_id=lgo.mawb_id 
AND lgo.customer_id='3041600'
and lgo.order_status=2
and tme2.event_time {} 
and lgo.is_deleted='n' 
and tbe.is_deleted='n' 
and lgb.is_deleted='n' 
and lgm.is_deleted='n' 
and lbor.is_deleted='n' 
and tme2.is_deleted='n'
group by 2,3,4,5,6,7,8
""".format(days)

# 交付节点
s10="""SELECT 
count(1) c ,
lgo.channel_code,
lgo.des,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d')jfdate,
date_format(date_add(tme2.event_time,interval 8 hour),'%Y-%m-%d') qfdate,
round(timestampdiff(hour,tme2.event_time,tbe.event_time)/24,1) 起飞至交付用时,
round(timestampdiff(hour,tbe.event_time,now())/24,1) 交付至今,
lgb.bag_no,
lgm.mawb_no 
from lg_order lgo, 
track_bag_event tbe,
lg_bag_order_relation lbor, 
lg_bag lgb,
lg_mawb lgm,
track_mawb_event tme2 
where 
tbe.bag_id=lgb.id 
and tme2.event_code in("SDFO","DEPC","DEPT","LKJC") 
and tme2.mawb_id=lgo.mawb_id 
and lgb.id=lbor.bag_id 
and lbor.order_id=lgo.id 
and lgm.id=lgo.mawb_id   
AND tbe.event_code="JFMD" 
and lgo.order_status=2
AND lgo.customer_id='3041600'
and tme2.event_time {} 
and lgo.is_deleted='n'  
and lgb.is_deleted='n' 
and tbe.is_deleted='n' 
and lbor.is_deleted='n' 
and tme2.is_deleted='n'
group by 
2,3,4,5,6,7,8,9
""".format(days)

# 断更监控
s11="""SELECT
date_format(date_add(tme2.event_time,interval 8 hour),'%Y-%m-%d') gmtdate,
lgo.channel_code,
lgo.des,
(case when lgo.order_status=2 then '未妥投' else '其它' end ) 是否妥投,
round(timestampdiff(hour,olt.event_time,now())/24,1) 追踪信息日期差,
# DATEDIFF(CURDATE(),date_format(date_add(olt.event_time,interval 8 hour),'%Y-%m-%d')) 追踪信息日期差,
round(timestampdiff(hour,tme2.event_time,olt.event_time)/24,1) 起飞至末条总用时,
count(DISTINCT lgo.order_no) c
from
lg_order lgo inner join lg_bag_order_relation lbor on lbor.order_id=lgo.id
inner join order_last_track olt on olt.order_id=lgo.id #末条信息
inner join lg_bag lgb on lgb.id=lbor.bag_id
inner join track_bag_event tbe on tbe.bag_id=lgb.id,
track_mawb_event tme2
where  tbe.event_code="JFMD" #派送公司收货
and tme2.event_code in("SDFO","DEPC","DEPT","LKJC") 
and tme2.mawb_id=lgo.mawb_id
and tme2.event_time {}
and olt.event_time {}
AND lgo.customer_id='3041600'
and lgo.is_deleted='n'
and lgb.is_deleted='n'
and tbe.is_deleted='n'
and lbor.is_deleted='n'
and olt.is_deleted='n'
and tme2.is_deleted='n'
GROUP BY 1,2,3,4,5,6
""".format(days,days)

#主单全

s12 = """select  
distinct lgm.mawb_no
from 
lg_order lgo,
lg_mawb lgm,
track_mawb_event tme2
where  
lgo.mawb_id=lgm.id 
and tme2.event_code in("SDFO","DEPC","DEPT","LKJC") 
and tme2.mawb_id=lgo.mawb_id
and tme2.event_time {} 
AND lgo.customer_id='3041600'
and lgo.is_deleted='n' 
and lgm.is_deleted='n'  
and isnull(lgo.mawb_id)=0
""".format(days)

def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df


#落地
dl = pd.concat([execude_sql(s6), execude_sql(s13), execude_sql(s14)])
print(dl)


#清关
dq = pd.concat([execude_sql(s7), execude_sql(s8), execude_sql(s9)])
print(dq)
dq['当日']=datetime.date.today()
dq['fxdate']=pd.to_datetime(dq['fxdate'])
# print()
dq['清关距离当天']=(pd.to_datetime(dq['当日']) - dq['fxdate']).dt.days

#交付节点
djf = execude_sql(s10)
djf['c'] = djf['c'].astype('int')

#交付监控
dj = djf.groupby(['mawb_no', 'bag_no', 'channel_code', 'des', 'jfdate', 'qfdate'])['c'].sum().reset_index()

# #落地节点
dl['c']=dl['c'].astype('int')
dll = dl.groupby(['mawb_no', 'bag_no', 'channel_code', 'des', 'lddate', 'qfdate'])['c'].sum().reset_index()

#断更监控
dg=execude_sql(s11)

name_l = [ ['落地监控', dl], ['清关全', dq], ['主单全', execude_sql(s12)],['交付节点', djf], ['交付监控', dj],['断更监控',dg], ['落地节点', dll]]

def file_xlsx(name, df):
    bf = r'F:\PBI临时文件\平世分段监控\{}.xlsx'.format(name)
    writer = pd.ExcelWriter(bf)
    df.to_excel(writer, 'sheet1', index=False)
    writer.save()
for n in name_l:
    file_xlsx(n[0], n[1])