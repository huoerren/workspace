import pandas as pd
import pymysql
import datetime
from functools import reduce
import json

nows = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
print(nows)

# #数仓连接
con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8",
                      autocommit=True, database='logisticscore')
cur = con.cursor()


days = "BETWEEN '2021-10-31 16:00:00' and '2021-11-31 16:00:00' "
print(days)

def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df
dw = pd.DataFrame(pd.date_range(start='20210101', end=nows, periods=None, freq='D'), columns=['下单日期'])
dw['gmtdate'] = pd.to_datetime(dw['下单日期'])
dw['周序数'] = dw['gmtdate'].dt.isocalendar().week
dw['周序数'] = dw['周序数']+1
dw['moon'] = dw['gmtdate'].dt.month.astype('str')
dw['day'] = dw['gmtdate'].dt.day.astype('str')
dw['日期'] = dw['moon'] + '.' + dw['day']
dw_min = dw[['周序数', '日期']].drop_duplicates(['周序数'], keep='first')
dw_max = dw[['周序数', '日期']].drop_duplicates(['周序数'], keep='last')
dwm = pd.merge(dw_min, dw_max, on=['周序数'], how='outer')
dwm['周期'] = dwm['日期_x'] + '-' + dwm['日期_y']
print(dwm)
dw = pd.merge(dw, dwm, on=['周序数'], how='left')
# dw['周期']=dwm['周序数'].map(dict(zip(dwm['周序数'],dwm['周期'])))
# # dw=dw.set_index(['周序数','dayofweek']).stack().unstack(['dayofweek',-1])
print(dw)


#计算90分位时效时间
def jisuan(d1,names,dw):
    if names=="落地-清关":
        print(1111)
    d2 = d1.sort_values(by=['channel_code', 'des', '用时'], axis=0, ascending=[True, True, True],inplace=False)
    print(d2)
    d2['gmtdate'] = pd.to_datetime(d2['gmtdate'])
    d2 = pd.merge(d2, dw[['gmtdate', '周序数', '周期', 'moon']], on=['gmtdate'], how='left')
    list2 = []
    try:
        d2 = d2.groupby(['channel_code', 'des', '用时','周序数','周期'])['c'].sum().reset_index()
        nums = d2.groupby(['channel_code', 'des','周序数','周期'])['c'].sum().reset_index()
    except:
        d2 = d2.groupby(['channel_code', 'des','用时','周序数','周期'])['c_x'].sum().reset_index()
        nums = d2.groupby(['channel_code', 'des','周序数','周期'])['c_x'].sum().reset_index()
    for index, row in nums.iterrows():
        num = 0
        dis = {}
        try:
            fw = int(row["c"] * 0.9)+1
        except:
            fw = int(row["c_x"] * 0.9)+1
        for index1, row1 in d2.iterrows():
            if row["des"] == row1["des"] and row["channel_code"] == row1["channel_code"] and row["周序数"] == row1["周序数"]:
                try:
                    num += int(row1["c"])
                except:
                    num += int(row1["c_x"])
                if num>=fw:
                    dis["90分位时效"] = float(row1["用时"])
                    try:
                        dis["有节点票数"] = int(row["c"])
                    except:
                        dis["有节点票数"] = int(row["c_x"])
                    dis["des"] = row["des"]
                    dis["节点名称"] = names
                    dis["周期"] = row['周期']
                    dis["周序数"] = row["周序数"]
                    dis["上期数据"] = ""
                    dis["channel_code"] = row["channel_code"]
                    list2.append(dis)
                    break
                else:
                    continue
            else:
                continue

    d2 = d1.sort_values(by=['channel_code', 'des', '用时'], axis=0, ascending=[True, True, True], inplace=False)
    print(d2)
    d2['gmtdate'] = pd.to_datetime(d2['gmtdate'])
    d2 = pd.merge(d2, dw[['gmtdate', '周序数', '周期', 'moon']], on=['gmtdate'], how='left')
    try:
        d3 = d2.groupby(['channel_code', 'des', '用时','moon'])['c'].sum().reset_index()
        nums1 = d3.groupby(['channel_code', 'des','moon'])['c'].sum().reset_index()
    except:
        d3 = d2.groupby(['channel_code', 'des','用时','moon'])['c_x'].sum().reset_index()
        nums1 = d3.groupby(['channel_code', 'des','moon'])['c_x'].sum().reset_index()

    for index, row in nums1.iterrows():
        num = 0
        dis = {}
        try:
            fw = int(row["c"] * 0.9)+1
        except:
            fw = int(row["c_x"] * 0.9)+1
        for index1, row1 in d3.iterrows():
            if row["des"] == row1["des"] and row["channel_code"] == row1["channel_code"] and row["moon"] == row1["moon"]:
                try:
                    num += int(row1["c"])
                except:
                    num += int(row1["c_x"])
                if num>=fw:
                    dis["90分位时效"] = float(row1["用时"])
                    try:
                        dis["有节点票数"] = int(row["c"])
                    except:
                        dis["有节点票数"] = int(row["c_x"])
                    dis["des"] = row["des"]
                    dis["节点名称"] = names
                    dis["周序数"] = row['moon']
                    dis["周期"] = row['moon']
                    dis["上期数据"] = ""
                    dis["channel_code"] = row["channel_code"]
                    list2.append(dis)
                    break
                else:
                    continue
            else:
                continue
    lists = list2
    for g in list2:
        for f in lists:
            try:
                if f["des"] == g["des"] and f["channel_code"]==g["channel_code"] and str(f["周序数"])==str(int(g["周序数"])-1) and f["节点名称"]==g["节点名称"]:
                    sum = 0
                    if g["节点名称"]=="入库-出库" and g["channel_code"]=="CNE全球优先" and g["des"]!="CZ":
                        g["上期数据"] = 0.5
                        sum = 1
                    elif g["节点名称"]=="入库-出库" and g["channel_code"]!="CNE全球优先":
                        g["上期数据"] = 1
                        sum = 1
                    elif g["节点名称"] == "入库-出库" and g["channel_code"] == "CNE全球优先" and g["des"]=="CZ":
                        g["上期数据"] = 1
                        sum = 1
                    if g["节点名称"]=="出库-装车" and g["channel_code"]=="CNE全球优先":
                        g["上期数据"] = 1
                        sum = 1
                    elif g["节点名称"]=="出库-装车" and g["channel_code"]!="CNE全球优先":
                        g["上期数据"] = 2.5
                        sum = 1
                    if g["节点名称"] == "装车-起飞":
                        g["上期数据"] = 1.5
                        sum = 1
                    if sum==0:
                        g["上期数据"] = f["90分位时效"]
                    else:
                        break
                else:
                    continue
            except Exception as e:
                print(e)
                continue
    print(list2)
    return list2
#入库-出库
s2 = """select 
count(1) c,
lgm.mawb_no,
lgo.channel_code,
date_format(date_add(lgb.sealing_bag_time,interval 8 hour),'%Y-%m-%d') fddate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
round(TIMESTAMPDIFF(hour,lgo.gmt_create,lgb.sealing_bag_time)/24,1) 用时,
lgo.des
from
lg_order lgo,
lg_mawb lgm,lg_bag_order_relation lbor,lg_bag lgb
where  lgo.gmt_create {}
and   lgo.customer_id in  (3282094)
and lgo.order_status in(1,2,3)
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
and  lgo.mawb_id=lgm.id 
and lgo.is_deleted='n' 
and lgb.is_deleted='n'
and lgm.is_deleted='n' 
and lbor.is_deleted='n' 
and isnull(lgo.mawb_id)=0 
and isnull(lgb.sealing_bag_time)=0  
group by lgm.mawb_no, fddate, gmtdate, lgo.channel_code, lgo.des,用时
""".format(days)

#出库--装车
s3 = """SELECT 
count(1) c, 
lgo.channel_code,
lgo.des, 
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
round(timestampdiff(hour,lgb.sealing_bag_time,tbe.event_time)/24,1) 用时,
lgm.mawb_no 
from lg_order lgo,lg_bag lgb, track_bag_event tbe, lg_bag_order_relation lbor, lg_mawb lgm
where tbe.bag_id=lbor.bag_id
and lbor.order_id=lgo.id  
and lbor.bag_id=lgb.id 
and lgo.mawb_id=lgm.id 
AND tbe.event_code="DEPS"
AND lgo.gmt_create {}
and   lgo.customer_id in (3282094) 
and lgo.order_status in(1,2,3)
and lgo.is_deleted='n'
and lgb.is_deleted='n' 
and tbe.is_deleted='n'
and lbor.is_deleted='n' 
and lgm.is_deleted='n'
and isnull(lgo.mawb_id)=0 
and isnull(lgb.sealing_bag_time)=0  
group by 2,3,4,5,6
""".format(days)

#装车--起飞
s4 = """select  count(1) c, 
lgm.mawb_no,
lgo.channel_code, 
round(timestampdiff(hour,tbe.event_time,tme.event_time)/24,1) 用时,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
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
and   customer_id in  (3282094) 
and lgo.order_status in(1,2,3)
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
lgm.mawb_no, lgo.channel_code, lgo.des,用时,gmtdate
""".format(days)


#起飞--落地

s6 = """
select  
count(1) c,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des, 
date_format(date_add(tme1.event_time,interval 8 hour),'%Y-%m-%d %H:%I:%S') lddate,
date_format(date_add(tme2.event_time,interval 8 hour),'%Y-%m-%d %H:%I:%S') qfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
round(timestampdiff(hour,tme2.event_time,tme1.event_time)/24,1) 用时
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
AND lgo.customer_id in  (3282094) 
and lgo.order_status in(1,2,3) 
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
2,3,4,5,6,7,8,9
""".format(days)

# 落地bag
s13 = """SELECT
count(1) c ,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d %H:%I:%S') lddate,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d %H:%I:%S') qfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
round(timestampdiff(hour,tme.event_time,tbe.event_time)/24,1) 用时
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
 
AND lgo.customer_id in (3282094)
and lgo.order_status in(1,2,3)
and lgo.is_deleted='n'
and tbe.is_deleted='n'
and lbor.is_deleted='n'
and lgm.is_deleted='n'
and tme.is_deleted='n'
group by
2,3,4,5,6,7,8,9
""".format(days)

# 落地order
s14 = """select
count(1) c,
lgm.mawb_no,
lgb.bag_no,
lgo.channel_code,
lgo.des,
date_format(date_add(toe.event_time,interval 8 hour),'%Y-%m-%d %H:%I:%S') lddate,
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d %H:%I:%S') qfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
round(timestampdiff(hour,tme.event_time,toe.event_time)/24,1) 用时
from lg_order lgo,
track_order_event toe,
lg_mawb lgm,
lg_bag lgb,
lg_bag_order_relation lbor,
track_mawb_event tme
where  toe.event_code in("ARIR","ABCD","ABAD","AECD","ARMA")
and tme.event_code in("SDFO","DEPC","DEPT","LKJC") 
and lgo.gmt_create {}
AND lgo.customer_id in  (3282094)
and lgo.order_status in(1,2,3)
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
group by 2,3,4,5,6,7,8,9
""".format(days)
#落地监控
dl = pd.concat([execude_sql(s6), execude_sql(s13), execude_sql(s14)])
dl['c']=dl['c'].astype('int')
#落地节点
dll = dl.groupby(['mawb_no', 'bag_no', 'channel_code', 'des', 'lddate', 'gmtdate'])['c'].sum().reset_index()

#落地--清关

#清关主单
s7="""select  count(1) c,
lgo.channel_code, 
lgo.des, 
lgm.mawb_no,
lgb.bag_no,  
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d %H:%I:%S') fxdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
"主单" as Dimension
from 
track_mawb_event tme, 
lg_order lgo, 
lg_mawb lgm,
lg_bag lgb,
lg_bag_order_relation lbor 
where tme.event_code in("IRCM","PVCS","IRCN","RFIC")  
and lgo.gmt_create {}
 
AND customer_id in  (3282094) 
and lgo.order_status in(2,3) 
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
""".format(days)

# 清关order
s8="""SELECT 
count(1) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no, 
date_format(date_add(toe.event_time ,interval 8 hour),'%Y-%m-%d %H:%I:%S') fxdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
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
AND lgo.order_status in(2,3) 
and lgo.gmt_create {} 
and lgo.is_deleted='n' 
and lgm.is_deleted='n' 
and toe.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
 
AND customer_id in  (3282094) 
group by 
2,3,4,5,6,7,8
""".format(days)

# 清关bag
s9="""SELECT   
count(1) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d %H:%I:%S')fxdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
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
 
AND lgo.customer_id in (3282094) 
and lgo.order_status in(2,3) 
and lgo.is_deleted='n' 
and tbe.is_deleted='n' 
and lgb.is_deleted='n' 
and lgm.is_deleted='n' 
and lbor.is_deleted='n' 
group by 2,3,4,5,6,7,8
""".format(days)

dq = pd.concat([execude_sql(s7), execude_sql(s8), execude_sql(s9)])
dq['fxdate']=pd.to_datetime(dq['fxdate'])
dq['c']=dq['c'].astype('int')

qg = pd.merge(dll, dq, on=["mawb_no", "bag_no", "channel_code", "des","gmtdate"], how='left')
print(qg)
qg['lddate']=pd.to_datetime(qg['lddate'])
qg = qg.drop(columns=['c_y'])
qg.rename(columns={'c_x':'c'})
qg["用时"] = (qg["fxdate"]-qg["lddate"]).astype('timedelta64[s]')
qg["用时"] = round(qg["用时"]/86400,2)
qg = qg.drop(index=qg[qg['用时'].isnull()].index[0])
print(qg)

#清关--交付

s10="""SELECT 
count(1) c ,
lgo.channel_code,
lgo.des,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d %H:%I:%S')jfdate,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate, 
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
AND tbe.event_code in("JFMD","AAPS") 
and lgo.gmt_create {} 
 
AND lgo.customer_id in (3282094) 
and lgo.order_status in(2,3)  
and lgo.is_deleted='n'  
and lgb.is_deleted='n' 
and tbe.is_deleted='n' 
and lbor.is_deleted='n' 
group by 
2,3,4,5,6,7
""".format(days)
#交付节点
djf = execude_sql(s10)
djf['c'] = djf['c'].astype('int')
#交付监控
dj = djf.groupby(['mawb_no', 'bag_no', 'channel_code', 'des', 'jfdate', 'gmtdate'])['c'].sum().reset_index()
jfjk = pd.merge(qg, dj, on=["bag_no", "channel_code", "des","gmtdate"], how='left')

jfjk = jfjk.drop(columns=['c_x','lddate','用时',"mawb_no_y"])
jfjk['jfdate']=pd.to_datetime(jfjk['jfdate'])
jfjk["用时"] = (jfjk["jfdate"]-jfjk["fxdate"]).astype('timedelta64[s]')
jfjk["用时"] = round(jfjk["用时"]/86400,2)
jfjk = jfjk.drop(index=jfjk[jfjk['用时'].isnull()].index[0])
#交付
print(jfjk)


#交付--妥投

s17 = """SELECT 
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate, 
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d')jfdate,
lgo.channel_code,
lgo.des, 
round(timestampdiff(hour,tbe.event_time,lgo.delivery_date)/24,1) 用时,
count(DISTINCT lgo.order_no) c 
from 
lg_order lgo inner join lg_bag_order_relation lbor on lbor.order_id=lgo.id 
inner join lg_bag lgb on lgb.id=lbor.bag_id 
inner join track_bag_event tbe on tbe.bag_id=lgb.id  
where  
tbe.event_code="JFMD" #派送公司收货 
and lgo.gmt_create {}
 
AND lgo.customer_id in (3282094)   
and lgo.order_status =3
and lgo.is_deleted='n' 
and lgb.is_deleted='n'  
and tbe.is_deleted='n' 
and lbor.is_deleted='n' 
GROUP BY 1,2,3,4,5
""".format(days)

#总单量

s18 = """SELECT
lgo.channel_code,
lgo.des,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
count(*) 单量
FROM
lg_order lgo
WHERE
lgo.gmt_create {}
 
AND customer_id in  (3282094) 
GROUP BY 1,2,3
""".format(days)



dck = execude_sql(s2)
dzc = execude_sql(s3)
dqf = execude_sql(s4)
dt=execude_sql(s17)
danliang = execude_sql(s18)
danliang['gmtdate'] = pd.to_datetime(danliang['gmtdate'])
danliang = pd.merge(danliang, dw[['gmtdate', '周序数', '周期', 'moon']], on=['gmtdate'], how='left')
danliang = danliang.groupby(['channel_code', 'des','周序数','周期', 'moon'])['单量'].sum().reset_index()
danliang2 = danliang.groupby(['channel_code', 'des', 'moon'])['单量'].sum().reset_index()
# dck  入库-出库
# dzc   出库--装车
# dqf   装车--起飞
# dl    起飞--落地
# qg  落地--清关
# jfjk  清关--交付
# dt   交付--妥投
dck = jisuan(dck,"入库-出库",dw)
dzc = jisuan(dzc,"出库-装车",dw)
dqf = jisuan(dqf,"装车-起飞",dw)
dl = jisuan(dl,"起飞-落地",dw)
qg = jisuan(qg,"落地-清关",dw)
jfjk = jisuan(jfjk,"清关-交付",dw)
dt = jisuan(dt,"交付-妥投",dw)
lists = dck+dzc+dqf+dl+qg+jfjk+dt
list5 = []
for i in lists:
    die  ={}
    die["channel_code"]= i["channel_code"]
    die["des"] = i["des"]
    die["周序数"] = i["周序数"]
    die["周期"] = i["周期"]
    list5.append(die)
unique_list = reduce(lambda x, y: y in x and x or x + [y], list5, [])
print(unique_list)
list6 = []
for g in unique_list:
    dip = {}
    for f in lists:
        if g["channel_code"]==f["channel_code"] and g["des"]==f["des"] and f["节点名称"]=="入库-出库" and g["周序数"]==f["周序数"]:
            dip["channel_code"] = g["channel_code"]
            dip["des"] = g["des"]
            dip["周序数"] = g["周序数"]
            dip["周期"] = g["周期"]
            dip["总单量"] = ""
            dip["入库-出库有节点票数"] = f["有节点票数"]
            dip[f["节点名称"]] = f["90分位时效"]
            dip["入库上期数据"] = f["上期数据"]
        elif g["channel_code"]==f["channel_code"] and g["des"]==f["des"] and f["节点名称"]=="出库-装车" and g["周序数"]==f["周序数"]:
            dip["出库-装车有节点票数"] = f["有节点票数"]
            dip[f["节点名称"]] = f["90分位时效"]
            dip["出库上期数据"] = f["上期数据"]
        elif g["channel_code"]==f["channel_code"] and g["des"]==f["des"] and f["节点名称"]=="装车-起飞" and g["周序数"]==f["周序数"]:
            dip["装车-起飞有节点票数"] = f["有节点票数"]
            dip[f["节点名称"]] = f["90分位时效"]
            dip["装车上期数据"] = f["上期数据"]
        elif g["channel_code"]==f["channel_code"] and g["des"]==f["des"] and f["节点名称"]=="起飞-落地" and g["周序数"]==f["周序数"]:
            dip["起飞-落地有节点票数"] = f["有节点票数"]
            dip[f["节点名称"]] = f["90分位时效"]
            dip["起飞上期数据"] = f["上期数据"]
        elif g["channel_code"]==f["channel_code"] and g["des"]==f["des"] and f["节点名称"]=="落地-清关" and g["周序数"]==f["周序数"]:
            dip["落地-清关有节点票数"] = f["有节点票数"]
            dip[f["节点名称"]] = f["90分位时效"]
            dip["落地上期数据"] = f["上期数据"]
        elif g["channel_code"]==f["channel_code"] and g["des"]==f["des"] and f["节点名称"]=="清关-交付" and g["周序数"]==f["周序数"]:
            dip["清关-交付有节点票数"] = f["有节点票数"]
            dip[f["节点名称"]] = f["90分位时效"]
            dip["清关上期数据"] = f["上期数据"]
        elif g["channel_code"]==f["channel_code"] and g["des"]==f["des"] and f["节点名称"]=="交付-妥投" and g["周序数"]==f["周序数"]:
            dip["交付-妥投有节点票数"] = f["有节点票数"]
            dip[f["节点名称"]] = f["90分位时效"]
            dip["交付上期数据"] = f["上期数据"]
        else:
            continue
    list6.append(dip)

for index,row in danliang.iterrows():
    for t in list6:
        if row["channel_code"]==t["channel_code"] and row["des"]==t["des"] and row["周序数"]==t["周序数"]:
            t["总单量"] = row["单量"]
            break
        else:
            continue
for index,row in danliang2.iterrows():
    for t in list6:
        if row["channel_code"]==t["channel_code"] and row["des"]==t["des"] and row["moon"]==t["周序数"]:
            t["总单量"] = row["单量"]
            break
        else:
            continue
dv = pd.DataFrame(list6)
dv.to_excel(r'./周维度.xlsx',index=False)
# with open('./周维度.json', 'a', encoding="UTF-8")as f:
#     for data in list6:
#         result = json.dumps(data, ensure_ascii=False) + ',\n'
#         f.write(result)
#
# import json
# import pandas as pd
# def json2csv(file_name:str):
#     """将json格式文件转换成csv文件"""
#     with open(file_name, encoding="utf8") as f:
#         datas = [json.loads(line[:-2]) for line in f]
#     pd.DataFrame(datas).to_csv(file_name.replace("json","csv"),encoding="gbk",index=False)
# json2csv("./周维度.json")