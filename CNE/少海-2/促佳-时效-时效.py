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

days1 = "BETWEEN '2021-12-05 16:00:00' and '2021-12-12 16:00:00' "


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


#总票数
s1 = """
SELECT
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
lgo.channel_code,
lgo.des,
count(*) 总票数
FROM
lg_order lgo
WHERE
lgo.gmt_create {}
AND lgo.customer_id in  (3282094) 
GROUP BY 1,2,3
""".format(days1)
#已妥投票数
s2 = """
SELECT
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
lgo.channel_code,
lgo.des,
count(*) 妥投
FROM
lg_order lgo
WHERE
lgo.gmt_create {}
AND lgo.customer_id in  (3282094) 
AND order_status=3
GROUP BY 1,2,3
""".format(days1)
#退件票数
s3 = """
SELECT
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') gmtdate,
lgo.channel_code,
lgo.des,
count(*) 退件
FROM
lg_order lgo
WHERE
lgo.gmt_create {}
and platform='WISH_ONLINE'
AND lgo.customer_id in  (3282094) 
AND order_status=8
GROUP BY 1,2,3
""".format(days1)






zps = execude_sql(s1)
tt = execude_sql(s2)
tj = execude_sql(s3)

d1 = pd.merge(zps,tt,on=["channel_code","des","gmtdate"], how='left')
d2 = pd.merge(d1,tj,on=["channel_code","des","gmtdate"], how='left')
d2['gmtdate'] = pd.to_datetime(d2['gmtdate'])
d2 = pd.merge(d2, dw[['gmtdate', '周序数', '周期']], on=['gmtdate'], how='left')
d3 = d2.groupby(['channel_code','des','周序数','周期'])['总票数'].sum().reset_index()
d4 = d2.groupby(['channel_code','des','周序数','周期'])['妥投'].sum().reset_index()
d5 = d2.groupby(['channel_code','des','周序数','周期'])['退件'].sum().reset_index()
d6= pd.merge(d3,d4,on=['channel_code','des','周序数','周期'], how='left')
d7= pd.merge(d6,d5,on=['channel_code','des','周序数','周期'], how='left')
d7["妥投率"] = d7["妥投"]/d7["总票数"]



list1 = []
for index, row in d7.iterrows():
    channel_code = row["channel_code"]
    gmtdate = str(row["周期"])
    yue = gmtdate.split("-")[0].split(".")[0]
    ri = gmtdate.split("-")[0].split(".")[1]
    ri = int(ri)-1
    strat_time = "2021-"+yue+"-"+str(ri)+" 16:00:00"

    yue1 = gmtdate.split("-")[1].split(".")[0]
    ri1 = gmtdate.split("-")[1].split(".")[1]
    end_time = "2021-" + yue1 + "-" + ri1 + " 16:00:00"
    time1 = "BETWEEN "+"'"+strat_time+"'"+" and "+"'"+end_time+"'"
    print(time1)
    print(channel_code)
    des = row["des"]
    print(des)
    if channel_code =="CNE全球优先":
        s4 = """
            with 
        HH AS
        (
        select
        count(1) as c
        from
             lg_order lgo
        where
        lgo.gmt_create {}
        AND lgo.channel_code = "{}"
        AND lgo.customer_id in  (3282094)  
        AND lgo.des = "{}"
        and lgo.order_status=3
        AND lgo.is_deleted='n'
        )
        select 渠道,国家 des,min(delivery) AS 90分位妥投时效
        from(
        select 
        channel_code as 渠道,
        des as 国家,
        delivery,
        sum(c) OVER(PARTITION BY channel_code,des ORDER BY delivery)/(select c from HH) as per
        from 
        (
        select 
        channel_code,
        des,
        round(TIMESTAMPDIFF(hour,lgo.gmt_create,lgo.delivery_date)/24,1) as delivery,
        count(*) as c
        from 
            lg_order lgo
        where
            lgo.gmt_create {}
            AND lgo.customer_id in  (3282094)  
            and lgo.channel_code = "{}"
            and lgo.order_status=3
            and lgo.des = "{}"
            and  lgo.is_deleted = 'n'
        GROUP BY 1,2,3 ) as t
        ) as t1
        where t1.per>=0.90
        GROUP BY 1,2
        """.format(time1,channel_code,des,time1,channel_code,des)
    else:
        s4 = """
                    with 
                HH AS
                (
                select
                count(1) as c
                from
                     lg_order lgo
                where
                lgo.gmt_create {}
                AND lgo.channel_code = "{}"
                AND lgo.customer_id in  (3282094)  
                AND lgo.des = "{}"
                and lgo.order_status=3
                AND lgo.is_deleted='n'
                )
                select 渠道,国家 des,min(delivery) AS 90分位妥投时效
                from(
                select 
                channel_code as 渠道,
                des as 国家,
                delivery,
                sum(c) OVER(PARTITION BY channel_code,des ORDER BY delivery)/(select c from HH) as per
                from 
                (
                select 
                channel_code,
                des,
                round(TIMESTAMPDIFF(hour,lgo.gmt_create,lgo.delivery_date)/24,1) as delivery,
                count(*) as c
                from 
                    lg_order lgo
                where
                    lgo.gmt_create {}
                    AND lgo.customer_id in  (3282094)  
                    and lgo.channel_code = "{}"
                    and lgo.order_status=3
                    and lgo.des = "{}"
                    and  lgo.is_deleted = 'n'
                GROUP BY 1,2,3 ) as t
                ) as t1
                where t1.per>=0.90
                GROUP BY 1,2
                """.format(time1, channel_code, des, time1, channel_code, des)
    fw = execude_sql(s4)
    dic = {}
    dic["channel_code"] = channel_code
    dic["des"] = des
    dic["周序数"] = int(row["周序数"])
    dic["周期"] = str(row["周期"])
    try:
        dic["时效"] = float(fw.iloc[0,2])
    except:
        dic["时效"] = ""
    list1.append(dic)
    print(dic)
print(list1)
for i in list1:
    d7.loc[(d7.channel_code == i["channel_code"]) & (d7.des == i["des"]) & (d7.周序数 == i["周序数"]) & (d7.周期 == i["周期"]) , '90分位时效'] = i["时效"]
print(d7)
d7.loc[(d7.channel_code == "CNE全球优先") & (d7.des == "ES") , 'KPI/期望值'] = "10"
d7.loc[(d7.channel_code == "CNE全球优先") & (d7.des == "FR") , 'KPI/期望值'] = "10"
d7.loc[(d7.channel_code == "CNE全球优先") & (d7.des == "CZ") , 'KPI/期望值'] = "12"
d7.loc[(d7.channel_code == "CNE全球优先") & (d7.des == "GB") , 'KPI/期望值'] = "7"
d7.loc[(d7.channel_code == "CNE华东欧电") & (d7.des == "RO") , 'KPI/期望值'] = ""
d7.loc[(d7.channel_code == "CNE全球特惠") & (d7.des == "PL") , 'KPI/期望值'] = "14"
d7.loc[(d7.channel_code == "CNE全球特惠") & (d7.des == "GR") , 'KPI/期望值'] = "16"
d7.loc[(d7.channel_code == "CNE全球特惠") & (d7.des == "SK") , 'KPI/期望值'] = "16"
d7.loc[(d7.channel_code == "CNE全球特惠") & (d7.des == "SE") , 'KPI/期望值'] = "16"
d7.loc[(d7.channel_code == "CNE全球特惠") & (d7.des == "HU") , 'KPI/期望值'] = "16"
d7.loc[(d7.channel_code == "CNE全球特惠") & (d7.des == "RO") , 'KPI/期望值'] = "22"
d7.loc[(d7.channel_code == "CNE全球特惠") & (d7.des == "LT") , 'KPI/期望值'] = "20"
d7.to_excel("./时效.xlsx")
