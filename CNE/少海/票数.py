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

                 #优先
days1 = "BETWEEN '2021-11-28 16:00:00' and '2021-12-05 16:00:00' "

                #特惠
days2 = "BETWEEN '2021-11-21 16:00:00' and '2021-11-28 16:00:00' "




def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df

#总票数
s1 = """
SELECT
lgo.channel_code,
lgo.des,
count(*) 总票数
FROM
lg_order lgo
WHERE
lgo.gmt_create {}

and channel_code = "CNE全球优先"
AND customer_id in  (3282094) 
GROUP BY 1,2
""".format(days1)


print('------------------- s1 : --------------------')
print(s1)


#已妥投票数
s2 = """
SELECT
lgo.channel_code,
lgo.des,
count(*) 妥投
FROM
lg_order lgo
WHERE
lgo.gmt_create {}

and channel_code = "CNE全球优先"
AND customer_id in  (3282094) 
AND order_status=3
GROUP BY 1,2
""".format(days1)

#退件票数
s3 = """
SELECT
lgo.channel_code,
lgo.des,
count(*) 退件
FROM
lg_order lgo
WHERE
lgo.gmt_create {}

and channel_code = "CNE全球优先"
AND customer_id in  (3282094) 
AND order_status=8
GROUP BY 1,2
""".format(days1)



#总票数
s5 = """
SELECT
lgo.channel_code,
lgo.des,
count(*) 总票数
FROM
lg_order lgo
WHERE
lgo.gmt_create {}

and channel_code <> "CNE全球优先"
AND customer_id in  (3282094) 
GROUP BY 1,2
""".format(days2)
#已妥投票数
s6 = """
SELECT
lgo.channel_code,
lgo.des,
count(*) 妥投
FROM
lg_order lgo
WHERE
lgo.gmt_create {}

and channel_code <> "CNE全球优先"
AND customer_id in  (3282094) 
AND order_status=3
GROUP BY 1,2
""".format(days2)
#退件票数
s7 = """
SELECT
lgo.channel_code,
lgo.des,
count(*) 退件
FROM
lg_order lgo
WHERE
lgo.gmt_create {}

and channel_code <> "CNE全球优先"
AND customer_id in  (3282094) 
AND order_status=8
GROUP BY 1,2
""".format(days2)






zps = execude_sql(s1)
tt = execude_sql(s2)
tj = execude_sql(s3)

zps1 = execude_sql(s5)
tt1 = execude_sql(s6)
tj1 = execude_sql(s7)

d1 = pd.merge(zps,tt,on=["channel_code","des"], how='left')
d2 = pd.merge(d1,tj,on=["channel_code","des"], how='left')

d3 = pd.merge(zps1,tt1,on=["channel_code","des"], how='left')
d4 = pd.merge(d3,tj1,on=["channel_code","des"], how='left')

d2["妥投率"] = d2["妥投"]/d2["总票数"]
d4["妥投率"] = d4["妥投"]/d4["总票数"]

d5 = d2.merge(d4,on=['channel_code','des',"总票数","妥投","退件","妥投率"], how='outer')
list1 = []
for index, row in d5.iterrows():
    channel_code = row["channel_code"]
    des = row["des"]
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
        """.format(days1,channel_code,des,days1,channel_code,des)
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
                """.format(days2, channel_code, des, days2, channel_code, des)
    fw = execude_sql(s4)
    dic = {}
    dic["channel_code"] = channel_code
    dic["des"] = des
    try:
        dic["时效"] = float(fw.iloc[0,2])
    except:
        dic["时效"] = ""
    list1.append(dic)
    print(dic)
print(list1)
for i in list1:
    d5.loc[(d5.channel_code == i["channel_code"]) & (d5.des == i["des"]), '90分位时效'] = i["时效"]
print(d5)
d5.to_excel("./时效.xlsx")
