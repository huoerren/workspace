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

days = "BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00' "
print(days)


def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df

df = pd.read_excel('./wish.xlsx')

def run(d2,des,qd,kh,jd):
    min = d2[d2["用时"]>=0]
    min = min.min()
    max = d2["用时"].max()
    nums = d2["票数"].sum()
    d2['sums'] = d2["用时"]*d2["票数"]
    nums1 = d2["sums"].sum()
    avge = nums1/nums
    print(max)
    print(min)
    print(avge)
    dic = {}
    dic["渠道"] = qd
    dic["des"] = des
    dic["客户"] = kh
    dic["阶段名称"] = jd
    dic["min"] = float(min["用时"])
    dic["max"] = float(max)
    dic['avge'] = float(avge)
    return dic


lists  = []
for index,row in df.iterrows():
    qd = row["渠道"]
    des = row["国家"]
    kh = row["客户"]
    if kh=="wish-4PL":
        s1 = """
        select 
        round(TIMESTAMPDIFF(second,lgo.gmt_create,lgb.sealing_bag_time)/3600,1) 用时,
        count(*) 票数
        from
        lg_order lgo,
        lg_mawb lgm,lg_bag_order_relation lbor,lg_bag lgb
        where  lgo.gmt_create {}
        and lgo.platform='WISH_ONLINE'
        AND lgo.customer_id in  (1151368,1151370,1181372,1181374)
        and  lgo.channel_code = '{}'
        and  lgo.des = '{}'
        and lgo.id=lbor.order_id 
        and lbor.bag_id=lgb.id 
        and  lgo.mawb_id=lgm.id 
        AND lgo.order_status =3 
        and lgo.is_deleted='n' 
        and lgb.is_deleted='n'
        and lgm.is_deleted='n' 
        and lbor.is_deleted='n' 
        and isnull(lgo.mawb_id)=0 
        and isnull(lgb.sealing_bag_time)=0  
        group by 1
        """.format(days,qd,des)

        s2 = """
        SELECT 
        count(1) 票数, 
        round(timestampdiff(second,lgb.sealing_bag_time,tbe.event_time)/3600,1) 用时
        from lg_order lgo,lg_bag lgb, track_bag_event tbe, lg_bag_order_relation lbor, lg_mawb lgm
        where tbe.bag_id=lbor.bag_id
        and lbor.order_id=lgo.id  
        and lbor.bag_id=lgb.id 
        and lgo.mawb_id=lgm.id 
        AND tbe.event_code="DEPS"
        AND lgo.gmt_create {}
        and lgo.platform='WISH_ONLINE' 
        AND lgo.order_status =3 
        AND lgo.customer_id in (1151368,1151370,1181372,1181374) 
        and  lgo.channel_code = '{}'
        and  lgo.des = '{}'
        and lgo.is_deleted='n'
        and lgb.is_deleted='n' 
        and tbe.is_deleted='n'
        and lbor.is_deleted='n' 
        and lgm.is_deleted='n'
        and isnull(lgo.mawb_id)=0 
        and isnull(lgb.sealing_bag_time)=0  
        group by 2
        """.format(days,qd,des)

        s3 = """
            select  count(1) 票数, 
            round(timestampdiff(second,tbe.event_time,tme.event_time)/3600,1) 用时
            from 
            lg_order lgo, 
            track_bag_event tbe, 
            lg_bag_order_relation lbor, 
            track_mawb_event tme, 
            lg_bag lgb, 
            lg_mawb lgm 
            where  
            tbe.event_code="DEPS" 
            AND tme.event_code in("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF','PMWC')
            and lgo.gmt_create {}
            and platform='WISH_ONLINE' 
            AND customer_id in  (1151368,1151370,1181372,1181374) 
            and  lgo.channel_code = '{}'
            and  lgo.des = '{}'
            and lgo.id=lbor.order_id 
            and lbor.bag_id=tbe.bag_id 
            AND lgo.order_status =3 
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
            2
        """.format(days,qd,des)

        s4 = """
        SELECT DISTINCT 
        round(timestampdiff(second,tme1.event_time,tme.event_time)/3600,1) 用时,
        count(*) 票数
        from 
        track_mawb_event tme,
        track_mawb_event tme1,
        lg_order lgo
        where   
        tme.mawb_id = lgo.mawb_id
        AND tme1.mawb_id = lgo.mawb_id
        AND lgo.order_status =3 
        AND lgo.gmt_create {}
        and platform='WISH_ONLINE' 
        AND customer_id in  (1151368,1151370,1181372,1181374) 
        and  lgo.channel_code = '{}'
        and  lgo.des = '{}'
        AND tme.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
        AND tme1.event_code in("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF','PMWC') 
        and lgo.is_deleted='n' 
        AND tme.is_deleted='n'
        AND tme1.is_deleted='n'
        group by 1
        """.format(days,qd,des)

        s5 = """
        SELECT   
        round(timestampdiff(second,tme1.event_time,tme.event_time)/3600,1) 用时,
        count(*) 票数
        from 
        track_mawb_event tme,
        track_mawb_event tme1,
        lg_order lgo
        where   
        tme.mawb_id = lgo.mawb_id
        AND tme1.mawb_id = lgo.mawb_id
        AND lgo.gmt_create {}
        AND lgo.order_status =3 
        and platform='WISH_ONLINE' 
        AND customer_id in  (1151368,1151370,1181372,1181374) 
        and  lgo.channel_code = '{}'
        and  lgo.des = '{}'
        AND tme.event_code in("IRCM","PVCS","IRCN","RFIC") 
        AND tme1.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
        and lgo.is_deleted='n' 
        AND tme.is_deleted='n'
        AND tme1.is_deleted='n'
        group by 1
        union
        SELECT   
        round(timestampdiff(second,tme1.event_time,tbe.event_time)/3600,1) 用时,
        count(*) 单量
        from 
        track_bag_event tbe,
        track_mawb_event tme1,
        lg_order lgo,
        lg_bag_order_relation lbor
        where   
        tme1.mawb_id = lgo.mawb_id
        AND lgo.id = lbor.order_id
        and tbe.bag_id=lbor.bag_id 
        AND lgo.gmt_create {}
        AND lgo.order_status =3 
        and platform='WISH_ONLINE' 
        AND customer_id in  (1151368,1151370,1181372,1181374) 
        and  lgo.channel_code = '{}'
        and  lgo.des = '{}'
        AND tbe.event_code in("IRCM","PVCS","IRCN","RFIC") 
        AND tme1.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
        and lgo.is_deleted='n' 
        AND tbe.is_deleted='n'
        AND tme1.is_deleted='n'
        group by 1
        """.format(days,qd,des,days,qd,des)

        s6 = """
            SELECT   
            round(timestampdiff(second,tme.event_time,tbe1.event_time)/3600,1) 用时,
            count(*) 票数
            from 
            track_mawb_event tme, 
            track_bag_event tbe1, 
            lg_bag lgb, 
            lg_mawb lgm, 
            lg_order lgo,
            lg_bag_order_relation lbor 
            where   
            lgb.id=tbe1.bag_id 
            and tbe1.bag_id=lbor.bag_id 
            and lbor.order_id=lgo.id 
            and lgo.mawb_id=lgm.id 
            AND lgo.mawb_id = tme.mawb_id
            AND lgo.gmt_create {}
            and platform='WISH_ONLINE' 
            AND customer_id in  (1151368,1151370,1181372,1181374) 
            and  lgo.channel_code = '{}'
            and  lgo.des = '{}'
            AND tme.event_code in("IRCM","PVCS","IRCN","RFIC") 
            AND tbe1.event_code in("JFMD") 
            AND lgo.order_status =3 
            and lgo.is_deleted='n' 
            and tbe1.is_deleted='n' 
            and lgb.is_deleted='n' 
            and tme.is_deleted='n' 
            and lgm.is_deleted='n' 
            and lbor.is_deleted='n' 
            group by 1
            union
            SELECT   
            round(timestampdiff(second,tbe.event_time,tbe1.event_time)/3600,1) 用时,
            count(*) 单量
            from 
            track_bag_event tbe, 
            track_bag_event tbe1, 
            lg_bag lgb, 
            lg_mawb lgm, 
            lg_order lgo,
            lg_bag_order_relation lbor 
            where   
            lgb.id=tbe.bag_id 
            and tbe.bag_id=lbor.bag_id 
            and tbe1.bag_id=lbor.bag_id 
            and lbor.order_id=lgo.id 
            and lgo.mawb_id=lgm.id 
            AND lgo.gmt_create {}
            and platform='WISH_ONLINE' 
            AND customer_id in  (1151368,1151370,1181372,1181374) 
            and  lgo.channel_code = '{}'
            and  lgo.des = '{}'
            AND tbe.event_code in("IRCM","PVCS","IRCN","RFIC") 
            AND tbe1.event_code in("JFMD") 
            AND lgo.order_status =3 
            and lgo.is_deleted='n' 
            and tbe.is_deleted='n' 
            and lgb.is_deleted='n' 
            and lgm.is_deleted='n' 
            and lbor.is_deleted='n' 
            group by 1
        """.format(days,qd,des,days,qd,des)

        s7 = """
            SELECT 
            round(timestampdiff(second,tbe.event_time,lgo.delivery_date)/3600,1) 用时,
            count(*) 票数
            from 
            lg_order lgo inner join lg_bag_order_relation lbor on lbor.order_id=lgo.id 
            inner join lg_bag lgb on lgb.id=lbor.bag_id 
            inner join track_bag_event tbe on tbe.bag_id=lgb.id  
            where  
            tbe.event_code="JFMD"
            AND lgo.gmt_create {}
            and platform='WISH_ONLINE' 
            AND customer_id in  (1151368,1151370,1181372,1181374) 
            and  lgo.channel_code = '{}'
            and  lgo.des = '{}'
            and lgo.order_status =3
            and lgo.is_deleted='n' 
            and lgb.is_deleted='n'  
            and tbe.is_deleted='n' 
            and lbor.is_deleted='n' 
            group by 1
        """.format(days,qd,des)

        s8 = """
        SELECT 
            round(timestampdiff(second,lgo.gmt_create,lgo.delivery_date)/3600,1) 用时,
            count(*) 票数
            from 
            lg_order lgo
            where  
            lgo.gmt_create {}
            and platform='WISH_ONLINE' 
            AND customer_id in  (1151368,1151370,1181372,1181374) 
            and  lgo.channel_code = '{}'
            and  lgo.des = '{}'
            and lgo.order_status =3
            and lgo.is_deleted='n' 
            group by 1
        """.format(days,qd,des)
    else:
        s1 = """
            select 
            round(TIMESTAMPDIFF(second,lgo.gmt_create,lgb.sealing_bag_time)/3600,1) 用时,
            count(*) 票数
            from
            lg_order lgo,
            lg_mawb lgm,lg_bag_order_relation lbor,lg_bag lgb
            where  lgo.gmt_create {}
            and  lgo.channel_code = '{}'
            and  lgo.des = '{}'
            and lgo.id=lbor.order_id 
            and lbor.bag_id=lgb.id 
            and  lgo.mawb_id=lgm.id 
            AND lgo.order_status =3 
            and lgo.is_deleted='n' 
            and lgb.is_deleted='n'
            and lgm.is_deleted='n' 
            and lbor.is_deleted='n' 
            and isnull(lgo.mawb_id)=0 
            and isnull(lgb.sealing_bag_time)=0  
            group by 1
            """.format(days, qd, des)

        s2 = """
                SELECT 
                count(1) 票数, 
                round(timestampdiff(second,lgb.sealing_bag_time,tbe.event_time)/3600,1) 用时
                from lg_order lgo,lg_bag lgb, track_bag_event tbe, lg_bag_order_relation lbor, lg_mawb lgm
                where tbe.bag_id=lbor.bag_id
                and lbor.order_id=lgo.id  
                and lbor.bag_id=lgb.id 
                and lgo.mawb_id=lgm.id 
                AND lgo.order_status =3 
                AND tbe.event_code="DEPS"
                AND lgo.gmt_create {}
                and  lgo.channel_code = '{}'
                and  lgo.des = '{}'
                and lgo.is_deleted='n'
                and lgb.is_deleted='n' 
                and tbe.is_deleted='n'
                and lbor.is_deleted='n' 
                and lgm.is_deleted='n'
                and isnull(lgo.mawb_id)=0 
                and isnull(lgb.sealing_bag_time)=0  
                group by 2
                """.format(days, qd, des)

        s3 = """
                    select  count(1) 票数, 
                    round(timestampdiff(second,tbe.event_time,tme.event_time)/3600,1) 用时
                    from 
                    lg_order lgo, 
                    track_bag_event tbe, 
                    lg_bag_order_relation lbor, 
                    track_mawb_event tme, 
                    lg_bag lgb, 
                    lg_mawb lgm 
                    where  
                    tbe.event_code="DEPS" 
                    AND tme.event_code in("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF','PMWC')
                    and lgo.gmt_create {}
                    and  lgo.channel_code = '{}'
                    and  lgo.des = '{}'
                    and lgo.id=lbor.order_id 
                    AND lgo.order_status =3 
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
                    2
                """.format(days, qd, des)

        s4 = """
                SELECT DISTINCT 
                round(timestampdiff(second,tme1.event_time,tme.event_time)/3600,1) 用时,
                count(*) 票数
                from 
                track_mawb_event tme,
                track_mawb_event tme1,
                lg_order lgo
                where   
                tme.mawb_id = lgo.mawb_id
                AND tme1.mawb_id = lgo.mawb_id
                AND lgo.order_status =3 
                AND lgo.gmt_create {}
                and  lgo.channel_code = '{}'
                and  lgo.des = '{}'
                AND tme.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
                AND tme1.event_code in("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF','PMWC') 
                and lgo.is_deleted='n' 
                AND tme.is_deleted='n'
                AND tme1.is_deleted='n'
                group by 1
                """.format(days, qd, des)

        s5 = """
                SELECT   
                round(timestampdiff(second,tme1.event_time,tme.event_time)/3600,1) 用时,
                count(*) 票数
                from 
                track_mawb_event tme,
                track_mawb_event tme1,
                lg_order lgo
                where   
                tme.mawb_id = lgo.mawb_id
                AND tme1.mawb_id = lgo.mawb_id
                AND lgo.order_status =3 
                AND lgo.gmt_create {}
                and  lgo.channel_code = '{}'
                and  lgo.des = '{}'
                AND tme.event_code in("IRCM","PVCS","IRCN","RFIC") 
                AND tme1.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
                and lgo.is_deleted='n' 
                AND tme.is_deleted='n'
                AND tme1.is_deleted='n'
                group by 1
                union
                SELECT   
                round(timestampdiff(second,tme1.event_time,tbe.event_time)/3600,1) 用时,
                count(*) 单量
                from 
                track_bag_event tbe,
                track_mawb_event tme1,
                lg_order lgo,
                lg_bag_order_relation lbor
                where   
                tme1.mawb_id = lgo.mawb_id
                AND lgo.id = lbor.order_id
                and tbe.bag_id=lbor.bag_id 
                AND lgo.order_status =3 
                AND lgo.gmt_create {}
                and  lgo.channel_code = '{}'
                and  lgo.des = '{}'
                AND tbe.event_code in("IRCM","PVCS","IRCN","RFIC") 
                AND tme1.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
                and lgo.is_deleted='n' 
                AND tbe.is_deleted='n'
                AND tme1.is_deleted='n'
                group by 1
                """.format(days, qd, des, days, qd, des)

        s6 = """
                    SELECT   
                    round(timestampdiff(second,tme.event_time,tbe1.event_time)/3600,1) 用时,
                    count(*) 票数
                    from 
                    track_mawb_event tme, 
                    track_bag_event tbe1, 
                    lg_bag lgb, 
                    lg_mawb lgm, 
                    lg_order lgo,
                    lg_bag_order_relation lbor 
                    where   
                    lgb.id=tbe1.bag_id 
                    and tbe1.bag_id=lbor.bag_id 
                    and lbor.order_id=lgo.id 
                    and lgo.mawb_id=lgm.id 
                    AND lgo.order_status =3 
                    AND lgo.mawb_id = tme.mawb_id
                    AND lgo.gmt_create {}
                    and  lgo.channel_code = '{}'
                    and  lgo.des = '{}'
                    AND tme.event_code in("IRCM","PVCS","IRCN","RFIC") 
                    AND tbe1.event_code in("JFMD") 
                    and lgo.is_deleted='n' 
                    and tbe1.is_deleted='n' 
                    and lgb.is_deleted='n' 
                    and tme.is_deleted='n' 
                    and lgm.is_deleted='n' 
                    and lbor.is_deleted='n' 
                    group by 1
                    union
                    SELECT   
                    round(timestampdiff(second,tbe.event_time,tbe1.event_time)/3600,1) 用时,
                    count(*) 单量
                    from 
                    track_bag_event tbe, 
                    track_bag_event tbe1, 
                    lg_bag lgb, 
                    lg_mawb lgm, 
                    lg_order lgo,
                    lg_bag_order_relation lbor 
                    where   
                    lgb.id=tbe.bag_id 
                    and tbe.bag_id=lbor.bag_id 
                    and tbe1.bag_id=lbor.bag_id 
                    and lbor.order_id=lgo.id 
                    AND lgo.order_status =3 
                    and lgo.mawb_id=lgm.id 
                    AND lgo.gmt_create {}
                    and  lgo.channel_code = '{}'
                    and  lgo.des = '{}'
                    AND tbe.event_code in("IRCM","PVCS","IRCN","RFIC") 
                    AND tbe1.event_code in("JFMD") 
                    and lgo.is_deleted='n' 
                    and tbe.is_deleted='n' 
                    and lgb.is_deleted='n' 
                    and lgm.is_deleted='n' 
                    and lbor.is_deleted='n' 
                    group by 1
                """.format(days, qd, des, days, qd, des)

        s7 = """
                    SELECT 
                    round(timestampdiff(second,tbe.event_time,lgo.delivery_date)/3600,1) 用时,
                    count(*) 票数
                    from 
                    lg_order lgo inner join lg_bag_order_relation lbor on lbor.order_id=lgo.id 
                    inner join lg_bag lgb on lgb.id=lbor.bag_id 
                    inner join track_bag_event tbe on tbe.bag_id=lgb.id  
                    where  
                    tbe.event_code="JFMD"
                    AND lgo.gmt_create {}
                    and  lgo.channel_code = '{}'
                    and  lgo.des = '{}'
                    and lgo.order_status =3
                    and lgo.is_deleted='n' 
                    and lgb.is_deleted='n'  
                    and tbe.is_deleted='n' 
                    and lbor.is_deleted='n' 
                    group by 1
                """.format(days, qd, des)

        s8 = """
                SELECT 
                    round(timestampdiff(second,lgo.gmt_create,lgo.delivery_date)/3600,1) 用时,
                    count(*) 票数
                    from 
                    lg_order lgo
                    where     
                    lgo.gmt_create {}
                    and  lgo.channel_code = '{}'
                    and  lgo.des = '{}'
                    and lgo.order_status =3
                    and lgo.is_deleted='n' 
                    group by 1
                """.format(days, qd, des)
    d1 = execude_sql(s1)
    d1["jd"] = "入库-出库"
    # r1 = run(d1,des,qd,kh,jd)


    d2 = execude_sql(s2)
    d2["jd"] = "出库-装车"
    # r2 = run(d2, des, qd, kh, jd)


    d3 = execude_sql(s3)
    d3["jd"] = "装车-起飞"
    # r3 = run(d3, des, qd, kh, jd)


    d4 = execude_sql(s4)
    d4["jd"] = "起飞-落地"
    # r4 = run(d4, des, qd, kh, jd)


    d5 = execude_sql(s5)
    d5["jd"] = "落地-清关"
    # r5 = run(d5, des, qd, kh, jd)


    d6 = execude_sql(s6)
    d6["jd"] = "清关-交付"
    # r6 = run(d6, des, qd, kh, jd)


    d7 = execude_sql(s7)
    d7["jd"] = "交付-妥投"
    # r7 = run(d7, des, qd, kh, jd)

    d8 = execude_sql(s8)
    d8["jd"] = "首扫-妥投"
    # r8 = run(d8, des, qd, kh, jd)
    name = qd+"-"+des+"-"+kh
    res = pd.concat([d1,d2,d3,d4,d5,d6,d7,d8],axis=0,ignore_index=True)
    res.to_excel(r'./{}.xlsx'.format(name))
#
# df2 = pd.DataFrame(lists, columns=['渠道', 'des', '客户', '阶段名称', 'min', 'max', 'avge'])
# df2.to_excel('./数据2.xlsx')

#


