#!/usr/bin/env python
# coding: utf-8

import pandas as pd

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)
import openpyxl
import pymysql
import datetime, time

nows = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

print(datetime.date.today())
import numpy as np


global startDate
global endDate

def sop():

    # #数仓连接
    con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8",
                          autocommit=True, database='logisticscore')
    cur = con.cursor()


    def execude_sql(SQL):
        cur.execute(SQL)
        data = cur.fetchall()
        column = cur.description
        columns = [column[i][0] for i in range(len(column))]
        df = pd.DataFrame([list(i) for i in data], columns=columns)
        return df




    # 首扫
    S4 = """
       SELECT
        order_no 内单号,
        date_add(gmt_create,interval 8 hour) 首扫时间
    FROM
        lg_order lgo
    where
        lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
          and platform='JDW'
    and lgo.is_deleted='n'
    """

    print(S4)
    d4 = execude_sql(S4)
    print(d4.shape)
    d4 = d4.drop_duplicates(['内单号'], keep='last')
    print(d4.shape)


    # 封袋
    S2 = """
       	SELECT
    	order_no 内单号,
    	date_add(sealing_bag_time,interval 8 hour) 封袋时间
    	FROM
    	lg_order lgo,lg_bag_order_relation lbor,lg_bag lgb
    	where
    	lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
      and platform='JDW'
    	and lgo.is_deleted='n'
    	and lbor.order_id=lgo.id
    	and lbor.is_deleted='n'
    	and lbor.bag_id=lgb.id
    	and lgb.is_deleted='n'

    """


    d2 = execude_sql(S2)
    print(d2.shape)
    d2 = d2.drop_duplicates(['内单号'], keep='last')
    print(d2.shape)

    # 计算 “入库-出库” 时效
    





    df = pd.concat([d4, d2, d2]).drop_duplicates(subset=['内单号'], keep=False)  # df1-df2


    print(df)


    # 装车
    S1 = """
       SELECT
    	order_no 内单号,
    	date_add(event_time,interval 8 hour) 装车时间
    	FROM
    	lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
    	where
    	lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
     and platform='JDW'
    	and lgo.is_deleted='n'
    	and lbor.order_id=lgo.id
    	and lbor.is_deleted='n'
    	and lbor.bag_id=tbe.bag_id
    	and tbe.is_deleted='n'
    	and tbe.event_code='DEPS'
    """
    d1 = execude_sql(S1)
    print(d1.shape)

    d1 = d1.drop_duplicates(['内单号'], keep='last')
    print(d1.shape)


    df_zhuangche_fengdai = pd.concat([d2, d1, d1]).drop_duplicates(subset=['内单号'], keep=False)  # df1-df2
    print(df_zhuangche_fengdai.shape)
    print(df_zhuangche_fengdai)


    # 出口清关
    S01 = """
        SELECT
            order_no 内单号,
            date_add(event_time,interval 8 hour) 主单出口清关时间
        FROM
            lg_order lgo,track_mawb_event tme
        where
            lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
        and platform='JDW'
        and lgo.is_deleted='n'
        and lgo.mawb_id=tme.mawb_id
        and event_code in ("RFEC", "HGFX", "HGCK")
    """

    S02 = """
        SELECT
        order_no 内单号,
        date_add(event_time,interval 8 hour) 包裹出口清关时间
        FROM
            lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
        where
            lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
        and platform='JDW'
        and lgo.is_deleted='n'
        and lbor.order_id=lgo.id
        and lbor.is_deleted='n'
        and lbor.bag_id=tbe.bag_id
        and tbe.is_deleted='n'
        and event_code in ("RFEC", "HGFX", "HGCK")
    """

    S03 = """
        SELECT
        order_no 内单号,
        date_add(event_time,interval 8 hour) 正式单出口清关时间
        FROM
            lg_order lgo,track_order_event toe
        where
            lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
        and platform='JDW'
        and lgo.is_deleted='n'
        and toe.order_id=lgo.id
        and toe.is_deleted='n'
        and event_code in ("RFEC", "HGFX", "HGCK")
    """

    # In[26]:


    d01 = execude_sql(S01)
    print(d01.shape)

    d02 = execude_sql(S02)
    print(d02.shape)


    d03 = execude_sql(S03)
    print(d03.shape)


    d_01_02_03 = pd.concat([d01, d02, d03])
    d_01_02_03 = d_01_02_03.drop_duplicates(['内单号'], keep='last')
    print(d_01_02_03.shape)


    zhuangche_ckqingguan = pd.merge(d1, d_01_02_03[['内单号', '主单出口清关时间']], left_on='内单号', right_on='内单号', how='inner')


    print(zhuangche_ckqingguan.shape)


    # 起飞
    S5 = """
        SELECT
            order_no 内单号,
            date_add(event_time,interval 8 hour) 起飞时间
        FROM
            lg_order lgo,track_mawb_event tme
        where
        lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
        and platform='JDW'
        and lgo.is_deleted='n'
        and   tme.is_deleted='n'
        and lgo.mawb_id=tme.mawb_id
        and event_code in ("SDFO","DEPC","DEPT","LKJC")
    """
    d5 = execude_sql(S5)

    d5 = d5.drop_duplicates(['内单号'], keep='last')
    print(d5.shape)


    # 落地
    S61 = """
    SELECT
        order_no 内单号,
        date_add(event_time,interval 8 hour) 主单落地时间
    FROM
        lg_order lgo,track_mawb_event tme
    where
    lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
        and platform='JDW'
    and lgo.is_deleted='n'
    and tme.is_deleted='n'
    and lgo.mawb_id=tme.mawb_id
    and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
    """
    S62 = """
    SELECT
    order_no 内单号,
    date_add(event_time,interval 8 hour) 包裹落地时间
    FROM
    lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
    where
    lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
        and platform='JDW'
    and lgo.is_deleted='n'
    and lbor.order_id=lgo.id
    and lbor.is_deleted='n'
    and lbor.bag_id=tbe.bag_id
    and tbe.is_deleted='n'
    and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
    """
    S63 = """
    SELECT
    order_no 内单号,
    date_add(event_time,interval 8 hour) 正式单落地时间
    FROM
    lg_order lgo,track_order_event toe
    where
    lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
        and platform='JDW'
    and lgo.is_deleted='n'
    and toe.order_id=lgo.id
    and toe.is_deleted='n'
    and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
    """

    d61 = execude_sql(S61)
    print(d61.shape)

    d62 = execude_sql(S62)
    print(d62.shape)

    d63 = execude_sql(S63)
    print(d63.shape)

    d_61_62_63 = pd.concat([d61, d62, d63])
    d_61_62_63 = d_61_62_63.drop_duplicates(['内单号'], keep='last')
    print(d_61_62_63.shape)

    # 进口清关
    S71 = """
        SELECT
        order_no 内单号,
        date_add(event_time,interval 8 hour) 主单清关时间
        FROM
        lg_order lgo,track_mawb_event tme
        where
        lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
        and platform='JDW'
        and lgo.is_deleted='n'
        and tme.mawb_id=lgo.mawb_id
        and tme.is_deleted='n'
        and event_code in ("IRCM","PVCS","IRCN","RFIC")
    """
    d71 = execude_sql(S71)


    S72 = """
        SELECT
        order_no 内单号,
        date_add(event_time,interval 8 hour) 包裹清关时间
        FROM
        lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
        where
        lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
        and platform='JDW'
        and lgo.is_deleted='n'
        and lbor.order_id=lgo.id
        and lbor.is_deleted='n'
        and lbor.bag_id=tbe.bag_id
        and tbe.is_deleted='n'
        and event_code in ("IRCM","PVCS","IRCN","RFIC")
    """
    d72 = execude_sql(S72)

    print(d72.shape)

    S73 = """
        SELECT
        order_no 内单号,
        date_add(event_time,interval 8 hour) 正式单清关时间
        FROM
        lg_order lgo,track_order_event toe
        where
        lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
        and platform='JDW'
        and lgo.is_deleted='n'
        and toe.order_id=lgo.id
        and toe.is_deleted='n'
        and event_code in ("IRCM","PVCS","IRCN","RFIC")
    """
    d73 = execude_sql(S73)


    print(d73.shape)


    d_71_72_73 = pd.concat([d71, d72, d73])
    print(d_71_72_73.shape)

    d_71_72_73['内单号'].value_counts()




    d_71_72_73 = d_71_72_73.drop_duplicates(['内单号'], keep='last')

    d_71_72_73.shape


    df_luodi_qingguan = pd.concat([d_61_62_63, d_71_72_73, d_71_72_73]).drop_duplicates(subset=['内单号'],
                                                                                        keep=False)  # df1-df2

    df_luodi_qingguan


    # 交付
    S3 = """
        SELECT
        order_no 内单号,
        date_add(event_time,interval 8 hour) 交付时间
        FROM
        lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
        where
        lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
        and platform='JDW'
        and lgo.is_deleted='n'
        and lbor.order_id=lgo.id
        and lbor.is_deleted='n'
        and lbor.bag_id=tbe.bag_id
        and tbe.is_deleted='n'
        and event_code in ("JFMD")
    """
    d3 = execude_sql(S3)

    d3 = d3.drop_duplicates(['内单号'], keep='last')


    print(d3.shape)


    # 妥投
    S8 = """
        SELECT
        order_no 内单号,
        date_add(delivery_date,interval 8 hour) 妥投时间
        FROM
        lg_order lgo
        where
        lgo.gmt_create BETWEEN '2021-09-30 16:00:00' and '2021-10-31 16:00:00'
        and platform='JDW'
        and lgo.is_deleted='n'
        and delivery_date is not null
    """

    d8 = execude_sql(S8)

    print(d8.shape)

    d8 = d8.drop_duplicates(['内单号'], keep='last')
    print(d8.shape)

    df_jiaofu_tuotou = pd.concat([d3, d8, d8]).drop_duplicates(subset=['内单号'], keep=False)  # df1-df2
    print(df_jiaofu_tuotou)


    df_tuotou_jiaofu = pd.concat([d8, d3, d3]).drop_duplicates(subset=['内单号'], keep=False)  # df1-df2
    print(df_tuotou_jiaofu)





if __name__ == '__main__':

    sop()









