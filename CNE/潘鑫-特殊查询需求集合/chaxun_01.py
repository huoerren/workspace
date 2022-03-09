import pandas as pd

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)
import openpyxl
import pymysql
import datetime, time
import os.path

nows = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

print(datetime.date.today())
import numpy as np

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


orderList=[]


def getOrderNoes():
    no = pd.read_excel(r'C:\Users\hp\Desktop\内单号.xlsx')
    noes_01 = "','".join(list(no['内单号']))
    noes_02 = "('" + noes_01 + "')"

    # 首扫
    S4 = """
           SELECT 
            order_no 内单号,
            des  国家 , 
            weight 重量 , 
            date_add(gmt_create,interval 8 hour) 首扫时间 
             
        FROM
            lg_order lgo
        where 1=1
            and lgo.order_no in {} 
        and lgo.is_deleted='n'
        """.format(noes_02)

    print('----------------- 首扫 -----------------')
    print(S4)
    d4 = execude_sql(S4)
    d4['内单号'] = d4['内单号'].apply(lambda x: x + '\t')
    d4 = d4.drop_duplicates(['内单号'], keep='last')
    print(d4)

    # 封袋
    S2 = """
        SELECT 
            order_no 内单号,
            date_add(sealing_bag_time,interval 8 hour) 封袋时间 
            
        FROM
            lg_order lgo,lg_bag_order_relation lbor,lg_bag lgb
        where 1=1
            and lgo.order_no in {} 
        and lgo.is_deleted='n'
        and lbor.order_id=lgo.id
        and lbor.is_deleted='n'
        and lbor.bag_id=lgb.id
        and lgb.is_deleted='n'
    """.format(noes_02)

    print('---------- 封袋 -------------')
    d2 = execude_sql(S2)
    d2['内单号'] = d2['内单号'].apply(lambda x: x + '\t')
    d2 = d2.drop_duplicates(['内单号'], keep='last')
    print('封袋 数量：')
    print(d2.shape)

    # 装车
    S1 = """
           SELECT 
                order_no 内单号,
                date_add(event_time,interval 8 hour) 装车时间 
                 
           FROM
                lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
            where 1 = 1
            and lgo.order_no in {} 
            and lgo.is_deleted='n'
            and lbor.order_id=lgo.id
            and lbor.is_deleted='n'
            and lbor.bag_id=tbe.bag_id
            and tbe.is_deleted='n'
            and tbe.event_code='DEPS'
        """.format( noes_02)

    print('--------- 装车： ----------')

    d1 = execude_sql(S1)
    d1['内单号'] = d1['内单号'].apply(lambda x: x + '\t')
    d1 = d1.drop_duplicates(['内单号'], keep='last')
    print('装车 数量：')
    print(d1.shape)

# --------------------------- 到达机场-出口 ----------------------------------------
    # 到达机场-出口（主单）
    S81 = """
            SELECT 
            order_no 内单号,
            date_add(event_time,interval 8 hour) 到达机场出口时间 
        FROM
            lg_order lgo,track_mawb_event tme
        where  1=1
            and lgo.order_no in {}
            and lgo.is_deleted='n'
            and tme.is_deleted='n'
            and lgo.mawb_id=tme.mawb_id
            and event_code in ('ARRA','AREA','AAEP')
            """.format(noes_02)

    print(S81)

    # 到达机场-出口（bag）
    S82 = """
            SELECT 
            order_no 内单号,
            date_add(event_time,interval 8 hour) 到达机场出口时间  
        FROM
            lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
        where 1=1
          and lgo.order_no in  {}
            and lgo.is_deleted='n'
            and lbor.order_id=lgo.id
            and lbor.is_deleted='n'
            and lbor.bag_id=tbe.bag_id
            and tbe.is_deleted='n'
            and event_code in ('ARRA','AREA','AAEP')
            """.format(noes_02)

    # 到达机场-出口（order）
    S83 = """
        SELECT 
            order_no 内单号,
            date_add(event_time,interval 8 hour) 到达机场出口时间 
        FROM
            lg_order lgo,track_order_event toe
        where 1=1
            and lgo.order_no in  {}
            and lgo.is_deleted='n'
            and toe.order_id=lgo.id
            and toe.is_deleted='n'
            and event_code in ('ARRA','AREA','AAEP')
        """.format(noes_02)

    d81 = execude_sql(S81)
    d82 = execude_sql(S82)
    d83 = execude_sql(S83)

    d_81_82_83 = pd.concat([d81, d82, d83])
    d_81_82_83 = d_81_82_83.drop_duplicates(['内单号'], keep='last')
    d_81_82_83['内单号'] = d_81_82_83['内单号'].apply(lambda x: x + '\t')
    print('到达机场-出口 数量：')
    print(d_81_82_83.shape)



# -- 海关放行  start ---------------------------
    # 海关放行-出口（主单）
    S11 = """
               SELECT 
               order_no 内单号,
               date_add(event_time,interval 8 hour) 海关放行出口时间 
           FROM
               lg_order lgo,track_mawb_event tme
           where  1=1
               and lgo.order_no in {}
               and lgo.is_deleted='n'
               and tme.is_deleted='n'
               and lgo.mawb_id=tme.mawb_id
               and event_code in ('RFEC','HGFX','HGCK')
               """.format(noes_02)

    # 海关放行-出口（bag）
    S12 = """
               SELECT 
               order_no 内单号,
               date_add(event_time,interval 8 hour) 海关放行出口时间  
           FROM
               lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
           where 1=1
             and lgo.order_no in  {}
               and lgo.is_deleted='n'
               and lbor.order_id=lgo.id
               and lbor.is_deleted='n'
               and lbor.bag_id=tbe.bag_id
               and tbe.is_deleted='n'
               and event_code in ('RFEC','HGFX','HGCK')
               """.format(noes_02)

    # 海关放行-出口（order）
    S13 = """
           SELECT 
               order_no 内单号,
               date_add(event_time,interval 8 hour) 海关放行出口时间 
           FROM
               lg_order lgo,track_order_event toe
           where 1=1
               and lgo.order_no in  {}
               and lgo.is_deleted='n'
               and toe.order_id=lgo.id
               and toe.is_deleted='n'
               and event_code in ('RFEC','HGFX','HGCK')
           """.format(noes_02)
    d11 = execude_sql(S11)
    d12 = execude_sql(S12)
    d13 = execude_sql(S13)

    d_11_12_13 = pd.concat([d11, d12, d13])
    d_11_12_13 = d_11_12_13.drop_duplicates(['内单号'], keep='last')
    print('海关放行-出口 数量：')
    print(d_11_12_13.shape)
    d_11_12_13['内单号'] = d_11_12_13['内单号'].apply(lambda x: x + '\t')



    # -- 海关放行 end ------------------------------

    # 起飞
    S5 = """
        SELECT 
            order_no 内单号,
            date_add(event_time,interval 8 hour) 起飞时间 
             
        FROM
            lg_order lgo,track_mawb_event tme
        where 1 = 1
            and lgo.order_no in {} 
       
        and lgo.is_deleted='n'
        and   tme.is_deleted='n'
        and lgo.mawb_id=tme.mawb_id
        and event_code in ('SDFO','DEPC','DEPT','LKJC','SYFD','SYYF','PMWC')
    """.format(noes_02)

    print('------------ 起飞情况 -----------')
    d5 = execude_sql(S5)
    # print(d5['内单号'].value_counts())
    d5 = d5.drop_duplicates(['内单号'], keep='last')
    d5['内单号'] = d5['内单号'].apply(lambda x: x + '\t')
    print('起飞 数量：')
    print(d5.shape)


    print('-------------------- 落地情况 ： -------------------')
    # 落地（主单）
    S61 = """
        SELECT 
            order_no 内单号,
            date_add(event_time,interval 8 hour) 落地时间 
        FROM
            lg_order lgo,track_mawb_event tme
        where 1 = 1
                and lgo.order_no in {} 
    
        and lgo.is_deleted='n'
        and tme.is_deleted='n'
        and lgo.mawb_id=tme.mawb_id
        and event_code in ('ARIR','ABCD','ABAD','AECD','ARMA')
        """.format(noes_02)
    print('------ 主单落地 --------')

    # 落地（bag）
    S62 = """
        SELECT 
        order_no 内单号,
        date_add(event_time,interval 8 hour) 落地时间 
        FROM
        lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
        where 1 = 1
            and lgo.order_no in {} 
    
        and lgo.is_deleted='n'
        and lbor.order_id=lgo.id
        and lbor.is_deleted='n'
        and lbor.bag_id=tbe.bag_id
        and tbe.is_deleted='n'
        and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
        """.format(noes_02)
    print('------ bag 落地 --------')

    # 落地（order）
    S63 = """
        SELECT 
            order_no 内单号,
            date_add(event_time,interval 8 hour) 落地时间 
        FROM
            lg_order lgo,track_order_event toe
        where 1 = 1
            and lgo.order_no in {} 
        and lgo.is_deleted='n'
        and toe.order_id=lgo.id
        and toe.is_deleted='n'
        and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
        """.format(noes_02)
    print('------ Order 落地 --------')

    print('------- 落地 结束 ！ ---------')

    d61 = execude_sql(S61)
    d62 = execude_sql(S62)
    d63 = execude_sql(S63)

    # 合并三个落地时间 统一改称为 “落地时间”

    d_61_62_63 = pd.concat([d61, d62, d63])
    d_61_62_63 = d_61_62_63.drop_duplicates(['内单号'], keep='last')
    print('落地 数量：')
    print(d_61_62_63.shape)
    d_61_62_63['内单号'] = d_61_62_63['内单号'].apply(lambda x: x + '\t')


    print('------------- 清关 -------------')
    # 进口清关
    S71 = """
        SELECT 
            order_no 内单号,
            date_add(event_time,interval 8 hour) 进口清关时间 
         
        FROM
            lg_order lgo,track_mawb_event tme
        where 1 = 1
            and lgo.order_no in {} 
        and lgo.is_deleted='n'
        and tme.mawb_id=lgo.mawb_id
        and tme.is_deleted='n'
        and event_code in ("IRCM","PVCS","IRCN","RFIC","BGRK")
    """.format(noes_02)
    d71 = execude_sql(S71)

    S72 = """
        SELECT 
        order_no 内单号,
        date_add(event_time,interval 8 hour) 进口清关时间 
        FROM
        lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
        where 1 = 1
            and lgo.order_no in {} 
        and lgo.is_deleted='n'
        and lbor.order_id=lgo.id
        and lbor.is_deleted='n'
        and lbor.bag_id=tbe.bag_id
        and tbe.is_deleted='n'
        and event_code in ("IRCM","PVCS","IRCN","RFIC","BGRK")
    """.format(noes_02)
    d72 = execude_sql(S72)

    S73 = """
        SELECT 
        order_no 内单号,
        date_add(event_time,interval 8 hour) 进口清关时间 
        FROM
        lg_order lgo,track_order_event toe
        where 1 = 1
            and lgo.order_no in {} 
        and lgo.is_deleted='n'
        and toe.order_id=lgo.id
        and toe.is_deleted='n'
        and event_code in ("IRCM","PVCS","IRCN","RFIC","BGRK")
    """.format(noes_02)

    d73 = execude_sql(S73)

    # 合并 “进口清关”时间

    d_71_72_73 = pd.concat([d71, d72, d73])
    d_71_72_73 = d_71_72_73.drop_duplicates(['内单号'], keep='last')

    print('进口清关 数量：')
    print(d_71_72_73.shape)
    d_71_72_73['内单号'] = d_71_72_73['内单号'].apply(lambda x: x + '\t')

    # 交付
    S3 = """
            SELECT 
            order_no 内单号,
            date_add(event_time,interval 8 hour) 交付时间 
            FROM
            lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
           where 1 = 1
            and lgo.order_no in {} 
            and lgo.is_deleted='n'
            and lbor.order_id=lgo.id
            and lbor.is_deleted='n'
            and lbor.bag_id=tbe.bag_id
            and tbe.is_deleted='n'
            and event_code in ("JFMD")
        """.format(noes_02)
    d3 = execude_sql(S3)
    d3 = d3.drop_duplicates(['内单号'], keep='last')

    print('--------- S3 -------------')
    print(S3)
    print('交付 数量：')
    print(d3.shape)
    d3['内单号'] = d3['内单号'].apply(lambda x: x + '\t')

    # 妥投
    S8 = """
            SELECT 
                order_no 内单号,
                date_add(delivery_date,interval 8 hour) 妥投时间 
            FROM
            lg_order lgo
            where 1 = 1
            and lgo.order_no in {} 
            and lgo.is_deleted='n'
            and delivery_date is not null 
        """.format(noes_02 )

    print('----------- 妥投:  ----------')
    print(S8)
    d8 = execude_sql(S8)

    d8 = d8.drop_duplicates(['内单号'], keep='last')
    print('妥投 数量：')
    print(d8.shape)
    d8['内单号'] = d8['内单号'].apply(lambda x: x + '\t')

    # 主单号的查询
    S9 = """
        select lgo.order_no 内单号 , lgm.mawb_no 主单号  
        from lg_order lgo, lg_mawb lgm 
        where 1 = 1
        and lgo.order_no in {} 
        and lgo.mawb_id=lgm.id and lgo.is_deleted='n'
    """.format(noes_02)

    print('----------- 妥投:  ----------')
    d9 = execude_sql(S9)

    d9 = d9.drop_duplicates(['内单号'], keep='last')
    d9['内单号'] = d9['内单号'].apply(lambda x: x + '\t')



    # 开始数据合并
    result_01 = pd.merge(d4,d2,on=['内单号'],how='left')
    result_02 = pd.merge(result_01,d1,on=['内单号'],how='left')
    result_x = pd.merge(result_02, d_81_82_83, on=['内单号'], how='left')
    result_y = pd.merge(result_x, d_11_12_13, on=['内单号'], how='left')

    result_03 = pd.merge(result_y,d5,on=['内单号'],how='left')
    result_04 = pd.merge(result_03,d_61_62_63,on=['内单号'],how='left')

    result_05 = pd.merge(result_04, d_71_72_73, on=['内单号'], how='left')
    result_06 = pd.merge(result_05, d3, on=['内单号'], how='left')
    result_07 = pd.merge(result_06, d8, on=['内单号'], how='left')

    result_08 = pd.merge(result_07, d9, on=['内单号'], how='left')

    print('----------------------- 分割线 start : -----------------------------')
    print(result_08.columns.tolist())
    result_08['首扫时间'] = pd.to_datetime(result_08['首扫时间'])
    result_08['封袋时间'] = pd.to_datetime(result_08['封袋时间'])
    result_08['装车时间'] = pd.to_datetime(result_08['装车时间'])
    result_08['到达机场出口时间'] = pd.to_datetime(result_08['到达机场出口时间'])
    result_08['海关放行出口时间'] = pd.to_datetime(result_08['海关放行出口时间'])
    result_08['起飞时间'] = pd.to_datetime(result_08['起飞时间'])
    result_08['落地时间'] = pd.to_datetime(result_08['落地时间'])
    result_08['进口清关时间'] = pd.to_datetime(result_08['进口清关时间'])
    result_08['交付时间'] = pd.to_datetime(result_08['交付时间'])
    result_08['妥投时间'] = pd.to_datetime(result_08['妥投时间'])

    result_08["首扫-封袋_用时"] = (result_08["封袋时间"] - result_08["首扫时间"]).astype('timedelta64[s]')
    result_08["首扫-封袋_用时"] = round(result_08["首扫-封袋_用时"] / 86400, 2)

    result_08["封袋-装车_用时"] = (result_08["装车时间"] - result_08["封袋时间"]).astype('timedelta64[s]')
    result_08["封袋-装车_用时"] = round(result_08["封袋-装车_用时"] / 86400, 2)

    result_08["装车-到达机场_用时"] = (result_08["到达机场出口时间"] - result_08["装车时间"]).astype('timedelta64[s]')
    result_08["装车-到达机场_用时"] = round(result_08["装车-到达机场_用时"] / 86400, 2)

    result_08["到达机场-海关放行出口_用时"] = (result_08["海关放行出口时间"] - result_08["到达机场出口时间"]).astype('timedelta64[s]')
    result_08["到达机场-海关放行出口_用时"] = round(result_08["到达机场-海关放行出口_用时"] / 86400, 2)

    result_08["海关放行出口-起飞_用时"] = (result_08["起飞时间"] - result_08["海关放行出口时间"]).astype('timedelta64[s]')
    result_08["海关放行出口-起飞_用时"] = round(result_08["海关放行出口-起飞_用时"] / 86400, 2)

    result_08["起飞-落地_用时"] = (result_08["落地时间"] - result_08["起飞时间"]).astype('timedelta64[s]')
    result_08["起飞-落地_用时"] = round(result_08["起飞-落地_用时"] / 86400, 2)

    result_08["落地-进口清关_用时"] = (result_08["进口清关时间"] - result_08["落地时间"]).astype('timedelta64[s]')
    result_08["落地-进口清关_用时"] = round(result_08["落地-进口清关_用时"] / 86400, 2)

    result_08["进口清关-交付_用时"] = (result_08["交付时间"] - result_08["进口清关时间"]).astype('timedelta64[s]')
    result_08["进口清关-交付_用时"] = round(result_08["进口清关-交付_用时"] / 86400, 2)

    result_08["交付-妥投_用时"] = (result_08["妥投时间"] - result_08["交付时间"]).astype('timedelta64[s]')
    result_08["交付-妥投_用时"] = round(result_08["交付-妥投_用时"] / 86400, 2)

    print('----------------------- 分割线 end  -----------------------------')


    result_08.to_excel('result.xlsx' , index=False )


def getAllTimeAndShixiao():
    getOrderNoes()







if __name__ == '__main__':
    # 获得相关 order_no
    ## getOrderNoes()
    # 根据 order_no 获得相关节点时间和分段时效
    getAllTimeAndShixiao()



