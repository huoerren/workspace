import pandas as pd

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)
import openpyxl
import pymysql
import datetime, time
from dateutil.parser import parse
from business_duration import businessDuration
from dateutil import rrule

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


dateStr = """ BETWEEN '2022-02-06 16:00:00' and '2022-02-13 16:00:00'   """
channelCode = """ 'CNE尾程清派' """
desList = ['IT']

# 'IT'   # 周一到周五
def forIT(received_time,complete_time):
    if (pd.isnull(received_time) or pd.isnull(complete_time)):
        return np.nan
    period = businessDuration(received_time, complete_time, unit='min')
    return round(period/(60*24) , 2)


def forMX(received_time,complete_time): # 周一到周六
    if(pd.isnull(received_time) or pd.isnull(complete_time)):
        return np.nan
    workdays = [x for x in range(7) if x not in [6]]
    time_period = rrule.rrule(rrule.MINUTELY, dtstart=received_time, until=complete_time, byweekday=workdays).count()
    return round(time_period / (60 * 24), 2)


def forMX_02(received_time,complete_time): # 周一到周五及周六上午半天
    if(pd.isnull(received_time) or pd.isnull(complete_time)):
        return np.nan
    else:
        period = businessDuration(received_time, complete_time, unit='min')
        period = round(period / (60), 2)  # 将‘min’ 转为 ‘hour’，得到周一到周五 时常（小时）

        # case01: 开始时间和结束时间是在同一天
        if received_time.strftime('%Y-%m-%d') == complete_time.strftime('%Y-%m-%d'):
            if (received_time.weekday()!= 5) :
                period = round((complete_time - received_time).total_seconds()/3600 ,2 )
                period = round(period/24 ,2 )
                return period
            else:
                period = round((complete_time - received_time).total_seconds()/3600 ,2 )
                if period>12:
                    period=12
                period = round(period/24 ,2 )
                return period

        # case02:开始时间和结束时间不是在同一天
        else:

            # 判断起始时间是否在周六且在周六12点前，如果是 则 需要考虑起始时间 至 周六 12点前的时间
            if (received_time.weekday()== 5) : # 周六
                date_compare = received_time.strftime('%Y-%m-%d')  + " 12:00:00"
                sub_time_01_seconds = (parse(date_compare) - received_time).total_seconds()  #计算周六起始时间用时
                sub_time_01_hours = round(sub_time_01_seconds/(3600),2)
                period = period + sub_time_01_hours

            # 判断结束时间是否在周六且在周六12点前，如果是 则 需要考虑周六 00:00:00 至 周六的时间
            # 结束时间 只有两种情况，case01: 在 周一至周五 ； case02:周六 12点前。
            if (complete_time.weekday()== 5) : # 周六
                date_compare = complete_time.strftime('%Y-%m-%d')  + " 00:00:00"
                sub_time_02_seconds = ( complete_time - parse(date_compare)).total_seconds()  #计算结束时间完成用时
                sub_time_02_hours = round(sub_time_02_seconds/(3600),2)
                if(sub_time_02_hours>12):
                    sub_time_02_hours=12
                period = period + sub_time_02_hours
            # 判断在 (开始时间, 结束时间) 之间有几个周六, 有x个周六，则需要在总时间之上再加上 x个半天
            days = complete_time - received_time
            count=0
            received_time=received_time + datetime.timedelta(days=1)
            while(received_time.strftime('%Y-%m-%d') <= (complete_time+datetime.timedelta(days=-1)).strftime('%Y-%m-%d')):
                if (received_time).weekday() == 5:
                    count+=1
                received_time=received_time + datetime.timedelta(days=1)
            period = period + 12 * count
        period = round(period/24 ,2 )
        return  period



def getLuodiData(i):
    # 落地（主单）
    S61 = """
            SELECT 
                order_no 内单号,
                date_add(event_time,interval 8 hour) 落地时间,
                des,channel_name

            FROM
                lg_order lgo,track_mawb_event tme
            where
           1=1
                and lgo.gmt_create {} 

            and lgo.platform = 'EBAY_ONLINE'

            and lgo.channel_code = {}
            and lgo.des='{}' 
            and lgo.is_deleted='n'
            and tme.is_deleted='n'
            and lgo.mawb_id=tme.mawb_id
            and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
            """.format(dateStr, channelCode, i)
    # print('------ 主单落地 --------')
    # print(S61)

    # 落地（bag）
    S62 = """
            SELECT 
            order_no 内单号,
            date_add(event_time,interval 8 hour) 落地时间,
            des,channel_name

            FROM
            lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
            where
           1=1
                and lgo.gmt_create {} 

            and lgo.platform = 'EBAY_ONLINE'

                  and lgo.channel_code = {}
            and lgo.des='{}' 
            and lgo.is_deleted='n'
            and lbor.order_id=lgo.id
            and lbor.is_deleted='n'
            and lbor.bag_id=tbe.bag_id
            and tbe.is_deleted='n'
            and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
            """.format(dateStr, channelCode, i)
    # print('------ bag 落地 --------')
    # print(S62)

    # 落地（order）
    S63 = """
            SELECT 
            order_no 内单号,
            date_add(event_time,interval 8 hour) 落地时间,
            des,channel_name

            FROM
            lg_order lgo,track_order_event toe
            where
            1=1
                and lgo.gmt_create {} 

            and lgo.platform = 'EBAY_ONLINE'

                  and lgo.channel_code = {}
            and lgo.des='{}' 
            and lgo.is_deleted='n'
            and toe.order_id=lgo.id
            and toe.is_deleted='n'
            and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
            """.format(dateStr, channelCode, i)
    # print('------ Order 落地 --------')
    # print(S63)

    # print('------- 落地 结束 ！ ---------')

    d61 = execude_sql(S61)
    d62 = execude_sql(S62)
    d63 = execude_sql(S63)

    # 合并三个落地时间 统一改称为 “落地时间”

    d_61_62_63 = pd.concat([d61, d62, d63])
    d_61_62_63 = d_61_62_63.drop_duplicates(['内单号'], keep='last')
    print('落地 数量：')
    print(d_61_62_63.shape)
    # print(d_61_62_63.to_csv( '落地详情.csv',index= False, encoding="utf_8_sig"))
    return d_61_62_63


def getTuotou(i):
    S8 = """
            SELECT 
            order_no 内单号,
            date_add(delivery_date,interval 8 hour) 妥投时间,
            des,channel_name
            FROM
            lg_order lgo
            where
            1=1
            and lgo.gmt_create {} 

             and lgo.platform = 'EBAY_ONLINE'
            and lgo.channel_code = {}
            and lgo.des='{}' 
            and lgo.is_deleted='n'
            and delivery_date is not null 
        """.format(dateStr, channelCode, i)

    # print('----------- 妥投:  ----------')
    # print(S8)
    d8 = execude_sql(S8)

    d8 = d8.drop_duplicates(['内单号'], keep='last')
    print('妥投 数量：')
    print(d8.shape)
    return d8


def getTihuo(i):
    # 提货（主单）
    S_tihuo_mawb = """
            SELECT 
        order_no 内单号,
        date_add(event_time,interval 8 hour) 提货时间 ,
        des,channel_name

    FROM
        lg_order lgo,track_mawb_event tme
    where 1=1
         and  lgo.platform = 'EBAY_ONLINE' 
		 and lgo.gmt_create  {}
		 and  channel_code = {} 
		 and DES ='{}'

    and lgo.is_deleted='n'
    and tme.is_deleted='n'
    and lgo.mawb_id=tme.mawb_id
    and event_code in ('DDTH','ZDTH','ZDTZ','RPPU','MWPU')
            """.format(dateStr, channelCode, i)
    # print('------ 主单提货  --------')

    # print(S_tihuo_mawb)

    # 提货（bag）
    S_tihuo_bag = """
             SELECT 
                order_no 内单号,
                date_add(event_time,interval 8 hour) 落地时间,
                des,channel_name

                FROM
                lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
                where 1=1
                     and   lgo.platform = 'EBAY_ONLINE' 
                     AND lgo.gmt_create  {} 
                     AND  channel_code = {}
                     AND DES   ='{}'

                and lgo.is_deleted='n'
                and lbor.order_id=lgo.id
                and lbor.is_deleted='n'
                and lbor.bag_id=tbe.bag_id
                and tbe.is_deleted='n'
                and event_code in ('DDTH','ZDTH','ZDTZ','RPPU','MWPU')	
                """.format(dateStr, channelCode, i)
    # print('------ bag 提货 --------')
    # print(S_tihuo_bag)

    # 提货（order）
    S_tihuo_order = """
                select order_no ,toe.event_code , toe.event_time  from  lg_order lgo left join  track_order_event toe 
                    on  lgo.id = toe.order_id  where 1=1 
                 and  lgo.platform = 'EBAY_ONLINE' 
                  AND lgo.gmt_create  {}  
                    AND  channel_code = {}
                     AND   DES ='{}'
                         and  toe.event_code in ('DDTH','ZDTH','ZDTZ','RPPU','MWPU')
                """.format(dateStr, channelCode, i)

    # print('------ Order 提货 --------')
    # print(S_tihuo_order)
    #
    # print('------- 提货  结束 ！ ---------')

    d61 = execude_sql(S_tihuo_mawb)
    d62 = execude_sql(S_tihuo_bag)
    d63 = execude_sql(S_tihuo_order)

    # 合并三个落地时间 统一改称为 “提货时间”

    d_61_62_63 = pd.concat([d61, d62, d63])
    d_61_62_63 = d_61_62_63.drop_duplicates(['内单号'], keep='last')
    print('提货 数量：')
    print(d_61_62_63.shape)
    return d_61_62_63


def getHaiGuanFangxing(i):
    # 海关放行（主单）
    S_fangxing_mawb = """
                SELECT 
            order_no 内单号,
            date_add(event_time,interval 8 hour) 放行时间 ,
            des,channel_name

        FROM
            lg_order lgo,track_mawb_event tme
        where 1=1
             and  lgo.platform = 'EBAY_ONLINE' 
    		 and lgo.gmt_create  {}
    		 and  channel_code = {} 
    		 and DES ='{}'

        and lgo.is_deleted='n'
        and tme.is_deleted='n'
        and lgo.mawb_id=tme.mawb_id
        and event_code in ('BGRK' ,'IRCM' ,'IRCN', 'RFIC')
                """.format(dateStr, channelCode, i)
    # print('------ 主单放行时间  --------')
    #
    # print(S_fangxing_mawb)

    # 海关放行（bag）
    S_fangxing_bag = """
                 SELECT 
                    order_no 内单号,
                    date_add(event_time,interval 8 hour) 放行时间,
                    des,channel_name

                    FROM
                    lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
                    where 1=1
                         and   lgo.platform = 'EBAY_ONLINE' 
                         AND lgo.gmt_create  {} 
                         AND  channel_code = {}
                         AND DES   ='{}'

                    and lgo.is_deleted='n'
                    and lbor.order_id=lgo.id
                    and lbor.is_deleted='n'
                    and lbor.bag_id=tbe.bag_id
                    and tbe.is_deleted='n'
                    and event_code in ('BGRK' ,'IRCM' ,'IRCN', 'RFIC')	
                    """.format(dateStr, channelCode, i)
    # print('------ bag 放行时间 --------')
    # print(S_fangxing_bag)
    #
    # 海关放行（order）
    S_fangxing_order = """
                    select order_no ,toe.event_code , toe.event_time as '放行时间' from  lg_order lgo left join  track_order_event toe 
                        on  lgo.id = toe.order_id  where 1=1 
                     and  lgo.platform = 'EBAY_ONLINE' 
                      AND lgo.gmt_create  {}  
                        AND  channel_code = {}
                         AND   DES ='{}'
                             and  toe.event_code in ('BGRK' ,'IRCM' ,'IRCN', 'RFIC')
                    """.format(dateStr, channelCode, i)

    # print('------ Order 放行时间 --------')
    # print(S_fangxing_order)
    #
    # print('------- 放行时间  结束 ！ ---------')

    d61 = execude_sql(S_fangxing_mawb)
    d62 = execude_sql(S_fangxing_bag)
    d63 = execude_sql(S_fangxing_order)

    # 合并三个落地时间 统一改称为 “放行时间”

    d_61_62_63 = pd.concat([d61, d62, d63])
    d_61_62_63 = d_61_62_63.drop_duplicates(['内单号'], keep='last')
    print('海关放行 数量：')
    print(d_61_62_63.shape)
    return d_61_62_63


def getPaisongCen(i):
    # 抵达派送中心（主单）
    S_paisong_mawb = """
                    SELECT 
                order_no 内单号,
                date_add(event_time,interval 8 hour) 抵达派送中心时间 ,
                des,channel_name

            FROM
                lg_order lgo,track_mawb_event tme
            where 1=1
                 and  lgo.platform = 'EBAY_ONLINE' 
        		 and lgo.gmt_create  {}
        		 and  channel_code = {} 
        		 and DES ='{}'

            and lgo.is_deleted='n'
            and tme.is_deleted='n'
            and lgo.mawb_id=tme.mawb_id
            and event_code in ('RERP','JFMD')
                    """.format(dateStr, channelCode, i)
    # print('------ 主单放行时间  --------')
    #
    # print(S_paisong_mawb)

    # 派送中心时间（bag）
    S_paisong_bag = """
                     SELECT 
                        order_no 内单号,
                        date_add(event_time,interval 8 hour) 抵达派送中心时间,
                        des,channel_name

                        FROM
                        lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
                        where 1=1
                             and   lgo.platform = 'EBAY_ONLINE' 
                             AND lgo.gmt_create  {} 
                             AND  channel_code = {}
                             AND DES   ='{}'

                        and lgo.is_deleted='n'
                        and lbor.order_id=lgo.id
                        and lbor.is_deleted='n'
                        and lbor.bag_id=tbe.bag_id
                        and tbe.is_deleted='n'
                        and event_code in ('RERP','JFMD')	
                        """.format(dateStr, channelCode, i)
    # print('------ bag 放行时间 --------')
    # print(S_paisong_bag)

    # 派送中心时间（order）
    S_paisong_order = """
                        select order_no ,toe.event_code , toe.event_time as '抵达派送中心时间'  from  lg_order lgo left join  track_order_event toe 
                            on  lgo.id = toe.order_id  where 1=1 
                         and  lgo.platform = 'EBAY_ONLINE' 
                          AND lgo.gmt_create  {}  
                            AND  channel_code = {}
                             AND   DES ='{}'
                                 and  toe.event_code in ('RERP','JFMD')
                        """.format(dateStr, channelCode, i)

    # print('------ Order 派送中心时间 --------')
    # print(S_paisong_order)

    # print('------- 派送中心时间  结束 ！ ---------')

    d61 = execude_sql(S_paisong_mawb)
    d62 = execude_sql(S_paisong_bag)
    d63 = execude_sql(S_paisong_order)

    # 合并三个落地时间 统一改称为 “派送时间”

    d_61_62_63 = pd.concat([d61, d62, d63])
    d_61_62_63 = d_61_62_63.drop_duplicates(['内单号'], keep='last')
    print('派送中心时间 数量：')
    print(d_61_62_63.shape)
    return d_61_62_63


def getMoDuanCen(i):
    S_moduanfenjianCen = """
                            SELECT  order_no  as  '内单号',event_time as '离开派送中心'
                                FROM lg_order lgo  , track_supplier_en_event te  
                                    where 1=1 
                                        and  lgo.id  = te.order_id
                                         and  lgo.platform = 'EBAY_ONLINE' 
                                          AND lgo.gmt_create  {}  
                                            AND  channel_code = {}
                                             AND   lgo.DES ='{}'
                                               AND te.standard_event_id = 90042
                                    """.format(dateStr, channelCode, i)

    # print('------ 到达 末端分拣中心 --------')
    # print(S_moduanfenjianCen)
    df_likaiPaisongCen = execude_sql(S_moduanfenjianCen)
    return df_likaiPaisongCen


def getdaodaMoDuanFenJianCen(i):
    S_moduanfenjianCen = """
                        SELECT  order_no  as  '内单号',event_time as '离开派送中心'
                            FROM lg_order lgo  , track_supplier_en_event te  
                                where 1=1 
                                    and  lgo.id  = te.order_id
                                     and  lgo.platform = 'EBAY_ONLINE' 
                                      AND lgo.gmt_create  {}  
                                        AND  channel_code = {}
                                         AND   lgo.DES ='{}'
                                           AND te.standard_event_id = 56
                                """.format(dateStr, channelCode, i)

    # print('------ 到达 末端分拣中心 --------')
    # print(S_moduanfenjianCen)
    d61 = execude_sql(S_moduanfenjianCen)
    print('到达末端分拣中心数量：')
    print(d61.shape)
    return d61




def getAcount(i):
    # 获得 ‘落地’ 数据 -- 4710
    df_luodi = getLuodiData(i)

    # 获得 ‘海关放行’（清关段）数据
    df_haiguangfangxing = getHaiGuanFangxing(i)

    # 获得 ‘抵达末端派送中心’（也叫交付 - 也叫 抵达转运中心 ） 数据
    df_didaPaisongCen = getPaisongCen(i)

    # 获得 ‘妥投’ 数据
    df_tuotou = getTuotou(i)


    # 总的 ： 落地--妥投
    d_luodi_tuotou = pd.merge(df_luodi, df_tuotou[["内单号", "妥投时间"]], left_on="内单号", right_on="内单号", how='inner')
    d_luodi_tuotou['用时'] = d_luodi_tuotou.apply(lambda x: forIT(x['落地时间'], x['妥投时间']), axis=1 )# 针对IT
    # d_luodi_tuotou['用时'] = d_luodi_tuotou.apply(lambda x: forMX(x['落地时间'], x['妥投时间']), axis=1)# 针对MX
    df3 = d_luodi_tuotou.sort_values(by=["用时"], ascending=True)
    print('---------------------------------------')
    print('落地--妥投')
    print(df3.describe(percentiles=[0.95]))

    #   落地 - 清关（海关放行）
    d_luodi_qingguan  =  pd.merge(df_luodi, df_haiguangfangxing[["内单号", "放行时间"]], left_on="内单号", right_on="内单号", how='inner')
    d_luodi_qingguan['用时'] = d_luodi_qingguan.apply(lambda x: forIT(x['落地时间'], x['放行时间']), axis=1)# 针对IT
    # d_luodi_qingguan['用时'] = d_luodi_qingguan.apply(lambda x: forMX_02(x['落地时间'], x['放行时间']), axis=1)# 针对MX
    df4 = d_luodi_qingguan.sort_values(by=["用时"], ascending=True)
    print('---------------------------------------')
    print('落地 - 清关（海关放行）')
    print(df4.describe(percentiles=[0.95]))


    #   清关- 抵达末端派送中心（交付）
    d_didaPaisongCen_haiguangfangxing = pd.merge(df_haiguangfangxing, df_didaPaisongCen[["内单号", "抵达派送中心时间"]], left_on="内单号", right_on="内单号", how='inner')
    d_didaPaisongCen_haiguangfangxing['用时'] = d_didaPaisongCen_haiguangfangxing.apply(lambda x: forIT(x['放行时间'], x['抵达派送中心时间']), axis=1)# 针对IT
    # d_didaPaisongCen_haiguangfangxing['用时'] = d_didaPaisongCen_haiguangfangxing.apply(lambda x: forMX(x['放行时间'], x['抵达派送中心时间']), axis=1)# 针对MX
    df5 = d_didaPaisongCen_haiguangfangxing.sort_values(by=["用时"], ascending=True)
    print('---------------------------------------')
    print('清关- 抵达末端派送中心（交付）')
    print(df5.describe(percentiles=[0.95]))


    #    交付 - 妥投
    d_jiaofu_tuotou =  pd.merge(df_didaPaisongCen, df_tuotou[["内单号", "妥投时间"]], left_on="内单号", right_on="内单号", how='inner')
    d_jiaofu_tuotou['用时'] = d_jiaofu_tuotou.apply(lambda x: forIT(x['抵达派送中心时间'], x['妥投时间']), axis=1)# 针对IT
    # d_jiaofu_tuotou['用时'] = d_jiaofu_tuotou.apply(lambda x: forMX(x['抵达派送中心时间'], x['妥投时间']), axis=1)  # 针对MX
    df6 = d_jiaofu_tuotou.sort_values(by=["用时"], ascending=True)
    print('---------------------------------------')
    print('交付 - 妥投')
    print(df6.describe(percentiles=[0.95]))


if __name__ == '__main__':
    for i in desList:
        getAcount(i)

    # startTime = '2022-01-05 19:47:36'
    # endTime  =  '2022-01-10 11:12:00'
    # 计算时效（剔除 周六周日 -- it ）
    # getTimeJianGe(startTime , endTime)













