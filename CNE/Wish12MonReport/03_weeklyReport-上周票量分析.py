#coding=utf-8

import pymysql
import re

import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)

import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("darkgrid")

plt.rcParams['font.sans-serif'] = ['SimHei'] # 用来正常显示中文标签
plt.rcParams['font.family']='sans-serif'
plt.rcParams['axes.unicode_minus'] = False # 用来正常显示负号

con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8", autocommit=True, database='logisticscore')
cur = con.cursor()

lastWeek = "  BETWEEN '2021/12/19 16:00:00' and '2021/12/26 16:00:00' "

#数仓连接

def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df


def getLastColumnDetail(platform,dataframe):
    print(platform)
    print('---------------------------')
    print(dataframe.head(3))
    print(dataframe.shape[0])


def getLastDetail():

    S_getLastDetail = """
        select YEARWEEK(DATE_ADD(lgo.gmt_create,INTERVAL 8 hour),1) as weekly,platform , channel_code ,des,weight,zh_name
        from   lg_order lgo
        where  lgo.gmt_create BETWEEN '2021/12/19 16:00:00' and '2021/12/26 16:00:00'
            and (  ( (lgo.platform='WISH_ONLINE') and (lgo.customer_id in (1151368,1151370,1181372,1181374)) ) 
                    OR (lgo.customer_id = 3282094  ) 
                    OR (customer_id=3161297 ) 
                    OR (lgo.platform="DHLINK" ) 
                )
    """
    S_getLast2Detail = """
        select YEARWEEK(DATE_ADD(lgo.gmt_create,INTERVAL 8 hour),1) as weekly,platform , channel_code ,des,weight,zh_name
                from   lg_order lgo
                where  lgo.gmt_create BETWEEN '2021/12/12 16:00:00' and '2021/12/19 16:00:00'
                    and (  ( (lgo.platform='WISH_ONLINE') and (lgo.customer_id in (1151368,1151370,1181372,1181374)) ) 
                            OR (lgo.customer_id = 3282094  ) 
                            OR (customer_id=3161297 ) 
                            OR (lgo.platform="DHLINK" ) 
                        )

        """
    dataLastDetail = execude_sql(S_getLastDetail)
    dataLast2Detail = execude_sql(S_getLast2Detail)
    for i , j  in dataLastDetail.groupby(['platform']):
        getLastColumnDetail(i,j)




# 展示 每个平台总体单量 的箱型图 和 线型图

def  getBoxplotForWeekCountAndLine(dataframe):
    df_sub_01 = dataframe.sort_values(by=['weekly'] , ascending=True)
    df_sub_01['weekly'] = df_sub_01['weekly'].apply(lambda x: str(x))
    print('------------ dataframe: tail -----------')
    print(df_sub_01.tail(10))
    print('------------- end --------------')

    df_sub_02 = df_sub_01[['weekly', 'weekCount']].drop_duplicates(keep='first')
    df_sub_02['weekCount'] = df_sub_02['weekCount'].apply(lambda x:int(x))
    print(df_sub_02.describe(percentiles=[0.25, 0.75]))
    # print(df_sub_02.head(2))
    print('------------------')

    # 显示 箱型图
    sns.boxplot( y= df_sub_02["weekCount"] )
    # 显示 线型图
    sns.lineplot(x= "weekly" , y="weekCount" ,markers="r",data=df_sub_02)

    plt.show()



# 获得 具体平台每一周的正式单 数量
def getPerWeekCount():

    s_PerWeekCount="""
    select date , weekly , acount , sum(acount)over( PARTITION by weekly) as `weekCount`, 'wish' as platform  from (
        select date_format(gmt_create,'%Y-%m-%d') as `date` ,YEARWEEK(gmt_create,1) as `weekly` ,  count(1)  as `acount`
        from  lg_order lgo 
        where (lgo.platform='WISH_ONLINE' and lgo.customer_id in (1151368,1151370,1181372,1181374)) and lgo.gmt_create >'2021/06/30 16:00:00' 
        group by date_format(gmt_create,'%Y-%m-%d'), YEARWEEK(gmt_create,1)  order by date_format(gmt_create,'%Y-%m-%d') asc 
    ) a 
    union 
    select date , weekly , acount , sum(acount)over( PARTITION by weekly)  as `weekCount`, '促佳' as platform  from (
        select date_format(gmt_create,'%Y-%m-%d') as `date` ,YEARWEEK(gmt_create,1) as `weekly` ,  count(1)  as `acount`
        from  lg_order lgo 
        where customer_id = '3282094'  and lgo.gmt_create >'2021/06/30 16:00:00' 
        group by date_format(gmt_create,'%Y-%m-%d'), YEARWEEK(gmt_create,1)  order by date_format(gmt_create,'%Y-%m-%d') asc 
    ) b 
    union 
    select date , weekly , acount , sum(acount)over( PARTITION by weekly)  as `weekCount`, '兰亭' as platform  from (
        select date_format(gmt_create,'%Y-%m-%d') as `date` ,YEARWEEK(gmt_create,1) as `weekly` ,  count(1)  as `acount`
        from  lg_order lgo 
        where customer_id = '3161297'  and lgo.gmt_create >'2021/06/30 16:00:00' 
        group by date_format(gmt_create,'%Y-%m-%d'), YEARWEEK(gmt_create,1)  order by date_format(gmt_create,'%Y-%m-%d') asc 
    ) c
    union 
    select date , weekly , acount , sum(acount)over( PARTITION by weekly)  as `weekCount` , '敦煌' as platform  from (
        select date_format(gmt_create,'%Y-%m-%d') as `date` ,YEARWEEK(gmt_create,1) as `weekly` , count(1)  as `acount`
        from  lg_order lgo 
        where lgo.platform="DHLINK" and lgo.gmt_create >'2021/06/30 16:00:00' 
        group by date_format(gmt_create,'%Y-%m-%d'), YEARWEEK(gmt_create,1)  order by date_format(gmt_create,'%Y-%m-%d') asc 
    ) d 
    """

    df_PerWeekCount = execude_sql(s_PerWeekCount)
    for m , n in df_PerWeekCount.groupby(['platform']):
        # 展示 每个平台每周的 箱型图 和线性图
        # print(n.head())
        getBoxplotForWeekCountAndLine(n)


def getWeightPlot(platform,df):
    print(platform)
    df['weight'] = df['weight'].apply(lambda x:round(float(x),3))
    sns.catplot(x="weekly", y='weight', data=df )
    plt.xticks(rotation=45 , )
    plt.show()

    print('-------------')




# 获得 具体平台票单重量分布
def getWeightDis():

    s_PerWeekWight ="""
        select date_format(gmt_create,'%Y-%m-%d') as `date`,YEARWEEK(gmt_create,1) as `weekly` , weight, 'WISH' as platform 
        from  lg_order lgo 
        where (lgo.platform='WISH_ONLINE' and lgo.customer_id in (1151368,1151370,1181372,1181374))  and (lgo.gmt_create >'2021/08/31 16:00:00')
    union 
         select date_format(gmt_create,'%Y-%m-%d') as `date`,YEARWEEK(gmt_create,1) as `weekly` , weight, '促佳' as platform 
        from  lg_order lgo 
        where  customer_id = '3282094'  and lgo.gmt_create >'2021/06/30 16:00:00'
    union 
        select date_format(gmt_create,'%Y-%m-%d') as `date`,YEARWEEK(gmt_create,1) as `weekly` , weight, '兰亭' as platform 
        from  lg_order lgo 
        where customer_id = '3161297' and lgo.gmt_create >'2021/06/30 16:00:00'
    union 
        select date_format(gmt_create,'%Y-%m-%d') as `date`,YEARWEEK(gmt_create,1) as `weekly` , weight, '敦煌' as platform 
        from  lg_order lgo 
        where lgo.platform="DHLINK" and lgo.gmt_create >'2021/06/30 16:00:00' 
    """

    df_PerWeekCount = execude_sql(s_PerWeekWight)
    for w , q in df_PerWeekCount.groupby(['platform']):
        # 展示 每个平台每周的 箱型图 和线性图
        # getBoxplotForWeekCountAndLine(n)
        getWeightPlot(w , q)


#接-getAllFeeDis()
def lastWeekFeeDis(platform ,df):
    print(platform)
    df['total_fee'] = df['total_fee'].apply(lambda x:float(x))
    total_fee = df['total_fee'].sum()
    df_channelCode_Dis = df.groupby(['channel_code', 'des'], as_index=False)['total_fee'].sum()
    df_channelCode_Dis['rate'] = df_channelCode_Dis['total_fee'].apply(lambda x: round(x/total_fee , 2))
    print('-----***************----')
    # 每个颗粒度下的 total_fee 分布占比
    new_df_01 = df_channelCode_Dis.pivot("channel_code", "des", "rate")
    print(new_df_01)
    print('**************************************')
    sns.heatmap(new_df_01, annot=True, cmap="CMRmap_r", fmt='g')
    plt.show()

#接-getLastDis()
def lastRateDis(platform ,df):
    print(" platform : " + platform)
    df_channelCode_Des = df.groupby(['channel_code', 'des'], as_index=False).count()
    df_count = df.shape[0]
    print('------------------')
    # 单量比例分布占比
    df_channelCode_Des['rate'] = df_channelCode_Des['zh_name'].apply(lambda x: round(x / df_count, 2))
    new_df_04 = df_channelCode_Des.pivot("channel_code", "des", "rate")
    sns.heatmap(new_df_04, annot=True, cmap="CMRmap_r", fmt='g')
    plt.show()

#接-getLastDis()
def lastCountDis(platform ,df):
    print(" platform : " + platform)
    df_channelCode_Des = df.groupby(['channel_code', 'des'], as_index=False).count()
    df_count = df.shape[0]
    print('------------------')
    # 单量分布占比
    new_df_03 = df_channelCode_Des.pivot("channel_code", "des", "zh_name")
    sns.heatmap(new_df_03, annot=True, cmap="CMRmap_r", fmt='g')
    plt.show()


def getLastDis():
    S_getLastDetail = """
            select YEARWEEK(DATE_ADD(lgo.gmt_create,INTERVAL 8 hour),1) as weekly,platform, channel_code ,des,weight,zh_name,total_fee
            from   lg_order lgo
            where  lgo.gmt_create BETWEEN '2021/12/19 16:00:00' and '2021/12/26 16:00:00'
                and (  ( (lgo.platform='WISH_ONLINE') and (lgo.customer_id in (1151368,1151370,1181372,1181374)) ) 
                        OR (lgo.customer_id = 3282094  ) 
                        OR (customer_id=3161297 ) 
                        OR (lgo.platform="DHLINK" ) 
                    )
        """
    dataLastDetail = execude_sql(S_getLastDetail)
    for i , j in dataLastDetail.groupby(['platform']):
        # print('-----------')
        # lastRateDis(i, j)   #  rate 占比
        # lastCountDis(i,j)   #  count 占比
        lastWeekFeeDis(i,j)   # total_fee 占比




if __name__ == '__main__':
    # 获得上周的单量
    # getLastDetail()

    # 获得上周单量 ，周环比，线型图，在整体单量中的位置
    # getPerWeekCount()

    # 拆分上周 weight 分布
    # getWeightDis()

    # 拆分上周的票单 channel_code-des 级别颗粒度 单量、当量占比、单量费用 分布
    getLastDis()

    # 获得具体的订单分布







