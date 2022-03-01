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

# 获得各个channel_code * des 矩阵
def getMatrixByDf(df):

    df_detail = df.groupby(['channel_code','des'],as_index=False)
    df_piaoliang = df_detail.count()
    print(df_piaoliang)
    # df_piaoliang['rate'] = df_piaoliang['zh_name'].apply(lambda x: round(x / 1699, 2))
    # new_df_04 = df_piaoliang.pivot("channel_code", "des", "rate")
    # sns.heatmap(new_df_04, annot=True, cmap="CMRmap_r", fmt='g')


    print('-----------')

    # all_count = df['count'].sum()
    # df['rate'] = df['count'].apply(lambda x: round(x / all_count, 3))
    #
    # # 票单量
    # heatmap_count = df.pivot("channel_code", "des", "count")
    # sns.heatmap(heatmap_count,annot=True ,cmap = "CMRmap_r", fmt='g')
    #
    # # 票单比例
    # heatmap_rate = df.pivot("channel_code", "des", "rate")
    # sns.heatmap(heatmap_rate, annot=True, cmap="Accent_r", fmt='g')
    #
    # plt.show()


# 获得上周期间 渠道-目的国-平台 单量
def getLastWeekData():
    s_lastweek = """
        select YEARWEEK(DATE_ADD(lgo.gmt_create,INTERVAL 8 hour),1) as weekly,platform , channel_code ,des,weight,zh_name
        from   lg_order lgo
        where  lgo.gmt_create  {} 
        """.format(lastWeek)

    df = execude_sql(s_lastweek)
    for i ,j in df.groupby(['platform']):
        getMatrixByDf(j)


# 获得 上周期间的正式单 数量
def getLastWeekCount():
    s_lastWeekCount = """
    SELECT  platform  , count(1) as count 
        FROM    lg_order lgo  
        WHERE   ((lgo.platform='WISH_ONLINE' and lgo.customer_id in (1151368,1151370,1181372,1181374) ) or(lgo.customer_id = 3282094  ) OR( customer_id=3161297 ) OR ( lgo.platform="DHLINK" ) )
                                             and lgo.gmt_create   BETWEEN '2021/12/19 16:00:00' and '2021/12/26 16:00:00'  
        group by  platform 
        order by platform desc 
    """
    df = execude_sql(s_lastWeekCount)
    print(df)


# 展示 每个平台总体单量 的箱型图 和 线型图

def  getBoxplotForWeekCountAndLine(dataframe):
    df_sub_01 = dataframe.sort_values(by=['weekly'] , ascending=True)
    df_sub_01['weekly'] = df_sub_01['weekly'].apply(lambda x: str(x))
    df_sub_02 = df_sub_01[['weekly', 'weekCount']].drop_duplicates(keep='first')
    df_sub_02['weekCount'] = df_sub_02['weekCount'].apply(lambda x:int(x))
    print(df_sub_02.describe(percentiles=[0.25, 0.75]))
    # print(df_sub_02.info())

    # 显示 箱型图
    # sns.boxplot( y= df_sub_02["weekCount"] )
    # 显示 线型图
    sns.lineplot(x= "weekly" , y="weekCount" , markers="o" ,  data=df_sub_02)

    plt.show()




# 获得 具体平台每一周的正式单 数量
def getPerWeekCount():

    s_PerWeekCount="""
    select date , weekly , acount , sum(acount)over( PARTITION by weekly) as `weekCount`, 'wish' as platform  from (
        select date_format(gmt_create,'%Y-%m-%d') as `date` ,YEARWEEK(gmt_create,1) as `weekly` ,  count(1)  as `acount`
        from  lg_order lgo 
        where (lgo.platform='WISH_ONLINE' and lgo.customer_id in (1151368,1151370,1181372,1181374)) 
        group by date_format(gmt_create,'%Y-%m-%d'), YEARWEEK(gmt_create,1)  order by date_format(gmt_create,'%Y-%m-%d') asc 
    ) a 
    union 
    select date , weekly , acount , sum(acount)over( PARTITION by weekly)  as `weekCount`, '促佳' as platform  from (
        select date_format(gmt_create,'%Y-%m-%d') as `date` ,YEARWEEK(gmt_create,1) as `weekly` ,  count(1)  as `acount`
        from  lg_order lgo 
        where customer_id = '3282094' 
        group by date_format(gmt_create,'%Y-%m-%d'), YEARWEEK(gmt_create,1)  order by date_format(gmt_create,'%Y-%m-%d') asc 
    ) b 
    union 
    select date , weekly , acount , sum(acount)over( PARTITION by weekly)  as `weekCount`, '兰亭' as platform  from (
        select date_format(gmt_create,'%Y-%m-%d') as `date` ,YEARWEEK(gmt_create,1) as `weekly` ,  count(1)  as `acount`
        from  lg_order lgo 
        where customer_id = '3161297' 
        group by date_format(gmt_create,'%Y-%m-%d'), YEARWEEK(gmt_create,1)  order by date_format(gmt_create,'%Y-%m-%d') asc 
    ) c
    union 
    select date , weekly , acount , sum(acount)over( PARTITION by weekly)  as `weekCount` , '敦煌' as platform  from (
        select date_format(gmt_create,'%Y-%m-%d') as `date` ,YEARWEEK(gmt_create,1) as `weekly` ,  count(1)  as `acount`
        from  lg_order lgo 
        where lgo.platform="DHLINK"  
        group by date_format(gmt_create,'%Y-%m-%d'), YEARWEEK(gmt_create,1)  order by date_format(gmt_create,'%Y-%m-%d') asc 
    ) d 
    """

    df_PerWeekCount = execude_sql(s_PerWeekCount)
    for m , n in df_PerWeekCount.groupby(['platform']):
        # 展示 每个平台每周的 箱型图 和线性图
        print(n.head())
        getBoxplotForWeekCountAndLine(n)


# 获得上周正式单的重量分布
def getWeightDis(dataframe):
    print(dataframe.describe())
    sns.catplot( x="prod", y='weight',  data=dataframe )
    plt.show()


# 获得 每周正式单的重量分布
def getPerWeekWeightDis(dataframe):
    dataframe['weekly'] = dataframe['weekly'].apply(lambda x:str(x))
    sns.catplot( x="weekly", y='weight',kind="swarm",dodge=False,  data=dataframe )
    plt.show()



# 获得 每周单量的明细（重量，体积，中文名称，费用 ）
def getDetails():
    s_details = """
        select channel_code , des ,date_format(gmt_create,'%Y-%m-%d') as date ,YEARWEEK(gmt_create,1) as weekly , weight, total_fee, zh_name ,'Wish' as platform  
        from  lg_order lgo 
        where lgo.platform='WISH_ONLINE' AND lgo.customer_id in  (1151368,1151370,1181372,1181374)
            and gmt_create  BETWEEN '2021-11-15 16:00:00' and '2021-12-31 16:00:00' 
        
        union all  
        
        select channel_code , des ,date_format(gmt_create,'%Y-%m-%d') as date ,YEARWEEK(gmt_create,1) as weelky , weight, total_fee, zh_name ,'促佳' as platform  from  lg_order lgo where customer_id = 3282094
        union all 
        select channel_code , des ,date_format(gmt_create,'%Y-%m-%d') as date ,YEARWEEK(gmt_create,1) as weelky , weight, total_fee, zh_name ,'兰亭' as platform  from  lg_order lgo where customer_id= 3161297 
        union all 
        select channel_code , des ,date_format(gmt_create,'%Y-%m-%d') as date ,YEARWEEK(gmt_create,1) as weelky , weight, total_fee, zh_name ,'敦煌' as platform  from  lg_order lgo where lgo.platform='DHLINK' 
    """

    dataDetails = execude_sql(s_details )
    for g,p in dataDetails.groupby(['platform']):
        print(p.head())
        print('--------------')
        getPerWeekWeightDis(p)


# 显示 上周,上上周 数据中 channelCode-des 热力图
def getHeatplotPerWeekCount():
    s_lastweekLast2WeekItem = """
        
        # Wish  
        select channel_code , des, total_fee,weight , zh_name, 'lastWeek' as prod ,'Wish' as platform  
        from lg_order lgo
        where  (lgo.platform='WISH_ONLINE' and lgo.customer_id in (1151368,1151370,1181372,1181374))
            and gmt_create BETWEEN '2021-12-19 16:00:00' and '2021-12-26 16:00:00'
        
        union all 
        select channel_code , des, total_fee,weight , zh_name	 , 'last2Week' as prod ,'Wish' as platform 
        from lg_order lgo
        where  (lgo.platform='WISH_ONLINE' and lgo.customer_id in (1151368,1151370,1181372,1181374))
            and gmt_create BETWEEN '2021-12-12 16:00:00' and '2021-12-19 16:00:00'
        
        union all 
        # 促佳 
        select channel_code , des, total_fee,weight , zh_name	, 'lastWeek' as prod  ,'促佳' as platform 
        from lg_order lgo
        where customer_id = '3282094' 
            and gmt_create BETWEEN '2021-12-19 16:00:00' and '2021-12-26 16:00:00'
        
        union all 
        
        select channel_code , des, total_fee,weight , zh_name	 , 'last2Week' as prod ,'促佳' as platform 
        from lg_order lgo
        where customer_id = '3282094' 
            and gmt_create BETWEEN '2021-12-12 16:00:00' and '2021-12-19 16:00:00'
            union all 
        # 兰亭 
        select channel_code , des, total_fee,weight , zh_name	, 'lastWeek' as prod  ,'兰亭' as platform 
        from lg_order lgo
        where customer_id = '3161297' 
            and gmt_create BETWEEN '2021-12-19 16:00:00' and '2021-12-26 16:00:00'
        
        union all 
        
        select channel_code , des, total_fee,weight , zh_name	 , 'last2Week' as prod ,'兰亭' as platform 
        from lg_order lgo
        where customer_id = '3161297' 
            and gmt_create BETWEEN '2021-12-12 16:00:00' and '2021-12-19 16:00:00'
        
        union all 	
        
        # 敦煌 
        select channel_code , des, total_fee,weight , zh_name	, 'lastWeek' as prod  ,'敦煌' as platform 
        from lg_order lgo
        where lgo.platform="DHLINK"
            and gmt_create BETWEEN '2021-12-19 16:00:00' and '2021-12-26 16:00:00'
        union all 
        select channel_code , des, total_fee,weight , zh_name	 , 'last2Week' as prod ,'敦煌' as platform 
        from lg_order lgo
        where lgo.platform="DHLINK"
            and gmt_create BETWEEN '2021-12-12 16:00:00' and '2021-12-19 16:00:00'
            
    """

    df = execude_sql(s_lastweekLast2WeekItem)

    for i,t  in df.groupby(['platform']):
        # t 具体某个平台 下 lastweek和 last2week 的明细数据
        getWeightDis(t)
        print('-----------------')


if __name__ == '__main__':
    getLastWeekData()
    # getLastWeekCount()
    # getPerWeekCount()
    # getHeatplotPerWeekCount()
    # getDetails()



