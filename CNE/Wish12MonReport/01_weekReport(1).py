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

plt.rcParams['font.sans-serif'] = ['SimHei'] # 用来正常显示中文标签
plt.rcParams['font.family']='sans-serif'
plt.rcParams['axes.unicode_minus'] = False # 用来正常显示负号

os.chdir(r'C:\Users\hp\Desktop\weekly')

# 当前考核周中 在整体数据中所处分位数
def getPost_weekly_12():
    df = pd.read_csv('weekly.csv')
    df_sub_01 = df[['weekly','acount_weekly']].drop_duplicates(keep='first')
    # print(df_sub_01)
    print(df_sub_01.describe(percentiles=[0.25,0.75]))
    # sns.boxplot( y= df_sub_01["acount_weekly"] )
    # sns.despine(offset=10, trim=True)

    df_02 = pd.read_csv('weekly_52_item.csv', encoding='gbk')
    # print(df_02.head())
    print('----------------------------------------------------')
    df_03 = df_02.groupby(['channel_code','des'] ,as_index=False)

    plt.figure(figsize=(12,2))
    # 票量占比
    df_piaoliang = df_03.count()
    df_piaoliang['rate'] = df_piaoliang['zh_name'].apply(lambda x: round(x/1699,2) )
    new_df_04 = df_piaoliang.pivot("channel_code","des","rate")
    sns.heatmap(new_df_04,annot=True ,cmap = "CMRmap_r", fmt='g')

    # 费用占比
    total_fee = df_02['total_fee'].sum()
    new_df_fee = df_02.groupby(['channel_code','des'], sort=False)['total_fee'].sum().reset_index()
    new_df_fee['rate'] = new_df_fee['total_fee'].apply(lambda  x: round(x/ total_fee ,2))
    new_df_05 = new_df_fee.pivot("channel_code","des","rate")
    # sns.heatmap(new_df_05,annot=True ,cmap = "CMRmap_r", fmt='g')

    # 重量占比
    total_weight = df_02['weight'].sum()
    new_df_weight = df_02.groupby(['channel_code','des'], sort=False)['weight'].sum().reset_index()
    new_df_weight['rate'] = new_df_weight['weight'].apply(lambda  x: round(x/ total_weight ,2))
    new_df_06 = new_df_weight.pivot("channel_code","des","rate")
    # sns.heatmap(new_df_06,annot=True ,cmap = "CMRmap_r", fmt='g')




# 12 月份 的相关数据
s12 = """  
select channel_code , des, total_fee,weight , zh_name	 
from lg_order
where customer_id = '3282094' 
and gmt_create BETWEEN '2021-11-30 16:00:00' and '2021-12-29 16:00:00'
"""

# 每周所有订单的重量分布
s_weight_fenbu = """
    select date_format(gmt_create,'%Y-%m-%d' ),YEARWEEK(gmt_create,1) as weely , weight  from    lg_order lgo where customer_id = '3282094' order by YEARWEEK(gmt_create,1)  asc
"""


con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8", autocommit=True, database='logisticscore')
cur = con.cursor()
#数仓连接

def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df


# data_12 : 获得整个12月份的(渠道_DES) 单量分布
def getZhongLiang_12():

    data_12 = execude_sql(s12)
    zhongliang_12 = data_12.shape[0]
    # 票量占比
    df_12Month_count = data_12.count() # 注意区别 .size: dataframe 中的 宽*高
    data_12_group_by_channel_des_count = data_12.groupby(['channel_code','des']).size()
    data_12_group_by_channel_des_count = data_12_group_by_channel_des_count.reset_index(name='count')
    data_12_group_by_channel_des_count['rate'] = data_12_group_by_channel_des_count['count'].apply(lambda  x: round(x/zhongliang_12 , 2))
    new_data_12_group_by_channel_des_count = data_12_group_by_channel_des_count.pivot("channel_code","des","rate")
    sns.heatmap(new_data_12_group_by_channel_des_count,annot=True ,  fmt='g')


# 每周单量的重量分布

def getDataWeightFenBu():
    data_weight_fenbu = execude_sql(s_weight_fenbu)
    print(data_weight_fenbu)
    sns.catplot(x= 'weely',  y='weight' , data=data_weight_fenbu  ,dodge=True )
    plt.show()
    # sns.catplot(x="day", y="total_bill",kind='swarm', hue='sex',data=tips,height=5,s=5.5)




# 上周各个渠道-国内表现


# 分段数据 (渠道-目的国)， 不同渠道-目的国的 KPI 不一样


# 上个周期中 不同渠道-目的国-KPI  表现




# 异常处理





if __name__ == '__main__':
    # getDataWeightFenBu()
    getPost_weekly_12()
    plt.show()
    # getDanliangWeekly()
#








