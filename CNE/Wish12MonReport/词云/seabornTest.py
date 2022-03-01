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
# sns.set_style("darkgrid")

plt.rcParams['font.sans-serif'] = ['SimHei'] # 用来正常显示中文标签
plt.rcParams['font.family']='sans-serif'
plt.rcParams['axes.unicode_minus'] = False # 用来正常显示负号

os.chdir(r'C:\Users\hp\Desktop\Wish_12月份数据\12MonthData')


def getFirstPlot():
    df = pd.read_csv('12月份的Wish数据.csv',encoding='gbk' )
    print(df.shape)
    print(df.head())
    sns.barplot(x="weight_strategy" , data=df )



def getSecondPlot():
    df = pd.read_excel('channel_组别_count.xlsx' )
    print(df.shape)
    print(df.head())
    # sns.barplot(x="weight_strategy", data=df)
    # plt.show()

    sns.barplot(x="组别",y="单量",hue ="渠道",  data=df)
    plt.show()



if __name__ == '__main__':
    # getFirstPlot()
    getSecondPlot()


