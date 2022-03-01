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





def xiangxingtu():
    df = pd.DataFrame({"数量": [1, 6160, 27229, 8141, 10942, 7532]})
    # sns.catplot(x='数量' , data=df , dodge=True)
    sns.boxplot(x='数量', data=df)
    plt.show()





# 重量部分图
def getWeightDes_06_week():

    filePath = r'C:\Users\hp\Desktop\data\促佳_202201-month.csv'
    df = pd.read_csv(filePath ,encoding='gbk')
    df['分组'] = pd.cut(df['weight'], [0, 0.34, 0.453, 1.100, 2.00, 5], right=True)
    df['标签'] = pd.cut(df['weight'], [0, 0.34, 0.454, 1.101, 2.00, 5], labels=['0-0.34KG', '0.341-0.453KG',  '0.454-1.1KG','1.101-2KG', '2.001-5KG'], right=True)
    df.to_csv('促佳_202111-month-result.csv', index=False, encoding="utf_8_sig")


    df_detail = df.groupby(['标签'])
    df_piaoliang = df_detail.count()
    print(df_piaoliang)

    # # 挑出 异常的 组别
    # df_sub_01 = df[df['标签'] == '0-0.34KG']
    # print(df_sub_01.to_csv('df_sub_01.csv' ,  index= False, encoding="utf_8_sig"))
    df_sub_03 = df[df['标签'] == '0.454-1.1KG']
    print(df_sub_03.to_csv('df_sub_03.csv' , index= False, encoding="utf_8_sig" ))


if __name__ == '__main__':
    getWeightDes_06_week()

















