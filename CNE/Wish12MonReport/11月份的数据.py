#coding=utf-8

import pymysql
import re

import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)

from decimal import *
import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("darkgrid")
sns.set_context("paper")
# 设置风格、尺度

import warnings
warnings.filterwarnings('ignore')
# 不发出警告
plt.rcParams['font.sans-serif'] = ['SimHei'] # 用来正常显示中文标签
plt.rcParams['font.family']='sans-serif'
plt.rcParams['axes.unicode_minus'] = False # 用来正常显示负号

# os.chdir(r'C:\Users\hp\Desktop\Wish_12月份数据\12MonthData')
os.chdir(r'C:\Users\hp\Desktop\Cujia12MonthData')

df = pd.read_csv('促佳-11月份数据.csv',encoding='gbk' )
print(df.head(2))

df['分组'] = pd.cut(df['weight'], [0, 0.2, 0.453, 2, 20], right=True)
df['标签'] = pd.cut(df['weight'], [0, 0.2, 0.453, 2, 20], labels=['0-0.2KG', '0.201-0.453KG', '0.454-2KG', '2.001-20KG'], right=True)
print(df.groupby("分组").agg({"标签": "count"}))
print('--------------')
for m , n  in  df.groupby(['channel_code','des']):
    print(m)



# 重量区间标签-channel_code 分布图
totalCount = df.shape[0]
df_Sign_channelCode = df.groupby(['标签', 'channel_code'], as_index=False).count()
df_Sign_channelCode['rate'] = df_Sign_channelCode['zh_name'].apply(lambda x: round(x / totalCount, 2))
# print(df_channelCode_Des.to_csv('./单量统计.csv', index= False, encoding="utf_8_sig" ))
df_sign_chan = df_Sign_channelCode.pivot("channel_code", "标签", "rate")
sns.heatmap(df_sign_chan, annot=True, cmap="CMRmap_r", fmt='g')
plt.show()

