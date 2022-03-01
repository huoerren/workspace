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
os.chdir(r'C:\Users\hp\Desktop\促佳详情')

df_duo = pd.read_csv('cujia_duo.csv',encoding='gbk' )
print(df_duo.shape)
df_shao = pd.read_csv('cujia_shao.csv',encoding='gbk' )
print(df_shao.shape)
# df = pd.read_csv('12月份的Wish数据.csv',encoding='gbk' )
print('--------------------- 多: ------------------------------')
df_duo['分组'] = pd.cut(df_duo['weight'], [0, 0.2, 0.34, 0.453, 1.555, 2, 20], right=True)
df_duo['标签'] = pd.cut(df_duo['weight'], [0, 0.2, 0.34, 0.453, 1.555, 2, 20], labels=['0-0.2KG', '0.201-0.34KG', '0.341-0.453KG','0.454-1.555KG','1.556-2KG', '2.001-20KG'], right=True)
print(df_duo.groupby(['channel_code','标签']).agg({"标签": "count"}))
print('*******************************************************')
print('--------------------- 少 : ------------------------------')
df_shao['分组'] = pd.cut(df_shao['weight'], [0, 0.2, 0.34, 0.453, 1.555, 2, 20], right=True)
df_shao['标签'] = pd.cut(df_shao['weight'], [0, 0.2, 0.34, 0.453, 1.555, 2, 20], labels=['0-0.2KG', '0.201-0.34KG', '0.341-0.453KG','0.454-1.555KG','1.556-2KG', '2.001-20KG'], right=True)
print(df_shao.groupby(['channel_code','标签']).agg({"标签": "count"}))
print('*******************************************************')
