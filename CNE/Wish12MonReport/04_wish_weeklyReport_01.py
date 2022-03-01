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

df = pd.read_csv('促佳-12月份数据.csv',encoding='gbk' )
# df = pd.read_csv('12月份的Wish数据.csv',encoding='gbk' )
print(df.shape[0])
# print(df['weight'].describe(percentiles=[ 0.5,0.65,0.75,0.85 ,0.95 ]))

# print(df.groupby(['channel_code','des']).count())



# # by 优先GB
# df_youxian_GB = df[(df['channel_code']=='CNE全球优先') & (df['des']=='GB') ]
# print(df_youxian_GB.shape)
# #查看 促佳优先GB 的重量分布
#
# df_youxian_GB['分组'] = pd.cut(df_youxian_GB['weight'], [0, 0.2, 0.34, 0.453, 1.555, 2, 20], right=True)
# df_youxian_GB['标签'] = pd.cut(df_youxian_GB['weight'], [0, 0.2, 0.34, 0.453, 1.555, 2, 20], labels=['0-0.2KG', '0.201-0.34KG', '0.341-0.453KG','0.454-1.555KG','1.556-2KG', '2.001-20KG'], right=True)
# print(df_youxian_GB.groupby(['channel_code']).agg({"标签": "count"}))
# print('*******************************************************')
# print(df_youxian_GB.groupby(['channel_code',"分组"]).agg({"标签": "count"}))
# print('---- 以上为 促佳优先GB 的重量分布 ----- ')


# # by 优先GB
# df_tehui_GB = df[(df['channel_code']=='CNE全球特惠') & (df['des']=='GB') ]
# print(df_tehui_GB.shape)
# print('--- nihao ---')
# #查看 促佳优先GB 的重量分布
#
# df_tehui_GB['分组'] = pd.cut(df_tehui_GB['weight'], [0, 0.2, 0.34, 0.453, 1.555, 2, 20], right=True)
# df_tehui_GB['标签'] = pd.cut(df_tehui_GB['weight'], [0, 0.2, 0.34, 0.453, 1.555, 2, 20], labels=['0-0.2KG', '0.201-0.34KG', '0.341-0.453KG','0.454-1.555KG','1.556-2KG', '2.001-20KG'], right=True)
# print(df_tehui_GB.groupby(['channel_code']).agg({"标签": "count"}))
# print('*******************************************************')
# print(df_tehui_GB.groupby(['channel_code',"分组"]).agg({"标签": "count"}))
#
#
# print('---- 以上为 促佳特惠GB 的重量分布 ----- ')




# # “重量分组”
# df['分组'] = pd.cut(df['weight'], [0, 0.2, 0.34, 0.453, 1.555, 2, 20], right=True)
# df['标签'] = pd.cut(df['weight'], [0, 0.2, 0.34, 0.453, 1.555, 2, 20], labels=['0-0.2KG', '0.201-0.34KG', '0.341-0.453KG','0.454-1.555KG','1.556-2KG', '2.001-20KG'], right=True)
# print(df.groupby(['channel_code']).agg({"标签": "count"}))
# print('*******************************************************')
# print(df.groupby(['channel_code',"分组"]).agg({"标签": "count"}))
# # print('--------------')
# # for m , n  in  df.groupby(['channel_code','des']):
# #     print(m , n.shape[0])





# print(df.shape)


# 注意95成 的相关代码是针对 Wish_4PL 的数据的
# #95成 单量分布
# df_95 = pd.read_excel('95成单量分布.xlsx')
# print(df_95)
# df_new_95 = df_95.groupby(['渠道', '国家'], as_index=False).count()
# new_df_95_2 = df_95.pivot("渠道", "国家", "单量")
# sns.heatmap(new_df_95_2, annot=True, cmap="CMRmap_r", fmt='g')
# plt.show()


# #95成 单量比例分布
# df_95_rate = pd.read_excel('95成单量分布.xlsx',sheet_name='Sheet2')
# df_95_rate['占比'] = df_95_rate['占比'].apply(lambda x: round(x ,3))
# new_df_95_rate_2 = df_95_rate.pivot("渠道", "国家", "占比")
# sns.heatmap(new_df_95_rate_2, annot=True, cmap="CMRmap_r", fmt='g')
# plt.show()


# #95成 营收比例分布
# df_95_yingshou = pd.read_excel('95成单量分布.xlsx',sheet_name='95成营收占比')
# print(df_95_yingshou)
# # df_95_yingshou['占比'] = df_95_yingshou['占比'].apply(lambda x: round(x ,3))
# new_df_95_yingshou_2 = df_95_yingshou.pivot("渠道", "国家", "占比")
# sns.heatmap(new_df_95_yingshou_2, annot=True, cmap="CMRmap_r", fmt='g')
# plt.show()



# 单量分布占比
# df['channel_code'].value_counts()
# df_channelCode_Des = df.groupby(['channel_code', 'des'], as_index=False).count()
# # print(df_channelCode_Des.to_csv('./单量统计.csv', index= False, encoding="utf_8_sig" ))
# new_df_03 = df_channelCode_Des.pivot("channel_code", "des", "zh_name")
# sns.heatmap(new_df_03, annot=True, cmap="CMRmap_r", fmt='g')
# plt.show()

# 单量比例分布占比
# df_count = df.shape[0]
# df_channelCode_Des['rate'] = df_channelCode_Des['zh_name'].apply(lambda x: round(x / df_count, 2))
# new_df_04 = df_channelCode_Des.pivot("channel_code", "des", "rate")
# sns.heatmap(new_df_04, annot=True, cmap="CMRmap_r", fmt='g')
# plt.show()

# 计算营收占比
# total_fee = df['total_fee'].sum()
# df_channelCode_Dis = df.groupby(['channel_code', 'des'], as_index=False)['total_fee'].sum()
# df_channelCode_Dis['rate'] = df_channelCode_Dis['total_fee'].apply(lambda x: round(x/total_fee , 3))
# print('-----***************----')
# # 每个颗粒度下的 total_fee 分布占比
# new_df_01 = df_channelCode_Dis.pivot("channel_code", "des", "rate")
# print(new_df_01)
# print('**************************************')
# sns.heatmap(new_df_01, annot=True, cmap="CMRmap_r", fmt='g')
# plt.show()


# # 重量区间标签-channel_code 分布图
# df_Sign_channelCode = df.groupby(['标签', 'channel_code'], as_index=False).count()
# print('=======*********========')
# print(df_Sign_channelCode)
# # print(df_channelCode_Des.to_csv('./单量统计.csv', index= False, encoding="utf_8_sig" ))
# df_sign_chan = df_Sign_channelCode.pivot("channel_code", "标签", "zh_name")
# sns.heatmap(df_sign_chan, annot=True, cmap="CMRmap_r", fmt='g')
# plt.show()

# # 重量区间标签-channel_code 分布图
# totalCount = df.shape[0]
# df_Sign_channelCode = df.groupby(['标签', 'channel_code'], as_index=False).count()
# df_Sign_channelCode['rate'] = df_Sign_channelCode['zh_name'].apply(lambda x: round(x / totalCount, 2))
# # print(df_channelCode_Des.to_csv('./单量统计.csv', index= False, encoding="utf_8_sig" ))
# df_sign_chan = df_Sign_channelCode.pivot("channel_code", "标签", "rate")
# sns.heatmap(df_sign_chan, annot=True, cmap="CMRmap_r", fmt='g')
# plt.show()




#  单量称重分布（展示 US-优先 ， US-特惠 重量分布箱型图）
# df_us = df[df['des'] == 'US']
# print(df_us['channel_code'].value_counts())
#
# for i ,j in df_us.groupby('channel_code'):
#     # print(j.describe(percentiles=[0.25,.5,.75,.8,0.85,0.9]))
#     print('------')
#     if i == 'CNE全球优先':
#         print('CNE全球优先: ')
#         # print(j[j['weight'] <= 1.75]['weekly'].count())
#         # print(j[j['weight'] > 1.75]['weekly'].count())
#         j['分组'] = pd.cut(j['weight'], [0, 0.2, 0.453, 0.855,1.555, 2, 20], right=True)
#         j['标签'] = pd.cut(j['weight'], [0, 0.2, 0.453, 0.855,1.555, 2, 20], labels=['0-0.2KG', '0.201-0.453KG', '0.454-0.855KG', '0.856-1.555KG','1.556-2KG', '2.001-20KG'], right=True)
#         print(j.groupby("分组").agg({"标签": "count"}))
#     elif i == 'CNE全球特惠':
#         print('CNE全球特惠: ')
#         # print(j[j['weight'] <= 1.75]['weekly'].count())
#         # print(j[j['weight'] > 1.75 ]['weekly'].count())
#         j['分组'] = pd.cut(j['weight'], [0, 0.2, 0.453, 0.855,1.555, 2, 20] , right =True)
#         j['标签'] = pd.cut(j['weight'],[0, 0.2, 0.453, 0.855,1.555, 2, 20], labels=['0-0.2KG', '0.201-0.453KG', '0.454-0.855KG', '0.856-1.555KG','1.556-2KG', '2.001-20KG' ] , right =True  )
#         print(j.groupby("分组").agg({"标签":"count"}))
# sns.swarmplot(x=df_us['channel_code'], y=df_us['weight'])
# sns.boxplot(x="channel_code", y="weight", data = df_us )
# plt.show()
#





