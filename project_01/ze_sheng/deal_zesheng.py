#coding=utf-8

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as ss
import time

import datetime

sns.set_style(style="darkgrid")#配置样式
sns.set_context(context="poster",font_scale=1.5)#配置字体
sns.set_palette(sns.color_palette("RdBu", n_colors=7))#配置色板


pd.set_option('expand_frame_repr', False) # 禁止数据换行显示
pd.set_option('display.max_columns',100)
pd.set_option('display.width',1000)

df = pd.read_excel('downlog.xlsx')
df['pkid'] = df['pkid'].astype('str')

# print(df.shape )  # (39596, 8)
# print(df.columns)
# 'pkid', 'downloaderCode', 'downloaderName', 'datatype', 'downloadType', 'downloadTime', 'payintegral', 'Ip'

df = df.dropna(subset=['pkid']) # (39596, 8)

pkid_count = df['pkid'].value_counts()
datatype_count = df['datatype'].value_counts()

# print(datatype_count) # 1:法规 33968  ； 2:标准 -  5628
df_datatype_1 = df[df['datatype'] ==1 ]['downloadType'].value_counts()
# print(datatype_1) #  0-VIP会员下载 : 22015 ; 1-积分下载: 11953
df_datatype_2 = df[df['datatype'] ==2 ]['downloadType'].value_counts()
# print(datatype_2) #  0-VIP会员下载 : 2733  ; 1-积分下载 : 2895

df['dDate'] = df['downloadTime'].apply(lambda x: x.strftime('%Y-%m-%d'))
df['dTime'] = df['downloadTime'].apply(lambda x: x.strftime('%H:%M:%S'))

# df.drop('downloadTime' ,axis=1 , inplace=True )

dDateCount   = df['dDate'].value_counts()
df.loc[df['datatype'] == 1 ,'datatype'] = '法规'
df.loc[df['datatype'] == 2, 'datatype'] = '标准'

df.loc[df['downloadType'] == 0 ,'downloadType'] = 'VIP会员下载'
df.loc[df['downloadType'] == 1, 'downloadType'] = '积分下载'


df_d_downLoadType = df.join(pd.get_dummies(df['downloadType'],prefix='downloadType'))
print(df_d_downLoadType)


# print(df.tail())

# print(dDateCount)



