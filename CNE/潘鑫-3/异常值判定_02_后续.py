#coding=utf-8

import pandas as pd

import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)
import openpyxl
import pymysql
import datetime,time
nows=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')


print(datetime.date.today())
import numpy as np

filePath = r'C:\Users\hp\Desktop\cujia-11data\02_出库-装车.csv'
df = pd.read_csv(filePath)
print(df.head())
print('============================================')
df['首扫时间'] = pd.to_datetime(df['首扫时间'])
df['封袋时间'] = pd.to_datetime(df['封袋时间'])

df["用时"] = (df["封袋时间"]-df["首扫时间"]).astype('timedelta64[s]')
df["用时"] = round(df["用时"]/86400,2)

for df1, df2 in df.groupby(['channel_name','des']):
    df3 = df2.sort_values(by=["用时"] ,ascending= True)
    # print(df3.describe(percentiles=[0.9])['用时'] )
    print(df1,'------ ',  df3.shape[0] ,' ----- ' ,  (df3.describe(percentiles=[0.75])['用时']['75%'] - df3.describe(percentiles=[0.25])['用时']['25%'])*3 + df3.describe(percentiles=[0.75])['用时']['75%']  )
    print('----------------------------')



