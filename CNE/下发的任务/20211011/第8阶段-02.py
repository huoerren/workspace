#coding=utf-8

import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)

import time
import datetime
import numpy as np

filePath_01_02 = r'C:\Users\hp\Desktop\交付到妥投-下\xin-下.csv'
df_01_02 = pd.read_csv(filePath_01_02)

df_01_02['交付时间'] =  df_01_02['交付时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
df_01_02['妥投时间'] =  df_01_02['妥投时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
df_01_02['时间差'] =  df_01_02['妥投时间']  - df_01_02['交付时间']
print('----------- 01-02 ----------- ')
print(df_01_02['时间差'].mean() )
print(df_01_02.describe(percentiles=[.25,.50,.75,.90,.95]))
# print(df_01_02[df_01_02['内单号'] == '3A5V587215421'])
# df_01_02.to_csv(r'C:\Users\hp\Desktop\demo-2.csv' , index= False, encoding="utf_8_sig" )

# print(np.percentile(df_01_02['时间差'], [0, 25, 75, 90, 100]))





