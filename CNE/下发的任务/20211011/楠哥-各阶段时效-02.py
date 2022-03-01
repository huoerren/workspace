#coding=utf-8


import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)


import datetime
import time
# df_01_02['交付时间'] =  df_01_02['交付时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))

# -- 01-02
filePath_01_02 = r'C:\Users\hp\Desktop\ban\xin-01-02.csv'
df_01_02 = pd.read_csv(filePath_01_02 )
df_01_02['装车时间'] = df_01_02['装车时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
df_01_02['首扫时间'] = df_01_02['首扫时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
df_01_02['时间差'] = df_01_02['装车时间']  - df_01_02['首扫时间']
print('----------- 01-02 ----------- ')
print(df_01_02.describe(percentiles=[.9,.95]))
df_01_02.to_csv(r'C:\Users\hp\Desktop\ban\export-01-02.csv' , index= False, encoding="utf_8_sig")


# -- 03-04
filePath_03_04 = r'C:\Users\hp\Desktop\ban\xin-03-04.csv'
df_03_04 = pd.read_csv(filePath_03_04 )
df_03_04['正式单到达机场时间'] = df_03_04['正式单到达机场时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
df_03_04['起飞时间'] =          df_03_04['起飞时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
df_03_04['时间差'] =            df_03_04['起飞时间']  - df_03_04['正式单到达机场时间']
print('----------- 03-04 ----------- ')
print(df_03_04.describe(percentiles=[.9,.95]))
df_03_04.to_csv(r'C:\Users\hp\Desktop\ban\export-03-04.csv' , index= False, encoding="utf_8_sig")

# -- 04-05
filePath_04_05 = r'C:\Users\hp\Desktop\ban\xin-04-05.csv'
df_04_05 = pd.read_csv(filePath_04_05 )
df_04_05['起飞时间'] = df_04_05['起飞时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
df_04_05['主单落地时间'] = df_04_05['主单落地时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
df_04_05['时间差'] = df_04_05['主单落地时间']  - df_04_05['起飞时间']
print('----------- 04-05 ----------- ')
print(df_04_05.describe(percentiles=[.9,.95]))
df_04_05.to_csv(r'C:\Users\hp\Desktop\ban\export-04-05.csv' , index= False, encoding="utf_8_sig")
#
# -- 05-06
filePath_05_06 = r'C:\Users\hp\Desktop\ban\xin-05-06.csv'
df_05_06 = pd.read_csv(filePath_05_06 )
df_05_06['主单清关时间'] = df_05_06['主单清关时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
df_05_06['主单落地时间'] = df_05_06['主单落地时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
df_05_06['时间差'] = df_05_06['主单清关时间']  - df_05_06['主单落地时间']
print('----------- 05-06 ----------- ')
print(df_05_06.describe(percentiles=[.9,.95]))
df_05_06.to_csv(r'C:\Users\hp\Desktop\ban\export-05-06.csv' , index= False, encoding="utf_8_sig")
#
# # -- 06-07
# filePath_06_07 = r'C:\Users\hp\Desktop\楠哥-20211014\xin-06-07.csv'
# df_06_07 = pd.read_csv(filePath_06_07 )
# df_06_07['主单清关时间'] = df_06_07['主单清关时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
# df_06_07['交付时间'] = df_06_07['交付时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
# df_06_07['时间差'] = df_06_07['交付时间']  - df_06_07['主单清关时间']
# print('----------- 06-07 ----------- ')
# print(df_06_07.describe(percentiles=[.9,.95]))
#
# # -- 07-08
# filePath_07_08 = r'C:\Users\hp\Desktop\楠哥-20211014\xin-07-08.csv'
# df_07_08 = pd.read_csv(filePath_07_08 )
# df_07_08['妥投时间'] = df_07_08['妥投时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
# df_07_08['交付时间'] = df_07_08['交付时间'].map(lambda  x: datetime.datetime.strptime(x, '%d/%m/%Y %H:%M:%S'))
# df_07_08['时间差'] = df_07_08['妥投时间']  - df_07_08['交付时间']
# print('----------- 07-08 ----------- ')
# print(df_07_08.describe(percentiles=[.9,.95]))
#

