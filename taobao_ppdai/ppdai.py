
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import  os
import sys

pd.set_option('display.max_columns',1000)
pd.set_option('display.width',10100)

plt.rcParams['font.sans-serif'] = ['SimHei'] # 用来正常显示中文标签
plt.rcParams['font.family']='sans-serif'
plt.rcParams['axes.unicode_minus'] = False # 用来正常显示负号

# LC_data   = pd.read_csv('LC.csv')
# df = pd.read_csv('LC.csv' ,sep='delimiter',engine='python' ,encoding='ISO-8859-1')

# df = pd.read_csv('2011.csv')
# print(len(df.columns.values.tolist())) # 74
df2 = pd.read_csv('E:/LoanStats_securev1_2018Q1.csv',encoding='gbk')
print(len(df2.columns.values.tolist()))


# pd_issue_d = pd.to_datetime(df['issue_d'])
# df['pd_issue_d'] = pd_issue_d
# print(df['pd_issue_d'].value_counts())
# df['pd_issue_d'] = pd_issue_d
# df = df[(df['pd_issue_d']>='2011-01-01') & (df['pd_issue_d']< '2012-01-01')]
# print(df.shape)


























