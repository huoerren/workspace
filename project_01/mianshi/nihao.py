#coding=utf-8

import warnings
warnings.filterwarnings('ignore')

import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_columns',100)
pd.set_option('display.width',1000)

import numpy as np

from matplotlib import pyplot as plt
plt.rcParams['font.family']="SimHei"
import seaborn as sns


from sklearn import linear_model

# 读入原始数据
filePath = 'C:/Users/Administrator/Desktop/20190922-TM case estimated.xlsx'

df = pd.read_excel(filePath,skiprows=1)
df = df.drop(['Unnamed: 0'] ,axis=1)
df = df.rename(columns = {'Unnamed: 1':'year','reported AIDS':'amount'})
df = df[(df['year']>1994) & (df['year']<2016) ]

# ['year', 'reported AIDS']
df_train = df[~df['amount'].isnull()]
x_train = df_train[df_train.columns[0:1]]
y_train = df_train[df_train.columns[1:]]

y_train= np.log1p(y_train)


df_test = df[df['amount'].isnull()]
x_test = df_test[df_test.columns[0:1]]

model_BRidge = linear_model.BayesianRidge(n_iter=1200 ,alpha_1=1.e-9)
model_BRidge.fit(x_train , y_train)
pres = model_BRidge.predict(x_test)

# print(np.expm1(pres))
# model_BRidge2 = linear_model.BayesianRidge()
# model_BRidge2.fit(x_train , y_train)
# pres2 = model_BRidge2.predict(x_test)
# print(np.expm1(pres2))


for i in range(len(x_test)):
    df.amount[df.year == x_test['year'].to_list()[i] ] = np.expm1(pres[i])

df['amount'] = df['amount'].apply(lambda x : round(x,0))
fig, ax = plt.subplots()

ax.plot(df['year'], df['amount'],color='b' )
# ax.fill_between(
#                  df['amount']-1.96 ,
#                  df['amount']+1.96  )

plt.xticks(df['year'],rotation=30 )
ax.set_xlabel('年份')
ax.set_ylabel('数量')

plt.show()



