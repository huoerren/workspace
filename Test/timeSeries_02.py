#coding=utf-8

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


from statsmodels.tsa.stattools import adfuller as ADF
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

data = pd.read_csv('C:/Users/hp/Desktop/促佳单量.csv',encoding='gbk',index_col = '业务日期')
# data.plot()
# plt.show()

# 自相关图
from statsmodels.graphics.tsaplots import plot_acf
plot_acf(data)


# *稳性检测
from statsmodels.tsa.stattools import adfuller as ADF
print(u'原始序列的ADF检验结果为：', ADF(data[u'单量']))

# 差分后的结果
D_data = data.diff().dropna()
D_data.columns = [u'单量差分']
D_data.plot()  # 时序图
plot_acf(D_data).show()  # 自相关图
from statsmodels.graphics.tsaplots import plot_pacf
plot_pacf(D_data).show()  # 偏自相关图
print(u'差分序列的ADF检验结果为：', ADF(D_data[u'单量差分']))  # *稳性检测
plt.show()



