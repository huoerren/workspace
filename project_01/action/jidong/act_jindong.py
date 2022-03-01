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

import copy
import os
os.chdir('C:/Users/Administrator/Desktop')

filePath = 'churn-bigml-80.csv'
df = pd.read_csv(filePath)
# print(df.isnull().values)
# print(df[df.isnull().values == True]) # 有空值的行会被打印出来

# 查看缺失值的
# import missingno as msno
# columns = df.columns
# msno.bar(df[columns])
# plt.show()

# print(df.isnull().sum(axis=1)) # 返回每一行缺失值的总数
# print(df.isnull().sum(axis=0)) # 返回每一列缺失值的总数

# df = df.dropna(subset=["name"]) # 每一行中，如果 name 字段为空值，则删除该行
# df = df[np.isnan(df['name']) == False] # 获取name 字段不为NaN的行


# print(df['State'].dtype)
# 注意 df.columns  和 df.columns.values两者之间的区别
# df.columns : Index  ;  df.columns.values : ndarray

cat_columns = [col for col in df.columns.values if df[col].dtype == 'object']
cat = df[cat_columns]
df02 = copy.copy(df)
df02 = pd.DataFrame(df02)
num = df.drop(columns= cat_columns , axis=1,inplace=True)
num = df.drop(columns=['Churn'],axis=1 ) # Churn 这列是目标列
num02 = copy.copy(num) # copy之后的数据类型会变化（num是DataFrame,num02是 ndarry）
num03 = copy.copy(num)


y_label = df['Churn']
y_label = y_label.to_frame() # 目标列需要转换成 dataframe
y_label['Churn'] = y_label['Churn'].apply(lambda x:int(x))

corr_matrix = num.corr()
cols = num.columns
# cm = np.corrcoef(num[cols].values)

# 第一种
# plt.figure(figsize=(20,10))
# sns.set(font_scale=1)
# sns.heatmap(corr_matrix , cmap='YlGnBu' , annot= True)
#
# plt.show()

from statsmodels.stats.outliers_influence import variance_inflation_factor

# 第二种
# VIF 膨胀因子 （和线性相关有联系）
VIF = pd.DataFrame()
VIF['features'] = num.columns
VIF['VIF'] = [variance_inflation_factor(
    num.values,i) for i in range(num.shape[1])]

# print(VIF)

#第三种(比较消耗资源)
# sns.pairplot(num)


#对共线性特征做处理（一般做法是取两者相除，再删掉两者）
num['average day charge'] = num['Total day charge']/num['Total day minutes']
num['average eve charge'] = num['Total eve minutes']/num['Total eve charge']
num['average night charge'] = num['Total night minutes']/num['Total night charge']
num['average intl charge'] = num['Total intl minutes']/num['Total intl charge']

num = num.drop(columns=['Total day minutes','Total eve minutes','Total night minutes','Total intl minutes',
                        'Total day charge','Total eve charge','Total night charge','Total intl charge'])


corr_matrix = num.corr()
cols = num.columns
cm = np.corrcoef(num[cols].values)
plt.figure(figsize=(40,20))
sns.set(font_scale=2)
sns.heatmap(corr_matrix,cmap='YlGnBu',annot=True)

# plt.show()


num = num.fillna(0) # 为空值赋值0
num = num[num.isnull().values == True]
# print(num)


# PCA

from scipy import io as spio
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

X = num02
pca = PCA(n_components=12) # 由上面计算结果可知有4组变量之间的存在线性共性
X = pca.fit_transform(X)
X = pd.DataFrame(X)
corr_matrix = X.corr()
cols = X.columns
plt.figure(figsize=(40,20))
sns.set(font_scale=2)
sns.heatmap(corr_matrix,cmap='YlGnBu',annot=True)

from sklearn import preprocessing
scaler = preprocessing.StandardScaler()
num03 = pd.DataFrame(num03)
# print(num03['Area code'].value_counts())
num04 = num03.drop(['Area code'] ,axis= 1)
for i in  num04.columns:
    scale_param = scaler.fit(num04[[i]])
    num04[i] = scaler.fit_transform(num04[[i]],scale_param)

# sklearn 中的OneHotEncoder 类得到亚特征，pandas 中的get_dummies()得到亚变量
# get_dummies()默认对DataFrame中所有字符串类型的列进行独热编码

Areacode_dummies = pd.get_dummies(num03['Area code'],prefix= 'Area_code')
# print(Areacode_dummies['Area_code_408'].dtype) # uint8
# uint8 这种数据类型实际上是一个 char , 会输出ASCII 码，不是真正的数字
for i in Areacode_dummies.columns:
    Areacode_dummies[i] = np.int64(Areacode_dummies[i])

num05 = pd.concat([num04,Areacode_dummies] ,axis=1,join='inner')
# print(num05.head())
# plt.show()

cat = df02[cat_columns]
cat = pd.get_dummies(cat)


processed_data = pd.concat([num04,cat,y_label] , axis=1)
processed_data.to_csv('processed_data.csv')

# cat = pd.get_dummies(cat)
# print(cat.head())






