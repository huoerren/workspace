#coding=utf-8

import copy
import pandas as pd

dicts={'name':['xiaohua','xiaogang','xiaoming'] ,
       'age':[12.0,3.0,4.0],
       'add':['xiamen','shenzhen','gunagzhou'],
       'class':[2,1,2]}
data = pd.DataFrame(dicts,index=['l0','l1','l2'])

print(data['name'].dtype , data['name'].dtype == 'object')
print(data['age'].dtype , data['age'].dtype == 'int' )

data.rename(columns = {"age":'age_new'},
            index={'l1':'l1_new'},
            inplace = True)

# print(data)
print('-------------------------------')
data_02 = copy.copy(data)
data_02.drop(['age_new'] ,axis= 1 ,inplace=True)

data_03 = copy.copy(data)
data_03_01 = data_03.dropna(how='all')
data_03_02 = data_03.dropna(how='any')

data_04 = copy.copy(data)
# print(data_04.values)

# print(data_04['class'].sum())

df = pd.DataFrame({'类别':['水果','水果','水果','蔬菜','蔬菜','肉类','肉类'],
                '产地':['美国','中国','中国','中国','新西兰','新西兰','美国'],
                '水果':['苹果','梨','草莓','番茄','黄瓜','羊肉','牛肉'],
               '数量':[5,5,9,3,2,10,8],
               '价格':[5,5,10,3,3,13,20]})

print(df)
print('-------------------------------')
print(df.pivot_table(index=['类别','产地']))
print('*******************************')
print(df.pivot_table('价格',index='产地',columns='类别',
                     margins=False ,fill_value=0))
print('-=============================-')
print( pd.crosstab(df['产地'],df['类别'],margins= False ))  # 按类别分组，统计各个分组中产地的频数)