# python 实现
import pandas as pd

df= pd.DataFrame([['专业技术人员','A',1],['国家机关人员','C',2],
                  ['国家机关人员','A',1],['商业人员','C',4],['国家机关人员','B',5]],
                 columns=['job','class','value'])
# print(df)
# print('-----------')
# columns表示你要引入分箱的变量，drop_first=0 代表使用 n-1个虚拟变量
df = pd.get_dummies(df,columns=['job','class'],drop_first=0)
# print(df)


df2 = pd.DataFrame(['正常','3级高血压','正常','2级高血压','正常','正常高值','1级高血压'],columns=['blood_pressure'])
dic_blood = {'正常':0,'正常高值':1,'1级高血压':2,'2级高血压':3,'3级高血压':4}
print('----------------------------------')
# print(df2)
df2['blood_pressure_enc'] = df2['blood_pressure'].map(dic_blood)
# print(df2)


df3 = pd.DataFrame([[22,1],[13,1],[33,1],[52,0],[16,0],[42,1],[53,1],[39,1],[26,0],[66,0]],
                   columns=['age','Y'])
#print(df)
df3['age_bin_1'] = pd.qcut(df3['age'],3) #新增一列存储等频划分的分箱特征
df3['age_bin_2'] = pd.cut(df3['age'],3)  #新增一列存储等距划分的分箱特征
print(df3)

