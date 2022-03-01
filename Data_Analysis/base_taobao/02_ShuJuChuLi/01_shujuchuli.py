#coding=utf-8

import os
import numpy as np
import pandas as pd



pd.set_option('expand_frame_repr', False) # 禁止数据换行显示
pd.set_option('display.max_columns',200)
pd.set_option('display.width',1000)


os.chdir('D:/forMySelf/data/data')
df_CH2018 = pd.read_csv('CH2018.csv')
df_CH2013 = pd.read_csv('business_census2013.csv')

# print(df_CH2018.columns)#数据如下 (4270312, 55)->(行，列)
# print(df_CH2013.columns)#数据如下 (3186156, 33)->(行，列)

def mapper(x): #
    return x.replace(' ','').lower() # 列名全部小写且去空格

# 规范表的列名：2013 和 2018 这两张表中列名有不规范的地方 如大小写不统一，列名中有空格等
df_CH2018.rename(columns=mapper, inplace=True)
df_CH2013.rename(columns=mapper, inplace=True)

#df_CH2018.columns  #查看 2018年表的列名
#df_CH2013.columns  #查看 2013年表的列名


df_CH2018.rename(columns={'siccode.sictext_1':'siccode','regaddress.postcode':'postcode'}, inplace = True)


# Step one_1：

df_CH2018.dropna(subset=['postcode','incorporationdate','siccode','companynumber'],inplace=True)
# print(df_CH2018.shape) # (4270312, 55)
# df_CH2018.to_csv('C:/Users/panxin/Desktop/steps/step_1/df_CH2018.csv')

df_CH2013.dropna(subset=['postcode','incorporationdate','siccode','companynumber'],inplace=True)
# print(df_CH2013.shape)#(2429973 x 33 )
# df_CH2018.to_csv('C:/Users/panxin/Desktop/steps/step_1/df_CH2013.csv')

dList= [
                26110,26120,26200,26400,26511,26512,26800,33130,
                58210,58290,62011,62012,62020,62030,62090,63110,
                63120,95110,21100,21200,26600,26701,32500,72110,75000,86101,
                86102,86210,86220,86230,86900,26301,26309,26702,58110,58120,58130,58141,58142,
                   58190,59111,59112,59113,59120,59131,59132,59133,
                   59140,59200,60100,60200,61100,61200, 61300,61900,
                   63910,63990,73110,73120,73200,74100,74201,74202,
                   74203,74209,95120,19201,19209,20110,20120,20130,95210,95220,95250,
                     20140,20150,20160,20170,20200,20301,20302,20411,
                     20412,20420,20510,20520,20530,20590,20600,25210,
                     25300,25400,26513,26514,26520,27110,27120,27200,
                     27310,27320,27330,27400,27510,27520,27900,28110,
                     28120,28131,28132,28140,28150,28210,28220,28230,
                     28240,28250,28290,28301,28302,28410,28490,28910,
                     28921,28922,28923,28930,28940,28950,28960,28990,
                     29100,29201,29202,29203,29310,29320,30110,30120,
                     30200,30300,30400,30910,30920,30990,32120,32401,
                     33120,33140,33150,33160,33170,51101,51102,51210,51220,71111,71112,71121,71122,
                        71129,71200,72190,72200,74901,74902,85410,85421,
                        85422
        ]

#Step one_2
df_CH2013 = df_CH2013[df_CH2013['siccode'].isin(dList)]

df_CH2018["siccode_01"] = df_CH2018["siccode"].map(lambda x: x.split(' ')[0])

#去掉 df_CH2018['siccode_01'] == None 的行
df_CH2018.dropna(subset=['siccode_01'],inplace=True)

def changIntoInt(x):
    if x != None and x.strip() != 'None' and len(x.strip())> 0 :
        return int(x)
    else:
        return None

df_CH2018['siccode_01'] = df_CH2018.apply(lambda row: changIntoInt(row['siccode_01'] ), axis=1)

#去掉 df_CH2018['siccode_01'] == None 的行
df_CH2018.dropna(subset=['siccode_01'],inplace=True)
df_CH2018 = df_CH2018[df_CH2018['siccode_01'].isin(dList)]


# #Step one_3
df_merge = pd.merge(df_CH2018,df_CH2013, on='companynumber') # 利用 companynumber 这个字段进行关联
df_List_2 = list(df_merge['companynumber'])
df_CH2018_1  = df_CH2018[df_CH2018['companynumber'].isin(df_List_2)]
df_CH2013_1  = df_CH2013[df_CH2013['companynumber'].isin(df_List_2)] # 文档上要求得到的新表

print(df_CH2018_1.shape) # (250780, 56)
print('------------------------------------------------------------------')
print(df_CH2013_1.shape) # (250780, 33)

# Step two
df_CH2013_2 = df_CH2013_1[ (df_CH2013_1['countryoforigin'].str.contains('UNITED KINGDOM'))
                          & ( df_CH2013_1['incorporationdate'].str.contains('2013')  )]

#为 df_CH2013_2 添加新列 column_other，默认值为 0
df_CH2013_2['column_other'] = 0
def valuation_formula_02(x):
    if x == None or str(x) == '':
        return 1
    else:
        return 0

df_CH2013_2['column_other'] = df_CH2013_2.apply(lambda row: valuation_formula_02(row['dissolutiondate'] ), axis=1)
df_CH2013_2.to_csv('C:/Users/panxin/Desktop/df_CH2013_2_countryoforigin.csv')

#Step three

#为 df_CH2018_1 添加新列 ifCompanyExisted，默认值为 1
# df_CH2018_1['ifCompanyExisted'] = 1
# def valuation_formula(x):
#     if x == None or str(x) == '':
#         return 1
#     else:
#         return 0
#
# df_CH2018_1['ifCompanyExisted'] = df_CH2018_1.apply(lambda row: valuation_formula(row['dissolutiondate'] ), axis=1)
# df_CH2018_1.to_csv('C:/Users/panxin/Desktop/df_CH2018_1.csv')



