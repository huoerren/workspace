#coding=utf-8


import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)

df = pd.read_excel('测试.xlsx')


sub_01 = df.dropna(subset=["装车用时"]) # 通过参数来删除age和sex中含有空数据的全部
sub_01_2 = sub_01.sort_values(by=["装车用时"], ascending=True)
print(sub_01_2.describe(percentiles=[0.95])['装车用时'])
print('-------------')

sub_02 = df.dropna(subset=["起飞用时"]) # 通过参数来删除age和sex中含有空数据的全部
sub_02_2 = sub_02.sort_values(by=["起飞用时"], ascending=True)
print(sub_02_2.describe(percentiles=[0.95])['起飞用时'])
print('-------------')
sub_03 = df.dropna(subset=["飞行用时"]) # 通过参数来删除age和sex中含有空数据的全部
sub_03_2 = sub_03.sort_values(by=["飞行用时"], ascending=True)
print(sub_02_2.describe(percentiles=[0.95])['飞行用时'])










# 全程
# sub_all = df.dropna(subset=["妥投用时"]) # 通过参数来删除age和sex中含有空数据的全部
# sub_all.to_excel("./全程.xlsx")

# 统计出 /全程.xlsx 文档中的 全程时效后 再执行下面的代码
# df = pd.read_excel('全程.xlsx')
# sub_all = df.sort_values(by=["全程"], ascending=True)
# print(sub_all.describe(percentiles=[0.95])['全程'])



#
# df_01 = df.sort_values(by=["装车用时"], ascending=True)
# print(df_01.describe(percentiles=[0.95])['装车用时'])
#
# df_02 = df.sort_values(by=["起飞用时"], ascending=True)
# print(df_02.describe(percentiles=[0.95])['起飞用时'])
#
# df_03 = df.sort_values(by=["飞行用时"], ascending=True)
# print(df_03.describe(percentiles=[0.95])['飞行用时'])

# print(df.head())
















