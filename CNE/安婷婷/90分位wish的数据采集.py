#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)


import openpyxl
import pymysql
import datetime,time
import pyecharts.options as opts
from pyecharts.charts import Line,Grid,Page
from pyecharts.globals import ThemeType
nows=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
# 目的文件存放地址
file=r'F:\其他部门\分拣路由90分位妥投天数统计'
print(datetime.date.today())
import numpy as np
# #数仓连接
con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()

# days="BETWEEN '2021-03-31 16:00:00' and '"+time_yes+"'"1
# days="BETWEEN '2021-06-30 16:00:00' and '2021-06-30 16:00:00' "
days="BETWEEN '2021-06-30 16:00:00' and "+"'"+nows+"'"

print(days)

S1 ="""
SELECT 
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') 业务日期, 
lgo.channel_code 渠道,
lgo.des 国家, 
lgo.supply_channel_code 尾端配送商,
(case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
count(1) 票数,
round(TIMESTAMPDIFF(hour,lgo.gmt_create,lgo.delivery_date)/24,1) 妥投时间间隔
FROM 
lg_order lgo
where
lgo.customer_id in(1151368,1181372,1151370,1181374)
and lgo.platform ='WISH_ONLINE'
and lgo.gmt_create {}
and lgo.is_deleted='n'
and lgo.channel_code in('CNE全球经济','CNE全球特惠','CNE全球优先','CNE全球通挂号','CNE全球通平邮')
group by
1,2,3,4,5,7
""".format(days)
print('-------------- S1 --------------')
print(S1)

def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df
d1=execude_sql(S1)

d1['业务日期']=pd.to_datetime(d1['业务日期'])
print('--------------- 001 ----- d1.head() ---------------')
print(d1.head())

d1=d1.reset_index()
print('---------------- 002 ---- d1.head() ---------------')
print(d1.head())

d1['year']=d1['业务日期'].map(lambda X:X.year)
d1['month']=d1['业务日期'].map(lambda X:X.month)
print('---------- 003 ----- d1.head() : -------------------------------')
print(d1.head())

def tj(a,b):
    if a==2020:
        return "20201"
    elif b in [1,9,10,11,12]:
        return "20211"
    else:
        return "20210"
d1['淡旺季']=d1.apply(lambda x:tj(x['year'],x['month']),axis=1)
print('------------ 004 --------  d1.head(): ----------------')
print(d1.head())

dw = pd.DataFrame(pd.date_range(start='20210101',end=nows,periods=None,freq='D'),columns=['业务日期'])

dw['业务日期']=pd.to_datetime(dw['业务日期'])

print('--------------- 001 dw.head() ---------------')
print(dw.head())
#原来的代码 -by antingting
# dw['周序数']=dw['业务日期'].dt.isocalendar().week

#-------------------------- 替换的代码 -by panxin  --- 开始 ------------------------

# 判断本年度 01月01号是不是在为第1周，如果是则返回 1 ，如果不是 返回 0
def pDate():
    if pd.to_datetime('2021-01-01').isocalendar()[1] == 1:
        return 1
    else:
        return 0

flag = pDate()

dw['计算列'] = dw['业务日期'].apply(lambda x: x.isocalendar()[0])
dw['周序数'] = dw['业务日期'].apply(lambda x: x.isocalendar()[1]+1 if flag == 0 else x.isocalendar()[1])
print('------------------ 修改前的 ----------------')
print(dw.head())

for i in dw.index:
    if dw.loc[i,'计算列'] == 2020:
        dw.loc[i , '周序数' ] = 1
print('---------------- 修改后的： -----------------')
print(dw.head())
print('------------- 注意点1 -----------------')
print(dw.columns.to_list())
del dw['计算列']
print('------------- 注意点2 -----------------')
print(dw.columns.to_list())

#-------------------------- 替换的代码 -by panxin  --- 结束 ------------------------

dw['moon']=dw['业务日期'].dt.month.astype('str')
dw['day']=dw['业务日期'].dt.day.astype('str')
dw['日期']=dw['moon']+'.'+dw['day']
dw_min=dw[['周序数','日期']].drop_duplicates(['周序数'],keep='first')
dw_max=dw[['周序数','日期']].drop_duplicates(['周序数'],keep='last')
dwm=pd.merge(dw_min,dw_max,on=['周序数'],how='outer')
dwm['周期']=dwm['日期_x']+'-'+dwm['日期_y']
print('------------ dwm.head() -----------------')
print(dwm.head())

dw=pd.merge(dw,dwm,on=['周序数'],how='left')
# dw['周期']=dwm['周序数'].map(dict(zip(dwm['周序数'],dwm['周期'])))
# # dw=dw.set_index(['周序数','dayofweek']).stack().unstack(['dayofweek',-1])
print('---------------- dw.head(): -----------')
print(dw.head())

d2=pd.merge(d1,dw[['业务日期','周序数','周期']],on=['业务日期'],how='left')
print('-------------------- d2.head(): ----------------')
print(d2.head())

dyt=d2
print(dyt)

dyt['妥投时间间隔']=dyt['妥投时间间隔'].replace(np.nan,60)
# 按周聚合
d3=dyt.groupby(['淡旺季','周序数','周期','渠道', '国家', '尾端配送商', '是否妥投', '妥投时间间隔'])['票数'].sum().reset_index()
d3t=dyt.groupby(['淡旺季','周序数','周期','渠道', '国家',  '是否妥投', '妥投时间间隔'])['票数'].sum().reset_index()
print('-------------- d3.head(): ------------------')
print(d3.head())
print('-------------- d3t.head(): ------------------')
print(d3t.head())

print('------------------------------------- d4=dyt.groupby - start ----------------------------------------')
print(dyt.head())
print('------------------------------------- d4=dyt.groupby - end -----------------------------------------')
#总单量
# d4=dyt.groupby(['淡旺季','周序数','周期','渠道', '国家', '尾端配送商']).sum(['票数']).reset_index() # 原来的
d4=dyt.groupby(['淡旺季','周序数','周期','渠道', '国家', '尾端配送商'])['票数'].sum().reset_index() # 现在的
# d4t=dyt.groupby(['淡旺季','周序数','周期','渠道', '国家']).sum(['票数']).reset_index() # 原来的
d4t=dyt.groupby(['淡旺季','周序数','周期','渠道', '国家'])['票数'].sum().reset_index() # 现在的

d4=d4.rename(columns={'票数':'总票数'})
d4t=d4t.rename(columns={'票数':'总票数'})

print('------------------------- d4.head() ------------------')
print(d4.head())
print('------------------------- d4t.head() ------------------')
print(d4t.head())

# 已妥投
# d4 = d4.sort_values(by=['总票数',],ascending=False)
d5=pd.merge(d3[d3.是否妥投=='已妥投'],d4,on=['淡旺季','周序数','周期','渠道', '国家', '尾端配送商'],how='right').sort_values(['周序数','渠道', '尾端配送商', '国家','妥投时间间隔'])
d5t=pd.merge(d3t[d3t.是否妥投=='已妥投'],d4t,on=['淡旺季','周序数','周期','渠道', '国家'],how='right').sort_values(['周序数','渠道',  '国家', '妥投时间间隔'])
# d5=d5.sort_values(['妥投时间间隔','周序数'])
print('------------------ d5.head() ----------------')
print(d5.head())
print('------------------ d5t.head() ----------------')
print(d5t.head())

d5t['妥投票数']=d5t.groupby(['淡旺季','周序数','周期','渠道', '国家'])['票数'].cumsum()
d5['妥投票数']=d5.groupby(['淡旺季','周序数','周期','渠道', '国家', '尾端配送商'])['票数'].cumsum()
d5['妥投率']=d5['妥投票数']/d5['总票数']
d5t['妥投率']=d5t['妥投票数']/d5t['总票数']

# 妥投率 达标了则设置为1 否则 为空 ：
d5.loc[d5[d5['妥投率']>=0.9].index,'是否达标']=1
d5t.loc[d5t[d5t['妥投率']>=0.9].index,'是否达标']=1

print('------------------ d5.head() ----------------')
print(d5.head())

print('------------------ d5t.head() ----------------')
print(d5t.head())

print(list(d5.columns))
print(list(d5t.columns))

# 达标的取其中妥投率最小 值
d6=d5[d5['是否达标']==1].copy(deep=True)
d6t=d5t[d5t['是否达标']==1].copy(deep=True)
d6=d6.drop_duplicates(['淡旺季', '周序数', '渠道', '国家', '尾端配送商','是否达标','总票数'],keep='first' ,)
d6t=d6t.drop_duplicates(['淡旺季', '周序数', '渠道', '国家','是否达标','总票数'],keep='first' ,)

print('----------------------------------- d6.head() -----------------------------')
print(d6.head())
print('----------------------------------- d6t.head() -----------------------------')
print(d6t.head())

# 未达标的取其中妥投率最大 值
d7=d5[d5['是否达标'].isnull()==True].copy(deep=True)
d7t=d5t[d5t['是否达标'].isnull()==True].copy(deep=True)

d7=d7.drop_duplicates(['淡旺季', '周序数', '渠道', '国家', '尾端配送商','是否达标'],keep='last' ,)
d7t=d7t.drop_duplicates(['淡旺季', '周序数', '渠道', '国家', '是否达标'],keep='last' ,)
print('----------------------------------- d7.head() -----------------------------')
print(d7.head())
print('----------------------------------- d7t.head() -----------------------------')
print(d7t.head())

# 以上交叉合并
# 1\取唯一目录
d8=d6.append(d7)
d8t=d6t.append(d7t)
d8=d8.drop_duplicates(['淡旺季', '周序数', '渠道', '国家', '尾端配送商'],keep='first').reset_index(drop=True)
d8t=d8t.drop_duplicates(['淡旺季', '周序数', '渠道', '国家'],keep='first').reset_index(drop=True)
d8.loc[d8['是否达标'].isnull()==True,'妥投时间间隔']=60
d8t.loc[d8t['是否达标'].isnull()==True,'妥投时间间隔']=60
print(d8.head())
print(d8t.head())

# 导入kpi
dk=pd.read_excel(r'D:\PBI\BI\Wish-4PL-SLA-1134.xlsx',sheet_name='KPI调整表', dtype=dict(zip(['TTD（TDD)','年月'],['int','str',])))
dk=dk.rename(columns={'国家简码':'国家','国家':'国家1','年月':'淡旺季','物流渠道':'渠道','TTD（TDD)':'KPI'})
dk=dk.reindex(columns=['国家','淡旺季','渠道','KPI','期望D'])
# print(dk.columns)
# dk.info()
print('----------- dk.head() ---------------')
print(dk.head())


d9=pd.merge(d8,dk,on=['淡旺季','渠道', '国家'],how='left')
d9t=pd.merge(d8t,dk,on=['淡旺季','渠道', '国家'],how='left')

d9=d9.sort_values(['渠道','总票数'],ascending=False)
d9t=d9t.sort_values(['渠道','总票数'],ascending=False)

d9=d9.sort_values(['国家','尾端配送商', '周序数'])
d9t=d9t.sort_values(['国家', '周序数'])

print('--------------------- d9t.head() ---------------')
print(d9t.head())

d10=d9[['渠道', '国家', '尾端配送商',  '妥投时间间隔', '周序数','周期', '妥投率','KPI','妥投票数','总票数','期望D']].copy(deep=True)
d10t=d9t[['渠道', '国家',  '妥投时间间隔', '周序数','周期', '妥投率','KPI','妥投票数','总票数','期望D']].copy(deep=True)

d10['未妥投票数'] = d10['总票数']-d10['妥投票数']
d10t['未妥投票数'] = d10t['总票数']-d10t['妥投票数']

print('--------------------- d10.head() 前 ---------------')
print(d10.head())
print('--------------------- d10t.head() 前 ---------------')
print(d10t.head())

d10['主题']=d10['渠道']+'-'+d10['国家']+'-'+d10['尾端配送商']
d10t['主题']=d10t['渠道']+'-'+d10t['国家']
print('--------------------- d10.head()-hou ---------------')
print(d10.head())
print('--------------------- d10t.head()-hou  ---------------')
print(d10t.head())
# pd.options.display.max_columns = None # print(d10[d10['主题']=='CNE全球优先-DE-快捷_DEDHL'])
# print(d10)
# print(d10t)


d10.to_csv(r"F:\其他部门\data\d10.csv" , index= False, encoding="utf_8_sig")
d10t.to_csv(r"F:\其他部门\data\d10t.csv" , index= False, encoding="utf_8_sig")


bf = r'{}\渠道妥投天数概率统计.xlsx'.format(file)
writer = pd.ExcelWriter(bf)
d10.to_excel(writer, '数据')
d10t.to_excel(writer, '数据total')
dyt.to_excel(writer, '原数据')
d3.to_excel(writer, 'd3')
d4.to_excel(writer, 'd4')
d5.to_excel(writer, 'd5')
d6.to_excel(writer, 'd6')
d7.to_excel(writer, 'd7')
d8.to_excel(writer, 'd8')
d9.to_excel(writer, 'd9')
writer.save()

print('---------------- 数据准备阶段结束 ！ ----------------')


# In[ ]:




