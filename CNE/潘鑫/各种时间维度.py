#coding=utf-8

import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)

import datetime

dw = pd.DataFrame(pd.date_range(start='20210101',end='20211230',periods=None,freq='D'),columns=['具体日期'])

dw['年份'] = dw['具体日期'].apply(lambda y:y.year)
dw['季度'] = dw['具体日期'].apply(lambda z:z.quarter)

dw['月份'] = dw['具体日期'].apply(lambda z:z.month )
dw['周序数'] =   dw['具体日期'].dt.strftime('%W').apply(lambda n:int(n) +1 )
dw['年季度'] = dw['具体日期'].apply(lambda z: '{}Q{}'.format(z.year, (z.quarter)))
dw['星期几']   = dw['具体日期'].dt.day_name()
dw['星期几_数字话表达']=dw['具体日期'].dt.weekday + 1
dw.to_excel('具体日期.xlsx' ,index= False)

# today = datetime.date.today()
# print(today)
# quarter = (today.month - 1) // 3 + 1
# print('{}Q{}'.format(today.year, quarter)) # out: '2019Q1'

week = datetime.datetime.strptime('20210106','%Y%m%d').strftime('%W')
print(int(week))


