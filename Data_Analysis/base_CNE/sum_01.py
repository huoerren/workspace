#coding=utf-8

import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)

import openpyxl
import pymysql
import datetime, time

# nows = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
import numpy as np

#数仓连接
con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8", autocommit=True, database='logisticscore')
cur = con.cursor()

# days = "BETWEEN '2021-09-15 16:00:00' and " + "'" + nows + "' "

def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df

s1 = '''
    select  year(gmt_create) as `year`, channel_code , des , count(1) as amount from lg_order  group by year(gmt_create), channel_code , des  order by  year(gmt_create) , channel_code , count(1) desc  
'''
dp = execude_sql(s1)
dp_2021 = dp[dp['year'] == 2021 ]
dp_2020 = dp[dp['year'] == 2020 ]
dp_2019 = dp[dp['year'] == 2019 ]

print(dp_2021.shape)


alist = []
for i, j in dp_2021.sort_values(by = ['channel_code','amount'], ascending= [True,False]).groupby('channel_code'):
    # print( i ,list(j.head(1)['amount'])[0] )
    alist.append([i ,list(j.head(1)['amount'])[0]] )
    print('----------------')

print(alist)

