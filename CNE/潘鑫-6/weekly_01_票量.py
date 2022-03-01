import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)
import openpyxl
import pymysql
import datetime, time
import os.path


nows = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

print(datetime.date.today())
import numpy as np

# #数仓连接
con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8", autocommit=True, database='logisticscore')
cur = con.cursor()


def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df


dateStr = """ BETWEEN '2021-01-31 16:00:00' and '2021-02-01 16:00:00'   """
channelCode =   [ "'CNE全球特惠'" ]
counList = ["'US'"  ]

def getAcount(i , j ):

    # 首扫-妥投
    S4 = """
       SELECT 
        order_no 内单号,
        date_add(gmt_create,interval 8 hour) 首扫时间,
        date_add(delivery_date,interval 8 hour) 妥投时间,
        des,
        channel_name
    FROM
        lg_order lgo
    where 1=1
        and lgo.gmt_create {}
        and lgo.channel_code = {}
        and lgo.des = {}
        and lgo.is_deleted='n'
    """.format(dateStr, i , j )

    # print('----------------- 首扫 -----------------')
    # print(S4)

    d4 = execude_sql(S4)
    d4['内单号'] = d4['内单号'].apply(lambda x: x + '\t')
    d4 = d4.drop_duplicates(['内单号'], keep='last')

    # print('首扫 数量：')
    # print(d4.shape)

    d4['首扫时间'] = pd.to_datetime(d4['首扫时间'])
    d4['妥投时间'] = pd.to_datetime(d4['妥投时间'])

    d4["用时"] = (d4["妥投时间"] - d4["首扫时间"]).astype('timedelta64[s]')
    d4["用时"] = round(d4["用时"] / 86400, 2)
    print(d4.info())

    # nat = np.datetime64('NaT')
    d4['nihao'] = d4['妥投时间'].apply(lambda  x : 'None' if  pd.isnull(x)  else 'No None')
    # lambda x: 1 if 'ing' in x else 0

    print(d4)

    df4 = d4.sort_values(by=["用时"], ascending=True)
    # print('===============================================')
    # print(df4.describe(percentiles=[0.4,0.95]))
    print('-----------------------------------------------')

    # print([ i, j , df4.describe(percentiles=[0.4])['用时']['40%'] ,df4.describe(percentiles=[0.95])['用时']['95%']] )
    # print(d4)





if __name__ == '__main__':
    # 获得规定条件下的票单量
    for i in  channelCode :
        for j in    counList :
            getAcount( i ,j )




















