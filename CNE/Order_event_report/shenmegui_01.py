import pandas as pd
import pymysql
import datetime, time

nows = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

print(datetime.date.today())

# #数仓连接
con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8",
                      autocommit=True, database='logisticscore')
cur = con.cursor()

days = "BETWEEN '2021-03-31 16:00:00' and " + "'" + nows + "'"


print(days)

# list

s1 = """
    select  gmt_create,last_operate,gmt_last_operate,pre_order_id,order_time,order_create_time,des,platform,customer_id,channel_id,supplier_channel_id,_1,_20,_40,_60,_80,_100,_120,_140,_160,_180
    from order_event_report limit 10 
"""

def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df


d1 = execude_sql(s1)
print(d1)



