import pandas as pd
import openpyxl
import pymysql
import datetime,time
nows=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')


print(datetime.date.today())
import numpy as np
# #数仓连接
con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()

days="BETWEEN '2021-03-31 16:00:00' and "+"'"+nows+"'"

print(days)
# list
s1 ="""SELECT 
channel_code 渠道名称,
des 目的地, 
date_format(date_add(tme2.event_time,interval 8 hour),'%Y-%m-%d') 起飞日期,
delivery_interval,
order_status 追踪状态,
ISNULL( lgo.mawb_id ),
count(1) c,
customer_id
from lg_order lgo,
track_mawb_event tme2
where 
tme2.event_code in("SDFO","DEPC","DEPT","LKJC")   
and tme2.event_time {}
and lgo.mawb_id=tme2.mawb_id
AND lgo.customer_id=3041600  
and lgo.is_deleted='n'
and tme2.is_deleted='n'   
group by 
1,2,3,4,5,6
order by c desc
""".format(days)


# 正式单
s3 = """SELECT 
order_no,
date_format(date_add(tme2.event_time,interval 8 hour),'%Y-%m-%d') 起飞日期
from 
lg_order lgo,
track_mawb_event tme2
where 
lgo.mawb_id=tme2.mawb_id 
and tme2.event_code in("SDFO","DEPC","DEPT","LKJC")   
and tme2.event_time >'2021-05-29 16:00:00'
and lgo.is_deleted='n'
and tme2.is_deleted='n'
AND lgo.customer_id='3041600'
"""

def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df

d1=execude_sql(s1)
d1['当日']=datetime.date.today()
d1['业务日期']=pd.to_datetime(d1['起飞日期'])
# print()
d1['距离当天']=(pd.to_datetime(d1['当日'])- d1['业务日期']).dt.days

# 追踪状态 的中文解释
o_s={0:'未发送',1 :'已发送',2 :'转运中', 3:'送达',4:'超时',5:'扣关',6:'地址错误',7:'快件丢失', 8:'退件',9:'其他异常'}
d1['状态']=d1['追踪状态'].map(o_s)
d1['状态']=d1['状态'].fillna('销毁')

# 间隔日
# d1.info()
d1['间隔日']=pd.cut(d1['距离当天'],[-999,-2,3, 5, 7, 12, 20, 30, 100000],
                 labels=['已发送','03天未发出', '05天未发出', '07天未发出', '12天未发出', '20天未发出','30天未发出','30天以上未发出',
                         ])
d1.loc[d1['状态']!='未发送',['间隔日']]='已发送'



#正式单
d3=execude_sql(s3)
name_l=[['总list',d1]]
def file_xlsx(name,df):
    bf =r'F:\PBI临时文件\平世总表监控\{}.xlsx'.format(name)
    writer = pd.ExcelWriter(bf)
    df.to_excel(writer,'sheet1',index=False)
    writer.save()
for n in name_l:
    file_xlsx(n[0],n[1])

d3.to_csv(r'F:\PBI临时文件\平世总表监控\正式单.csv',index=False)


