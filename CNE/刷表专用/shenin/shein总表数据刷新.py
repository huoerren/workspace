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

# days="BETWEEN '2021-03-31 16:00:00' and '"+time_yes+"'"1
# days="BETWEEN '2021-03-31 16:00:00' and "+"'"+nows+"'"
days="BETWEEN '2021-04-30 16:00:00' and "+"'"+nows+"'"

print(days)
# days="BETWEEN '2020-12-17 16:00:00' and '2021-02-15 15:59:59'"

# list
s1="""SELECT 
Datediff(lgo.delivery_date,lgo.effective_send_time) fq,
lgo.channel_code,
lgo.des,
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') as '业务日期',
lgo.order_status,
isnull(lgo.mawb_id),sum(lgo.weight) as Weight,
count(1) c
FROM 
lg_order lgo
where
lgo.gmt_create {}
and lgo.customer_id=441331
and lgo.is_deleted='n'
group BY 
lgo.channel_code,
fq,
isnull(lgo.mawb_id),
lgo.des,
业务日期,
lgo.order_status
order BY 
c desc
""".format(days)

# 预录单
s2="""
SELECT 
channel_code,
status, 
des,
date_format(date_add(gmt_create,interval 8 hour),'%Y-%m-%d') as '业务日期', 
count( 1 ) c 
FROM 
lg_pre_order lpo 
WHERE 
gmt_create {}  
AND customer_id=441331 
and is_deleted= 'n'  
GROUP BY 
channel_code, 
des,
业务日期,
status
ORDER BY c DESC 
""".format(days)

# 正式单
s3="""
SELECT lgo.order_no,lgo.channel_code
FROM lg_order lgo,
track_bag_event tbe,
lg_bag_order_relation lbor 
where tbe.event_code ='DEPS'
and tbe.event_time {}
and lgo.customer_id=441331
and lgo.is_deleted='n'
and lbor.bag_id=tbe.bag_id 
and lbor.order_id=lgo.id
and tbe.event_code ="DEPS"
group BY lgo.order_no,lgo.channel_code
""".format(days)

def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df
# 主表
d1=execude_sql(s1)
# print(d1['业务日期'].dtypes)
d1['当日']=datetime.date.today()
d1['业务日期']=pd.to_datetime(d1['业务日期'])
# print()
d1['距离当天']=(pd.to_datetime(d1['当日'])- d1['业务日期']).dt.days

# 追踪状态 的中文解释
o_s={0:'未发送',1 :'已发送',2 :'转运中', 3:'送达',4:'超时',5:'扣关',6:'地址错误',7:'快件丢失', 8:'退件',9:'其他异常'}
d1['状态']=d1['order_status'].map(o_s)
d1['状态']=d1['状态'].fillna('销毁')
print(d1['状态'])
# 间隔日
# d1.info()
d1['间隔日']=pd.cut(d1['距离当天'],[-999,-2,3, 5, 7, 12, 20, 30, 100000],
                 labels=['已发送','03天未发出', '05天未发出', '07天未发出', '12天未发出', '20天未发出','30天未发出','30天以上未发出',
                         ])
d1.loc[d1['状态']!='未发送',['间隔日']]='已发送'




# 妥投延迟
# d2=execude_sql(s2)
#
# d3=execude_sql(s3)
name_l=[['总list',d1],['预录单',execude_sql(s2)]]
def file_xlsx(name,df):
    bf =r'F:\PBI临时文件\shein总表监控\{}.xlsx'.format(name)
    writer = pd.ExcelWriter(bf)
    df.to_excel(writer,'sheet1',index=False)
    writer.save()
for n in name_l:
    file_xlsx(n[0],n[1])

# file_xlsx('正式单内单号',execude_sql(s3))

d3=execude_sql(s3)
d3.to_csv(r'F:\PBI临时文件\shein总表监控\正式单内单号.csv',index=False,encoding="utf_8_sig")