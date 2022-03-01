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
days="BETWEEN '2021-07-31 16:00:00' and "+"'"+nows+"'"

print(days)
# days="BETWEEN '2020-12-17 16:00:00' and '2021-02-15 15:59:59'"



# 促佳  customer_id='3282094'

# list
s1="""(SELECT channel_code "渠道名称",des "目的地", date_format(date_add(gmt_create,interval 8 hour),'%Y-%m-%d') as "业务日期" ,
 customer_id,round(timestampdiff(hour,lgo.gmt_create,lgo.delivery_date)/24,1) as delivery_interval,order_status "追踪状态" ,ISNULL( mawb_id ), count(1) c 
from lg_order lgo 
where gmt_create {}
and ((customer_id in ('3161297','3282094'))  or ( platform='JDW') )
and is_deleted='n'   
group by channel_code,5,ISNULL( mawb_id ), des,业务日期,customer_id,order_status 
order by c desc)
 
""".format(days )


print(s1)

# 预录单
s2="""
SELECT  channel_code,status,  des,date_format(date_add(gmt_create,interval 8 hour),'%Y-%m-%d') as datetime,  
    count( 1 ) c ,customer_id 
FROM  lg_pre_order lpo  
WHERE  gmt_create {}
    and ( (customer_id in ('3161297','3282094')) or  (platform='JDW') )
    and is_deleted= 'n'   
GROUP BY  channel_code,  des ,datetime,status,customer_id 
ORDER BY  c DESC 
 
""".format(days )


print(s2)

# # 3PL妥投订单
# s3="""SELECT lgo.channel_code,lgo.des,date_format(date_add(lgo.order_time,interval 8 hour),'%Y-%m-%d') orderdate,
# date_format(date_add(lgo.delivery_date,interval 8 hour),'%y-%m-%d')ttdate, count(1) c
# from lg_order lgo
# where  lgo.order_time {}
# and platform='WISH_ONLINE' AND customer_id not in (1151368,1151370,1181372,1181374) and lgo.is_deleted='n'
# and lgo.order_status=3
# group by lgo.channel_code, lgo.des, orderdate, ttdate
# """.format(days)

s3="""
SELECT 
    lgo.order_no,date_add(lgo.gmt_create,interval 8 hour) datetime,lgo.id,lgo.order_status,lgo.channel_code,lgo.des,customer_id 
from lg_order lgo
where 
# gmt_create>  '2021-2-28 16:00:00'
    gmt_create>  '2021-4-30 16:00:00'
    and ( (customer_id in ('3161297','3282094'))   or (platform='JDW') )
    and lgo.is_deleted='n' 
 
"""



s4="""
SELECT order_no,date_add(gmt_create,interval 8 hour) datetime ,customer_id 
from lg_order lgo
where 
# gmt_create>  '2021-2-28 16:00:00'
gmt_create>  '2021-4-30 16:00:00'
and ((  customer_id in ('3161297','3282094')) or (platform='JDW' ) )
and lgo.is_deleted='n' 
 
"""




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



d1.loc[d1['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
d1.loc[d1['customer_id'] == 3282094, 'customer_id'] = '促佳'
d1['项目名称'] = d1.apply(lambda row: "兰亭集势" if row['customer_id'] =='兰亭集势' else( '促佳' if row['customer_id'] =='促佳'  else "京东"), axis=1)


print(d1)


d1['当日']=datetime.date.today()
d1['业务日期']=pd.to_datetime(d1['业务日期'])
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



d3=execude_sql(s3)
d3.loc[d3['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
d3.loc[d3['customer_id'] == 3282094, 'customer_id'] = '促佳'
d3['项目名称'] = d3.apply(lambda row: "兰亭集势" if row['customer_id'] =='兰亭集势' else( '促佳' if row['customer_id'] =='促佳'  else "京东"), axis=1)

d3['状态']=d3['order_status'].map(o_s)
d3['状态']=d3['状态'].fillna('销毁')
d4=d3[d3['order_status']==2].copy(deep=True)
print(d4)
dno=d4['id']
print(dno)
str=tuple(dno)
print(str)

s5="""
select
olt.order_id id,
olt.track_desc,
date_add(olt.event_time,interval 8 hour) 末条信息时间
from 
order_last_track olt 
where
olt.order_id in {}
and olt.is_deleted='n'
""".format(str)
d5=execude_sql(s5)

d5['当日']=datetime.date.today()
d5['末条信息时间']=pd.to_datetime(d5['末条信息时间'])
# print()
d5['距离当天']=(pd.to_datetime(d5['当日'])- d5['末条信息时间']).dt.days

# 查找内单号
d6=pd.merge(d5,d4,on=['id'],how='left')

d6['间隔日']=pd.cut(d6['距离当天'],[-999,1,3, 5, 7, 12, 20, 30, 100000],
                 labels=['1天内','03天内', '05天内', '07天内', '12天内', '20天内','30天内','30天以上',
                         ])
# d1.loc[d5['状态']!='未发送',['间隔日']]='已发送'

print(d6)



# # 淡旺季
# d1['淡旺季']='20210'#先默认淡季
# # print(d1['业务日期'].dt.month)
# # print(d1.loc[1,'业务日期'].year)
# for i in d1.index:
#     if d1.loc[i,'业务日期'].year==2020:
#         d1.loc[i,'淡旺季']='20201'
#     elif d1.loc[i,'业务日期'].month in [1,9,10,11,12]:
#         d1.loc[i, '淡旺季'] = '20211'
# print(d1)

# 妥投延迟
d2=execude_sql(s2)
#
# d3=execude_sql(s3)

d2.loc[d2['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
d2.loc[d2['customer_id'] == 3282094, 'customer_id'] = '促佳'
d2['项目名称'] = d2.apply(lambda row: "兰亭集势" if row['customer_id'] =='兰亭集势' else( '促佳' if row['customer_id'] =='促佳'  else "京东"), axis=1)

name_l=[['总list',d1],['预录单',d2],['转运中订单末条信息',d6]]
def file_xlsx(name,df):
    bf =r'F:\PBI临时文件\合并总表监控\{}.xlsx'.format(name)
    writer = pd.ExcelWriter(bf)
    df.to_excel(writer,'sheet1',index=False)
    writer.save()
for n in name_l:
    file_xlsx(n[0],n[1])


d4=execude_sql(s4)
d4.loc[d4['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
d4.loc[d4['customer_id'] == 3282094, 'customer_id'] = '促佳'
d4['项目名称'] = d4.apply(lambda row: "兰亭集势" if row['customer_id'] =='兰亭集势' else( '促佳' if row['customer_id'] =='促佳'  else "京东"), axis=1)
d4.to_csv(r'F:\PBI临时文件\合并总表监控\正式单.csv',index=False)

