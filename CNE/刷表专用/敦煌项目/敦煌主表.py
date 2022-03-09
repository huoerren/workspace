import pandas as pd
import pymysql
import datetime

nows = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

print(datetime.date.today())

# #数仓连接
con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8",
                      autocommit=True, database='logisticscore')

cur = con.cursor()

days = "BETWEEN '2021-05-31 16:00:00' and " + "'" + nows + "'"

print(days)
s1 = """
SELECT 
channel_code 渠道名称,
des 目的地, 
date_format(date_add(gmt_create,interval 8 hour),'%Y-%m-%d')  业务日期, 
delivery_interval,
order_status 追踪状态,
ISNULL( mawb_id ),
count(1) c 
from lg_order lgo 
where gmt_create {}  
AND lgo.platform="DHLINK"
AND lgo.channel_code IN ("CNE经济专线DH","CNE特惠专线DH","CNE优先专线DH")
and is_deleted='n'   
group by 
1,2,3,4,5,6
order by c desc
""".format(days)

s2="""
SELECT  channel_code,status,  des,date_format(date_add(gmt_create,interval 8 hour),'%Y-%m-%d') as datetime,  
count( 1 ) c ,customer_id 
FROM  lg_pre_order lpo  
WHERE  lpo.gmt_create > '2021-07-31 16:00:00'
AND lpo.platform="DHLINK"
AND lpo.channel_code IN ("CNE经济专线DH","CNE特惠专线DH","CNE优先专线DH")
and is_deleted= 'n'   
GROUP BY  channel_code,  des ,datetime,status,customer_id 
ORDER BY  c DESC 
""".format(days)


# 正式单
s3 = """SELECT 
order_no,
date_add(lgo.gmt_create,interval 8 hour)ywdate,
lgo.id id,
lgo.order_status,
lgo.des,
lgo.channel_code
from 
lg_order lgo 
where 
gmt_create > '2021-07-31 16:00:00'  
AND lgo.platform="DHLINK"   
AND lgo.channel_code IN ("CNE经济专线DH","CNE特惠专线DH","CNE优先专线DH")
and lgo.is_deleted='n'
"""



# 退件
s4 = """SELECT 
lgo.order_no,
toe.event_code,
tec1.track_status, 
tec1.event_cn_desc 事件描述,   
tec1.event_en_desc,
date_add(toe.event_time,interval 8 hour) tjdate,
lgo.order_status,date_format(lgo.gmt_create,'%Y-%m-%d') ywdate
FROM logisticscore.lg_order lgo
INNER JOIN logisticscore.track_order_event toe on lgo.id = toe.order_id
INNER JOIN logisticscore.track_event_code tec1 on toe.event_code = tec1.event_code 
WHERE   lgo.gmt_create >='2021-07-31 16:00:00'  
AND lgo.platform="DHLINK" 
and lgo.is_deleted= 'n'
and toe.is_deleted= 'n'
and tec1.is_deleted= 'n'
and tec1.event_en_desc!='-1' 
AND lgo.channel_code IN ("CNE经济专线DH","CNE特惠专线DH","CNE优先专线DH")
AND toe.event_code IN  ('GNTJ',     'GYST',     'TKDZ','JCTJ','CFDG',     'CTBY',     'ATIN',     'CZLL',     'CHIC',     'YCJJ',     'LJIE',     'DIBJ',     'BGPS',   'SIRC',   'GNTJ','CSHD',
'CSIN','HGCY','HJFX','GNCY','HGYC','CKCY','HGXH')
"""




def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df



d1 = execude_sql(s1)
d1['当日'] = datetime.date.today()
d1['业务日期'] = pd.to_datetime(d1['业务日期'])
# print()
d1['距离当天'] = (pd.to_datetime(d1['当日']) - d1['业务日期']).dt.days


# 追踪状态 的中文解释
o_s = {0: '未发送', 1: '已发送', 2: '转运中', 3: '送达', 4: '超时', 5: '扣关', 6: '地址错误', 7: '快件丢失', 8: '退件', 9: '其他异常'}
d1['状态'] = d1['追踪状态'].map(o_s)
d1['状态'] = d1['状态'].fillna('销毁')

# 间隔日
# d1.info()
d1['间隔日'] = pd.cut(d1['距离当天'], [-999, -2, 3, 5, 7, 12, 20, 30, 100000],
                   labels=['已发送', '03天未发出', '05天未发出', '07天未发出', '12天未发出', '20天未发出', '30天未发出', '30天以上未发出',
                           ])
d1.loc[d1['状态'] != '未发送', ['间隔日']] = '已发送'

# 预录单
# d2 = execude_sql(s2)
# 正式单
d3 = execude_sql(s3)
# 退件
d4 = execude_sql(s4)

d7 = d3[d3.order_status==2]

tp1=tuple(d7['id'].tolist())

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
""".format(tp1)
last = execude_sql(s5)

last['当日'] = datetime.date.today()

last['末条信息时间'] = pd.to_datetime(last['末条信息时间'])

last['距离当天'] = (pd.to_datetime(last['当日']) - last['末条信息时间']).dt.days

d11 = pd.merge(last, d7, on='id', how='left')

d11['间隔日']=pd.cut(d11['距离当天'],[-999,0,1,3, 5, 7, 12, 20, 30, 100000],labels=['0天内','1天内','03天内', '05天内', '07天内', '12天内', '20天内','30天内','30天以上'])

d4_ck = d4[d4['事件描述'].str.contains('海关')]  # '仓外'的index

d4_ck = d4_ck.reindex(columns=['order_no'])

d4_ck['退件位置'] = '仓外'

d5 = d4.sort_values(by=['order_no', 'tjdate'], ascending=True)

d6 = d5.drop_duplicates(subset=['order_no'], keep='last')  # 去除id重复项

d6 = pd.merge(d6, d4_ck, on='order_no', how='left')

d6['退件位置'] = d6['退件位置'].fillna('仓内')

name_l = [['总list', d1],['退件单', d6],['末条信息数据',d11],['预录单',execude_sql(s2)]]


def file_xlsx(name, df):
    bf = r'F:\PBI临时文件\敦煌总表刷新\{}.xlsx'.format(name)
    writer = pd.ExcelWriter(bf)
    df.to_excel(writer, 'sheet1', index=False)
    writer.save()



for n in name_l:
    file_xlsx(n[0], n[1])



d3.to_csv(r'F:\PBI临时文件\敦煌总表刷新\正式单.csv', index=False)



