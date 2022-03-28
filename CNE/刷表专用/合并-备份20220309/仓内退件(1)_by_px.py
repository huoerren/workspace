import pandas as pd

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)

import pymysql
import re

# #数仓连接
con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8",
                      autocommit=True)
cur = con.cursor()

# days="BETWEEN '2021-04-05 16:00:00' and '"+time_yes+"'"
days = "BETWEEN '2021-07-31 16:00:00' and now()"
# days="BETWEEN '2021-3-31 16:00:00' and '2021-04-30 16:00:00'"


s1 = """
SELECT 
lgo.order_no,toe.event_code,customer_id,
tec1.track_status, tec1.event_cn_desc 事件描述,   tec1.event_en_desc,
date_add(toe.event_time,interval 8 hour) tjdate,lgo.order_status 
FROM logisticscore.lg_order lgo
INNER JOIN logisticscore.track_order_event toe on lgo.id = toe.order_id
INNER JOIN logisticscore.track_event_code tec1 on toe.event_code = tec1.event_code 
WHERE   lgo.gmt_create {}
and ((customer_id in ('3161297','3282094') ) or (platform='JDW') )
and lgo.is_deleted= 'n'
and toe.is_deleted= 'n'
and tec1.is_deleted= 'n'
and tec1.event_en_desc!='-1' 
AND toe.event_code IN 
( 'GNTJ', 'GYST', 'TKDZ', 'JCTJ', 'CFDG', 'CTBY', 'ATIN', 'CZLL', 'CHIC', 'YCJJ', 'LJIE', 'DIBJ', 'BGPS', 'SIRC', 'GNTJ','CSHD',
'CSIN','HGCY','HJFX','GNCY','HGYC','CKCY','HGXH')
 

""".format(days)


def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df


print('-------------------- s1 -------------')
print(s1)

d1 = execude_sql(s1)
# print(d1)
d1.loc[d1['customer_id'] == 3161297, 'customer_id'] = '兰亭集势'
d1.loc[d1['customer_id'] == 3282094, 'customer_id'] = '促佳'
d1['项目名称'] = d1.apply(
    lambda row: "兰亭集势" if row['customer_id'] == '兰亭集势' else ('促佳' if row['customer_id'] == '促佳' else "京东"), axis=1)
# 删除“仓库”
# d1.loc[d1[d1['事件描述'].str.contains('仓库')].index,'退件位置']='仓外'
# '仓外'
print('-------------------- d1 是最原始的数据 -----------------------')
print(d1.shape)

d1['退件位置'] = d1['事件描述'].apply(lambda x: re.search(r'仓库|海关', x, ))

print(d1.shape)

d1_ck = d1[['order_no', '退件位置']].copy(deep=True)
print('---------------- d1_ck.shape：前 ---------------')
print(d1_ck.shape)
d1_ck = d1_ck.dropna(axis=0, how='any')
print('---------------- d1_ck.shape：后 ---------------')
print(d1_ck.shape)

d1_ck['退件位置'] = '仓外'
d1_ck = d1_ck.drop_duplicates(subset=['order_no'], keep='last')
print('=============== d1_ck.shape ============')
print(d1_ck.shape)
# 之前版本
# d1_ck=d1[d1['事件描述'].str.contains('仓库')]#'仓外'的index
# d1_ck=d1_ck.reindex(columns=['order_no'])
# d1_ck['退件位置']='仓外'
# print(d1_ck)


# d1['退件位置']='仓内'
# d2=d1.drop(d1[d1['事件描述'].str.contains('仓库')].index,axis=0)
# 倒叙=删除
# d2=d2.sort_index(axis=0,ascending=False,by=['order_no','tjdate'])

print('------------------ d1.head() -----------------')
print(d1.head())

d2 = d1.drop(['退件位置'], axis=1)
print('------------------ d2.head() ----------------------------')

d2 = d2.sort_values(by=['order_no', 'tjdate'], ascending=True)
# print(d2)
d3 = d2.drop_duplicates(subset=['order_no'], keep='last')  # 去除id重复项
d3 = pd.merge(d3, d1_ck, on='order_no', how='left')
d3['退件位置'] = d3['退件位置'].fillna('仓内')

# d3.loc[d3[d3['位置']=='仓外'].index,'退件位置']='仓外'
# d3.drop(['位置'],axis=1,inplace=True)
print(d3.head())

bf = r'D:\PBI\BI\合并退件\当期数据\合并仓内去重.xlsx'
writer = pd.ExcelWriter(bf)
d3.to_excel(writer, '去重', index=False)
# d1.to_excel(writer,'原数据',index=False)

writer.save()
