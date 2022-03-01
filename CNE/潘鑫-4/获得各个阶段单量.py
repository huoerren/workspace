
import pandas as pd

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)
import openpyxl
import pymysql
import datetime, time

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


dateStr = """  BETWEEN '2021/12/21 16:00:00' and '2021/12/22 16:00:00'   """
channelCode=""" 'CNE全球优先' """
des=""" 'GB' """

print('------------- 首扫内容：------------------')
# 首扫
S4 = """
   SELECT 
    order_no 内单号,
    date_add(gmt_create,interval 8 hour) 首扫时间,
    des,channel_name
FROM
    lg_order lgo
where
    lgo.gmt_create {} 
     
and  lgo.platform = 'TIKTOK'
and lgo.channel_code = {}
and lgo.des={} 

and lgo.is_deleted='n'
""".format(dateStr,channelCode,des)

print('----------------- 首扫 -----------------')
print(S4)

d4 = execude_sql(S4)
d4['内单号'] = d4['内单号'].apply(lambda x: x + '\t')
d4 = d4.drop_duplicates(['内单号'], keep='last')

print('首扫 数量：')
print(d4.shape)
d4.to_csv('01_首扫数量.csv', index=False, encoding="utf_8_sig")

# In[4]:

print('-------------封袋内容：-------------------------')
# 封袋
S2 = """
   	SELECT 
	order_no 内单号,
	date_add(sealing_bag_time,interval 8 hour) 封袋时间,
    des,channel_name
	FROM
	lg_order lgo,lg_bag_order_relation lbor,lg_bag lgb
	where
	lgo.gmt_create {}
      
and lgo.customer_id in (3282094) 
      and lgo.channel_code = {}
and lgo.des={} 
	and lgo.is_deleted='n'
	and lbor.order_id=lgo.id
	and lbor.is_deleted='n'
	and lbor.bag_id=lgb.id
	and lgb.is_deleted='n'

""".format(dateStr,channelCode,des)

print('---------- 封袋 -------------')
print(S2)
d2 = execude_sql(S2)
d2['内单号'] = d2['内单号'].apply(lambda x: x + '\t')
d2 = d2.drop_duplicates(['内单号'], keep='last')
print('封袋 数量：')
print(d2.shape)
d2.to_csv('02_封袋数量.csv', index=False, encoding="utf_8_sig")

# In[6]:


df = pd.concat([d4, d2, d2]).drop_duplicates(subset=['内单号'], keep=False)  # df1-df2

# 合并 “入库-出库”

d_d4_d2 = pd.merge(d4, d2[["内单号", "封袋时间"]], left_on="内单号", right_on="内单号", how='inner')
d_d4_d2.to_csv(r'C:\Users\hp\Desktop\cujia-11data\01_入库-出库.csv', encoding="utf_8_sig", index=False)

# In[18]:

print('--------- 装车情况：-----------')
# 装车
S1 = """
   SELECT 
	order_no 内单号,
	date_add(event_time,interval 8 hour) 装车时间,
   des,channel_name
   FROM
	lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
	where
	lgo.gmt_create {}
 
and lgo.customer_id in (3282094) 
     and lgo.channel_code = {}

and lgo.des={} 
	and lgo.is_deleted='n'
	and lbor.order_id=lgo.id
	and lbor.is_deleted='n'
	and lbor.bag_id=tbe.bag_id
	and tbe.is_deleted='n'
	and tbe.event_code='DEPS'
""".format(dateStr,channelCode,des)

print('--------- 装车： ----------')
print(S1 )

d1 = execude_sql(S1)
d1['内单号'] = d1['内单号'].apply(lambda x: x + '\t')
d1 = d1.drop_duplicates(['内单号'], keep='last')
print('装车 数量：')
print(d1.shape)
d1.to_csv('03_装车数量.csv', index=False, encoding="utf_8_sig")

# In[20]:


# 合并 “入库-出库-装车”

d_d4_d2_d1 = pd.merge(d_d4_d2, d1[["内单号", "装车时间"]], left_on="内单号", right_on="内单号", how='inner')
d_d4_d2_d1.to_csv(r'C:\Users\hp\Desktop\cujia-11data\02_出库-装车.csv', encoding="utf_8_sig", index=False)

# In[21]:


df_zhuangche_fengdai = pd.concat([d2, d1, d1]).drop_duplicates(subset=['内单号'], keep=False)  # df1-df2

# In[22]:

# print('------- 出口清关情况 ： -------------')
# 出口清关
S01 = """
    SELECT 
        order_no 内单号,
        date_add(event_time,interval 8 hour) 出口清关时间,
        des,channel_name
    FROM
        lg_order lgo,track_mawb_event tme
    where
        lgo.gmt_create {}
and lgo.customer_id in (3282094) 
      and lgo.channel_code = {}
and lgo.des={} 
    and lgo.is_deleted='n'
    and lgo.mawb_id=tme.mawb_id
    and event_code in ("RFEC", "HGFX", "HGCK")
""".format(dateStr,channelCode,des)

S02 = """
    SELECT 
    order_no 内单号,
    date_add(event_time,interval 8 hour) 出口清关时间,
    des,channel_name
    FROM
        lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
    where
        lgo.gmt_create {}
      
and lgo.customer_id in (3282094) 
      and lgo.channel_code = {}

and lgo.des={} 
    and lgo.is_deleted='n'
    and lbor.order_id=lgo.id
    and lbor.is_deleted='n'
    and lbor.bag_id=tbe.bag_id
    and tbe.is_deleted='n'
    and event_code in ("RFEC", "HGFX", "HGCK")
""".format(dateStr,channelCode,des)

S03 = """
    SELECT 
    order_no 内单号,
    date_add(event_time,interval 8 hour) 出口清关时间,
    des,channel_name
    FROM
        lg_order lgo,track_order_event toe
    where
        lgo.gmt_create {}
       
and lgo.customer_id in (3282094) 
      and lgo.channel_code = {}
and lgo.des={} 
    and lgo.is_deleted='n'
    and toe.order_id=lgo.id
    and toe.is_deleted='n'
    and event_code in ("RFEC", "HGFX", "HGCK")
""".format(dateStr,channelCode,des)

# In[23]:


d01 = execude_sql(S01)
d02 = execude_sql(S02)
d03 = execude_sql(S03)

# d_01_02_03:清关合计 ; 三个清关时间（内单，主单，bag ）统一改称为 “出口清关时间”

d_01_02_03 = pd.concat([d01, d02, d03])
d_01_02_03 = d_01_02_03.drop_duplicates(['内单号'], keep='last')
# print('出口清关 数量：')
# print(d_01_02_03.shape)


# print('---------- # 合并 “入库-出库-装车-清关”  ----------')

# 合并 “入库-出库-装车-清关”

d_d4_d2_d1_d_01_02_03 = pd.merge(d_d4_d2_d1, d_01_02_03[["内单号", "出口清关时间"]], left_on="内单号", right_on="内单号", how='inner')

# In[28]:


zhuangche_ckqingguan = pd.merge(d1, d_01_02_03[['内单号', '出口清关时间']], left_on='内单号', right_on='内单号', how='inner')

# In[30]:


zhuangche_ckqingguan.to_csv(r'C:\Users\hp\Desktop\cujia-11data\03_装车-出口清关.csv', encoding="utf_8_sig", index=False)

print('----------------- 起飞情况：  ------------')
# 起飞
S5 = """
    SELECT 
        order_no 内单号,
        date_add(event_time,interval 8 hour) 起飞时间,
        des,channel_name
    FROM
        lg_order lgo,track_mawb_event tme
    where
    lgo.gmt_create {}
      
and lgo.customer_id in (3282094) 
      and lgo.channel_code = {}
and lgo.des={} 
    and lgo.is_deleted='n'
    and   tme.is_deleted='n'
    and lgo.mawb_id=tme.mawb_id
    and event_code in ("SDFO","DEPC","DEPT","LKJC",'SYFD','SYYF','PMWC')
""".format(dateStr,channelCode,des)

print('------------ 起飞情况 -----------')
print(S5)
d5 = execude_sql(S5)
# print(d5['内单号'].value_counts())
d5 = d5.drop_duplicates(['内单号'], keep='last')
d5['内单号'] = d5['内单号'].apply(lambda x: x + '\t')
print('起飞 数量：')
print(d5.shape)
d5.to_csv('04_起飞数量.csv', index=False, encoding="utf_8_sig")

# In[34]:


# print('------------ # 合并 “ 入库-出库-装车-出口清关-起飞 ” -------------')
# 合并 “ 入库-出库-装车-出口清关-起飞 ”


d_d4_d2_d1_d_01_02_03_d5 = pd.merge(d_d4_d2_d1_d_01_02_03, d5[["内单号", "起飞时间"]], left_on="内单号", right_on="内单号", how='inner')
d_d4_d2_d1_d_01_02_03_d5.to_csv(r'C:\Users\hp\Desktop\cujia-11data\04_出口清关-起飞.csv', encoding="utf_8_sig", index=False)

# In[35]:

print('-------------------- 落地情况 ： -------------------')
# 落地（主单）
S61 = """
SELECT 
    order_no 内单号,
    date_add(event_time,interval 8 hour) 落地时间,
    des,channel_name

FROM
    lg_order lgo,track_mawb_event tme
where
lgo.gmt_create {}
       
and lgo.customer_id in (3282094) 
      and lgo.channel_code = {}
and lgo.des={} 
and lgo.is_deleted='n'
and tme.is_deleted='n'
and lgo.mawb_id=tme.mawb_id
and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
""".format(dateStr,channelCode,des)

print(S61)



# 落地（bag）
S62 = """
SELECT 
order_no 内单号,
date_add(event_time,interval 8 hour) 落地时间,
des,channel_name

FROM
lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
where
lgo.gmt_create {}
       
and lgo.customer_id in (3282094) 
      and lgo.channel_code = {}
and lgo.des={} 
and lgo.is_deleted='n'
and lbor.order_id=lgo.id
and lbor.is_deleted='n'
and lbor.bag_id=tbe.bag_id
and tbe.is_deleted='n'
and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
""".format(dateStr,channelCode,des)
print(S62)


# 落地（order）
S63 = """
SELECT 
order_no 内单号,
date_add(event_time,interval 8 hour) 落地时间,
des,channel_name

FROM
lg_order lgo,track_order_event toe
where
lgo.gmt_create {}
       
and lgo.customer_id in (3282094 ) 
      and lgo.channel_code = {}
and lgo.des={} 
and lgo.is_deleted='n'
and toe.order_id=lgo.id
and toe.is_deleted='n'
and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
""".format(dateStr,channelCode,des)
print(S63)

print('------- 落地 结束 ！ ---------')


d61 = execude_sql(S61)
d62 = execude_sql(S62)
d63 = execude_sql(S63)

# 合并三个落地时间 统一改称为 “落地时间”

d_61_62_63 = pd.concat([d61, d62, d63])
d_61_62_63 = d_61_62_63.drop_duplicates(['内单号'], keep='last')
print('落地 数量：')
print(d_61_62_63.shape)
d_61_62_63['内单号'] = d_61_62_63['内单号'].apply(lambda x: x + '\t')
d_61_62_63.to_csv('05_落地数量.csv', index=False, encoding="utf_8_sig")

# In[41]:


# 合并 “ 入库-出库-装车-出口清关-起飞-落地 ”


d_d4_d2_d1_d_01_02_03_d5_d_61_62_63 = pd.merge(d_d4_d2_d1_d_01_02_03_d5, d_61_62_63[["内单号", "落地时间"]], left_on="内单号", right_on="内单号", how='inner')
d_d4_d2_d1_d_01_02_03_d5_d_61_62_63.to_csv(r'C:\Users\hp\Desktop\cujia-11data\05_起飞-落地.csv', encoding="utf_8_sig", index=False)

# 进口清关
S71 = """
    SELECT 
    order_no 内单号,
    date_add(event_time,interval 8 hour) 进口清关时间,
    des,channel_name
    FROM
    lg_order lgo,track_mawb_event tme
    where
    lgo.gmt_create {}
       
and lgo.customer_id in ( 3282094 ) 
      and lgo.channel_code = {}
and lgo.des={} 
    and lgo.is_deleted='n'
    and tme.mawb_id=lgo.mawb_id
    and tme.is_deleted='n'
    and event_code in ("IRCM","PVCS","IRCN","RFIC","BGRK")
""".format(dateStr,channelCode,des)
d71 = execude_sql(S71)

S72 = """
    SELECT 
    order_no 内单号,
    date_add(event_time,interval 8 hour) 进口清关时间,
    des,channel_name

    FROM
    lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
    where
    lgo.gmt_create {}
      
and lgo.customer_id in ( 3282094 ) 
      and lgo.channel_code = {}
and lgo.des={} 
    and lgo.is_deleted='n'
    and lbor.order_id=lgo.id
    and lbor.is_deleted='n'
    and lbor.bag_id=tbe.bag_id
    and tbe.is_deleted='n'
    and event_code in ("IRCM","PVCS","IRCN","RFIC","BGRK")
""".format(dateStr,channelCode,des)
d72 = execude_sql(S72)

S73 = """
    SELECT 
    order_no 内单号,
    date_add(event_time,interval 8 hour) 进口清关时间,
    des,channel_name
    FROM
    lg_order lgo,track_order_event toe
    where
    lgo.gmt_create {}
       
and lgo.customer_id in (3282094 ) 
      and lgo.channel_code = {}
and lgo.des={} 
    and lgo.is_deleted='n'
    and toe.order_id=lgo.id
    and toe.is_deleted='n'
    and event_code in ("IRCM","PVCS","IRCN","RFIC","BGRK")
""".format(dateStr,channelCode,des)

print('------------- 清关 内容开始 ： ----------')
print('---------- 清关 -主单 --------')
print(S71)
print('---------- 清关 - Bag --------')
print(S72)
print('---------- 清关 - Order --------')
print(S73)








d73 = execude_sql(S73)





# 合并 “进口清关”时间

d_71_72_73 = pd.concat([d71, d72, d73])
d_71_72_73 = d_71_72_73.drop_duplicates(['内单号'], keep='last')

print('进口清关 数量：')
print(d_71_72_73.shape)
d_71_72_73['内单号'] = d_71_72_73['内单号'].apply(lambda x: x + '\t')
d_71_72_73.to_csv('06_清关数量.csv', index=False, encoding="utf_8_sig")

# 合并 “ 入库-出库-装车-出口清关-起飞-落地--进口清关 ”

d_d4_d2_d1_d_01_02_03_d5_d_61_62_63_d_71_72_73 = pd.merge(d_d4_d2_d1_d_01_02_03_d5_d_61_62_63, d_71_72_73[["内单号", "进口清关时间"]], left_on="内单号", right_on="内单号", how='inner')
d_d4_d2_d1_d_01_02_03_d5_d_61_62_63_d_71_72_73.to_csv(r'C:\Users\hp\Desktop\cujia-11data\06_落地-进口清关.csv', encoding="utf_8_sig", index=False)

# 交付
S3 = """
    SELECT 
    order_no 内单号,
    date_add(event_time,interval 8 hour) 交付时间,
    des,channel_name
    FROM
    lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
    where
    lgo.gmt_create {}
       
and lgo.customer_id in (3282094 ) 
      and lgo.channel_code = {}
and lgo.des={} 
    and lgo.is_deleted='n'
    and lbor.order_id=lgo.id
    and lbor.is_deleted='n'
    and lbor.bag_id=tbe.bag_id
    and tbe.is_deleted='n'
    and event_code in ("JFMD")
""".format(dateStr,channelCode,des)
d3 = execude_sql(S3)
d3 = d3.drop_duplicates(['内单号'], keep='last')

print('--------- S3 -------------')
print(S3)
print('交付 数量：')
print(d3.shape)
d3['内单号'] = d3['内单号'].apply(lambda x: x + '\t')
d3.to_csv('07_交付数量.csv', index=False, encoding="utf_8_sig")

# 合并 “ 入库-出库-装车-出口清关-起飞-落地--进口清关-交付 ”

d_d4_d2_d1_d_01_02_03_d5_d_61_62_63_d_71_72_73_d3 = pd.merge(d_d4_d2_d1_d_01_02_03_d5_d_61_62_63_d_71_72_73,
                                                             d3[["内单号", "交付时间"]], left_on="内单号", right_on="内单号",
                                                             how='inner')
d_d4_d2_d1_d_01_02_03_d5_d_61_62_63_d_71_72_73_d3.to_csv(r'C:\Users\hp\Desktop\cujia-11data\07_进口清关-交付.csv',
                                                         encoding="utf_8_sig", index=False)

# 妥投
S8 = """
    SELECT 
    order_no 内单号,
    date_add(delivery_date,interval 8 hour) 妥投时间,
    des,channel_name
    FROM
    lg_order lgo
    where
    lgo.gmt_create {}
    
and lgo.customer_id in (3282094 ) 
      and lgo.channel_code = {}
and lgo.des={} 
    and lgo.is_deleted='n'
    and delivery_date is not null 
""".format(dateStr,channelCode,des)

print('----------- 妥投:  ----------')
print(S8)
d8 = execude_sql(S8)

d8 = d8.drop_duplicates(['内单号'], keep='last')
print('妥投 数量：')
print(d8.shape)
d8['内单号'] = d8['内单号'].apply(lambda x: x + '\t')
d8.to_csv('08_妥投数量.csv', index=False, encoding="utf_8_sig")

# In[60]:


# 合并 “ 入库-出库-装车-出口清关-起飞-落地--进口清关-交付-妥投 ”

d_d4_d2_d1_d_01_02_03_d5_d_61_62_63_d_71_72_73_d3_d8 = pd.merge(d_d4_d2_d1_d_01_02_03_d5_d_61_62_63_d_71_72_73_d3,
                                                                d8[["内单号", "妥投时间"]], left_on="内单号", right_on="内单号",
                                                                how='inner')

d_d4_d2_d1_d_01_02_03_d5_d_61_62_63_d_71_72_73_d3_d8.to_csv(r'C:\Users\hp\Desktop\by内单号分段时间和时效\分段时间和时效.csv',
                                                            index=False, encoding="utf_8_sig")
d_d4_d2_d1_d_01_02_03_d5_d_61_62_63_d_71_72_73_d3_d8.to_csv(r'C:\Users\hp\Desktop\cujia-11data\08_交付-妥投.csv',
                                                            encoding="utf_8_sig", index=False)

# In[ ]:

df_jiaofu_tuotou = pd.concat([d_d4_d2_d1_d_01_02_03_d5_d_61_62_63_d_71_72_73_d3, d8, d8]).drop_duplicates(
    subset=['内单号'], keep=False)  # df1-df2

df_tuotou_jiaofu = pd.concat([d8, d_d4_d2_d1_d_01_02_03_d5_d_61_62_63_d_71_72_73_d3,
                              d_d4_d2_d1_d_01_02_03_d5_d_61_62_63_d_71_72_73_d3]).drop_duplicates(subset=['内单号'],
                                                                                                  keep=False)  # df1-df2



