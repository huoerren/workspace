#coding=utf-8

import json
import pandas as pd
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)
pd.set_option('expand_frame_repr', False)


from pyecharts.charts import *
from pyecharts import options as opts
from pyecharts.faker import Faker
from pyecharts.commons.utils import JsCode

import pymysql
import datetime,time
nows = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

print(datetime.date.today())

con = pymysql.connect(host="139.198.189.25",
                          port=44000,
                          user="cnereader", passwd="read51096677",
                          charset="utf8", autocommit=True, database='logisticscore')
cur = con.cursor()


# 订单总数（本月 -“专线&挂号”）

S_order_total = """
   select order_id ,order_no , lgo.channel_name, DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y%m%d") as orderCreateDt  
	from amazon_pull_order apo  left join lg_order lgo 
		on apo.order_id = lgo.id 
			where (lgo.channel_name  like '%挂号%' or lgo.channel_name in (
			'CNE全球速达','CNE全球快捷（美英法德）','CNE全球快捷（其它国家）',
						'CNE全球优先','HERMES优先','CNE英国直飞',
						'CNE全球特惠','HERMES特惠','CNE全球特货','CNE全球经济',
						'CNE夏季促销','E速宝优先','E速宝特惠',
						'E速宝经济','E速宝特货','E速宝跟踪', 'CNE华东欧电'))
					and date_add(apo.order_create_dt,interval 8 hour) >= '2021-01-01 00:00:00'
					and date_add(apo.order_create_dt,interval 8 hour) < date_add(curdate()-day(curdate())+1,interval 1 month )
				group by  order_id ,order_no , lgo.channel_name  , DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y%m%d")
				order by orderCreateDt desc 
"""

# 系统妥投数（本月 - ‘专线&挂号’- 妥投）

S_benxitong_tuotou = """
     select order_id ,order_no , lgo.channel_name, DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y%m%d") as orderCreateDt, lgo.customer_code ,lgo.logistics_no,lgo.des 
        from amazon_pull_order apo  left join lg_order lgo 
            on apo.order_id = lgo.id 
                where (lgo.channel_name  like '%挂号%' or lgo.channel_name in (
                'CNE全球速达','CNE全球快捷（美英法德）','CNE全球快捷（其它国家）',
                            'CNE全球优先','HERMES优先','CNE英国直飞',
                            'CNE全球特惠','HERMES特惠','CNE全球特货','CNE全球经济',
                            'CNE夏季促销','E速宝优先','E速宝特惠',
                            'E速宝经济','E速宝特货','E速宝跟踪', 'CNE华东欧电'))
                        and date_add(apo.order_create_dt,interval 8 hour) >= '2021-01-01 00:00:00'
                        and date_add(apo.order_create_dt,interval 8 hour) < date_add(curdate()-day(curdate())+1,interval 1 month )

                        and ((lgo.order_status = 3) or (lgo.order_status != 3 and  concat(apo.effective_event_status,apo.effective_event_reason) in('301137','308000','407000','408000','304232' )  ))
                        
                        group by  order_id ,order_no , lgo.channel_name,DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y%m%d") ,lgo.customer_code ,lgo.logistics_no,lgo.des      
                    order by 	orderCreateDt  desc	
"""


# amazon妥投数（本月- ‘专线&挂号’-妥投）

S_amazon_tuotou = """
     select order_id ,order_no , lgo.channel_name, DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y%m%d") as orderCreateDt, lgo.customer_code ,lgo.logistics_no,lgo.des   
        from amazon_pull_order apo  left join lg_order lgo 
		on apo.order_id = lgo.id 
			where (lgo.channel_name  like '%挂号%' or lgo.channel_name in (
			'CNE全球速达','CNE全球快捷（美英法德）','CNE全球快捷（其它国家）',
						'CNE全球优先','HERMES优先','CNE英国直飞',
						'CNE全球特惠','HERMES特惠','CNE全球特货','CNE全球经济',
						'CNE夏季促销','E速宝优先','E速宝特惠',
						'E速宝经济','E速宝特货','E速宝跟踪', 'CNE华东欧电'))
					and date_add(apo.order_create_dt,interval 8 hour) >= '2021-01-01 00:00:00'
					and date_add(apo.order_create_dt,interval 8 hour) < date_add(curdate()-day(curdate())+1,interval 1 month )
					and concat(apo.effective_event_status,apo.effective_event_reason) in('301137','308000','407000','408000','304232' )
				group by  order_id ,order_no , lgo.channel_name, DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y%m%d"),lgo.customer_code ,lgo.logistics_no,lgo.des 
				order by 	orderCreateDt  desc 


"""


def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df


df_order_total = execude_sql(S_order_total)
df_benxitong_tuotou = execude_sql(S_benxitong_tuotou)
df_amazon_tuotou = execude_sql(S_amazon_tuotou)

print(df_order_total.head())
print('----------------------------')
print(df_benxitong_tuotou.head())
print('****************************')
print(df_amazon_tuotou.head())

# df_order_total["order_no"] = df_order_total["order_no"].astype("str")
# df_benxitong_tuotou["order_no"] = df_benxitong_tuotou["order_no"].astype("str")
# df_amazon_tuotou["order_no"] = df_amazon_tuotou["order_no"].astype("str")

df_order_total['orderCreateDt']=pd.to_datetime(df_order_total['orderCreateDt'])
df_benxitong_tuotou['orderCreateDt']=pd.to_datetime(df_benxitong_tuotou['orderCreateDt'])
df_amazon_tuotou['orderCreateDt']=pd.to_datetime(df_amazon_tuotou['orderCreateDt'])


def deal_str(data):
    data = str(data)
    return data

df_order_total['order_no'] = df_order_total['order_no'].map(deal_str)
df_order_total.to_csv('C:\\AmazonRate\\df_order_total.csv',index= False, encoding="utf_8_sig")

df_benxitong_tuotou['order_no'] = df_benxitong_tuotou['order_no'].map(deal_str)
print(df_benxitong_tuotou.head())
df_benxitong_tuotou.to_csv('C:\\AmazonRate\\df_benxitong_tuotou.csv',index= False, encoding="utf_8_sig")

df_amazon_tuotou['order_no'] = df_amazon_tuotou['order_no'].map(deal_str)
print(df_amazon_tuotou.head())
df_amazon_tuotou.to_csv('C:\\AmazonRate\\df_amazon_tuotou.csv',index= False, encoding="utf_8_sig")






