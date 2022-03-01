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
import base64
import os

import numpy as np
import streamlit_echarts

import streamlit as st


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
   select order_id ,order_no , lgo.channel_name, DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y%m%d") as groupBYMonth  
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
				order by groupBYMonth desc 
"""


# 系统妥投数（本月 - ‘专线&挂号’- 妥投）

S_benxitong_tuotou = """
     select order_id ,order_no , lgo.channel_name , DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y%m%d") as groupBYMonth
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

                        and lgo.order_status = 3
                    group by  order_id ,order_no , lgo.channel_name,DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y%m%d")   
                    order by 	groupBYMonth  desc	
"""



# amazon妥投数（本月- ‘专线&挂号’-妥投）

S_amazon_tuotou = """
     select order_id ,order_no , lgo.channel_name, DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y%m%d") as groupBYMonth   
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
				group by  order_id ,order_no , lgo.channel_name, DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y%m%d")
				order by 	groupBYMonth  desc 


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

df_order_total["order_no"] = df_order_total["order_no"].astype("object")
df_benxitong_tuotou["order_no"] = df_benxitong_tuotou["order_no"].astype("object")
df_amazon_tuotou["order_no"] = df_amazon_tuotou["order_no"].astype("object")


def zhoushuPanbei(x):
    if x in ['1', '2', '3', '4', '5', '6', '7']:
        return '1'
    elif x in ['8', '9', '10', '11', '12', '13', '14']:
        return '2'
    elif x in ['15', '16', '17', '18', '19', '20', '21']:
        return '3'
    elif x in ['22', '23', '24', '25', '26', '27', '28']:
        return '4'
    elif x in ['29', '30', '31']:
        return '5'

df_order_total['groupBYMonth']=pd.to_datetime(df_order_total['groupBYMonth'])
df_benxitong_tuotou['groupBYMonth']=pd.to_datetime(df_benxitong_tuotou['groupBYMonth'])
df_amazon_tuotou['groupBYMonth']=pd.to_datetime(df_amazon_tuotou['groupBYMonth'])


df_order_total['moon']=df_order_total['groupBYMonth'].dt.month.astype('str')
df_benxitong_tuotou['moon']=df_benxitong_tuotou['groupBYMonth'].dt.month.astype('str')
df_amazon_tuotou['moon']=df_amazon_tuotou['groupBYMonth'].dt.month.astype('str')


df_order_total['day']=df_order_total['groupBYMonth'].dt.day.astype('str')
df_benxitong_tuotou['day']=df_benxitong_tuotou['groupBYMonth'].dt.day.astype('str')
df_amazon_tuotou['day']=df_amazon_tuotou['groupBYMonth'].dt.day.astype('str')

df_order_total['weekNo'] = df_order_total['day'].apply(zhoushuPanbei)
df_benxitong_tuotou['weekNo'] = df_benxitong_tuotou['day'].apply(zhoushuPanbei)
df_amazon_tuotou['weekNo'] = df_amazon_tuotou['day'].apply(zhoushuPanbei)

df_order_total['moonAndWeeNo']=df_order_total['moon']+'月-第'+df_order_total['weekNo']+"周"
df_benxitong_tuotou['moonAndWeeNo']=df_benxitong_tuotou['moon']+'月-第'+df_benxitong_tuotou['weekNo']+"周"
df_amazon_tuotou['moonAndWeeNo']=df_amazon_tuotou['moon']+'月-第'+df_amazon_tuotou['weekNo']+"周"

df_order_total.to_csv('C:\\AmazonRate\\df_order_total.csv',index= False, encoding="utf_8_sig")
df_benxitong_tuotou.to_csv('C:\\AmazonRate\\df_benxitong_tuotou.csv',index= False, encoding="utf_8_sig")
df_amazon_tuotou.to_csv('C:\\AmazonRate\\df_amazon_tuotou.csv',index= False, encoding="utf_8_sig")
