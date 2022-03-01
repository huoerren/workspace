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
   select order_id ,order_no , lgo.channel_name, DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y-%m-%d %H:%i:%s") as orderCreateDt  
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
				group by  order_id ,order_no , lgo.channel_name  , DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y-%m-%d %H:%i:%s")
				order by orderCreateDt desc 
"""

# 系统妥投数（本月 - ‘专线&挂号’- 妥投）

S_benxitong_tuotou = """
     select order_id ,order_no , lgo.channel_name, DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y-%m-%d %H:%i:%s") as orderCreateDt, lgo.customer_code ,lgo.logistics_no,lgo.des 
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

                        and ((lgo.order_status = 3) or (lgo.order_status != 3 and  apo.effective_event_status in('301','308','407','408','304','D1','AV','A7','A3','AH' )  ) )
                        
                        group by  order_id ,order_no , lgo.channel_name,DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y-%m-%d %H:%i:%s") ,lgo.customer_code ,lgo.logistics_no,lgo.des      
                    order by 	orderCreateDt  desc	
"""


# amazon妥投数（本月- ‘专线&挂号’-妥投）

S_amazon_tuotou = """
     select order_id ,order_no , lgo.channel_name, DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y-%m-%d %H:%i:%s") as orderCreateDt, lgo.customer_code ,lgo.logistics_no,lgo.des   
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
					and apo.effective_event_status in('301','308','407','408','304','D1','AV','A7','A3','AH' )
				group by  order_id ,order_no , lgo.channel_name, DATE_FORMAT(date_add(apo.order_create_dt ,interval 8 hour),"%Y-%m-%d %H:%i:%s"),lgo.customer_code ,lgo.logistics_no,lgo.des 
				order by 	orderCreateDt  desc 


"""


###  add by px 20210910  --- start ---

# lg_order中存在而 amazon_pull_order 中不存在的单量（时间段是： 当前月的前2月1号 到 当前日期 ）
S_not_Be_catched = '''
    select  order_no ,logistics_no,	customer_code,	customer_id,	channel_name,	r_country,	gmt_create,	platform,	receiver,	des from  lg_order where id in  (
	    select  a.id  from (
		    select distinct id from lg_order lgo  
			    where date_add(lgo.gmt_create,interval 8 hour) >= date_add(curdate()-day(curdate())+1,interval -2 month)  and date_add(lgo.gmt_create,interval 8 hour) < DATE_SUB(curdate(),INTERVAL -1 DAY)      
						and lgo.platform = 'AMAZON' ) a 
		left join 
		( select distinct order_id from  amazon_pull_order apo  
			where date_add(apo.order_create_dt,interval 8 hour) >= date_add(curdate()-day(curdate())+1,interval -2 month)  and date_add(apo.order_create_dt,interval 8 hour) < DATE_SUB(curdate(),INTERVAL -1 DAY)  
		) b on a.id = b.order_id 
	where b.order_id is null  
) 

'''

### amazon_pull_order-AMAZON-平邮
S_pingyou_channel = '''
 select distinct order_id, order_create_dt, order_no,logistics_no  
    from amazon_pull_order apo left join lg_order lgo 
        on apo.order_id = lgo.id 
            where date_add(apo.order_create_dt,interval 8 hour) >= '2021-01-01 00:00:00'
					 and date_add(apo.order_create_dt,interval 8 hour) < date_add(curdate()-day(curdate())+1,interval 1 month )
							 and lgo.channel_name like '%平邮%'
							    and lgo.platform = 'AMAZON'
'''

### lg_order-'AMAZON'- ‘平邮’
S_lg_order_amazon_pingyou = '''
    select order_no,logistics_no,gmt_create from lg_order lgo 
        where  date_add(lgo.gmt_create,interval 8 hour) >= '2021-01-01 00:00:00' and date_add(lgo.gmt_create ,interval 8 hour) < DATE_SUB(curdate(),INTERVAL -1 DAY) 
            and lgo.channel_name like '%平邮%' and platform = 'AMAZON'
'''

### amazon_pull_order-AMAZON- ‘挂号’或‘专线’-入库
S_guahao_zhuanxian_channel = '''
 select distinct order_id, order_create_dt, order_no,logistics_no  
    from amazon_pull_order apo left join lg_order lgo 
        on apo.order_id = lgo.id 
            where date_add(apo.order_create_dt,interval 8 hour) >= '2021-01-01 00:00:00'
					 and date_add(apo.order_create_dt,interval 8 hour) < date_add(curdate()-day(curdate())+1,interval 1 month )
							 and  (lgo.channel_name  like '%挂号%' or lgo.channel_name in (
			        'CNE全球速达','CNE全球快捷（美英法德）','CNE全球快捷（其它国家）',
						'CNE全球优先','HERMES优先','CNE英国直飞',
						    'CNE全球特惠','HERMES特惠','CNE全球特货','CNE全球经济',
						        'CNE夏季促销','E速宝优先','E速宝特惠',
						            'E速宝经济','E速宝特货','E速宝跟踪', 'CNE华东欧电'))
							    and lgo.platform = 'AMAZON'
'''

### lg_order-'AMAZON'- ‘挂号’或‘专线’- 中的单
S_lg_order_amazon_guahao_zhuanxian = '''
    select  order_no,logistics_no,gmt_create from lg_order lgo 
        where  date_add(lgo.gmt_create,interval 8 hour) >= '2021-01-01 00:00:00' and date_add(lgo.gmt_create ,interval 8 hour) < DATE_SUB(curdate(),INTERVAL -1 DAY) 
            and (lgo.channel_name  like '%挂号%' or lgo.channel_name in (
			        'CNE全球速达','CNE全球快捷（美英法德）','CNE全球快捷（其它国家）',
						'CNE全球优先','HERMES优先','CNE英国直飞',
						    'CNE全球特惠','HERMES特惠','CNE全球特货','CNE全球经济',
						        'CNE夏季促销','E速宝优先','E速宝特惠',
						            'E速宝经济','E速宝特货','E速宝跟踪', 'CNE华东欧电'))
						            and platform = 'AMAZON'

'''


### add by px 20210910  --- end ---



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

### add by px  20210910 -- start
df_not_be_catched = execude_sql(S_not_Be_catched)

# amazon_pull_order 中‘平邮’|‘挂号-专线’ - 入库
df_pingyou           = execude_sql(S_pingyou_channel)
df_guahao_zhuanxian  = execude_sql(S_guahao_zhuanxian_channel)

df_lg_order_amazon_pingyou            = execude_sql(S_lg_order_amazon_pingyou)
df_lg_order_amazon_guanhao_zhuanxian  = execude_sql(S_lg_order_amazon_guahao_zhuanxian)


### add by px  20210910 -- end

df_order_total["order_no"] = df_order_total["order_no"].astype("str")
df_benxitong_tuotou["order_no"] = df_benxitong_tuotou["order_no"].astype("str")
df_amazon_tuotou["order_no"] = df_amazon_tuotou["order_no"].astype("str")

# df_order_total['orderCreateDt']=pd.to_datetime(df_order_total['orderCreateDt'])
# df_benxitong_tuotou['orderCreateDt']=pd.to_datetime(df_benxitong_tuotou['orderCreateDt'])
# df_amazon_tuotou['orderCreateDt']=pd.to_datetime(df_amazon_tuotou['orderCreateDt'])

print('--------------- 三张表的数据 -------------------')
print(df_order_total['orderCreateDt'].dtype)


def deal_str(data):
    data = str(data)
    return data

df_order_total['order_no'] = df_order_total['order_no'].map(deal_str)
df_order_total.to_csv('C:\\AmazonRate\\df_order_total.csv',index= False, encoding="utf_8_sig")

df_benxitong_tuotou['order_no'] = df_benxitong_tuotou['order_no'].map(deal_str)
df_benxitong_tuotou.to_csv('C:\\AmazonRate\\df_benxitong_tuotou.csv',index= False, encoding="utf_8_sig")

df_amazon_tuotou['order_no'] = df_amazon_tuotou['order_no'].map(deal_str)
df_amazon_tuotou.to_csv('C:\\AmazonRate\\df_amazon_tuotou.csv',index= False, encoding="utf_8_sig")

#### add by px 20210910
df_not_be_catched['order_no'] = df_not_be_catched['order_no'].map(deal_str)
df_not_be_catched.to_csv('C:\\AmazonRate\\df_not_be_catched.csv',index= False, encoding="utf_8_sig")

df_pingyou['order_no'] = df_pingyou['order_no'].map(deal_str)
df_pingyou['logistics_no'] = df_pingyou['logistics_no'].map(deal_str)
df_pingyou.to_csv('C:\\AmazonRate\\df_pingyou.csv',index= False, encoding="utf_8_sig")

df_guahao_zhuanxian['order_no'] = df_guahao_zhuanxian['order_no'].map(deal_str)
df_guahao_zhuanxian['logistics_no'] = df_guahao_zhuanxian['logistics_no'].map(deal_str)
df_guahao_zhuanxian.to_csv('C:\\AmazonRate\\df_guahao_zhuanxian.csv',index= False, encoding="utf_8_sig")

df_lg_order_amazon_pingyou['order_no'] = df_lg_order_amazon_pingyou['order_no'].map(deal_str)
df_lg_order_amazon_pingyou['logistics_no'] = df_lg_order_amazon_pingyou['logistics_no'].map(deal_str)
df_lg_order_amazon_pingyou.to_csv('C:\\AmazonRate\\df_lg_order_amazon_pingyou.csv',index= False, encoding="utf_8_sig")

df_lg_order_amazon_guanhao_zhuanxian['order_no'] = df_lg_order_amazon_guanhao_zhuanxian['order_no'].map(deal_str)
df_lg_order_amazon_guanhao_zhuanxian['logistics_no'] = df_lg_order_amazon_guanhao_zhuanxian['logistics_no'].map(deal_str)
df_lg_order_amazon_guanhao_zhuanxian.to_csv('C:\\AmazonRate\\df_lg_order_amazon_guanhao_zhuanxian.csv',index= False, encoding="utf_8_sig")


