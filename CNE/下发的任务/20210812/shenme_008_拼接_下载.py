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


def getDate(startStr, endStr):
    qishi_yue = startStr.split('-')[0].replace('月','')
    if len(qishi_yue)<2:
        qishi_yue = "0"+qishi_yue


    zhongzhi_yue = endStr.split('-')[0].replace('月', '')
    if len(zhongzhi_yue) < 2:
        zhongzhi_yue = "0" + zhongzhi_yue
    zhongzhi_yue = zhongzhi_yue

    hao_start = startStr.split('-')[1].replace('第', '').replace('周', '')
    if hao_start == '1':
        hao_start = '01'
    elif hao_start == '2':
        hao_start = '08'
    elif hao_start == '3':
        hao_start = '15'
    elif hao_start == '4':
        hao_start = '22'
    elif hao_start == '5':
        hao_start = '29'

    hao_end = endStr.split('-')[1].replace('第','').replace('周','')
    if hao_end == '1':
        hao_end = '07'
    elif hao_end == '2':
        hao_end = '14'
    elif hao_end == '3':
        hao_end = '21'
    elif hao_end == '4':
        hao_end = '28'
    elif hao_end == '5':
        hao_end = '31'

    qishi_date = '2021-'+qishi_yue+'-'+ hao_start
    zhongzhi_date = '2021-'+zhongzhi_yue+'-'+hao_end

    print(qishi_date , zhongzhi_date)
    return (qishi_date , zhongzhi_date)

# 利用 streamlit 获取页面出入的时间
st.subheader('2.选择时间序列')
options = np.array(df_order_total['moonAndWeeNo']).tolist()
(end_time,start_time ) = st.select_slider("请选择时间序列长度：",
                                          options=options,
                                          value=('7月-第1周','8月-第2周',),
                                          )


st.write("时间序列开始时间:", start_time)
st.write("时间序列结束时间:", end_time)

shenme = getDate(start_time,end_time)
print(shenme[0] , shenme[1])

# 获得 df_order_total、df_benxitong_tuotou、df_amazon_tuotou 中符合时间条件的 数据

data_order_total = df_order_total[(df_order_total['groupBYMonth']>=  shenme[0]) & (df_order_total['groupBYMonth']<= shenme[1])]
print('-------- data_order_total.shape() --')
print(data_order_total.shape )

data_benxitong_tuotou = df_benxitong_tuotou[(df_benxitong_tuotou['groupBYMonth']>=  shenme[0]) & (df_benxitong_tuotou['groupBYMonth']<= shenme[1])]
print('-------- data_benxitong_tuotou.shape() --')
print(data_benxitong_tuotou.shape )
data_amazon_tuotou = df_amazon_tuotou[(df_amazon_tuotou['groupBYMonth']>=  shenme[0]) & (df_amazon_tuotou['groupBYMonth']<= shenme[1])]
print('-------- data_amazon_tuotou.shape() --')
print(data_amazon_tuotou.shape )

# 获取 amazon 和 ‘系统’ 的差异数据并生成相关文件
df_chaji = pd.concat([data_benxitong_tuotou, data_amazon_tuotou, data_amazon_tuotou]).drop_duplicates(subset=['order_id', 'order_no'], keep=False)#df1-df2

print('------------ df_chaji.shape -------------')
print(df_chaji.shape)
df_chaji.to_csv('C:\\AmazonRate\\df_chaji.csv',index= False, encoding="utf_8_sig")


# 将 data_order_total 、data_benxitong_tuotou 、data_amazon_tuotou 组合成 能 图表展示的 dataframe

series_order_total = data_order_total['moonAndWeeNo'].value_counts().sort_index()
series_benxitong = data_benxitong_tuotou['moonAndWeeNo'].value_counts().sort_index()
series_amazon = data_amazon_tuotou['moonAndWeeNo'].value_counts().sort_index()

df_order_total = series_order_total.to_frame().reset_index()
df_benxitong = series_benxitong.to_frame().reset_index()
df_amazon = series_amazon.to_frame().reset_index()

mergedf_total_xitong = pd.merge(df_order_total ,df_benxitong,on="index",how='left')
mergedf_total_xitong_amazon = pd.merge(mergedf_total_xitong ,df_amazon,on="index",how='left')


# 准备图像化展示

mergedf_total_xitong_amazon.replace(np.nan, 0, inplace=True)
mergedf_total_xitong_amazon.replace(np.inf, 0, inplace=True)

mergedf_total_xitong_amazon['moonAndWeeNo_x'] = mergedf_total_xitong_amazon['moonAndWeeNo_x'].astype("int")
mergedf_total_xitong_amazon['moonAndWeeNo_y'] = mergedf_total_xitong_amazon['moonAndWeeNo_y'].astype("int")
mergedf_total_xitong_amazon['moonAndWeeNo'] = mergedf_total_xitong_amazon['moonAndWeeNo'].astype("int")
mergedf_total_xitong_amazon['rate'] = round( mergedf_total_xitong_amazon['moonAndWeeNo']/mergedf_total_xitong_amazon['moonAndWeeNo_y'], 3)
mergedf_total_xitong_amazon['rate'] = mergedf_total_xitong_amazon['rate'].apply(lambda x : 1 if x>1 else x )

mergedf_total_xitong_amazon.rename(columns={"index":"周序号" ,"moonAndWeeNo_x":"订单总数" ,"moonAndWeeNo_y":"系统妥投数" ,
                                            "moonAndWeeNo":"amazon妥投数" ,"rate":"查询成功率" }, inplace=True)

# print(mergedf_total_xitong_amazon.head())

zhouXuHaoList = list(mergedf_total_xitong_amazon['周序号'])
valueList_order_total = list(mergedf_total_xitong_amazon['订单总数'])
valueList_benxitong_tuotou = list(mergedf_total_xitong_amazon['系统妥投数'])
valueList_amazon_tuotou = list(mergedf_total_xitong_amazon['amazon妥投数'])
rateList = list(mergedf_total_xitong_amazon['查询成功率'])


def page_simple_layout():

    js_code_str = """
        function(params){
            console.log(params);
            return  params.data[0]  +' : '+ (  window.parseFloat(params.data[1]).toFixed(2)   )*100 + '%' ;
                        }
    """

    js_per = """
        function(params){
            console.log(params);
            return (  window.parseFloat(params.data[1]).toFixed(2)   )*100 + '%' ; 
        }
            """

    js_per_kedu = """
            function(value){            
                return   (value*100)+'%' ; 
                }
           """

    # 图表1
    bar_1 = Bar(init_opts=opts.InitOpts(theme='light', width='1300px', height='700px' ))

    x_valList = zhouXuHaoList
    bar_1.add_xaxis(x_valList)
    bar_1.add_yaxis('订单总数', valueList_order_total ,stack='stack1')
    bar_1.add_yaxis('系统妥投数', valueList_benxitong_tuotou , stack='stack1')
    bar_1.add_yaxis('amazon妥投数', valueList_amazon_tuotou , stack='stack1')

    bar_1.set_global_opts(title_opts=opts.TitleOpts(title='妥投订单数量统计图（2021.06-自今）', pos_left='center', pos_top='top'),
                          legend_opts=opts.LegendOpts(is_show=True, orient='vertical', pos_top='15%', pos_right='10%')
                          )

    # 图表2
    line = Line(init_opts=opts.InitOpts(theme='light', width='1200px', height='600px'))
    sub_x_valList = zhouXuHaoList
    line.add_xaxis(sub_x_valList)
    line.add_yaxis('比例', rateList, label_opts=opts.LabelOpts(formatter=JsCode(js_per)))
    line.set_global_opts(
        title_opts=opts.TitleOpts(title='亚马逊妥投节点成功抓取率统计（2021.06-自今）', pos_left='center', pos_top='top'),
        legend_opts=opts.LegendOpts(is_show=True, orient='vertical', pos_top='25%', pos_right='10%'),
        xaxis_opts=opts.AxisOpts(type_='category', name='查询周', axislabel_opts={"rotate": 10}),
        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(formatter=JsCode(js_per_kedu)), type_='value', max_=1),
        tooltip_opts=opts.TooltipOpts(formatter=JsCode(js_code_str)) )



    return bar_1

def page_simple_layout_02():

    js_code_str = """
        function(params){
            console.log(params);
            return  params.data[0]  +' : '+ (  window.parseFloat(params.data[1]).toFixed(2)   )*100 + '%' ;
                        }
    """

    js_per = """
        function(params){
            console.log(params);
            return (  window.parseFloat(params.data[1]).toFixed(2)   )*100 + '%' ; 
        }
            """

    js_per_kedu = """
            function(value){            
                return   (value*100)+'%' ; 
                }
           """

    # 图表1
    bar_1 = Bar(init_opts=opts.InitOpts(theme='light', width='1300px', height='700px' ))

    x_valList = zhouXuHaoList
    bar_1.add_xaxis(x_valList)
    bar_1.add_yaxis('订单总数', valueList_order_total)
    bar_1.add_yaxis('系统妥投数', valueList_benxitong_tuotou)
    bar_1.add_yaxis('amazon妥投数', valueList_amazon_tuotou)

    bar_1.set_global_opts(title_opts=opts.TitleOpts(title='妥投订单数量统计图（2021.06-自今）', pos_left='center', pos_top='top'),
                          legend_opts=opts.LegendOpts(is_show=True, orient='vertical', pos_top='15%', pos_right='10%')
                          )

    # 图表2
    line = Line(init_opts=opts.InitOpts(theme='light', width='1200px', height='600px'))
    sub_x_valList = zhouXuHaoList
    line.add_xaxis(sub_x_valList)
    line.add_yaxis('比例', rateList, label_opts=opts.LabelOpts(formatter=JsCode(js_per)))
    line.set_global_opts(
        title_opts=opts.TitleOpts(title='亚马逊妥投节点成功抓取率统计（2021.06-自今）', pos_left='center', pos_top='top'),
        legend_opts=opts.LegendOpts(is_show=True, orient='vertical', pos_top='25%', pos_right='10%'),
        xaxis_opts=opts.AxisOpts(type_='category', name='查询周', axislabel_opts={"rotate": 10}),
        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(formatter=JsCode(js_per_kedu)), type_='value', max_=1),
        tooltip_opts=opts.TooltipOpts(formatter=JsCode(js_code_str)),
        datazoom_opts=opts.DataZoomOpts(pos_bottom=-5, range_start=50, range_end=100))

    return line


def get_binary_file_downloader_html(bin_file, file_label='File'):
    with open(bin_file, 'rb') as f:
        data = f.read()
    bin_str = base64.b64encode(data).decode()
    href = f'<a href="data:application/octet-stream;base64,{bin_str}" download="{os.path.basename(bin_file)}">点击下载 {file_label}</a>'
    return href

file_path = filePath = 'C:\\AmazonRate\\df_chaji.csv'
file_label = ''
st.markdown(get_binary_file_downloader_html(file_path, file_label), unsafe_allow_html=True)

chart = page_simple_layout()
chart_02 = page_simple_layout_02()

streamlit_echarts.st_pyecharts( chart )
streamlit_echarts.st_pyecharts( chart_02 )



