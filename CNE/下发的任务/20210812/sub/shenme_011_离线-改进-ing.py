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
# st.set_page_config(layout="wide")#设置屏幕展开方式，宽屏模式布局更好

import pymysql
import datetime,time
nows = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

df_order_total_path = 'C:\\AmazonRate_old\\df_order_total.csv'
df_benxitong_tuotou_path = 'C:\\AmazonRate_old\\df_benxitong_tuotou.csv'
df_amazon_tuotou_path = 'C:\\AmazonRate_old\\df_amazon_tuotou.csv'

df_order_total = pd.read_csv(df_order_total_path)
df_benxitong_tuotou = pd.read_csv(df_benxitong_tuotou_path)
df_amazon_tuotou =   pd.read_csv(df_amazon_tuotou_path)

df_order_total["order_no"] = df_order_total["order_no"].astype("object")
df_benxitong_tuotou["order_no"] = df_benxitong_tuotou["order_no"].astype("object")
df_amazon_tuotou["order_no"] = df_amazon_tuotou["order_no"].astype("object")

df_order_total['orderCreateDt']= pd.to_datetime(df_order_total['orderCreateDt'])
df_benxitong_tuotou['orderCreateDt']= pd.to_datetime(df_benxitong_tuotou['orderCreateDt'])
df_amazon_tuotou['orderCreateDt']= pd.to_datetime(df_amazon_tuotou['orderCreateDt'])


print('-------------------  最初的数据量： -------------')
print(df_order_total.shape , df_benxitong_tuotou.shape, df_amazon_tuotou.shape)


# 利用 streamlit 获取页面出入的时间
st.subheader('选择时间序列')

import datetime

todayDate = datetime.datetime.now()
print(todayDate.strftime('%Y-%m-%d'))

fifteenDaysAgo = todayDate - datetime.timedelta(days = 15)
print(fifteenDaysAgo.strftime('%Y-%m-%d'))

# 最开始的
#----- 开始 ----
# start_time =  st.date_input('开始日期：',  datetime.datetime.strptime('2021-08-16', '%Y-%m-%d'))
# end_time = st.date_input('结束日期：',  datetime.datetime.strptime('2021-08-30', '%Y-%m-%d'))
#----- 结束 ----

# 改进的（by px 20210830）
# ---- 开始 ----
start_time =  st.date_input('开始日期：',  fifteenDaysAgo)
end_time = st.date_input('结束日期：',  todayDate)
#----- 结束 ---



start_time = start_time.strftime('%Y-%m-%d')
end_time   = end_time.strftime('%Y-%m-%d')

st.write("时间段:", start_time  ," - ", end_time  )

shenme = (start_time ,end_time )

print(shenme[0] , shenme[1])

# 获得 df_order_total、df_benxitong_tuotou、df_amazon_tuotou 中符合时间条件的 数据
data_order_total = df_order_total[(df_order_total['orderCreateDt']>=  shenme[0]) & (df_order_total['orderCreateDt']<= shenme[1])]

data_benxitong_tuotou = df_benxitong_tuotou[(df_benxitong_tuotou['orderCreateDt']>=  shenme[0]) & (df_benxitong_tuotou['orderCreateDt']<= shenme[1])]
data_benxitong_tuotou.to_csv('C:\\AmazonRate_old\\result_benxitong_tuotou.csv',index= False, encoding="utf_8_sig")

data_amazon_tuotou = df_amazon_tuotou[(df_amazon_tuotou['orderCreateDt']>=  shenme[0]) & (df_amazon_tuotou['orderCreateDt']<= shenme[1])]
data_amazon_tuotou.to_csv('C:\\AmazonRate_old\\result_amazon_tuotou.csv',index= False, encoding="utf_8_sig")

print('-------- 修改后的 shape() ----------')
print( data_order_total.shape ,data_benxitong_tuotou.shape , data_amazon_tuotou.shape )

# 获取 amazon 和 ‘系统’ 的差异数据并生成相关文件
df_chaji = pd.concat([data_benxitong_tuotou, data_amazon_tuotou, data_amazon_tuotou]).drop_duplicates(subset=['order_id','order_no', 'channel_name','orderCreateDt','customer_code','logistics_no','des' ], keep=False) #df1-df2
print('------------- df_chaji的数据： -------------')
print(df_chaji.head())


# df_chaji_02 = pd.concat([data_amazon_tuotou, data_benxitong_tuotou, data_benxitong_tuotou]).drop_duplicates(subset=['order_id','order_no', 'channel_name','orderCreateDt','customer_code','logistics_no','des' ], keep=False) #df1-df2






def deal_str(data):
    data = str(data)+'\t'
    return data

df_chaji['order_no'] = df_chaji['order_no'].map(deal_str)
df_chaji['logistics_no'] = df_chaji['logistics_no'].map(deal_str)

df_chaji_result = df_chaji[['orderCreateDt','customer_code','order_no','logistics_no','des','channel_name' ]]
df_chaji_result.rename(columns={"order_no":"内单号" , "channel_name":"渠道名称" , "orderCreateDt":"业务日期" ,
                                "customer_code":"客户编码" , "logistics_no":"转单号","des":"目的国" } ,inplace=True )

print('------------ df_chaji_result.shape & head   -------------')
print(df_chaji_result.shape  )

df_chaji_result.to_csv('C:\\AmazonRate_old\\本系统和亚马逊之间的差集.csv',index= False, encoding="utf_8_sig")


# 将 data_order_total 、data_benxitong_tuotou 、data_amazon_tuotou 组合成 能 图表展示的 dataframe

print('-------------------------')
print(data_order_total.shape)
print(data_benxitong_tuotou.shape)
print(data_amazon_tuotou.shape)
print('-=-=-=-=-=-=-=-=-=-=-=-=-')



# 准备图像化展示

# mergedf_total_xitong_amazon.rename(columns={"index":"周序号" ,
#                                             "moonAndWeeNo_x":"订单总数" ,
#                                             "moonAndWeeNo_y":"系统妥投数" ,
#                                             "moonAndWeeNo":"amazon妥投数" ,
#                                             "rate":"查询成功率" }, inplace=True)

# print(mergedf_total_xitong_amazon.head())


print('----------------- rateList: --------------')


def page_simple_layout():

    xitongCount = data_benxitong_tuotou.shape[0]
    amazongCount = data_amazon_tuotou.shape[0]
    zhanbi = '{:.2f}%'.format(amazongCount / xitongCount * 100)

    js_code_str = """
            function(params){
                    console.log(params);
                    return   params.seriesName+ '：' +params.data +', 抓取成功率： """+zhanbi+"""'  
                }
        """

    bar_1 = Bar(init_opts=opts.InitOpts(theme='light' ))

    x_valList = ['从 '+start_time+' 至 '+end_time ]
    bar_1.add_xaxis(x_valList)
    bar_1.add_yaxis('订单总数',  [data_order_total.shape[0]],stack='stack1')
    bar_1.add_yaxis('系统妥投数',  [data_benxitong_tuotou.shape[0]], stack='stack1')
    bar_1.add_yaxis('amazon妥投数', [data_amazon_tuotou.shape[0]] , stack='stack1')

    bar_1.set_global_opts(title_opts=opts.TitleOpts(title='妥投订单数量统计图（'+start_time+' - '+end_time+'）', pos_left='center', pos_top='top'),
                          legend_opts=opts.LegendOpts(is_show=True, orient='vertical', pos_top='5%', pos_right='1%'),
                          tooltip_opts=opts.TooltipOpts(formatter=JsCode(js_code_str))
                          )
    return bar_1


def get_binary_file_downloader_html(bin_file, file_label='File'):
    with open(bin_file, 'rb') as f:
        data = f.read()
    bin_str = base64.b64encode(data).decode()
    href = f'<a href="data:application/octet-stream;base64,{bin_str}" download="{os.path.basename(bin_file)}">点击下载 {file_label}</a>'
    return href

file_path = filePath = 'C:\\AmazonRate_old\\本系统和亚马逊之间的差集.csv'
file_label = ''
st.markdown(get_binary_file_downloader_html(file_path, file_label), unsafe_allow_html=True)

chart = page_simple_layout()

streamlit_echarts.st_pyecharts(chart,height='550px')



