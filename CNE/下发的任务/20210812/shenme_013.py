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
from streamlit_echarts import st_echarts
import streamlit as st
st.set_page_config(layout="wide")#设置屏幕展开方式，宽屏模式布局更好

import pymysql
import datetime,time
nows = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

df_order_total_path = 'C:\\AmazonRate\\df_order_total.csv'
df_benxitong_tuotou_path = 'C:\\AmazonRate\\df_benxitong_tuotou.csv'
df_amazon_tuotou_path = 'C:\\AmazonRate\\df_amazon_tuotou.csv'

df_not_be_catched_path = 'C:\\AmazonRate\\df_not_be_catched.csv'

#平邮渠道
df_pingyou_path = 'C:\\AmazonRate\\df_pingyou.csv'




df_order_total = pd.read_csv(df_order_total_path)
df_benxitong_tuotou = pd.read_csv(df_benxitong_tuotou_path)
df_amazon_tuotou =   pd.read_csv(df_amazon_tuotou_path)

df_not_be_catched = pd.read_csv(df_not_be_catched_path)
df_pingyou = pd.read_csv(df_pingyou_path)


df_order_total["order_no"] = df_order_total["order_no"].astype("object")

df_benxitong_tuotou["order_no"] = df_benxitong_tuotou["order_no"].astype("object")
df_amazon_tuotou["order_no"] = df_amazon_tuotou["order_no"].astype("object")

df_not_be_catched["order_no"] = df_not_be_catched["order_no"].astype("object")
df_pingyou["order_no"] = df_pingyou["order_no"].astype("object")


df_order_total['orderCreateDt']= pd.to_datetime(df_order_total['orderCreateDt'])
df_benxitong_tuotou['orderCreateDt']= pd.to_datetime(df_benxitong_tuotou['orderCreateDt'])
df_amazon_tuotou['orderCreateDt']= pd.to_datetime(df_amazon_tuotou['orderCreateDt'])


print('------------------------------- 最初的数据量：-------------------------')
print(df_order_total.shape , df_benxitong_tuotou.shape, df_amazon_tuotou.shape , df_not_be_catched.shape , df_pingyou.shape)


# 利用 streamlit 获取页面出入的时间
st.subheader('选择时间序列')

import datetime

todayDate = datetime.datetime.now()
fifteenDaysAgo = todayDate - datetime.timedelta(days = 15)

# 改进的（by px 20210830）
start_time =  st.date_input('开始日期：',  fifteenDaysAgo)
end_time = st.date_input('结束日期：',  todayDate)

start_time = start_time.strftime('%Y-%m-%d')
end_time   = end_time.strftime('%Y-%m-%d')

st.write("时间段:", start_time  ," - ", end_time  )

shenme = (start_time ,end_time )


# 获得 df_order_total、df_benxitong_tuotou、df_amazon_tuotou 、df_not_be_catched 中符合时间条件的 数据
data_order_total = df_order_total[(df_order_total['orderCreateDt']>=  shenme[0]) & (df_order_total['orderCreateDt']<= shenme[1])]
data_benxitong_tuotou = df_benxitong_tuotou[(df_benxitong_tuotou['orderCreateDt']>=  shenme[0]) & (df_benxitong_tuotou['orderCreateDt']<= shenme[1])]
data_amazon_tuotou = df_amazon_tuotou[(df_amazon_tuotou['orderCreateDt']>=  shenme[0]) & (df_amazon_tuotou['orderCreateDt']<= shenme[1])]

data_not_be_catched = df_not_be_catched[(df_not_be_catched['gmt_create']>=  shenme[0]) & (df_not_be_catched['gmt_create']<= shenme[1])]
data_pingyou = df_pingyou[(df_pingyou['order_create_dt']>=  shenme[0]) & (df_pingyou['order_create_dt']<= shenme[1])]



print('-------- 结果集shape() ----------')
print( data_order_total.shape ,data_benxitong_tuotou.shape , data_amazon_tuotou.shape, data_not_be_catched.shape ,data_pingyou.shape)

# 获取 amazon 和 ‘系统’ 的差异数据并生成相关文件（注：amazon 和 ‘系统’ 妥投差异的数据目前不需要导出，但代码中依旧保留，以备后用 ）
df_chaji = pd.concat([data_benxitong_tuotou, data_amazon_tuotou, data_amazon_tuotou]).drop_duplicates(subset=['order_id','order_no', 'channel_name','orderCreateDt','customer_code','logistics_no','des' ], keep=False) #df1-df2
print('------------- df_chaji的数据： -------------')
print(df_chaji.head())

def deal_str(data):
    data = str(data)+'\t'
    return data

df_chaji['order_no'] = df_chaji['order_no'].map(deal_str)
df_chaji['logistics_no'] = df_chaji['logistics_no'].map(deal_str)
df_chaji_result = df_chaji[['orderCreateDt','customer_code','order_no','logistics_no','des','channel_name' ]]
df_chaji_result.rename(columns={"order_no":"内单号" , "channel_name":"渠道名称" , "orderCreateDt":"业务日期" ,
                                "customer_code":"客户编码" , "logistics_no":"转单号","des":"目的国" } ,inplace=True )

print('------------ df_chaji_result.shape & head -------------')
print(df_chaji_result.shape  )
# df_chaji_result.to_csv('C:\\AmazonRate\\本系统和亚马逊之间的差集.csv',index= False, encoding="utf_8_sig")

#将符合日期条件的没有被amazon 抓到的数据导入到文件中( add by px 20210910 )
data_not_be_catched.to_csv('C:\\AmazonRate\\未被amazon抓到的单量.csv',index= False, encoding="utf_8_sig")


# 准备图像化展示


# pie一 ; pie二 ：
# def getPie():
#     pie = Pie(init_opts=opts.InitOpts(theme='light', width='600px', height='1200px'))
#     pie.add("",
#             [list(z) for z in zip(["平邮-入库数量" ,"平邮-未入库数量" ],  [data_pingyou.shape[0] , 0])],
#             radius=["40%", "65%"],
#             center=["25%", "65%"])
#
#     # 添加多个饼图
#     pie.add("",
#             [list(z) for z in zip([ '挂号&优先-amazon妥投数量','挂号&优先-amazon未妥投数量' ],[data_amazon_tuotou.shape[0] ,data_order_total.shape[0]- data_amazon_tuotou.shape[0] ])],
#             radius=["40%", "65%"],
#             center=["75%", "65%"])
#
#     return pie
#
# chart_03 = getPie()
# streamlit_echarts.st_pyecharts(chart_03)



option = {
    "tooltip": {
        "trigger": 'item',
        "formatter": "{a} <br/>{b}: {c} ({d}%)"
    },

    "color": ["#27D9C8", "#D8D8D8"],
    "title": {
        # "text": "80%",
        "left": "center",
        "top": "50%",
        "textStyle": {
            "color": "#27D9C8",
            "fontSize": 36,
            "align": "center"
        }
    },
    "graphic": {
        "type": "text",
        "left": "center",
        "top": "40%",
        "style": {
            "text": "平邮-总订单数",
            "fill": "#333",
            "fontSize": 20,
            "fontWeight": 700
        }
    },
    "series": [
        {
            "name": '平邮-入库情况',
            "type": 'pie',
            "radius": ['65%', '70%'],
            "avoidLabelOverlap": "false",
            "label": {
                "normal": {
                    "show": "false",
                    "position": 'center'
                },

            },
            "data": [
                {"value": data_pingyou.shape[0], "name": '入库数量'},
                {"value": 0, "name": '未入库数量'},

            ]
        }
    ]
};


option_02 = {

    "tooltip": {
        "trigger": 'item',
        "formatter": "{a} <br/>{b}: {c} ({d}%)"
    },
    "color": ["#27D9C8", "#D8D8D8"],
    "title": {
        "text": data_order_total.shape[0] ,
        "left": "center",
        "top": "50%",
        "textStyle": {
            "fontSize": 36,
            "align": "center"
        }
    },
    "graphic": {
        "type": "text",
        "left": "center",
        "top": "40%",
        "style": {
            "text": "挂号&专线-总单量",
            "textAlign": "center",
            "fontSize": 30,
            "fontWeight": 720
        }
    },
    "series": [
        {
            "name": 'Amazon抓取情况',
            "type": 'pie',
            "radius": ['65%', '70%'],
            "avoidLabelOverlap": "false",
            "label": {
                "normal": {
                    "show": "false",
                    "position": 'center'
                },
            },

            "data": [
                {"value": data_amazon_tuotou.shape[0], "name": '已抓取'},
                {"value": data_order_total.shape[0]- data_amazon_tuotou.shape[0] , "name": '未抓取'},

            ]
        }
    ]
};


st_echarts(options=option)

st_echarts(options=option_02)


option = st.selectbox('请选择维度：', ('customer_code', 'channel_name', 'des'))
topN = int(st.text_input('选择topN', 10) )


def get_binary_file_downloader_html(bin_file, file_label='File'):
    with open(bin_file, 'rb') as f:
        data = f.read()
    bin_str = base64.b64encode(data).decode()
    href = f'<a href="data:application/octet-stream;base64,{bin_str}" download="{os.path.basename(bin_file)}">未抓取订单 - 点击下载 {file_label}</a>'
    return href


file_path = 'C:\\AmazonRate\\未被amazon抓到的单量.csv'
file_label = ''
st.markdown(get_binary_file_downloader_html(file_path, file_label), unsafe_allow_html=True)




zongling = data_not_be_catched.shape[0]
df_ser_01 = data_not_be_catched[option].value_counts()
df_01 = {option:df_ser_01.index,'count':df_ser_01.values}
df_02 = pd.DataFrame(df_01)
df_02["cumsum"] = df_02["count"].cumsum(axis=0)
df_02["cumsumRate"] =  round(df_02["count"] / zongling ,2 )

dataVal = df_02.head(topN)

x_data = dataVal[option].values.tolist()
y_data_1 = dataVal['count'].values.tolist()
y_data_2 = dataVal['cumsumRate'].values.tolist()

print(x_data)
print(y_data_1)
print(y_data_2)

new_numbers = [];
for n in x_data:
  new_numbers.append(str(n));
x_data = new_numbers;



def bar_line_combine_with_two_axis():
    bar = Bar(init_opts=opts.InitOpts(theme='light', width='1000px', height='600px'))
    bar.add_xaxis(x_data)
    # 添加一个Y轴
    bar.extend_axis(yaxis=opts.AxisOpts())

    bar.add_yaxis(option, y_data_1, yaxis_index=0)

    line = Line(init_opts=opts.InitOpts(theme='light', width='1000px', height='600px'))
    line.add_xaxis(x_data)
    # 将line数据通过yaxis_index指向后添加的Y轴
    line.add_yaxis('占比', y_data_2, yaxis_index=1)

    bar.overlap(line)
    return bar


chart_sub = bar_line_combine_with_two_axis()
streamlit_echarts.st_pyecharts(chart_sub ,height='530px' )



