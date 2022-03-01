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
st.set_page_config(layout="wide")#设置屏幕展开方式，宽屏模式布局更好

import pymysql
import datetime,time
nows = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')

df_order_total_path = 'C:\\AmazonRate\\df_order_total.csv'
df_benxitong_tuotou_path = 'C:\\AmazonRate\\df_benxitong_tuotou.csv'
df_amazon_tuotou_path = 'C:\\AmazonRate\\df_amazon_tuotou.csv'

df_not_be_catched_path = 'C:\\AmazonRate\\df_not_be_catched.csv'

df_pingyou_path = 'C:\\AmazonRate\\df_pingyou.csv'
df_guahao_zhuanxian_path ='C:\\AmazonRate\\df_guahao_zhuanxian.csv'

df_lg_order_amazon_pingyou_path ='C:\\AmazonRate\\df_lg_order_amazon_pingyou.csv'
df_lg_order_amazon_guanhao_zhuanxian_path = 'C:\\AmazonRate\\df_lg_order_amazon_guanhao_zhuanxian.csv'




df_order_total      = pd.read_csv(df_order_total_path)
# print(df_order_total.head())
df_benxitong_tuotou = pd.read_csv(df_benxitong_tuotou_path)
# print(df_benxitong_tuotou.head())
df_amazon_tuotou =   pd.read_csv(df_amazon_tuotou_path)
# print(df_amazon_tuotou.head())
df_not_be_catched = pd.read_csv(df_not_be_catched_path)
# print(df_not_be_catched.head())
df_pingyou = pd.read_csv(df_pingyou_path)
# print(df_pingyou.head())
df_guahao_zhuanxian = pd.read_csv(df_guahao_zhuanxian_path)
# print(df_guahao_zhuanxian.head())
df_lg_order_amazon_pingyou  = pd.read_csv(df_lg_order_amazon_pingyou_path)
# print(df_lg_order_amazon_pingyou.head())
df_lg_order_amazon_guanhao_zhuanxian  = pd.read_csv(df_lg_order_amazon_guanhao_zhuanxian_path)
# print(df_lg_order_amazon_guanhao_zhuanxian.head())

df_order_total["order_no"] = df_order_total["order_no"].astype("object")
df_benxitong_tuotou["order_no"] = df_benxitong_tuotou["order_no"].astype("object")
df_amazon_tuotou["order_no"] = df_amazon_tuotou["order_no"].astype("object")

df_not_be_catched["order_no"] = df_not_be_catched["order_no"].astype("object")

df_pingyou["order_no"] = df_pingyou["order_no"].astype("object")
df_guahao_zhuanxian["order_no"] = df_guahao_zhuanxian["order_no"].astype("object")
df_lg_order_amazon_pingyou["order_no"] = df_lg_order_amazon_pingyou["order_no"].astype("object")
df_lg_order_amazon_guanhao_zhuanxian["order_no"] = df_lg_order_amazon_guanhao_zhuanxian["order_no"].astype("object")

# df_order_total['orderCreateDt']= pd.to_datetime(df_order_total['orderCreateDt'])
# df_benxitong_tuotou['orderCreateDt']= pd.to_datetime(df_benxitong_tuotou['orderCreateDt'])
# df_amazon_tuotou['orderCreateDt']= pd.to_datetime(df_amazon_tuotou['orderCreateDt'])


st.title('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp'
         '&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp'
         '&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp'
         '&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp'
         '&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp'
         ' 亚马逊抓取情况概览')

todayDate = datetime.datetime.now()
fifteenDaysAgo = todayDate - datetime.timedelta(days = 15)

# 改进的（by px 20210830）
start_time =  st.date_input('开始日期：',  fifteenDaysAgo)
end_time = st.date_input('结束日期：',  todayDate)

start_time_02 = start_time.strftime('%Y-%m-%d %H:%M:%S')
start_time = start_time.strftime('%Y-%m-%d')
end_time_02   = end_time.strftime('%Y-%m-%d %H:%M:%S')
end_time   = end_time.strftime('%Y-%m-%d')


st.write("时间段:", start_time  ," - ", end_time  )

# shenme = (start_time ,end_time )
shenme = (start_time_02 ,end_time_02 )
print(shenme)


print('------------- 填写时间类型 --------------')
print(shenme)
print(type(start_time) , type(end_time))

# 获得 df_order_total、df_benxitong_tuotou、df_amazon_tuotou 、df_not_be_catched 中符合时间条件的 数据
data_order_total = df_order_total[(df_order_total['orderCreateDt']>=  shenme[0]) & (df_order_total['orderCreateDt']<= shenme[1])]
data_benxitong_tuotou = df_benxitong_tuotou[(df_benxitong_tuotou['orderCreateDt']>=  shenme[0]) & (df_benxitong_tuotou['orderCreateDt']<= shenme[1])]
data_amazon_tuotou = df_amazon_tuotou[(df_amazon_tuotou['orderCreateDt']>=  shenme[0]) & (df_amazon_tuotou['orderCreateDt']<= shenme[1])]

data_not_be_catched = df_not_be_catched[(df_not_be_catched['gmt_create']>=  shenme[0]) & (df_not_be_catched['gmt_create']<= shenme[1])]

data_pingyou = df_pingyou[(df_pingyou['order_create_dt']>=  shenme[0]) & (df_pingyou['order_create_dt']<= shenme[1])]
data_guahao_zhuanxian = df_guahao_zhuanxian[(df_guahao_zhuanxian['order_create_dt']>=  shenme[0]) & (df_guahao_zhuanxian['order_create_dt']<= shenme[1])]
data_lg_order_amazon_pingyou = df_lg_order_amazon_pingyou[(df_lg_order_amazon_pingyou['gmt_create']>=  shenme[0]) & (df_lg_order_amazon_pingyou['gmt_create']<= shenme[1])]
data_lg_order_amazon_guanhao_zhuanxian = df_lg_order_amazon_guanhao_zhuanxian[(df_lg_order_amazon_guanhao_zhuanxian['gmt_create']>=  shenme[0]) & (df_lg_order_amazon_guanhao_zhuanxian['gmt_create']<= shenme[1])]

print('------------ 各个表的 时间字段类型 ------------')
print([ data_order_total["orderCreateDt"].dtype , data_benxitong_tuotou["orderCreateDt"].dtype , data_amazon_tuotou['orderCreateDt'].dtype ,
        data_not_be_catched["gmt_create"].dtype ,
        data_pingyou["order_create_dt"].dtype, data_guahao_zhuanxian["order_create_dt"].dtype ,
        data_lg_order_amazon_pingyou["gmt_create"].dtype, data_lg_order_amazon_guanhao_zhuanxian["gmt_create"].dtype])

# print(data_order_total["orderCreateDt"].head(3))
# print(data_benxitong_tuotou["orderCreateDt"].head(3))
# print(data_amazon_tuotou['orderCreateDt'].head(3))
#
# print(data_not_be_catched["gmt_create"].head(3))
#
# print(data_pingyou["order_create_dt"].head(3))
# print(data_guahao_zhuanxian["order_create_dt"].head(3))
# print(data_lg_order_amazon_pingyou["gmt_create"].head(3))
# print(data_lg_order_amazon_guanhao_zhuanxian["gmt_create"].head(3))

def deal_str(data):
    data = str(data)+'\t'
    return data


print('-------- 结果集shape() ----------')
print( data_order_total.shape ,data_benxitong_tuotou.shape , data_amazon_tuotou.shape, data_not_be_catched.shape ,data_pingyou.shape ,
       data_guahao_zhuanxian.shape, data_lg_order_amazon_pingyou.shape, data_lg_order_amazon_guanhao_zhuanxian.shape)

# 获取 amazon 和 ‘系统’ 的差异数据并生成相关文件（注：amazon 和 ‘系统’ 妥投差异的数据目前不需要导出，但代码中依旧保留，以备后用 ）
# df_chaji = pd.concat([data_benxitong_tuotou, data_amazon_tuotou, data_amazon_tuotou]).drop_duplicates(subset=['order_id','order_no', 'channel_name','orderCreateDt','customer_code','logistics_no','des' ], keep=False) #df1-df2
# print('------------- df_chaji的数据： -------------')
# print(df_chaji.head())
# df_chaji['order_no'] = df_chaji['order_no'].map(deal_str)
# df_chaji['logistics_no'] = df_chaji['logistics_no'].map(deal_str)
# df_chaji_result = df_chaji[['orderCreateDt','customer_code','order_no','logistics_no','des','channel_name' ]]
# df_chaji_result.rename(columns={"order_no":"内单号" , "channel_name":"渠道名称" , "orderCreateDt":"业务日期" ,
#                                 "customer_code":"客户编码" , "logistics_no":"转单号","des":"目的国" } ,inplace=True )
# print('------------ df_chaji_result.shape & head -------------')
# print(df_chaji_result.shape  )
# df_chaji_result.to_csv('C:\\AmazonRate\\本系统和亚马逊之间的差集.csv',index= False, encoding="utf_8_sig")

#将符合日期条件的没有被amazon 抓到的数据导入到文件中(  add by px 20210910  )
data_not_be_catched['order_no'] = data_not_be_catched['order_no'].map(deal_str)
data_not_be_catched['logistics_no'] = data_not_be_catched['logistics_no'].map(deal_str)
print(data_not_be_catched.columns.to_list())
data_not_be_catched = data_not_be_catched.rename(columns={"order_id":"订单ID" ,"logistics_no":"转单号","customer_code":"客户编码","customer_id":"客户Id", "channel_name":"渠道名称","r_country":"收件人国家","gmt_create":"业务日期","platform":"平台","receiver":"收件人","des":"目的国" } )
print('---------- data_not_be_catched.shape ------------')
print(data_not_be_catched.shape)

data_not_be_catched.to_csv('C:\\AmazonRate\\未被amazon抓到的单量.csv',index= False, encoding="utf_8_sig")


#------------------------------------第2部分开始（共4部分）------------------------------

### 平邮-入库占比
def getPie_01():

    js_code_str = """
                function(params){
                        console.log(params);
                        return   params.data.name+ '：' +params.data.value +', 占比：'+params.percent +'%' 
                    }
            """
    pie = Pie(init_opts=opts.InitOpts(theme='light', width='600px', height='1200px'))
    pie.add("",
            [list(z) for z in zip(["平邮渠道-亚马逊抓取到入库轨迹的订单数量","平邮渠道-亚马逊未抓取到入库轨迹的订单数量"],  [data_pingyou.shape[0], data_lg_order_amazon_pingyou.shape[0] - data_pingyou.shape[0] ])],
            radius=["40%", "65%"],
            center=["50%", "50%"],
             )
    pie.set_colors(["#DB7093", "#00BFFF" ])
    pie.set_global_opts(
            tooltip_opts=opts.TooltipOpts(formatter=JsCode(js_code_str)),
            title_opts= opts.TitleOpts(

                # 标题文本使用 \n 换行
                title='平邮-总单量 : ' +  str(data_lg_order_amazon_pingyou.shape[0]) ,
                pos_left='50%',
                pos_top='95%',
                title_textstyle_opts=(opts.TextStyleOpts(color='#2F4F4F', font_size='16')),

                ),
        )
    pie.set_series_opts(label_opts=opts.LabelOpts(is_show=False))

    return pie


chart_01 = getPie_01()
streamlit_echarts.st_pyecharts(chart_01,height='450px')

st.empty()
#------------------------------------第3部分开始（共4部分）------------------------------


### "专线&挂号" 两张图（入库占比 ， 妥投占比）
def getPie_02():

    js_code_str = """
                function(params){
                        console.log(params);
                        return   params.data.name+ '：' +params.data.value +', 占比：'+params.percent +'%' 
                    }
            """

    pie = Pie(init_opts=opts.InitOpts(theme='light', width='600px', height='1200px'))
    pie.add("",
            [list(z) for z in zip(["挂号&专线渠道-亚马逊抓取到入库轨迹的订单数量","挂号&专线渠道-亚马逊未抓取到入库轨迹的订单数量"],  [data_guahao_zhuanxian.shape[0] ,data_lg_order_amazon_guanhao_zhuanxian.shape[0] - data_guahao_zhuanxian.shape[0] ])],
            radius=["40%", "65%"],
            center=["35%", "50%"])

    # 添加多个饼图
    pie.add("",
            [list(z) for z in zip(['挂号&专线渠道-亚马逊抓取到妥投轨迹的订单数量','挂号&专线渠道-亚马逊未抓取到妥投轨迹的订单数量'],[data_amazon_tuotou.shape[0] ,data_benxitong_tuotou.shape[0]- data_amazon_tuotou.shape[0] ])],
            radius=["40%", "65%"],
            center=["65%", "50%"])

    pie.set_global_opts(
            tooltip_opts=opts.TooltipOpts(formatter=JsCode(js_code_str)),
            title_opts=opts.TitleOpts(
                title='挂号&专线-总单量 : ' +  str(data_lg_order_amazon_guanhao_zhuanxian.shape[0])  + '                                                                                                                                                                          '
                      + '挂号&专线-妥投总单量 : '+ str(data_benxitong_tuotou.shape[0]) ,

                pos_left='35%',
                pos_top='95%',
                title_textstyle_opts=(opts.TextStyleOpts(color='#2F4F4F', font_size='16')),

                )
        )

    pie.set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    return pie

chart_02 = getPie_02()
streamlit_echarts.st_pyecharts(chart_02,height='450px')


#------------------------------------第4部分开始（共4部分）------------------------------


option = st.selectbox('请选择维度：', ('客户编码', '渠道名称', '目的国'))

if option == '客户编码':
    option = '客户编码'
elif option == '渠道名称':
    option = '渠道名称'
elif option =='目的国':
    option = '目的国'


topN = int(st.text_input('选择topN', 10) )

def get_binary_file_downloader_html(shijian,bin_file, file_label='File'):
    with open(bin_file, 'rb') as f:
        data = f.read()
    bin_str = base64.b64encode(data).decode()
    href = f'<a href="data:application/octet-stream;base64,{bin_str}" download="{os.path.basename(bin_file)}">{shijian}期间亚马逊未抓取到订单目录 - 点击下载 {file_label}</a>'
    return href


file_path = 'C:\\AmazonRate\\未被amazon抓到的单量.csv'
file_label = ''
shijian=  start_time + " 至 "+ end_time
st.markdown(get_binary_file_downloader_html(shijian,file_path, file_label), unsafe_allow_html=True)

### 选定时间段内，lg_order表中存在，而 pull_order_amazon 中不存在的单量 （not_be_catched）

zongliang = data_not_be_catched.shape[0]
df_ser_01 = data_not_be_catched[option].value_counts()
df_01 = {option:df_ser_01.index,'count':df_ser_01.values}
df_02 = pd.DataFrame(df_01)
df_02["cumsum"] = df_02["count"].cumsum(axis=0)
df_02["cumsumRate"] =  round(df_02["count"] / zongliang ,2 )

dataVal = df_02.head(topN)

x_data = dataVal[option].values.tolist()
y_data_1 = dataVal['count'].values.tolist()
y_data_2 = dataVal['cumsumRate'].values.tolist()

new_numbers = [];
for n in x_data:
  new_numbers.append(str(n));
x_data = new_numbers;

st.title('&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp'
         '&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp'
         '&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp'
         '亚马逊未抓取订单概况')

def page_simple_layout():
    bar = Bar(init_opts=opts.InitOpts(theme='light', width='1000px',  height='600px'))
    bar.add_xaxis(x_data)
    bar.add_yaxis('', y_data_1)
    bar.set_global_opts(
        xaxis_opts=opts.AxisOpts(type_='category', name='维度', axislabel_opts={"rotate": 10}),
        yaxis_opts=opts.AxisOpts(type_='value', name='单量' ),)

    return bar

def line_with_two_xaxis():
    line = Line(init_opts=opts.InitOpts(theme='light', width='1000px', height='600px'))
    line.set_global_opts(
        xaxis_opts=opts.AxisOpts(type_='category', name='维度', axislabel_opts={"rotate": 10}),
        yaxis_opts=opts.AxisOpts(type_='value', name='占比' ))

    line.add_xaxis(x_data)
    line.add_yaxis('',y_data_2 )
    return line



chart   = page_simple_layout()
chart02 = line_with_two_xaxis()

col1, col2  = st.columns(2)

with col1:
   streamlit_echarts.st_pyecharts(chart,height='550px' )


with col2:
   streamlit_echarts.st_pyecharts(chart02,height='550px')





