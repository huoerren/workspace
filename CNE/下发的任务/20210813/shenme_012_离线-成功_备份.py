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

df_order_total_path = 'C:\\AmazonRate\\df_order_total.csv'
df_benxitong_tuotou_path = 'C:\\AmazonRate\\df_benxitong_tuotou.csv'
df_amazon_tuotou_path = 'C:\\AmazonRate\\df_amazon_tuotou.csv'

df_order_total = pd.read_csv(df_order_total_path)
df_benxitong_tuotou = pd.read_csv(df_benxitong_tuotou_path)
df_amazon_tuotou =   pd.read_csv(df_amazon_tuotou_path)

# print(df_order_total.head())
# print(df_benxitong_tuotou.head())
# print(df_amazon_tuotou.head())

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
print('-------------------  最初的数据量： -------------')
print(df_order_total.shape , df_benxitong_tuotou.shape, df_amazon_tuotou.shape)

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
        if zhongzhi_yue in ('04','06','09','11'):
            hao_end = '30'
        else:
            hao_end = '31'

    print(zhongzhi_yue)


    qishi_date = '2021-'+qishi_yue+'-'+ hao_start
    zhongzhi_date = '2021-'+zhongzhi_yue+'-'+hao_end

    return (qishi_date , zhongzhi_date)

# 利用 streamlit 获取页面出入的时间
st.subheader('2.选择时间序列')
options = np.array(df_order_total['moonAndWeeNo']).tolist()
(end_time,start_time ) = st.select_slider("请选择时间序列长度：",
                                          options=options,
                                          value=('7月-第1周','8月-第2周',),
                                          )


st.write("时间段:", start_time ," - ", end_time )

shenme = getDate(start_time,end_time)
print(shenme[0] , shenme[1])

# 获得 df_order_total、df_benxitong_tuotou、df_amazon_tuotou 中符合时间条件的 数据

data_order_total = df_order_total[(df_order_total['groupBYMonth']>=  shenme[0]) & (df_order_total['groupBYMonth']<= shenme[1])]
data_benxitong_tuotou = df_benxitong_tuotou[(df_benxitong_tuotou['groupBYMonth']>=  shenme[0]) & (df_benxitong_tuotou['groupBYMonth']<= shenme[1])]
data_amazon_tuotou = df_amazon_tuotou[(df_amazon_tuotou['groupBYMonth']>=  shenme[0]) & (df_amazon_tuotou['groupBYMonth']<= shenme[1])]

print('-------- 修改后的 shape() ----------')
print( data_order_total.shape  ,data_benxitong_tuotou.shape , data_amazon_tuotou.shape )

# 获取 amazon 和 ‘系统’ 的差异数据并生成相关文件
df_chaji = pd.concat([data_benxitong_tuotou, data_amazon_tuotou, data_amazon_tuotou]).drop_duplicates(subset=['order_id', 'order_no'], keep=False) #df1-df2

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

mergedf_total_xitong_amazon.rename(columns={"index":"周序号" ,
                                            "moonAndWeeNo_x":"订单总数" ,
                                            "moonAndWeeNo_y":"系统妥投数" ,
                                            "moonAndWeeNo":"amazon妥投数" ,
                                            "rate":"查询成功率" }, inplace=True)

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
            return  params.data[0]  +' : '+ (  window.parseFloat(params.data[1]).toFixed(2) )*100 + '%' ;
                        }
    """

    js_per = """
        function(params){
            console.log(params);
            return (  window.parseFloat(params.data[1]).toFixed(2) )*100 + '%' ;
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
        tooltip_opts=opts.TooltipOpts(formatter=JsCode(js_code_str)) )

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



