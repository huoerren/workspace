#coding=utf-8

import numpy as np
import pandas as pd
import streamlit as st
import streamlit_echarts

import plotly_express as px
import plotly.graph_objs as go
from PIL import Image

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
from pyecharts.globals import ThemeType


import os
os.chdir('C:\\Users\\hp\Desktop\\JupyterNotebook\\')
# 设置网页名称
# st.set_page_config(page_title='fdfsdf')
# 设置网页标题
st.header('亚马逊妥投节点查询成功率')
# 设置网页子标题
# st.subheader('2020年各部门对生产部的评分情况')

# 读取数据
excel_file = 'shemmegui.xlsx'

df = pd.read_excel(excel_file)

df.replace(np.nan, 0, inplace=True)
df.replace(np.inf, 0, inplace=True)

df['moonAndWeeNo_x'] = df['moonAndWeeNo_x'].astype("int")
df['moonAndWeeNo_y'] = df['moonAndWeeNo_y'].astype("int")
df['moonAndWeeNo'] = df['moonAndWeeNo'].astype("int")
df['rate'] = round( df['moonAndWeeNo']/df['moonAndWeeNo_y'], 3)
df['rate'] = df['rate'].apply(lambda x : 1 if x>1 else x )

df.rename(columns={"index":"周序号" ,"moonAndWeeNo_x":"订单总数" ,"moonAndWeeNo_y":"系统妥投数" ,
                   "moonAndWeeNo":"amazon妥投数" ,"rate":"查询成功率" }, inplace=True)

print(df)
department = df['周序号'].unique().tolist()
defaultList = ['7月-第1周','7月-第2周','7月-第3周','7月-第4周','7月-第5周','8月-第1周','8月-第2周']

department_selection = st.multiselect('周序号:', department, default=defaultList)
mask = (df['周序号'].isin(department_selection))

number_of_result = df[mask].shape[0]

result = df[mask]

zhouXuHaoList = list(result['周序号'])
valueList_order_total = list(result['订单总数'])
valueList_benxitong_tuotou = list(result['系统妥投数'])
valueList_amazon_tuotou = list(result['amazon妥投数'])

rateList = list(result['查询成功率'])

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

    page = Page(layout=Page.SimplePageLayout)
    # 需要自行调整每个 chart 的 height/width，布局会因为页面大小而不同
    page.add(bar_1, line)

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

    page = Page(layout=Page.SimplePageLayout)
    # 需要自行调整每个 chart 的 height/width，布局会因为页面大小而不同
    page.add(bar_1, line)

    return line


chart = page_simple_layout()
chart_02 = page_simple_layout_02()

streamlit_echarts.st_pyecharts(
    chart
)
streamlit_echarts.st_pyecharts(
    chart_02
)











