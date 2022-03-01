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


option = st.multiselect('请选择维度：', ('默认时间','6.28-7.4', '7.5-7.11', '7.12-7.18','7.19-7.25','7.25-7.31'), default='默认时间')
print(option)
if option == ['默认时间']:
    option = ['6.28-7.4','7.19-7.25']



df = pd.DataFrame({"时间":['6.28-7.4', '7.5-7.11','6.28-7.4', '7.5-7.11','7.5-7.11', '7.12-7.18','7.19-7.25','7.12-7.18','7.19-7.25','7.25-7.31'],
                   "结果":[50,56,32,94,56,15,86,56,25,64],
                   "地址":['上海','北京','上海','南京','成都','成都','杭州','南京','上海','温州'],
                   "成绩":[87,46,89,65,98,62,13,58,95,25] })

print(df)

df_result = df[df['时间'].isin(option)]



def line_with_one_xaxis(x_data, y_data):
    line = Line(init_opts=opts.InitOpts(theme='light', width='1000px', height='600px'))
    line.set_global_opts(
        xaxis_opts=opts.AxisOpts(type_='category', name='维度', axislabel_opts={"rotate": 10}),
        yaxis_opts=opts.AxisOpts(type_='value', name='占比' ))
    line.add_xaxis(x_data)
    line.add_yaxis('',y_data)
    return line


for i in ['结果','成绩']:
    line = line_with_one_xaxis(option, list(df_result[i]))

# chart01 = line_with_one_xaxis()
# nimeide = chart01
    streamlit_echarts.st_pyecharts(line, height='550px')



