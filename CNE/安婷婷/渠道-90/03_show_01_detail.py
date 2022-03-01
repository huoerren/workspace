#coding=utf-8

import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)
import openpyxl
import pymysql
import datetime,time
import pyecharts.options as opts
from pyecharts.charts import Line,Grid,Page
from pyecharts.globals import ThemeType


from pyecharts.charts import *
from pyecharts.commons.utils import JsCode
import base64
import os

import numpy as np
import streamlit_echarts

import streamlit as st
st.set_page_config(layout="wide")#设置屏幕展开方式，宽屏模式布局更好


# 作图*-----------------
# 类（共多少个图）

file=r'F:\其他部门\data'

d10 = pd.read_csv(r'F:\其他部门\data\d10.csv' )
d10t = pd.read_csv(r'F:\其他部门\data\d10t.csv' )

print(d10.head())
print('---------------------------------------------------------')
print(d10t.head())

zhouqi_tru = list(set(zip(d10['周序数'] , d10['周期'])))
zhouqi_tru.sort(key=lambda x: x[0])
zhouqi = []
for i in zhouqi_tru:
        zhouqi.append(i[1])
print(zhouqi)

# ‘时间’选择器
zhouqi.insert(0,'全部时间')
zhouqi_sub = list(d10['周期'])
zhouqi_opt = st.multiselect('请选择日期 ：', zhouqi, default='全部时间')
if zhouqi_opt == ['全部时间']: # 如果选择 ‘默认时间’ 则选择全部时间段
    zhouqi_opt = zhouqi_sub

# ‘渠道’选择器
qudao = list(set(d10['渠道']))
qudao_sub = list(set(d10['渠道']))
qudao.insert(0,'全部渠道')
qudao_opt = st.multiselect('请选择渠道 ：', qudao, default='全部渠道')
if qudao_opt == ['全部渠道']:
    qudao_opt = qudao_sub

# ‘国家’选择器
guojia = list(set(d10['国家']))
guojia_sub = list(set(d10['国家']))
guojia.insert(0,'全部国家')
guojia_opt = st.multiselect('请选择国家 ：', guojia, default='全部国家')
if guojia_opt == ['全部国家']:
    guojia_opt = guojia_sub



def data_ca(qd):
    dca =d10[d10['渠道']==qd]
    dca=dca[['国家', '尾端配送商','主题']].drop_duplicates(subset=None,keep='last',).reset_index(drop=True)
    return dca


def data_ca_02(qd1):
    dca1 =d10t[d10t['渠道']==qd1]
    dca1=dca1[['国家', '主题']].drop_duplicates(subset=None,keep='last',).reset_index(drop=True)
    return dca1


def p_Line(df,zt,amount):
    lx= list(df['周期'])
    ly= list(df['妥投时间间隔'])
    lk= list(df['KPI'])
    lq=list(df['期望D'])
    # '妥投率' 保留2位小数
    lttl = list(df['妥投率'])
    listE = []
    if len(lttl)>0:
        for i in lttl:
            listE.append(round(i, 2))

    c = (
        Line({"theme": ThemeType.MACARONS})
            .add_xaxis(lx)
            .add_yaxis("妥投时间间隔",ly, is_smooth=True,
                       label_opts=opts.LabelOpts(is_show=True, position="top"),
                       areastyle_opts=opts.AreaStyleOpts(opacity=0.5),linestyle_opts=opts.LineStyleOpts(color="#6FB0AC", width=2 ) )
            .add_yaxis("KPI",lk , label_opts=opts.LabelOpts(is_show=False),
                       markpoint_opts=opts.MarkPointOpts(
                           data=[opts.MarkPointItem(name="KPI", coord=[lx[0], lk[0]], value=lk[0])]),linestyle_opts=opts.LineStyleOpts(color="#E55D56", width=2 )
                       )
            .add_yaxis("期望D",lq , label_opts=opts.LabelOpts(is_show=False),
                       markpoint_opts=opts.MarkPointOpts(data=[opts.MarkPointItem(name="期望D", coord=[lx[0], lq[0]], value=lq[0])]),
                       linestyle_opts=opts.LineStyleOpts(color="#504F8B", width=2)
                       )
            .extend_axis(yaxis=opts.AxisOpts())
            .add_yaxis('妥投率', listE , is_smooth=True, yaxis_index=1 ,linestyle_opts=opts.LineStyleOpts(color="#0f414D", width=2 ))
            .set_colors(['#6FB0AC',   '#E55D56',   '#504F8B',    '#0f414D'])
            .set_global_opts(
            title_opts=opts.TitleOpts(title=zt , subtitle= '总票数: '+str(amount[2])+'；妥投票数：'+str(amount[1])+'；未妥投票数：'+ str(amount[0])),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
            yaxis_opts=opts.AxisOpts(
                type_="value",
                axistick_opts=opts.AxisTickOpts(is_show=True),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
            xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
        )
    )
    return c

# a.获得符合条件的 总数据并放到一个 resTruList 中 。

resTruList = []

for qd in set(d10['渠道']):
    dca=data_ca(qd)
    d11 = d10[d10['渠道'] == qd].copy(deep=True).reset_index(drop=True)
    for z in dca['主题']:
        zt = z
        df = d11[(d11['主题'] == z) & (d11['渠道'].isin(qudao_opt) ) & ( d11['周期'].isin(zhouqi_opt) ) &( d11['国家'].isin(guojia_opt) )]
        if df.size == 0:
            pass
        else:
            # amount = [int(df['未妥投票数'].sum()) , int(df['妥投票数'].sum()), int(df['总票数'].sum())]
            print('----------------------------------------------------------')
            resTru=(df,zt,int(df['未妥投票数'].sum()),int(df['妥投票数'].sum()),int(df['总票数'].sum()) )
            resTruList.append(resTru)

            # line = p_Line(df, zt, amount)
            # streamlit_echarts.st_pyecharts(line, height='550px')


# b.对resTruList中数据进行排序
if len(resTruList)>0:
    resTruList.sort(key=lambda x: x[4], reverse=True) # 利用‘总票数’ 排序（降序）
    for j in resTruList:
        line = p_Line(j[0],j[1],[j[2],j[3],j[4]])
        streamlit_echarts.st_pyecharts(line, height='550px')





