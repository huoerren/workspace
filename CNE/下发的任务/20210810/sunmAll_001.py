#coding=utf-8

import numpy as np
import pandas as pd
import streamlit as st
import plotly_express as px
import plotly.graph_objs as go


from PIL import Image

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

bar_chart = px.bar(result, x='周序号', y=['订单总数','系统妥投数','amazon妥投数'], width= 1300 ,height=600)
line_chart = px.line(result, x='周序号', y=['查询成功率'],   width= 1300 ,height=400 )

st.plotly_chart(bar_chart)
st.plotly_chart(line_chart)


