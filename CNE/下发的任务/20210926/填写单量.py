#coding=utf-8

import json
import pandas as pd
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)
pd.set_option('expand_frame_repr', False)

from pyecharts.charts import *
from pyecharts import options as opts

from pyecharts.commons.utils import JsCode
import base64
import os

import numpy as np








def getCountByCustomerCode(customerCode):
    # print(  customerCode )
    source_path = r'C:\Users\hp\Downloads\未被amazon抓到的单量 (3).csv'
    source_df = pd.read_csv(source_path)
    sub_df = source_df[source_df['客户编码'] ==  customerCode ]
    return sub_df.shape[0]


def startPro():
    countList = []
    base_path = r'D:\工作中产生的一些文件\dingmengnan\客戶对应销售_base.xlsx'
    base_df = pd.read_excel(base_path)
    base_kehubianma_list = base_df.客户编码.to_list()
    base_xiaoshou_list = base_df.销售.to_list()

    for index,row in base_df.iterrows():
        count = getCountByCustomerCode(row['客户编码'])
        countList.append(count)
        # row[1]['未抓取订单数量'] = 888
        # print(row['客户编码'], row['未抓取订单数量'])
        # row['未抓取订单数量'] = 85

    print(countList)
    c = {"客户编码": base_kehubianma_list , "销售": base_xiaoshou_list,"未抓取订单数量":countList}  # 将列表a，b转换成字典
    data = pd.DataFrame(c)  # 将字典转换成为数据框
    data.to_csv(r'D:\工作中产生的一些文件\dingmengnan\result.csv',index=False ,encoding="utf_8_sig" )




if __name__ == '__main__':
    startPro()












