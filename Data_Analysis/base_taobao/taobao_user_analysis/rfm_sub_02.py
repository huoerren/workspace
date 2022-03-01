#coding=utf-8


import  pandas as pd
import numpy  as np


filePath = 'E:/数据/tianchi_mobile_recommend_train_user/tb_user_2014.csv'
tb = pd.read_csv(filePath)
tb_buy = tb[tb['behavior_type'] == 'buy']
print(tb_buy.head())
print('-=-=-=-=-=-=-=-=-')
tb_item = tb_buy['item_id'].value_counts().reset_index()
print(tb_item.head())
tb_item.columns = ['item_id','销售次数']
print(tb_item.head())
# tb_item_num = tb_item['销售次数'].value_counts().reset_index()
# tb_item_num.columns = ['销售次数','商品数量']
# print(tb_item_num)




