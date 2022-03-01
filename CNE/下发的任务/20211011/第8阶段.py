#coding=utf-8


import pandas as pd
import datetime
import time



file_01_shousao = r'C:\Users\hp\Desktop\交付到妥投-下\20211011-楠哥-07-02-交付-下.csv'
file_02_zhuangche = r'C:\Users\hp\Desktop\交付到妥投-下\20211011-楠哥-08-妥投-下.csv'

re_01 = pd.read_csv(file_01_shousao, encoding='gbk')
re_02 = pd.read_csv(file_02_zhuangche, encoding='gbk')

re_01_02 = pd.merge(re_01,re_02 ,left_on='内单号',right_on='内单号', how='left')
re_01_02.to_csv(r'C:\Users\hp\Desktop\交付到妥投-下\下.csv' , index= False, encoding="utf_8_sig" )

