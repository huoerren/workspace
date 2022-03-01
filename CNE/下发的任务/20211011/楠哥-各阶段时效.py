#coding=utf-8


import pandas as pd
import datetime
import time


file_01_shousao = r'C:\Users\hp\Desktop\ban\20211011-楠哥-01-半.csv'
file_02_zhuangche = r'C:\Users\hp\Desktop\ban\20211011-楠哥-02-半.csv'
file_03_daodaojichang = r'C:\Users\hp\Desktop\ban\20211011-楠哥-03-半.csv'
file_04_qifei= r'C:\Users\hp\Desktop\ban\20211011-楠哥-04-半.csv'
file_05_luodi= r'C:\Users\hp\Desktop\ban\20211011-楠哥-05-半.csv'
file_06_qingguan= r'C:\Users\hp\Desktop\ban\20211011-楠哥-06-半.csv'
# file_07_jiaofu = r'C:\Users\hp\Desktop\楠\20211011-楠哥-07-交付时间.csv'
# file_08_toutuo = r'C:\Users\hp\Desktop\楠\20211011-楠哥-08-妥投时间.csv'


re_01 = pd.read_csv(file_01_shousao, encoding='gbk')
re_02 = pd.read_csv(file_02_zhuangche, encoding='gbk')

re_03 = pd.read_csv(file_03_daodaojichang, encoding='gbk')
re_04 = pd.read_csv(file_04_qifei, encoding='gbk')

re_05 = pd.read_csv(file_05_luodi, encoding='gbk')
re_06 = pd.read_csv(file_06_qingguan, encoding='gbk')

# re_07 = pd.read_csv(file_07_jiaofu, encoding='gbk')
# re_08 = pd.read_csv(file_08_toutuo, encoding='gbk')

# ------------- 01 和 02 ---------------------

re_01_02 = pd.merge(re_01,re_02 ,left_on='内单号',right_on='内单号', how='left') #ruku-zhuangche
re_03_04 = pd.merge(re_03,re_04 ,left_on='内单号',right_on='内单号', how='left')# jichang-qifei

re_04_05 = pd.merge(re_04,re_05 ,left_on='内单号',right_on='内单号', how='left')# qifei-luodi
re_05_06 = pd.merge(re_05,re_06 ,left_on='内单号',right_on='内单号', how='left')# luodi-qingguan

# re_06_07 = pd.merge(re_06,re_07 ,left_on='内单号',right_on='内单号', how='left')# qingguan-交付
# re_07_08 = pd.merge(re_07,re_08 ,left_on='内单号',right_on='内单号', how='left')# 交付-妥投

def deal_str(data):
    data = str(data)+"\t"
    return data

re_01_02['内单号'] = re_01_02['内单号'].map(deal_str)
re_03_04['内单号'] = re_03_04['内单号'].map(deal_str)

re_04_05['内单号'] = re_04_05['内单号'].map(deal_str)
re_05_06['内单号'] = re_05_06['内单号'].map(deal_str)

# re_06_07['内单号'] = re_06_07['内单号'].map(deal_str)
#
# re_07_08['内单号'] = re_07_08['内单号'].map(deal_str)

re_01_02.to_csv(r'C:\Users\hp\Desktop\ban\01-02.csv', index= False, encoding="utf_8_sig")
re_03_04.to_csv(r'C:\Users\hp\Desktop\ban\03-04.csv', index= False, encoding="utf_8_sig")

re_04_05.to_csv(r'C:\Users\hp\Desktop\ban\04-05.csv', index= False, encoding="utf_8_sig")
re_05_06.to_csv(r'C:\Users\hp\Desktop\ban\05-06.csv', index= False, encoding="utf_8_sig")

# re_06_07.to_csv(r'C:\Users\hp\Desktop\楠\06-07.csv', index= False, encoding="utf_8_sig")
# re_07_08.to_csv(r'C:\Users\hp\Desktop\楠\07-08.csv', index= False, encoding="utf_8_sig")



# file_07_shousao















