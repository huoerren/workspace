#coding=utf-8

import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)

import numpy as np
import os

os.chdir(r'C:\Users\hp\Desktop\dataForCompe\competitionData')
trainFilePath = '初赛数据.xlsx'
testFilePath = 'to_pred-初赛.csv'

trainData = pd.read_excel(trainFilePath)
print(trainData.head())
print(trainData.shape)









