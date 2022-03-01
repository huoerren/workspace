#coding=utf-8



import seaborn as sns

import pandas_profiling as pp
import pandas as pd

filePath = 'C:/Users/37884/Desktop/titanic_passenger_list.csv'
data = pd.read_csv(filePath)
report = pp.ProfileReport(data)
report.to_file('C:/Users/37884/Desktop/report.html')


