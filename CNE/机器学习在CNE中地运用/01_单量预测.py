
import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)

import numpy as np
import statsmodels.api as sm
import pandas as pd
from statsmodels.tsa.arima_process import arma_generate_sample
np.random.seed(12345)

filePath = r'C:\Users\hp\Desktop\powerBI练习\促佳项目内容.csv'
df = pd.read_csv(filePath,encoding='gbk')
df['gmt_create'] = pd.to_datetime(df['gmt_create'])
df['季度'] = df['gmt_create'].apply(lambda z:z.quarter)
df['月份'] = df['gmt_create'].apply(lambda z:z.month )
df['周序数'] =   df['gmt_create'].dt.strftime('%W').apply(lambda n:int(n) +1)
df['年季度'] = df['gmt_create'].apply(lambda z: '{}Q{}'.format(z.year, (z.quarter)))
df['星期几']   = df['gmt_create'].dt.day_name()
df['星期几_数字话表达']=df['gmt_create'].dt.weekday + 1



# print(df.head())
print(df.to_csv(r'C:\Users\hp\Desktop\powerBI练习\促佳项目内容-result.csv',index= False, encoding="gbk" ))



