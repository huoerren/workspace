#coding=utf-8

import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)

import tushare as ts


tokenStr = 'b31e0ac207a5a45e0f7503aff25bf6bd929b88fe1d017a034ee0d530'



#获得所有 stock code
def getTSCode():
    # 设置Token
    ts.set_token(tokenStr)
    # 初始化接口
    ts_api = ts.pro_api()
    data = ts_api.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    # data = ts_api.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    print(data)


#获得所有 stock data
def getInfo():
    ts_api = ts.pro_api()
    data = ts_api.daily(ts_code='688018.SH', start_date='20200101', end_date='20211210')
    data['difference'] = data['close'] - data['open']
    data['ifRise'] = data['difference'].apply(lambda x : 1 if x > 0 else 0 )
    print(data)
    # print(data.to_csv('688018.csv',  index= False, encoding="utf_8_sig") )

if __name__ == '__main__':
    # getTSCode()
    getInfo()






