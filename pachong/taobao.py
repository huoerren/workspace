#coding=utf-8

import requests
import pandas as pd
pd.set_option('display.max_columns',2000)
pd.set_option('display.width',1000)

from pandas import DataFrame,Series
import json


url="https://s.taobao.com/search"
keyword="香水"
params1={
    'data-key': 's',
    'data-value': '44',
    'ajax': 'true',
    'q': keyword,
    'commend': 'all',
    'ssid': 's5-e',
    'search_type': 'item',
    'sourceId': 'tb.index',
    'spm': 'a21bo.2017.201856-taobao-item.1',
    'ie': 'utf8',
    'initiative_id': 'tbindexz_20170306',
    'bcoffset': '3',
    'ntoffset': '0',
    'p4ppushleft': '1,48',
    'sort':'sale-desc',
}

headers={
    'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36',
    'cookie':'thw=cn; enc=znUfT6DuB8rQ65YIFckhgcFq9n0gD4q3DppR7RGu2AGDIqvSpd4S8agWYcTrGrlTtl%2BU8NmT%2BODHHJ8gAoaoyw%3D%3D; hng=CN%7Czh-CN%7CCNY%7C156; t=fff08d81529906954d9ec3381b2fd122; cna=pncDF6a5kwsCAd9o1PXceo61; lgc=panxin1989; tracknick=panxin1989; mt=ci=65_1; sgcookie=Ei7xSxuOrglXwmAfv8iXj; uc3=nk2=E6UoB9C3QlnUWg%3D%3D&lg2=WqG3DMC9VAQiUQ%3D%3D&vt3=F8dBxGR2Vt8xF9NVajg%3D&id2=WvmFucENRaQ%3D; uc4=nk4=0%40EbzJZnQt4563iQo3EXB8EweZQ59S&id4=0%40WD5jkgulW29ZfPQ4fPsETpB0MA%3D%3D; _cc_=WqG3DMC9EA%3D%3D; tfstk=cHpdBFOBgV0hUT9G9phGFUqAfmncZDrdrk_8e0NuQZH0LH2RiLJDHI1mOgzdBcC..; _m_h5_tk=d72df1c5fc2d63c714836e2e4498c447_1587629740973; _m_h5_tk_enc=ca5805181fe62cc3991f133f6d078aee; v=0; cookie2=1cfc27d2049d1c3a207f33f1278b2fa5; uc1=cookie14=UoTUPcqfFeyRmQ%3D%3D; _tb_token_=fb35067e603f1; JSESSIONID=A6B8E6E68D3AD57A8F0DE288DBC884AB; alitrackid=www.taobao.com; lastalitrackid=www.taobao.com; l=eBLcLUsIQB89e3_UBOfZnurza77OAIRVDuPzaNbMiT5P_u5esDhGWZjX3CTwCnhVHsXHR3yAaP04BuThmyhqJxpsw3k_J_mj3dC..; isg=BLi41PokErpdMX7ghpT9akyUiWZKIRyrU1OoDvIpK_OLDVr3mjD2O4vrxAW9EdSD',
    'referer':'https://s.taobao.com/search?q=%E7%89%9B%E5%A5%B6&imgfile=&commend=all&ssid=s5-e&search_type=item&sourceId=tb.index&spm=a21bo.2017.201856-taobao-item.1&ie=utf8&initiative_id=tbindexz_20170306'
}

df = DataFrame()

nodes = 'raw_title|nick'
columns = list()
if nodes == "" or nodes == None:
    pass
else:
    columns = nodes.split('|')
    pa = ['0,1' ]
    for i in pa:
        params1['data-value'] = i
        r = requests.get(url, params=params1, headers=headers)
        js = json.loads(r.text)
        for j in js['mods']['itemlist']['data']['auctions']:
            data = []
            for m in range(len(columns)):
                data.append(j[columns[m]])

            ser = Series(data, index=columns)
            df = df.append(ser, ignore_index=True)

print(df)












