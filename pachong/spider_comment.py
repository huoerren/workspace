import requests
import json
import re
import pandas as pd
from dateutil.parser import parse
from datetime import datetime
from pandas import DataFrame, Series
import os

try:
    os.mkdir('results')
except:
    pass

tm_url = "https://rate.tmall.com/list_detail_rate.htm"
tb_url = "https://rate.taobao.com/feedRateList.htm"

tm_params = {
    'itemId': '609599587051',
    'spuId': '938551185',
    'sellerId': '201749140',
    'order': '1',
    'currentPage': '3',
    'append': '0',
    'content': '1',
}
tb_params = {
    'auctionNumId': '611467824229',
    'userNumId': '2200757790074',
    'currentPageNum': '2',
    'pageSize': '20',
    'orderType': 'feedbackdate',
    'hasSku': 'false',
    'folded': '0',
}
tb_headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Safari/537.36',
    'cookie': 'cna=VIoSFdno0nICAd6oKBebEO09; hng=CN%7Czh-CN%7CCNY%7C156; x=e%3D1%26p%3D*%26s%3D0%26c%3D0%26f%3D0%26g%3D0%26t%3D0%26__ll%3D-1%26_ato%3D0; miid=1221490175770555062; tg=0; tracknick=%5Cu6211%5Cu7231%5Cu5F92%5Cu6B65%5Cu65C5%5Cu884C%5Cu8005; _cc_=V32FPkk%2Fhw%3D%3D; enc=IEHkV4%2BV4B%2F3J73vZM%2BIW2ig2N2Tdufo5Z9o98ddivR9wn5ADBRrTS1%2FGL5btIN9FOxFlRHdjJgMYXoZSsxElw%3D%3D; sgcookie=DxFxRrI8Fl%2FCgyBozUfj8; tfstk=crEVBdT-oiIVAM_lRDoZ18DMNs2AZYliO3kEoyYZkxKNeY0ciKRtETOtUAldqqf..; thw=cn; v=0; cookie2=1aeb9dc428179c98354703f51f27c223; t=91e877c8463545b224c8e6cd37215043; _tb_token_=95763ae3afea; _m_h5_tk=883e9eeaf06b49252c31fd605eda5c68_1587539373191; _m_h5_tk_enc=c508b539cca410241a67b29f6452c2a4; mt=ci%3D-1_1; _samesite_flag_=true; x5sec=7b22726174656d616e616765723b32223a22313365363261653538653563363833343435646336636439393265653761386243504f3267665546454976736e65764f364b365952513d3d227d; l=eBMkmE3lv6m8Rmj8BO5wourza77TaQRfGsPzaNbMiIHca1llKMpvJNQcDFYXjdtj_tfxbeKzxmHb6RUMPZUdgETNJqHzH13fRxJ6-; isg=BODgTfKQqoNNwRSBvsYSC1jase6y6cSzdILmkFrzdfq1VYR_A_2QQ-Aj7fVVZXyL',
    'referer': 'https://item.taobao.com/item.htm?spm=a230r.1.14.80.19744f55ipRuXe&id=611467824229&ns=1&abbucket=8',
}

tm_headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Safari/537.36',
    'cookie': 'cna=VIoSFdno0nICAd6oKBebEO09; otherx=e%3D1%26p%3D*%26s%3D0%26c%3D0%26f%3D0%26g%3D0%26t%3D0; OZ_1U_2061=vid=vc91d444fc78ca.0&ctime=1554629928&ltime=1553060939; lid=%E6%88%91%E7%88%B1%E5%BE%92%E6%AD%A5%E6%97%85%E8%A1%8C%E8%80%85; hng=CN%7Czh-CN%7CCNY%7C156; enc=IEHkV4%2BV4B%2F3J73vZM%2BIW2ig2N2Tdufo5Z9o98ddivR9wn5ADBRrTS1%2FGL5btIN9FOxFlRHdjJgMYXoZSsxElw%3D%3D; sgcookie=DxFxRrI8Fl%2FCgyBozUfj8; t=91e877c8463545b224c8e6cd37215043; tracknick=%5Cu6211%5Cu7231%5Cu5F92%5Cu6B65%5Cu65C5%5Cu884C%5Cu8005; _tb_token_=95763ae3afea; cookie2=1aeb9dc428179c98354703f51f27c223; x5sec=7b22726174656d616e616765723b32223a223063633832383765363931303834633737323263356465373333393933343435434b47692f2f51464549795738657a527a62694865673d3d227d; l=eBLDCTh7vmfBimg8BO5Nlurza77OeCAfCsPzaNbMiIHca1PhM6PG7NQcK5vHydtj_tfb1eKzxmHb6R3WPTadgZqhuJ1REpZNFY96-; isg=BAMDeI9JWfUVbhcVuUbv4XtPkseteJe6w0MlyTXkYGOS9CYWvExtCpIiboy61O-y',
    'referer': 'https://detail.tmall.com/item.htm?spm=a230r.1.14.1.19744f55ipRuXe&id=609599587051&ns=1&abbucket=8',
}


class Spider():
    def __init__(self):
        self.df1 = pd.read_excel('辅助数据.xlsx')
        self.columns = ['名称', '价格', '付款人数', '总评论数', '评论内容']
        print(self.df1.columns.tolist())
        for i in self.df1.values.tolist():
            df = DataFrame()
            name = i[4]
            price = i[2]
            comment_count = i[3]
            idd = i[0]
            num = i[1]
            ty = i[5]
            if ty == "tm":
                comments = self.get_comments_from_tm(idd)
            else:
                comments = self.get_comments_from_tb(idd)
            for content in comments:
                data = [name, price, num, comment_count, content]
                ser = Series(data, index=self.columns)
                df = df.append(ser, ignore_index=True)
            df.to_excel('results/%s.xlsx' % idd, index=False)
            self.df1 = self.df1.drop(self.df1['id'].isin([idd]).index.tolist()[0])
            self.df1.to_excel('辅助数据.xlsx', index=False)

    def get_comments_from_tb(self, idd):
        tb_params['auctionNumId'] = idd
        comments = []

        for page in range(1, 100):
            tb_params['currentPageNum'] = str(page)
            r = requests.get(tb_url, headers=tb_headers, params=tb_params)
            text = re.findall('\((.*)\)', r.text)[0]
            js = json.loads(text)
            for i in js['comments']:
                content = i['content']
                date = i['date']
                date = date.replace('年', '-').replace('月', '-').replace('日', '')
                if self.isout(date):
                    return comments
                comments.append(content)
                print(content)

    def get_comments_from_tm(self, idd):
        tm_params['itemId'] = idd
        comments = []
        for page in range(1, 100):
            tm_params['currentPage'] = str(page)
            r = requests.get(tm_url, headers=tm_headers, params=tm_params)
            text = re.findall('\((.*)\)', r.text)[0]
            js = json.loads(text)
            for i in js['rateDetail']['rateList']:

                rateContent = i['rateContent']  # 评论内容
                s = i['rateDate']  # 时间
                if self.isout(s):
                    return comments
                comments.append(rateContent)
                print(rateContent)

    def isout(self, s):
        date = parse(s)
        if (datetime.now() - date).days > 7:
            return 1
        else:
            return 0


if __name__ == "__main__":
    Spider()
