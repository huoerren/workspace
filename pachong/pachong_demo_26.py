
import requests
from pandas import DataFrame,Series
import json
import re
from pyquery import PyQuery as pq

headers = {
        'cookie': 'BAIDU_SSP_lcr=https://www.google.com/; __cfduid=de4c41e02eb4a2e86fd31e66dc9ebbbf31589717521; UM_distinctid=1727a63bcca1db-084213586823fa-f7d1d38-e1000-1727a63bccb86f; Hm_lvt_92877e89b7b6871e4dbbf66d5839d430=1589717522,1591191257,1591516611,1591516637; Hm_lvt_303541224a8d81c65040eb747f5ee614=1591191257,1591516611,1591516637; CNZZDATA2134022=cnzz_eid%3D1002382619-1591187699-https%253A%252F%252Fwww.google.com%252F%26ntime%3D1591523902; Hm_lpvt_92877e89b7b6871e4dbbf66d5839d430=1591526139; Hm_lpvt_303541224a8d81c65040eb747f5ee614=1591526139',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36'
    }

def fileInputFiles(txt):
    with open('E:/namefile/宝应县_names.txt' , 'a+')  as f:
        f.write(txt +"\n")

def getPattern(context):
    context = context.replace('　', '').replace( '\n','')

    peopel = ''
    tele = ''
    phone = ''
    add = ''

    pattern_01 = re.compile(r'地址：(.*?)邮编')
    match_01 = pattern_01.search(context)
    if match_01:
        add = match_01.group(1)

    pattern_02 = re.compile(r'电话：(.*?)地址')
    match_02 = pattern_02.search(context)
    if match_02:
        tele = match_02.group(1)

    pattern_03 = re.compile(r'联系人：(.*?)公司名称')
    match_03 = pattern_03.search(context)
    if match_03:
        peopel = match_03.group(1)

    if '手机' in context:
        pattern_04 = re.compile(r'手机：(\d{11})')
        pattern_04 = pattern_04.search(context)
        if pattern_04:
            peopel = pattern_04.group(1)

    return [peopel , tele ,phone ,add ]



def stepTwo(url):
    print(url)
    url = url.replace('https', 'http')
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200 :
        resp.encoding = 'gbk'
        doc = pq(resp.text)
        title = doc('h1.title').text()
        contentBody = doc('#contentBody p').text()
        details = getPattern(contentBody)
        address = ''
        telephone = ''
        allow = ''
        if details != None and len(details) == 3 :
            address   = details[0]
            telephone = details[1]
            allow     = details[2]

        return [url ,title ,address , telephone, allow ]


def stepOne():

    url = "http://b2b.huangye88.com/baoyingxian/"
    df = DataFrame()
    res = requests.get(url, headers=headers)
    res.encoding = 'utf-8'
    doc = pq(res.text)
    div = doc('#subcatlisting_10')
    div_ul = div('ul')
    lis = div_ul('li')
    for li in lis.items():
        content = li.text()
        if '汽配' in content or '机械' in content  or  '机床' in content:
            nihao(li('a').attr('href'))
            # print(li('a').attr('href'))



def nihao(url):
    # url = "http://b2b.huangye88.com/baoyingxian/qipei/"
    df = DataFrame()
    res = requests.get(url, headers=headers)
    res.encoding = 'utf-8'
    doc = pq(res.text)
    form = doc('form')
    dls = form('dl')
    for dl in dls.items():
        a= dl('dt h4 a')
        if '扬州' in a.text() or '宝应' in a.text():
            # print(a.text() +' : ' + a.attr('href'))
            women(a.text(), a.attr('href'))
            print('---------------------')



def women(name,url):
    # url = 'http://b2b.huangye88.com/gongsi/1546781/'
    res = requests.get(url, headers=headers)
    res.encoding = 'utf-8'
    doc = pq(res.text)
    ul = doc('div.nav ul')
    a_s = ul('a')
    for a in a_s.items():
        if '联系我们' in a.text():
            info = getInfo(a.attr('href') , name )
            print(info)


def getInfo(url,name):
    print(url)
    companyName = name
    peopel = ''
    tele = ''
    phone = ''
    add = ''

    res = requests.get(url, headers=headers)
    res.encoding = 'utf-8'
    doc = pq(res.text)
    lis = doc('ul.con-txt li')
    context = lis.text()
    details = getPattern(context)
    return details



if __name__ == '__main__':
    stepOne()
    # nihao()
    # women()


