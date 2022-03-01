#!/usr/bin/env python
# coding: utf-8

# In[1]:


import poplib
import email
import string
from email.parser import Parser
from email.header import decode_header
import time


# In[2]:


# mailServer=poplib.POP3_SSL('pop.gmail.com')
# mailServer.user('kun.qian@cne.com')
# mailServer.pass_('1358367328abc')
# (mailCount,size)=mailServer.stat()


# In[3]:


mailServer=poplib.POP3_SSL('pop.gmail.com')
mailServer.user('protosslex@gmail.com')
mailServer.pass_('zichrkiepezpiwau')
(mailCount,size)=mailServer.stat()


# In[4]:


mailCount


# In[5]:


import datetime
today=datetime.date.today()


# In[6]:


# today=(datetime.datetime.now()).strftime("%Y-%m-%d")


# In[7]:


today=(datetime.datetime.now()+datetime.timedelta(days=0)).strftime("%Y-%m-%d")


# In[8]:


today


# In[9]:


today1=(datetime.datetime.now()).strftime("%Y-%m-%d")


# In[10]:


import glob


# In[11]:


import re


# In[12]:


import pandas as pd
# df=pd.DataFrame()


# In[13]:


sen1='.*实际交接报告.*'+today+'.*'


# In[14]:


today = '.*实际交接报告.*'+today+'.*'
for i in range(1, mailCount+1):
    print('第{0}封邮件'.format(mailCount-i+1))
#     print('第{0}封邮件'.format(i))
    (hdr,messages,octet)=mailServer.retr(mailCount-i+1)
    mail=email.message_from_bytes('\n'.encode('utf-8').join(messages))
    subject = email.header.decode_header(mail.get('subject'))
    date1 = time.strptime(mail.get("Date")[0:24], '%a, %d %b %Y %H:%M:%S')
    # 邮件时间格式转换
    date2 = time.strftime("%Y-%m-%d", date1)
#     print(date2)
#     print(today1)
    if (date2<today):
        print("结束")
        break
    ename=''
    if type(subject[0][0]) in (type(b' '),):
#         print("邮件标题：" + subject[0][0].decode(subject[0][1]))
        ename=subject[0][0].decode(subject[0][1])
    else:
        ename=subject[0][0]
    print(ename)
    matchObj = re.match( today, ename)
    if matchObj:
        for par in mail.walk():
            if not par.is_multipart():
                #附件
                name = par.get_param('name')
                print(name)
                if name:
                    dh = email.header.decode_header(name)
                    if type(dh[0][0]) in (type(b' '), ):
                        if dh[0][1] == None:
                            fname = dh[0][0].decode()
                        else:
                            fname = dh[0][0].decode(dh[0][1])
                    else:
                        fname = dh[0][0]
                    print('保存附件名：' + fname)
                    data = par.get_payload(decode=True)
                    attachment_files = []
                    str1 = '交接明细.*'
                    print(fname)
                    m = re.match( str1, fname)
                    if m:
                        try:
                            f = open(fname, 'wb')
                            att_file = open('/Users/dingmengnan/Downloads/2021-06-10/' + fname, 'wb')#在指定目录下创建文件，注意二进制文件需要用wb模式打开
                            attachment_files.append(fname)
                            att_file.write(data)#保存附件
                            att_file.close()
                        except:
                            print('附件名有非法字符')
                        f.write(data)
                        f.close()
                else:
                    pass
                    #邮件内容
    #                 ch = par.get_content_charset()
    #                 if ch == None:
    #                     print(par.get_payload(decode=True).decode())
    #                 else:
    #                     print(par.get_payload(decode=True).decode(ch))

        print("=================================")


# In[15]:


import os


# In[16]:


dir = r"/Users/dingmengnan/Downloads/2021-06-10/"
filename_excel = []
frames = []
for root, dirs, files in os.walk(dir):
    for file in files:
        print(file)
        filename_excel.append(os.path.join(root,file))
#         df = pd.read_excel(os.path.join(root,file)) #excel转换成DataFrame
        df = pd.read_excel(os.path.join(root, file))
        frames.append(df)


# In[ ]:


frames


# In[ ]:


result = pd.concat(frames)


# In[ ]:


result.to_excel(r'/Users/dingmengnan/Downloads/2021-06-10/测试合并文档.xlsx',index = False)


# In[ ]:




