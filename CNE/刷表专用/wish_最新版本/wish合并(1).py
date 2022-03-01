#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd
# import numpy as np
import datetime,time

nows=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
# print(now)
dt_now = datetime.date.today()+ datetime.timedelta(-1)
# print(str(dt_now))
# f='实际交接报告'
dir = r"F:\未操作更新\实际交接报告"#设置工作路径
# dir = r"F:\未操作更新\2021-4-23实际交接报告"
#新建列表，存放文件名（可以忽略，但是为了做的过程能心里有数，先放上）
filename_excel = []
#新建列表，存放每个文件数据框（每一个excel读取后存放在数据框）
frames = []
for root, dirs, files in os.walk(dir):
    for file in files:
        #print(os.path.join(root,file))
        filename_excel.append(os.path.join(root,file))
        df = pd.read_excel(os.path.join(root,file),dtype={'集包序列号':'str','单号':'str'}) #excel转换成DataFrame
        frames.append(df)
# #打印文件名
# print(filename_excel)
# print(frames)

 #合并所有数据
result = pd.concat(frames)
# 调整格式
result['交接时间']= pd.to_datetime(result['交接时间'])
# result['集包序列号']=result['集包序列号'].astype('str')
print(result)
# #查看合并后的数据
# result.head()
# result.shape
# result.to_excel(r'C:\Users\hp\Desktop\{}实际交接报告.xlsx'.format(str(dt_now)),index = False)
result.to_excel(r'D:\PBI\BI\未操作票件\当前数据\{}实际交接报告.xlsx'.format(str(dt_now)),index = False)
result.to_excel(r'D:\PBI\BI\未操作票件1\当前数据\{}实际交接报告.xlsx'.format(str(dt_now)),index = False)


# In[2]:


r1=result.groupby(['仓库名称'])['交接时间'].count().reset_index()


# In[3]:


r2=pd.read_excel(r'D:\PBI\BI\仓库对应.xlsx')


# In[4]:


df1=r1['仓库名称']


# In[5]:


df2=r2['仓库名称']


# In[6]:


df3=pd.concat([df2,df1]).to_frame()


# In[7]:


df3['仓库名称']=df3['仓库名称'].astype('str')


# In[8]:


df4=df3.drop_duplicates(keep='first')


# In[9]:


df4


# In[10]:


rr=pd.merge(df4,r2,on=['仓库名称'],how='left')


# In[11]:


m=rr[rr['仓库'].isnull()]


# In[12]:


def tj(a,b):
    flag=pd.isnull(b)
    if flag:
        for i in re_list:
            if re.match(i,a):
                return "华南取件"
            else:
                continue
        return "华东取件"
    else:
        return b


# In[13]:


import re
if m.empty:
    print("无新增仓库")
    pass
else:
    re_list=['深圳.*','.*深圳.*','华南.*','.*华南.*','东莞.*','.*东莞.*','广州.*','.*广州.*']
    rr['仓库']=rr.apply(lambda x:tj(x['仓库名称'],x['仓库']),axis=1)
    print('仓库对应表已新增')
    rr.to_excel(r'D:\PBI\BI\仓库对应.xlsx',index=False)


# In[ ]:





# In[ ]:




