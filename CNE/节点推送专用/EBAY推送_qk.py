#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import openpyxl
import pymysql
import datetime,time


# In[2]:


con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()


# In[3]:


def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df


# In[4]:


days="BETWEEN '2021-08-01 16:00:00' and '2021-10-26 16:00:00'"


# In[5]:


S1="""
select 
DATE_FORMAT(DATE_ADD(order_time,interval 8 hour),'%Y%u') weeks,
channel_code,
des,
order_status,
mawb_no,
event_code,
count(distinct lgo.pre_order_id) 推送数
from 
lg_order lgo inner join ebay_track_record etr on lgo.pre_order_id=etr.pre_order_id
left join lg_mawb lgm on lgo.mawb_id=lgm.id
where
lgo.order_time {}
and lgo.platform='EBAY_ONLINE'
and lgo.customer_code=313115248
and etr.is_deleted='n'
group by 1,2,3,4,5,6
""".format(days)


# In[6]:


d1=execude_sql(S1)


# In[7]:


d1['weeks']=(d1['weeks'].astype('int')+1).astype('str')


# In[8]:


d1


# In[9]:


d1['mawb_no']=d1['mawb_no'].replace(np.nan,0)


# In[10]:


S2="""
select 
DATE_FORMAT(DATE_ADD(order_time,interval 8 hour),'%Y%u') weeks,
channel_code,
des,
order_status,
mawb_no,
count(*) 票数
from lg_order lgo left join lg_mawb lgm on lgm.id=lgo.mawb_id
where
lgo.order_time {} 
and lgo.platform='EBAY_ONLINE'
and lgo.customer_code=313115248
group by 1,2,3,4,5
""".format(days)


# In[11]:


d2=execude_sql(S2)


# In[12]:


d2['weeks']=(d2['weeks'].astype('int')+1).astype('str')


# In[13]:


d2['mawb_no']=d2['mawb_no'].replace(np.nan,0)


# In[14]:


list1=list(set(d1.event_code.tolist()))


# In[15]:


df1=pd.DataFrame()


# In[16]:


d1=d1.reset_index().drop(['index'],axis=1)


# In[17]:


count=0
for i in d1.groupby(['weeks','channel_code','des','order_status','mawb_no']):
    df1.loc[count,'weeks']=i[0][0]
    df1.loc[count,'channel_code']=i[0][1]
    df1.loc[count,'des']=i[0][2]
    df1.loc[count,'order_status']=str(i[0][3])
    df1.loc[count,'mawb_no']=i[0][4]
    for j in i[1].index:
        df1.loc[count,i[1].loc[j,'event_code']]=i[1].loc[j,'推送数']
    count+=1


# In[18]:


df1=df1.replace(np.nan,0)


# In[19]:


df1['order_status']=df1['order_status'].astype('int')


# In[20]:


r=pd.merge(d2,df1,on=['weeks','channel_code','des','order_status','mawb_no'],how='left')


# In[21]:


r=r.replace(np.nan,0)


# In[22]:


for i in list1:
    r[i]=r[i].astype('float')
    r[i]=r[i]/r['票数']


# In[23]:


list_c=r.columns


# In[24]:


# list_c.tolist()


# In[25]:


list_code=[
'PICK_UP_MAWB',
'ARRIVE_BONDED_WAREHOUSE',
'DEPART_DEST_CUSTOMS',
'DEPART_BONDED_WAREHOUSE',
'ARRIVE_TRANSITHUB',
'DEPART_TRANSITHUB',
'IN_TRANSIT',
'OUT_FOR_DELIVERY',
'DELIVERED',
'CUSTOMS_ IMPORT_INSPECTION',
'CONSIGNEE_ABSENCE',
'INCORRECT_ADDRESS',
'DELIVERY_FAILED_REFUSED',
'WAIT_CUSTOMER_PICK_UP',
'RETURNED_FROM_OVERSEA'
]


# In[26]:


ret_list=list(set(list_code).difference(set(list_c)))


# In[27]:


ret_list


# In[28]:


for i in ret_list:
    r[i]=0


# In[29]:


result=r[['weeks','channel_code','des','order_status','mawb_no','票数','PICK_UP_MAWB','ARRIVE_BONDED_WAREHOUSE','DEPART_DEST_CUSTOMS','DEPART_BONDED_WAREHOUSE','ARRIVE_TRANSITHUB','DEPART_TRANSITHUB','IN_TRANSIT','OUT_FOR_DELIVERY','DELIVERED','CUSTOMS_ IMPORT_INSPECTION','CONSIGNEE_ABSENCE','INCORRECT_ADDRESS','DELIVERY_FAILED_REFUSED','WAIT_CUSTOMER_PICK_UP','RETURNED_FROM_OVERSEA']]


# In[30]:


os={0:'未发送',1 :'已发送',2 :'转运中', 3:'送达',4:'超时',5:'扣关',6:'地址错误',7:'快件丢失', 8:'退件',9:'其他异常'}


# In[31]:


result['物流状态']=result['order_status'].map(os)


# In[32]:


import datetime,time


# In[33]:


def start(x):
    if(int(x)<2000):
        return np.NAN
    years = int(x[0:4])
    mon = x[4:6:1]
    days = (int(mon)-1) * 7
    fir_day = datetime.date(years, 1, 4)
    zone = datetime.timedelta(days-1)
    JS = fir_day + zone
    zones = datetime.timedelta(days-7)
    KS = fir_day + zones
    return KS.strftime('%m-%d')+'至'+JS.strftime('%m-%d')


# In[34]:


result['周期']=result['weeks'].apply(lambda x:start(x))


# In[35]:


result.to_excel(r'C:\Users\hp\Desktop\ebay推送\本地文件\ebay推送.xlsx',index=False)


# In[36]:


d2


# In[ ]:




