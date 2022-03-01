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


#本周
days = "BETWEEN '2021-11-28 16:00:00' and '2021-12-05 15:59:59'"


# In[5]:


#入库至出库
S1= """
select 
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 weeks,
lgo.channel_code,
lgo.des,
ROUND(TIMESTAMPDIFF(hour,lgo.gmt_create,lgb.sealing_bag_time)/24,1) as 间隔,
count(DISTINCT lgo.order_no) c
from
lg_order lgo,
lg_bag_order_relation lbor,
lg_bag lgb
where  lgo.gmt_create {}
and customer_id in  (3282094) 
and lgo.order_status in(1,2,3)
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
and lgo.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
and isnull(lgo.mawb_id)=0 
and isnull(lgb.sealing_bag_time)=0  
group by 1,2,3,4
""".format(days)


# In[6]:


d1=execude_sql(S1)


# In[7]:


d1['阶段']="入库至出库"


# In[8]:


#出库--装车
S2 = """ 
select 
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 weeks,
lgo.channel_code,
lgo.des,
ROUND(TIMESTAMPDIFF(hour,lgb.sealing_bag_time,tbe.event_time)/24,1) as 间隔,
count(DISTINCT lgo.order_no) c 
from lg_order lgo,lg_bag lgb, track_bag_event tbe, lg_bag_order_relation lbor
where tbe.bag_id=lbor.bag_id
and lbor.order_id=lgo.id  
and lbor.bag_id=lgb.id 
AND tbe.event_code="DEPS"
AND lgo.gmt_create {}
and customer_id in  (3282094) 
and lgo.order_status in(1,2,3)
and lgo.is_deleted='n'
and lgb.is_deleted='n' 
and tbe.is_deleted='n'
and lbor.is_deleted='n' 
and isnull(lgo.mawb_id)=0 
and isnull(lgb.sealing_bag_time)=0  
group by 1,2,3,4
""".format(days)


# In[9]:


d2=execude_sql(S2)


# In[10]:


d2['阶段']="出库至装车"


# In[11]:


#装车--起飞
S3= """
select 
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 weeks,
lgo.channel_code,
lgo.des,
ROUND(TIMESTAMPDIFF(hour,tbe.event_time,tme.event_time)/24,1) as 间隔,
count(DISTINCT lgo.order_no) c 
from 
lg_order lgo, 
track_bag_event tbe, 
lg_bag_order_relation lbor, 
track_mawb_event tme, 
lg_bag lgb, 
lg_mawb lgm 
where  
tbe.event_code="DEPS" 
AND tme.event_code in("SDFO","DEPC","DEPT","LKJC")
and lgo.gmt_create {}
and customer_id in  (3282094) 
and lgo.order_status in(1,2,3)
and lgo.id=lbor.order_id 
and lbor.bag_id=tbe.bag_id 
and tme.mawb_id=lgo.mawb_id 
AND lgb.id=lbor.bag_id
and lgo.mawb_id=lgm.id 
and lgo.is_deleted='n' 
and lgb.is_deleted='n' 
and tbe.is_deleted='n' 
and tme.is_deleted='n' 
and lgm.is_deleted='n' 
and lbor.is_deleted='n' 
group by 
1,2,3,4
""".format(days)


# In[12]:


d3=execude_sql(S3)


# In[13]:


d3['阶段']="装车至起飞"


# In[14]:


#起飞--落地
S4= """
select  
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 weeks,
lgo.channel_code,
lgo.des,
round(timestampdiff(hour,tme2.event_time,tme1.event_time)/24,1) as 间隔,
count(DISTINCT lgo.order_no) c 
from 
lg_order lgo,
track_mawb_event tme1, 
track_mawb_event tme2 
where  
tme1.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
and tme2.event_code in("SDFO","DEPC","DEPT","LKJC") 
and lgo.gmt_create {}
and customer_id in  (3282094) 
and lgo.order_status in(1,2,3) 
and tme1.mawb_id=lgo.mawb_id 
and tme2.mawb_id=lgo.mawb_id 
AND lgo.is_deleted='n'   
and tme1.is_deleted='n' 
and tme2.is_deleted='n' 
group by 
1,2,3,4
""".format(days)


# In[15]:


d4=execude_sql(S4)


# In[16]:


d4['阶段']="起飞至落地"


# In[17]:


#落地--清关
S51= """
select  
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 weeks,
lgo.channel_code,
lgo.des,
round(timestampdiff(hour,tme1.event_time,tbe.event_time)/24,1) as 间隔,
count(DISTINCT lgo.order_no) c 
from 
lg_order lgo,
track_mawb_event tme1, 
lg_bag_order_relation lbor,
track_bag_event tbe
where  
tme1.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
and tbe.event_code in("IRCM","PVCS","IRCN","RFIC")
and lgo.gmt_create {}
and customer_id in  (3282094) 
and lgo.order_status in(1,2,3) 
and tme1.mawb_id=lgo.mawb_id 
and lgo.id=lbor.order_id
and lbor.bag_id=tbe.bag_id
AND lgo.is_deleted='n'   
and tme1.is_deleted='n' 
and lbor.is_deleted='n' 
and tbe.is_deleted='n' 
group by 
1,2,3,4
""".format(days)

S52="""
select  
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 weeks,
lgo.channel_code,
lgo.des,
round(timestampdiff(hour,tme1.event_time,tme2.event_time)/24,1) as 间隔,
count(DISTINCT lgo.order_no) c 
from 
lg_order lgo,
track_mawb_event tme1, 
track_mawb_event tme2
where  
tme1.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
and tme2.event_code in("IRCM","PVCS","IRCN","RFIC")
and lgo.gmt_create {}
and customer_id in  (3282094) 
and lgo.order_status in(1,2,3) 
and tme1.mawb_id=lgo.mawb_id
and tme2.mawb_id=lgo.mawb_id
AND lgo.is_deleted='n'   
and tme1.is_deleted='n' 
and tme2.is_deleted='n' 
group by 
1,2,3,4 
""".format(days)

S53="""
select  
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 weeks,
lgo.channel_code,
lgo.des,
round(timestampdiff(hour,tme1.event_time,toe.event_time)/24,1) as 间隔,
count(DISTINCT lgo.order_no) c 
from 
lg_order lgo,
track_mawb_event tme1, 
track_order_event toe
where  
tme1.event_code in("ARIR","ABCD","ABAD","AECD","ARMA") 
and toe.event_code in("IRCM","PVCS","IRCN","RFIC")
and lgo.gmt_create {}
and customer_id in  (3282094) 
and lgo.order_status in(1,2,3) 
and tme1.mawb_id=lgo.mawb_id
and lgo.id=toe.order_id
AND lgo.is_deleted='n'   
and tme1.is_deleted='n' 
and toe.is_deleted='n' 
group by 
1,2,3,4 
""".format(days)


# In[18]:


d51=execude_sql(S51)
d52=execude_sql(S52)
d53=execude_sql(S53)


# In[19]:


d5=pd.concat([d51,d52,d53]).groupby(['weeks','channel_code','des','间隔'])['c'].sum().reset_index()


# In[20]:


d5['阶段']="落地至清关"


# In[21]:


#清关主单
S61="""select  count(1) c,
lgo.channel_code, 
lgo.des, 
lgm.mawb_no,
lgb.bag_no,  
date_format(date_add(tme.event_time,interval 8 hour),'%Y-%m-%d %H:%I:%S') fxdate,
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 weeks,
"主单" as Dimension
from 
track_mawb_event tme, 
lg_order lgo, 
lg_mawb lgm,
lg_bag lgb,
lg_bag_order_relation lbor 
where tme.event_code in("IRCM","PVCS","IRCN","RFIC")  
and lgo.gmt_create {}
and customer_id in  (3282094) 
and lgo.order_status in(2,3) 
and tme.mawb_id=lgo.mawb_id  
and lgo.mawb_id=lgm.id 
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
AND lgo.is_deleted='n'  
and tme.is_deleted='n'  
and lgm.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
group by 2,3,4,5,6,7,8
""".format(days)

# 清关order
S62="""SELECT 
count(1) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no, 
date_format(date_add(toe.event_time ,interval 8 hour),'%Y-%m-%d %H:%I:%S') fxdate,
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 weeks,
"order" as Dimension 
from 
lg_order lgo, 
lg_mawb lgm, 
track_order_event toe,
lg_bag lgb,
lg_bag_order_relation lbor 
where 
lgo.id=toe.order_id  
and lgm.id=lgo.mawb_id 
and lgo.id=lbor.order_id 
and lbor.bag_id=lgb.id 
AND toe.event_code in("IRCM","PVCS","IRCN","RFIC") 
AND lgo.order_status in(2,3) 
and lgo.gmt_create {} 
and lgo.is_deleted='n' 
and lgm.is_deleted='n' 
and toe.is_deleted='n' 
and lgb.is_deleted='n' 
and lbor.is_deleted='n' 
and customer_id in  (3282094) 
group by 
2,3,4,5,6,7,8
""".format(days)

# 清关bag
S63="""SELECT   
count(1) c,
lgo.channel_code,
lgo.des,
lgm.mawb_no,
lgb.bag_no,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d %H:%I:%S')fxdate,
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 weeks,
"bag" as Dimension
from 
track_bag_event tbe, 
lg_bag lgb, 
lg_mawb lgm, 
lg_order lgo,
lg_bag_order_relation lbor 
where   
lgb.id=tbe.bag_id 
and tbe.bag_id=lbor.bag_id 
and lbor.order_id=lgo.id 
and lgo.mawb_id=lgm.id 
AND tbe.event_code in("IRCM","PVCS","IRCN","RFIC") 
and lgo.gmt_create {} 
and customer_id in  (3282094) 
and lgo.order_status in(2,3) 
and lgo.is_deleted='n' 
and tbe.is_deleted='n' 
and lgb.is_deleted='n' 
and lgm.is_deleted='n' 
and lbor.is_deleted='n' 
group by 2,3,4,5,6,7,8
""".format(days)


# In[22]:


d61=execude_sql(S61)
d62=execude_sql(S62)
d63=execude_sql(S63)


# In[23]:


d6=pd.concat([d61,d62,d63])


# In[24]:


d6=d6.drop_duplicates(subset=['weeks','channel_code','des','mawb_no','bag_no'])


# In[25]:


S7="""
SELECT 
count(1) c ,
lgo.channel_code,
lgo.des,
date_format(date_add(tbe.event_time,interval 8 hour),'%Y-%m-%d %H:%I:%S')jfdate,
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 weeks,
lgb.bag_no,
lgm.mawb_no 
from lg_order lgo, 
track_bag_event tbe,
lg_bag_order_relation lbor, 
lg_bag lgb,
lg_mawb lgm 
where 
tbe.bag_id=lgb.id 
and lgb.id=lbor.bag_id 
and lbor.order_id=lgo.id 
and lgm.id=lgo.mawb_id   
AND tbe.event_code in("JFMD","AAPS") 
and lgo.gmt_create {} 
and customer_id in  (3282094) 
and lgo.order_status in(2,3)  
and lgo.is_deleted='n'  
and lgb.is_deleted='n' 
and tbe.is_deleted='n' 
and lbor.is_deleted='n' 
group by 
2,3,4,5,6,7
""".format(days)


# In[26]:


d7=execude_sql(S7)


# In[27]:


d7=pd.merge(d7,d6[['weeks','channel_code','des','bag_no','mawb_no','fxdate']],on=['weeks','channel_code','des','bag_no','mawb_no'],how='left')


# In[28]:


d7['jfdate']=pd.to_datetime(d7['jfdate'])
d7['fxdate']=pd.to_datetime(d7['fxdate'])


# In[29]:


d7['间隔']=(d7["jfdate"]-d7["fxdate"]).astype('timedelta64[s]')


# In[30]:


d7['间隔'] = round(d7["间隔"]/86400,2)


# In[31]:


d7=d7.groupby(['weeks','channel_code','des','间隔'])['c'].sum().reset_index()


# In[32]:


d7['阶段']="清关至交付"


# In[33]:


S8="""
SELECT 
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 weeks,
lgo.channel_code,
lgo.des, 
round(timestampdiff(hour,tbe.event_time,lgo.delivery_date)/24,1) 间隔,
count(DISTINCT lgo.order_no) c 
from 
lg_order lgo inner join lg_bag_order_relation lbor on lbor.order_id=lgo.id  
inner join track_bag_event tbe on tbe.bag_id=lbor.bag_id  
where  
tbe.event_code="JFMD" #派送公司收货 
and lgo.gmt_create {}
and customer_id in  (3282094)    
and lgo.order_status =3
and lgo.is_deleted='n'   
and tbe.is_deleted='n' 
and lbor.is_deleted='n' 
GROUP BY 1,2,3,4
""".format(days)


# In[34]:


d8=execude_sql(S8)


# In[35]:


d8['阶段']="交付至妥投"


# In[36]:


r1=pd.concat([d1,d2,d3,d4,d5,d7,d8])


# In[37]:


r2=r1.sort_values(by=['weeks','channel_code','des','阶段','间隔'])


# In[38]:


r2['累计票数']=r2.groupby(['weeks','channel_code','des','阶段'])['c'].cumsum()


# In[39]:


r3=r2.drop_duplicates(subset=['weeks','channel_code','des','阶段'],keep='last')


# In[40]:


r3=r3.rename(columns={'累计票数':'节点票数'})


# In[41]:


r4=pd.merge(r2,r3[['weeks','channel_code','des','阶段','节点票数']],on=['weeks','channel_code','des','阶段'],how='left')


# In[42]:


r4['per']=r4['累计票数']/r4['节点票数']


# In[43]:


result=r4[r4['per']>=0.9].drop_duplicates(subset=['weeks','channel_code','des','阶段'],keep='first')


# In[44]:


S9="""
select 
DATE_FORMAT(DATE_ADD(lgo.gmt_create,interval 8 hour),'%Y%u')+1 weeks,
lgo.channel_code,
lgo.des, 
count(*) 总票数
from lg_order lgo
where 
lgo.gmt_create {}
and customer_id in  (3282094) 
and lgo.is_deleted='n'
GROUP BY 1,2,3
""".format(days)


# In[45]:


d9=execude_sql(S9)


# In[46]:


result1=pd.merge(result,d9,on=['weeks','channel_code','des'],how='left')


# In[47]:


result2=result1.sort_values(by=['weeks','channel_code','总票数'],ascending=[False,True,False])


# In[48]:


r_max=result2['weeks'].max()
r_min=result2['weeks'].min()


# In[49]:


d_max=result2[result2['weeks']==r_max]
d_min=result2[result2['weeks']==r_min]


# In[50]:


d=pd.merge(d_max,d_min[['channel_code','des','阶段','间隔']],on=['channel_code','des','阶段'],how='left')


# In[51]:


list1=list(set(d['阶段'].tolist()))


# In[52]:


list2=[i+"票数" for i in list1]


# In[53]:


list3=[i+"上期" for i in list1]


# In[54]:


list4=[]


# In[55]:


list4.extend(list1)


# In[56]:


list4.extend(list2)


# In[57]:


list4.extend(list3)


# In[58]:


list4.append('channel_code')
list4.append('des')
list4.append('weeks')
# list4.append('业务类型')


# In[59]:


df1=pd.DataFrame(columns=list4)


# In[60]:


count=0
for i in d.groupby(['weeks','channel_code','des','总票数']):
    df1.loc[count,'weeks']=i[0][0]
    df1.loc[count,'channel_code']=i[0][1]
    df1.loc[count,'des']=i[0][2]
    df1.loc[count,'总票数']=i[0][3]
    for j in i[1].index:
        df1.loc[count,i[1].loc[j,'阶段']+'票数']=i[1].loc[j,'节点票数']
        df1.loc[count,i[1].loc[j,'阶段']]=i[1].loc[j,'间隔_x']
        df1.loc[count,i[1].loc[j,'阶段']+'上期']=i[1].loc[j,'间隔_y']
    count+=1


# In[61]:


df1=df1.replace(np.nan,"-")


# In[62]:


df2=df1[['weeks','channel_code','des','总票数','入库至出库票数','入库至出库','入库至出库上期','出库至装车票数','出库至装车','出库至装车上期','装车至起飞票数','装车至起飞','装车至起飞上期','起飞至落地票数','起飞至落地','起飞至落地上期','落地至清关票数','落地至清关','落地至清关上期','清关至交付票数','清关至交付','清关至交付上期','交付至妥投票数','交付至妥投','交付至妥投上期',]]


# In[63]:


df2.to_excel('全段90分位.xlsx',index=False)


# In[ ]:




