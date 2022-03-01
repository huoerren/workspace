#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import openpyxl
import pymysql
import re

import numpy as np

con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()


# In[2]:


def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df

days="BETWEEN '2021-03-31 16:00:00' and '2021-07-31 16:00:00'"

days1="BETWEEN '2021-03-31 16:00:00' and '2021-07-31 16:00:00'"

base_df=pd.read_excel(r'C:\Users\hp\Desktop\基础表.xlsx')
print('------------- base_df: ---------')
print(base_df.head())




# country_df=pd.read_excel(r'C:\Users\hp\Desktop\国家二字码转换.xlsx')
# print('------------- country_df: ---------')
# print(country_df.head())

# In[7]:


# country_dict=country_df[['国家','des']].to_dict(orient ='series')
# print('------------------ country_dict: ----------------')
# print(country_dict)
# In[8]:


# country_dict=country_df.set_index('国家')['des'].to_dict()
# print('------------------ country_dict-02: ----------------')
# print(country_dict)

# In[9]:


# base_df['des']=base_df['国家'].map(country_dict)
base_list=[]


base_list=base_df[['渠道','des']].values.tolist()
print('-------------------- base_list ----------------')
print(base_list)

new_list=[]
print('---------------------------------------------------------------')

count=0
for a,b in base_list:
    S1="""
    select 
    channel_code 渠道,
    des,
    max((case when 月=4 then 妥投时间间隔 else 0 end)) as 4月平均妥投间隔,
    max((case when 月=5 then 妥投时间间隔 else 0 end)) as 5月平均妥投间隔,
    max((case when 月=6 then 妥投时间间隔 else 0 end)) as 6月平均妥投间隔,
    max((case when 月=7 then 妥投时间间隔 else 0 end)) as 7月平均妥投间隔
    from(
    select 
    month(DATE_ADD(gmt_create,INTERVAL 8 hour)) 月,
    channel_code,
    des,
    round(avg(TIMESTAMPDIFF(hour,gmt_create,delivery_date)/24),1) 妥投时间间隔
    from lg_order 
    where 
    gmt_create {}
    and des = '{}'
    and channel_code = '{}'
    and order_status=3
    group by 1,2,3
    ) as t
    GROUP BY 1,2
    """.format(days,b,a)

    print('---------------------- S1 ----------------')
    print(S1)
    
    # S2="""
    # select
    # channel_code 渠道,
    # des,
    # max((case when 月=4 then 妥投率 else -1 end)) as 4月妥投率,
    # max((case when 月=5 then 妥投率 else -1 end)) as 5月妥投率,
    # max((case when 月=6 then 妥投率 else -1 end)) as 6月妥投率,
    # max((case when 月=7 then 妥投率 else -1 end)) as 7月妥投率
    # from(
    # select
    # month(DATE_ADD(gmt_create,INTERVAL 8 hour)) as 月,
    # channel_code,
    # des,
    # sum(case when order_status=3 then 1 else 0 end)/count(*) 妥投率
    # from lg_order
    # where
    # gmt_create {}
    # and des ='{}'
    # and channel_code ='{}'
    # group by 1,2,3
    # ) as t
    # GROUP BY 1,2
    # """.format(days,b,a)
    #
    # print('------------------------------ S2 -----------------------')
    # print(S2)

    
    # S3="""
    # with
    # HH AS
    # (
    # select
    # count(1) as c
    # from lg_order
    # where
    # gmt_create {}
    # AND channel_code = '{}'
    # AND des = '{}'
    # AND is_deleted='n'
    # )
    # select 渠道,国家 des,min(delivery) AS 90分位妥投时效
    # from(
    # select
    # channel_code as 渠道,
    # des as 国家,
    # delivery,
    # sum(c) OVER(PARTITION BY channel_code,des ORDER BY delivery)/(select c from HH) as per
    # from
    # (
    # select
    # channel_code,
    # des,
    # round(TIMESTAMPDIFF(hour,gmt_create,delivery_date)/24,1) as delivery,
    # count(*) as c
    # from lg_order
    # where
    # gmt_create {}
    # AND order_status=3
    # AND channel_code ='{}'
    # AND des ='{}'
    # AND is_deleted='n'
    # GROUP BY 1,2,3 ) as t
    # ) as t1
    # where t1.per>=0.9
    # GROUP BY 1,2
    # """.format(days1,a,b,days1,a,b)

    # print('------------------------------ S3 -----------------------')
    # print(S3)


    S4 = """
        with 
        HH AS
        (
        select
        month(DATE_ADD(gmt_create,INTERVAL 8 hour)) 月,
        count(1) as c
        from lg_order
        where
        gmt_create {}
        AND channel_code = '{}'
        AND des = '{}'
        AND is_deleted='n'
        GROUP BY 1
        )
        SELECT 
        渠道,
        des,
        max((case when 月=4 then 90分位妥投时效 else -1 end)) as 4月90分位妥投时效,
        max((case when 月=5 then 90分位妥投时效 else -1 end)) as 5月90分位妥投时效,
        max((case when 月=6 then 90分位妥投时效 else -1 end)) as 6月90分位妥投时效,
        max((case when 月=7 then 90分位妥投时效 else -1 end)) as 7月90分位妥投时效
        FROM(
        select 月,渠道,国家 des,min(delivery) AS 90分位妥投时效
        from(
        select 
        月,
        channel_code as 渠道,
        des as 国家,
        delivery,
        sum(c) OVER(PARTITION BY 月,channel_code,des ORDER BY delivery)/(select c from HH where HH.月=t.月) as per
        from 
        (
        select 
        month(DATE_ADD(gmt_create,INTERVAL 8 hour)) 月,
        channel_code,
        des,
        round(TIMESTAMPDIFF(hour,gmt_create,delivery_date)/24,1) as delivery,
        count(*) as c
        from lg_order
        where
        gmt_create {}
        AND order_status=3
        AND channel_code = '{}'
        AND des = '{}'
        AND is_deleted='n'
        GROUP BY 1,2,3,4 ) as t
        ) as t1
        where t1.per>=0.9
        GROUP BY 1,2,3) as t2
        GROUP BY 1,2
        """.format(days1, a, b, days1, a, b)

    print('------------------------------ S4 -----------------------')
    print(S4)


        
    d1=execude_sql(S1)
    # d2=execude_sql(S2)
    # d3=execude_sql(S3)
    d4=execude_sql(S4)
    
    base_df1=pd.merge(d1,d4,on=['渠道','des'],how='outer')
    # base_df1=pd.merge(base_df1,d3,on=['渠道','des'],how='outer')
    # base_df1=pd.merge(base_df1,d4,on=['渠道','des'],how='outer')
    new_list.append(base_df1)
    
new_df=pd.concat(new_list)
new_df=new_df.set_index('渠道').reset_index()
new_df=new_df.replace(-1,'未达标')

print(new_df)


# In[ ]:


new_df.to_excel(r'C:\Users\hp\Desktop\result_luo.xlsx', index=False)

