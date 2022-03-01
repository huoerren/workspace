# coding=utf-8


import pandas as pd

pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)
pd.set_option('expand_frame_repr', False)

import openpyxl
import pymysql
import re

import numpy as np

con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8",
                      autocommit=True, database='logisticscore')
cur = con.cursor()





qudaos = ['CNE全球特惠']
countries =[ ['DK','SE']]

def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns = [column[i][0] for i in range(len(column))]
    df = pd.DataFrame([list(i) for i in data], columns=columns)
    return df

global start
start = pd.DataFrame({ "月":[], "渠道名称":[] , "目的国":[],  "90分位时效":[],"平均时效":[], "妥投单量":[], "总单量":[] })

new_list=[]
print('---------------------------------------------------------------')

def formatDate(qudao,countryName):
    S_fenwei = '''

    select t3.`月`,t3.channel_code,t3.des,t3.`本组单量`, t3.acc_count,t3.`总单量`,t3.delivery,t3.acc_count/t3.`总单量` as fenwei from (
        select t1.`月`, t1.channel_code, t1.des,t1.delivery, t1.c as `本组单量`,  sum(t1.c) OVER(PARTITION BY `月`,channel_code,des  rows between unbounded preceding and current row) as acc_count,t2.`总单量` 
                from (
                                select  month(DATE_ADD(gmt_create,INTERVAL 8 hour)) `月`,
                                                channel_code,
                                                des,
                                                round(TIMESTAMPDIFF(hour,gmt_create,delivery_date)/24,1) as delivery ,
                                                count(*) as c  
                                                from lg_order 
                                                where   gmt_create BETWEEN '2021-08-31 16:00:00' and '2021-09-30 16:00:00'
                                                AND order_status=3
                                                AND channel_code = '{}'
                                                AND des ='{}'
                                                AND is_deleted='n'
                                                GROUP BY 1,2,3,4
                                                ORDER BY 月,channel_code , des,delivery   asc 
                            )t1 left join ( select
                                                month(DATE_ADD(gmt_create,INTERVAL 8 hour)) `月份`,
                                                count(1) as `总单量`
                                                from lg_order
                                                where
                                                gmt_create BETWEEN '2021-08-31 16:00:00' and '2021-09-30 16:00:00'
                                                AND channel_code  ='{}'
                                                AND des  ='{}'
                                                AND is_deleted='n'
                                                GROUP BY 1  ) t2
                                on t1.`月` = t2.`月份`
                    ) t3
    '''.format(qudao, countryName,qudao, countryName )

    S_shixiao = '''
          select `月`, channel_code, des, c, delivery, 
                        sum(zong_shijian) OVER(PARTITION BY `月`, channel_code, des ) as all_shijian, 
                            t2.`总单量`,   sum(zong_shijian) OVER(PARTITION BY `月`, channel_code, des )/t2.`总单量` as `平均时效` from (

                    select `月`, channel_code, des , delivery, c,  delivery*c as zong_shijian from (
                            select  month(DATE_ADD(gmt_create,INTERVAL 8 hour)) `月`,
                                channel_code,
                                des,
                                round(TIMESTAMPDIFF(hour,gmt_create,delivery_date)/24,1) as delivery ,
                                count(*) as c 
                                from lg_order 
                                where   gmt_create BETWEEN '2021-08-31 16:00:00' and '2021-09-30 16:00:00'
                                AND order_status=3
                                AND channel_code ='{}'
                                AND des ='{}'
                                AND is_deleted='n'
                                GROUP BY 1,2,3,4
                                ORDER BY `月`,channel_code , des,delivery  asc 
                            )t	
                    )t1 left join ( select
                                    month(DATE_ADD(gmt_create,INTERVAL 8 hour)) `月份`,
                                    count(1) as `总单量`
                                    from lg_order
                                    where
                                    gmt_create BETWEEN '2021-08-31 16:00:00' and '2021-09-30 16:00:00'
                                    AND order_status=3
                                    AND channel_code ='{}'
                                    AND des ='{}'
                                    AND is_deleted='n'
                                    GROUP BY 1  ) t2
                on t1.`月` = t2.`月份`

    '''.format(qudao, countryName,qudao, countryName )

    d_fenwei = execude_sql(S_fenwei)
    d_shixiao =execude_sql(S_shixiao)
    shixiao_DF = d_shixiao.drop_duplicates(subset=['月', 'channel_code', 'des'], keep="last")
    print('------------- shixiao_DF : --------------')
    print(shixiao_DF)



    for df1, df2 in d_fenwei.groupby(['月', 'channel_code', 'des']):

        # 获得df2['fenwei']的最大值，如果最大值是小于0.9 是不达标的
        fenweiMax = df2['fenwei'].max()
        if (float(fenweiMax) >= 0.95):
            df3 = df2[df2['fenwei'].apply(lambda x: float(x)) >= 0.95]
            df_sub_fenwei = df3.head(1)
            print('----------- 正常的 df_sub_fenwei ---------------')
            print(df_sub_fenwei)
        else:
            print('------------------不正常的 df_sub_fenwei （妥投率小于 0.9 ）---------------')
            df_sub_fenwei = df2.tail(1)
            print(df_sub_fenwei)
            # df_sub_fenwei[''] = '90分位时效'
            df_sub_fenwei.loc[:,'delivery'] = -1
            print('未达标')

        mer_df = pd.merge(df_sub_fenwei, shixiao_DF, on=['月', 'channel_code', 'des'])
        yesDf = mer_df[['月', 'channel_code', 'des', 'delivery_x', '平均时效', '总单量_y', '总单量_x']]
        canUsedDf = yesDf.rename(
            columns={"channel_code": "渠道名称", "des": "目的国", "delivery_x": "90分位时效", "总单量_x": "总单量", "总单量_y": "妥投单量"})
        # print(canUsedDf)
        new_list.append(canUsedDf)

def anpai():
    for i in range(len(qudaos)):
        qudao = qudaos[i]
        sub_countries = countries[i]
        for j in range(len(sub_countries)):
            result = qudao , sub_countries[j]
            formatDate(result[0] , result[1])
    new_df = pd.concat(new_list)



    print(new_df.shape)
    new_df.to_excel(r'C:\Users\hp\Desktop\result_panxin.xlsx', index=False)



if __name__ == '__main__':
    anpai()











