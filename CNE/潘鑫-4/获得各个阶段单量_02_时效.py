

import pandas as pd

import os
os.chdir(r'C:\Users\hp\Desktop\detacheck')

shousao = pd.read_csv('01_首扫数量.csv' )
fengdai = pd.read_csv('02_封袋数量.csv' )

zhuangche  = pd.read_csv('03_装车数量.csv' )
qifei = pd.read_csv('04_起飞数量.csv' )

luodi = pd.read_csv('05_落地数量.csv' )
qingguan = pd.read_csv('06_清关数量.csv')

jiaofu = pd.read_csv('07_交付数量.csv' )
tuotou = pd.read_csv('08_妥投数量.csv' )

# 首扫-封袋

print('**************** 首扫-封袋 start  ; *********** ')
shoudao_fengdai = pd.merge(shousao , fengdai[["内单号", "封袋时间"]], left_on="内单号", right_on="内单号", how='inner')
shoudao_fengdai['首扫时间'] = pd.to_datetime(shoudao_fengdai['首扫时间'])
shoudao_fengdai['封袋时间'] = pd.to_datetime(shoudao_fengdai['封袋时间'])

shoudao_fengdai["用时"] = (shoudao_fengdai["封袋时间"]-shoudao_fengdai["首扫时间"]).astype('timedelta64[s]')
shoudao_fengdai["用时"] = round(shoudao_fengdai["用时"]/86400,2)

for df1, df2 in shoudao_fengdai.groupby(['channel_name','des']):
    df3 = df2.sort_values(by=["用时"] ,ascending= True)
    # print(df3.describe(percentiles=[0.9])['用时'] )
    print(df1,'------ ',  df3.shape[0] ,' ----- ' , df3.describe(percentiles=[0.9])['用时']['90%'],'---', (df3.describe(percentiles=[0.75])['用时']['75%'] - df3.describe(percentiles=[0.25])['用时']['25%'])*1.5 + df3.describe(percentiles=[0.75])['用时']['75%'] )
    print('----------------------------')

print('**************** 首扫-封袋 end ; *********** ')

print('**************** 封袋-装车 start ; *********** ')
# 封袋-装车
fengdai_zhuangche = pd.merge(fengdai  , zhuangche[["内单号", "装车时间"]], left_on="内单号", right_on="内单号", how='inner')
fengdai_zhuangche['装车时间'] = pd.to_datetime(fengdai_zhuangche['装车时间'])
fengdai_zhuangche['封袋时间'] = pd.to_datetime(fengdai_zhuangche['封袋时间'])

fengdai_zhuangche["用时"] = (fengdai_zhuangche["装车时间"]- fengdai_zhuangche["封袋时间"]).astype('timedelta64[s]')
fengdai_zhuangche["用时"] = round(fengdai_zhuangche["用时"]/86400,2)

for df1, df2 in fengdai_zhuangche.groupby(['channel_name','des']):
    df3 = df2.sort_values(by=["用时"] ,ascending= True)
    # print(df3.describe(percentiles=[0.9])['用时'] )
    print(df1,'------ ',  df3.shape[0] ,' ----- ' ,df3.describe(percentiles=[0.9])['用时']['90%'],'---',  (df3.describe(percentiles=[0.75])['用时']['75%'] - df3.describe(percentiles=[0.25])['用时']['25%'])*1.5 + df3.describe(percentiles=[0.75])['用时']['75%']  )
    print('----------------------------')

print('**************** 封袋-装车 : end ****************')

# 装车-起飞
print('**************** 装车-起飞 : start  ****************')
qifei_zhuangche = pd.merge(zhuangche , qifei[["内单号", "起飞时间"]], left_on="内单号", right_on="内单号", how='inner')
qifei_zhuangche['装车时间'] = pd.to_datetime(qifei_zhuangche['装车时间'])
qifei_zhuangche['起飞时间'] = pd.to_datetime(qifei_zhuangche['起飞时间'])

qifei_zhuangche["用时"] = (qifei_zhuangche["起飞时间"]- qifei_zhuangche["装车时间"]).astype('timedelta64[s]')
qifei_zhuangche["用时"] = round(qifei_zhuangche["用时"]/86400,2)

for df1, df2 in qifei_zhuangche.groupby(['channel_name','des']):
    df3 = df2.sort_values(by=["用时"] ,ascending= True)
    # print(df3.describe(percentiles=[0.9])['用时'] )
    print(df1,'------ ',  df3.shape[0] ,' ----- ' ,df3.describe(percentiles=[0.9])['用时']['90%'],'---',  (df3.describe(percentiles=[0.75])['用时']['75%'] - df3.describe(percentiles=[0.25])['用时']['25%'])*1.5 + df3.describe(percentiles=[0.75])['用时']['75%']  )
    print('----------------------------')

print('**************** 装车-起飞 : end -----------')

# 起飞-落地
print('**************** 起飞-落地 : start  ****************')
qifei_luodi = pd.merge(qifei , luodi[["内单号", "落地时间"]], left_on="内单号", right_on="内单号", how='inner')
qifei_luodi['落地时间'] = pd.to_datetime(qifei_luodi['落地时间'])
qifei_luodi['起飞时间'] = pd.to_datetime(qifei_luodi['起飞时间'])

qifei_luodi["用时"] = (qifei_luodi["落地时间"]- qifei_luodi["起飞时间"]).astype('timedelta64[s]')
qifei_luodi["用时"] = round(qifei_luodi["用时"]/86400,2)

for df1, df2 in qifei_luodi.groupby(['channel_name','des']):
    df3 = df2.sort_values(by=["用时"] ,ascending= True)
    # print(df3.describe(percentiles=[0.9])['用时'] )
    print(df1,'------ ',  df3.shape[0] ,' ----- ' ,df3.describe(percentiles=[0.9])['用时']['90%'],'---',  (df3.describe(percentiles=[0.75])['用时']['75%'] - df3.describe(percentiles=[0.25])['用时']['25%'])*1.5 + df3.describe(percentiles=[0.75])['用时']['75%']  )
    print('----------------------------')

print('**************** 起飞-落地 : end -----------')


print('**************** 落地-清关 : start  ****************')
# 落地-清关
luodi_qingguan = pd.merge(luodi , qingguan[["内单号", "进口清关时间"]], left_on="内单号", right_on="内单号", how='inner')
luodi_qingguan['落地时间'] = pd.to_datetime(luodi_qingguan['落地时间'])
luodi_qingguan['进口清关时间'] = pd.to_datetime(luodi_qingguan['进口清关时间'])

luodi_qingguan["用时"] = (luodi_qingguan["进口清关时间"]- luodi_qingguan["落地时间"]).astype('timedelta64[s]')
luodi_qingguan["用时"] = round(luodi_qingguan["用时"]/86400,2)

for df1, df2 in luodi_qingguan.groupby(['channel_name','des']):
    df3 = df2.sort_values(by=["用时"] ,ascending= True)
    # print(df3.describe(percentiles=[0.9])['用时'] )
    print(df1,'------ ',  df3.shape[0] ,' ----- ' ,df3.describe(percentiles=[0.9])['用时']['90%'],'---',  (df3.describe(percentiles=[0.75])['用时']['75%'] - df3.describe(percentiles=[0.25])['用时']['25%'])*1.5 + df3.describe(percentiles=[0.75])['用时']['75%']  )
    print('----------------------------')

print('**************** 落地-清关 : end -----------')



print('**************** 清关-交付 : start  ****************')
# 清关-交付
qingguan_jiaofu = pd.merge(qingguan , jiaofu[["内单号", "交付时间"]], left_on="内单号", right_on="内单号", how='inner')
qingguan_jiaofu['交付时间'] = pd.to_datetime(qingguan_jiaofu['交付时间'])
qingguan_jiaofu['进口清关时间'] = pd.to_datetime(qingguan_jiaofu['进口清关时间'])

qingguan_jiaofu["用时"] = (qingguan_jiaofu["交付时间"]- qingguan_jiaofu["进口清关时间"]).astype('timedelta64[s]')
qingguan_jiaofu["用时"] = round(qingguan_jiaofu["用时"]/86400,2)

for df1, df2 in qingguan_jiaofu.groupby(['channel_name','des']):
    df3 = df2.sort_values(by=["用时"] ,ascending= True)
    # print(df3.describe(percentiles=[0.9])['用时'] )
    print(df1,'------ ',  df3.shape[0] ,' ----- ' ,df3.describe(percentiles=[0.9])['用时']['90%'],'---',  (df3.describe(percentiles=[0.75])['用时']['75%'] - df3.describe(percentiles=[0.25])['用时']['25%'])*1.5 + df3.describe(percentiles=[0.75])['用时']['75%']  )
    print('----------------------------')

print('**************** 清关-交付 : end -----------')

print('**************** 交付-妥投 : start  ****************')
# 交付-妥投
jiaofu_tuotou = pd.merge(jiaofu , tuotou[["内单号", "妥投时间"]], left_on="内单号", right_on="内单号", how='inner')
jiaofu_tuotou['交付时间'] = pd.to_datetime(jiaofu_tuotou['交付时间'])
jiaofu_tuotou['妥投时间'] = pd.to_datetime(jiaofu_tuotou['妥投时间'])

jiaofu_tuotou["用时"] = (jiaofu_tuotou["妥投时间"]- jiaofu_tuotou["交付时间"]).astype('timedelta64[s]')
jiaofu_tuotou["用时"] = round(jiaofu_tuotou["用时"]/86400,2)

for df1, df2 in jiaofu_tuotou.groupby(['channel_name','des']):
    df3 = df2.sort_values(by=["用时"] ,ascending= True)
    # print(df3.describe(percentiles=[0.9])['用时'] )
    print(df1,'------ ',  df3.shape[0] ,' ----- ' ,df3.describe(percentiles=[0.9])['用时']['90%'],'---',  (df3.describe(percentiles=[0.75])['用时']['75%'] - df3.describe(percentiles=[0.25])['用时']['25%'])*1.5 + df3.describe(percentiles=[0.75])['用时']['75%']  )
    print('----------------------------')

print('**************** 交付-妥投 : end -----------')




