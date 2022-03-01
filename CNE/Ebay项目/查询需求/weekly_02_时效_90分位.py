
import pandas as pd

import os
import re
import pandas as pd
import numpy as np
from dateutil.parser import parse
from business_duration import businessDuration
from dateutil import rrule
import datetime,time


def forMX(received_time,complete_time): # 周一到周六
    if(pd.isnull(received_time) or pd.isnull(complete_time)):
        return np.nan
    workdays = [x for x in range(7) if x not in [6]]
    time_period = rrule.rrule(rrule.MINUTELY, dtstart=received_time, until=complete_time, byweekday=workdays).count()
    return round(time_period / (60 * 24), 2)

def forMX_02(received_time,complete_time): # 周一到周五及周六上午半天
    if(pd.isnull(received_time) or pd.isnull(complete_time)):
        return np.nan
    else:
        period = businessDuration(received_time, complete_time, unit='min')
        period = round(period / (60), 2)  # 将‘min’ 转为 ‘hour’，得到周一到周五 时常（小时）

        # case01: 开始时间和结束时间是在同一天
        if received_time.strftime('%Y-%m-%d') == complete_time.strftime('%Y-%m-%d'):
            if (received_time.weekday()!= 5) :
                period = round((complete_time - received_time).total_seconds()/3600 ,2 )
                period = round(period/24 ,2 )
                return period
            else:
                period = round((complete_time - received_time).total_seconds()/3600 ,2 )
                if period>12:
                    period=12
                period = round(period/24 ,2 )
                return period

        # case02:开始时间和结束时间不是在同一天
        else:

            # 判断起始时间是否在周六且在周六12点前，如果是 则 需要考虑起始时间 至 周六 12点前的时间
            if (received_time.weekday()== 5) : # 周六
                date_compare = received_time.strftime('%Y-%m-%d')  + " 12:00:00"
                sub_time_01_seconds = (parse(date_compare) - received_time).total_seconds()  #计算周六起始时间用时
                sub_time_01_hours = round(sub_time_01_seconds/(3600),2)
                period = period + sub_time_01_hours

            # 判断结束时间是否在周六且在周六12点前，如果是 则 需要考虑周六 00:00:00 至 周六的时间
            # 结束时间 只有两种情况，case01: 在 周一至周五 ； case02:周六 12点前。
            if (complete_time.weekday()== 5) : # 周六
                date_compare = complete_time.strftime('%Y-%m-%d')  + " 00:00:00"
                sub_time_02_seconds = ( complete_time - parse(date_compare)).total_seconds()  #计算结束时间完成用时
                sub_time_02_hours = round(sub_time_02_seconds/(3600),2)
                if(sub_time_02_hours>12):
                    sub_time_02_hours=12
                period = period + sub_time_02_hours
            # 判断在 (开始时间, 结束时间) 之间有几个周六, 有x个周六，则需要在总时间之上再加上 x个半天
            days = complete_time - received_time
            count=0
            received_time=received_time + datetime.timedelta(days=1)
            while(received_time.strftime('%Y-%m-%d') <= (complete_time+datetime.timedelta(days=-1)).strftime('%Y-%m-%d')):
                if (received_time).weekday() == 5:
                    count+=1
                received_time=received_time + datetime.timedelta(days=1)
            period = period + 12 * count
        period = round(period/24 ,2 )
        return  period




def getAllShiXiao():

    # 判断 文件夹中所存在的Des
    df_ruku_chuku      = pd.read_csv('01_入库-出库.csv')
    df_chuku_qifei = pd.read_csv('03_出库-起飞.csv')

    df_qifei_luodi    = pd.read_csv('05_起飞-落地.csv')
    df_luodi_qingguan = pd.read_csv('06_落地-进口清关.csv')

    df_qingguan_jiaofu = pd.read_csv('07_进口清关-交付.csv')
    df_jiaofu_tuotou   = pd.read_csv('08_交付-妥投.csv')

    df_shousao_tuotou = pd.read_csv('09_首扫-妥投.csv')

    # 首扫-封袋

    print('**************** 首扫-封袋 start  ; *********** ')
    df_ruku_chuku['首扫时间'] = pd.to_datetime(df_ruku_chuku['首扫时间'])
    df_ruku_chuku['封袋时间'] = pd.to_datetime(df_ruku_chuku['封袋时间'])

    df_ruku_chuku["用时"] = (df_ruku_chuku["封袋时间"] - df_ruku_chuku["首扫时间"]).astype('timedelta64[s]')
    df_ruku_chuku["用时"] = round(df_ruku_chuku["用时"] / 86400, 2)

    df_ruku_chuku_sort = df_ruku_chuku.sort_values(by=["用时"], ascending=True)
    print(df_ruku_chuku_sort['用时'].describe(percentiles=[0.9,0.95]))



    # 出库-起飞
    print('**************** 出库-起飞 : start  ****************')
    df_chuku_qifei['封袋时间'] = pd.to_datetime(df_chuku_qifei['封袋时间'])
    df_chuku_qifei['起飞时间'] = pd.to_datetime(df_chuku_qifei['起飞时间'])

    df_chuku_qifei["用时"] = (df_chuku_qifei["起飞时间"] - df_chuku_qifei["封袋时间"]).astype('timedelta64[s]')
    df_chuku_qifei["用时"] = round(df_chuku_qifei["用时"] / 86400, 2)
    df_chuku_qifei_sort = df_chuku_qifei.sort_values(by=["用时"], ascending=True)
    print(df_chuku_qifei_sort['用时'].describe(percentiles=[0.9,0.95]))


    # 起飞-落地
    print('**************** 起飞-落地 : start  ****************')
    df_qifei_luodi['落地时间'] = pd.to_datetime(df_qifei_luodi['落地时间'])
    df_qifei_luodi['起飞时间'] = pd.to_datetime(df_qifei_luodi['起飞时间'])

    df_qifei_luodi["用时"] = (df_qifei_luodi["落地时间"] - df_qifei_luodi["起飞时间"]).astype('timedelta64[s]')
    df_qifei_luodi["用时"] = round(df_qifei_luodi["用时"] / 86400, 2)
    df_qifei_luodi_sort = df_qifei_luodi.sort_values(by=["用时"], ascending=True)
    print(df_qifei_luodi_sort['用时'].describe(percentiles=[0.9,0.95]))


    print('**************** 落地-清关 : start  ****************')
    # 落地-清关
    df_luodi_qingguan['落地时间'] = pd.to_datetime(df_luodi_qingguan['落地时间'])
    df_luodi_qingguan['进口清关时间'] = pd.to_datetime(df_luodi_qingguan['进口清关时间'])

    print('------ CD计算： ---------')
    df_luodi_qingguan["用时"] = (df_luodi_qingguan["进口清关时间"] - df_luodi_qingguan["落地时间"]).astype('timedelta64[s]')
    df_luodi_qingguan["用时"] = round(df_luodi_qingguan["用时"] / 86400, 2)
    df_luodi_qingguan_sort_CD = df_luodi_qingguan.sort_values(by=["用时"], ascending=True)
    print(df_luodi_qingguan_sort_CD['用时'].describe(percentiles=[0.9, 0.95]))

    print('------ BD计算： ---------')
    df_luodi_qingguan['落地-清关用时_BD'] = df_luodi_qingguan.apply(lambda x: forMX_02(x['落地时间'], x['进口清关时间']), axis=1)
    df_luodi_qingguan_sort_BD = df_luodi_qingguan.sort_values(by=["落地-清关用时_BD"], ascending=True)

    print(df_luodi_qingguan_sort_BD['落地-清关用时_BD'].describe(percentiles=[0.9,0.95]))






    print('**************** 清关-交付 : start  ****************')
    df_qingguan_jiaofu['交付时间'] = pd.to_datetime(df_qingguan_jiaofu['交付时间'])
    df_qingguan_jiaofu['进口清关时间'] = pd.to_datetime(df_qingguan_jiaofu['进口清关时间'])

    print('--------- CD计算 ------------')
    df_qingguan_jiaofu["用时"] = (df_qingguan_jiaofu["交付时间"] - df_qingguan_jiaofu["进口清关时间"]).astype('timedelta64[s]')
    df_qingguan_jiaofu["用时"] = round(df_qingguan_jiaofu["用时"] / 86400, 2)
    df_qingguan_jiaofu_sort_CD = df_qingguan_jiaofu.sort_values(by=["用时"], ascending=True)
    print(df_qingguan_jiaofu_sort_CD['用时'].describe(percentiles=[0.9, 0.95]))

    print('--------- BD计算 ------------')
    df_qingguan_jiaofu['清关-交付用时_BD'] = df_qingguan_jiaofu.apply(lambda x: forMX(x['进口清关时间'], x['交付时间']), axis=1)
    df_qingguan_jiaofu_sort_BD = df_qingguan_jiaofu.sort_values(by=["清关-交付用时_BD"], ascending=True)
    print(df_qingguan_jiaofu_sort_BD['清关-交付用时_BD'].describe(percentiles=[0.9, 0.95]))



    print('**************** 交付-妥投 : start  ****************')
    df_jiaofu_tuotou['交付时间'] = pd.to_datetime(df_jiaofu_tuotou['交付时间'])
    df_jiaofu_tuotou['妥投时间'] = pd.to_datetime(df_jiaofu_tuotou['妥投时间'])

    print('--------- CD计算 ------------')
    df_jiaofu_tuotou["用时"] = (df_jiaofu_tuotou["妥投时间"] - df_jiaofu_tuotou["交付时间"]).astype('timedelta64[s]')
    df_jiaofu_tuotou["用时"] = round(df_jiaofu_tuotou["用时"] / 86400, 2)
    df_jiaofu_tuotou_CD = df_jiaofu_tuotou.sort_values(by=["用时"], ascending=True)
    print(df_jiaofu_tuotou_CD['用时'].describe(percentiles=[0.9, 0.95]))

    print('--------- BD计算 ------------')
    df_jiaofu_tuotou['交付-妥投用时_BD'] = df_jiaofu_tuotou.apply(lambda x: forMX(x['交付时间'], x['妥投时间']), axis=1)
    df_jiaofu_tuotou_BD = df_jiaofu_tuotou.sort_values(by=["交付-妥投用时_BD"], ascending=True)
    print(df_jiaofu_tuotou_BD['交付-妥投用时_BD'].describe(percentiles=[0.9, 0.95]))



    print('**************** 首扫-妥投 : start  ****************')
    # 交付-妥投
    df_shousao_tuotou['首扫时间'] = pd.to_datetime(df_shousao_tuotou['首扫时间'])
    df_shousao_tuotou['妥投时间'] = pd.to_datetime(df_shousao_tuotou['妥投时间'])

    print('--------- CD计算 ---------')
    df_shousao_tuotou["用时"] = (df_shousao_tuotou["妥投时间"] - df_shousao_tuotou["首扫时间"]).astype('timedelta64[s]')
    df_shousao_tuotou["用时"] = round(df_shousao_tuotou["用时"] / 86400, 2)
    df_shousao_tuotou_CD = df_shousao_tuotou.sort_values(by=["用时"], ascending=True)
    print(df_shousao_tuotou_CD['用时'].describe(percentiles=[0.9, 0.95]))

    print('--------- BD计算 ------------')
    df_shousao_tuotou['首扫-妥投用时_BD'] = df_shousao_tuotou.apply(lambda x: forMX(x['首扫时间'], x['妥投时间']), axis=1)
    df_shousao_tuotou_BD = df_shousao_tuotou.sort_values(by=["首扫-妥投用时_BD"], ascending=True)
    print(df_shousao_tuotou_BD['首扫-妥投用时_BD'].describe(percentiles=[0.9, 0.95]))



def getAllFilePath():
    file_dir = r'C:\Users\hp\Desktop\cujia-11data'
    for root, dirs, files in os.walk(file_dir):
        if  len(dirs)> 0  :
            pass
        else:
            print('================= DES 分割线 ======================')
            zhmodel = re.compile(u'[\u4e00-\u9fa5]')  # 检查中文
            # zhmodel = re.compile(u'[^\u4e00-\u9fa5]')  #检查非中文
            match = zhmodel.search(root)  # 如果有中文，则说说明该目录是后来手动添加的，不需要处理（已经处理过了）
            if match:
                pass
            else:
                print('root_dir:', root)  # 当前目录路径
                os.chdir(root)
                ## 切换路径后 获得所有的 时效
                getAllShiXiao()




if __name__ == '__main__':

    # 获得所有路径
    getAllFilePath()
    # 获得所有分段时效
    # getAllShiXiao()



