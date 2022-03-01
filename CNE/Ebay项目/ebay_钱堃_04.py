#coding=utf-8

import pandas as pd
import numpy as np
import openpyxl
import pymysql
import datetime,time

from dateutil.parser import parse
from business_duration import businessDuration
from dateutil import rrule

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
                if period>12:period=12
                period = round(period/24 ,2 )
                return period

        # case02:开始时间和结束时间不是在同一天
        else:

            # 判断起始时间是否在周六且在周六12点前，如果是 则 需要考虑起始时间 至 周六 12点前的时间
            if (received_time.weekday()== 5) : # 周六
                date_compare = received_time.strftime('%Y-%m-%d')  + " 12:00:00"
                sub_time_01_seconds = (parse(date_compare) - received_time).total_seconds()
                sub_time_01_hours = round(sub_time_01_seconds/(3600),2)
                period = period + sub_time_01_hours

            # 判断结束时间是否在周六且在周六12点前，如果是 则 需要考虑周六 00:00:00 至 周六的时间
            # 结束时间 只有两种情况，case01: 在 周一至周五 ； case02:周六 12点前。
            if (complete_time.weekday()== 5) : # 周六
                date_compare = complete_time.strftime('%Y-%m-%d')  + " 00:00:00"
                sub_time_02_seconds = ( complete_time - parse(date_compare)).total_seconds()
                sub_time_02_hours = round(sub_time_02_seconds/(3600),2)
                if(sub_time_02_hours>12):sub_time_02_hours=12
                period = period + sub_time_02_hours
            # 判断在 (开始时间, 结束时间) 之间有几个周六, 有x个周六，则需要在总时间之上再加上 x个半天
            days = complete_time - received_time
            count=0
            while(received_time.strftime('%Y-%m-%d') <= (complete_time+datetime.timedelta(days=-1)).strftime('%Y-%m-%d')):
                received_time=received_time + datetime.timedelta(days=1)
                if (received_time).weekday() == 5:
                    count+=1
            period = period + 12 * count
        period = round(period/24 ,2 )
        return  period

if __name__ == '__main__':
    pass



