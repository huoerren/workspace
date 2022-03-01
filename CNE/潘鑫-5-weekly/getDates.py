#coding=utf-8

import pandas as pd
import datetime
import time

import datetime
# 获取前1天或N天的日期，beforeOfDay=1：前1天；beforeOfDay=N：前N天
def getdate( beforeOfDay):
    today = datetime.datetime.now()
    # 获取想要的日期的时间
    dateList = []
    for i in range(4):
        # 计算偏移量
        offset = datetime.timedelta(days=-(beforeOfDay+i))
        re_date = (today + offset ).strftime('%Y-%m-%d')
        dateList.append(re_date)
    print(dateList)
    return dateList

# # 获取前一周的所有日期(weeks=1)，获取前N周的所有日期(weeks=N)
# def getBeforeWeekDays(self,weeks=1):
#     # 0,1,2,3,4,5,6,分别对应周一到周日
#     week = datetime.datetime.now().weekday()
#     days_list = []
#     start = 7 * weeks +  week
#     end = week
#     for index in range(start, end, -1):
#         day = getdate(index) print(day)

# 获得前4个已经完成的4个周期的业务日期



for i in [7 ,8, 10, 12, 14, 16, 20, 22]:
    print('KPI为{} 天 的前四个周期的业务日期分别如下：'.format(i))
    getdate(i)


















