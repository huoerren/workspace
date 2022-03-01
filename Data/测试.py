#coding=utf-8
import pandas as pd
import random


# data = {"班级":['xiaoming','xiaohua','xiaoming','xiaoming','xiaohong','xiaohong','xiaohua'],
#             "年假":[4,8,6,5,3,2,9],
#             "嗲":['shen','me','shen','me','shen','me','shen'],
#             "发":[4,5,8,9,6,5,9] }
#
# df = pd.DataFrame(data)
# print(df)
# print('-----------------------------------')
# d4 = df.groupby(['班级','嗲'], sort=False)['年假'].count().reset_index()
# print(d4)
# d5 = df.groupby(['班级','嗲'], sort=False)['年假'].sum().reset_index()
# print(d5)

# import copy
# list = [20, 16, 10, 5]
# print( list)
# list2 = copy.deepcopy(list)
# list = [ 16, 10, 5,20 ]
# random.shuffle(list)
# print ("随机排序列表 : ",   list)
# print( list2 )

# nioha = pd.DataFrame({ "n":[2,3,4,6,5,7,9,8,7,7,4,5,6,4] ,"g":[2,3,4,6,5,7,9,8,7,7,4,5,6,4]  ,"h":[2,3,4,6,5,7,9,8,7,7,4,5,6,4]  })
# print(nioha)
# print('-----------------')
# print(nioha['n'].unique())
# print('=================')
# print(nioha['n'].nunique())
#
# df = pd.DataFrame({"sid":[1,2,3,4,5,6,7,8 ] ,
#                             "t_column":['2022-01-01 23:56:51','2021-12-01 09:56:51','2022-01-15 12:30:51','2022-01-01 23:56:51',
#                                     '2021-06-30 16:56:51','2021-09-01 18:56:51','2022-11-01 05:56:51','2022-02-01 23:56:51'] ,
#                             "name":[ 'xiaohua','xiaogang','xiaoming','xiaohai','xiaoqian','xiaozhang','xiaopan','xiaogou' ]}  )

# print(shenme.columns.tolist())

# print(df.head(4))
# print('--------------------------------------')
# df['t_column'] = pd.to_datetime(df['t_column'])

#日期的筛选
# df_sub = df[(df['t_column']>'2021-12-25') & (df['t_column']< '2022-01-15') ]
# print(df_sub)
#
# df['time'] = df['t_column'].apply(lambda x:x.split(' ')[1] )
# df_sub_time = df[(df['time']> '12:30:51') & (df['time'] < '16:59:25')]
# print(df_sub_time)


# 计算节假日


def getDateIn(startTime ,endTime ):

    pass


import calendar
def getMonthdays(yeartemp):
    c = calendar.TextCalendar()
    year = "year={"
    for ii in range(1, 13):
        message = ""
        message = message + calendar.month_abbr[ii] + "=["
        for week in c.monthdayscalendar(yeartemp, ii):
            for i in range(0, 5):
                if week[i] != 0:
                    if week[i] < 10:
                        message = message + "'0" + str(week[i]) + "',"
                    else:
                        message = message + "'" + str(week[i]) + "',"
        print (message[:-1] + "]")

    year = year + "'" + calendar.month_abbr[ii] + "':" + calendar.month_abbr[ii] + ","
    print (year[:-1] + "}")


import datetime
def nihao():
    # print([x for x in range(7) if x not in  [4,5,6]])
    print(datetime.datetime.strptime('2022-01-02','%Y-%m-%d') .weekday())

import datetime

if __name__ == '__main__':

    # startTime = '2022-01-02 12:58:12'
    # endTime   = '2022-01-15 01:32:10'

    # shenme = datetime.datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S')
    # a= shenme + datetime.timedelta(days=1)
    # print(a )



    # getDateIn(startTime ,endTime )

    # getMonthdays(2022)

    # 遍历两个日期之间的每一天

    nihao()




