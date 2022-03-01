#coding=utf-8



import pandas as pd
import datetime
import time


# 判断本年度 01月01号是不是在为第1周，如果是则返回 1 ，如果不是 返回 0
# def pDate():
#     if pd.to_datetime('2021-01-01').isocalendar()[1] == 1:
#         return 1
#     else:
#         return 0
#
# flag = pDate()
# data = {"班级":['一班','三班','八班'] ,"日期":['2021-01-01','2021-01-06' ,'2021-02-01']  }
# df = pd.DataFrame(data)
# df['日期'] = pd.to_datetime(df['日期'])
# df['day'] = df['日期'].dt.day.astype('str')
# df['计算列'] = df['日期'].apply(lambda x: x.isocalendar()[0])
# df['周序数'] = df['日期'].apply(lambda x: x.isocalendar()[1]+1 if flag == 0 else x.isocalendar()[1])
# print('------------------ 修改前的 ----------------')
# print(df)
#
# for i in df.index:
#     if df.loc[i,'计算列'] == 2020:
#         df.loc[i , '周序数' ] = 1
# print('---------------- 修改后的： -----------------')
# print(df)

# from dateutil.parser import parse
# a = parse('2017-10-01 12:12:12')
# b = parse('2017-10-04 14:12:12')
#
# print((b-a).days)
# print((b-a).seconds)
# print((b-a).total_seconds())
# # print(( b-a).minutes)


from datetime import datetime

# x = datetime.now()
# d = x.isocalendar()
# print("Original date:", x)
# print("Today's date in isocalendar is:", d)
# print('-------------------------------------------------------')

# x = datetime(2004, 10, 30, 12, 12, 12)
# d = x.isocalendar()
# dd = x.date()
# print("Date : ", str(dd), " ; in isocalendar is : ", d)
# print(d[0] , d[1] , d[2])
# print('-------------------------------------------------------')

# y = datetime(2021, 1, 1, 10, 3, 33)
# f  = y.isocalendar()
# ff = y.date()
# print(str(ff) , f)
# print(f[0] , f[1] , f[2]  )

data = {"班级":['xiaoming','shenme','rushang','sida','sfsdf','dfswew','xar'],
        "年假":[4,8,6,5,3,2,9],
        "嗲":['shen','me','shen','me','shen','me','shen'],
        "发":[4,5,8,9,6,5,9] }

df = pd.DataFrame(data)
print(df)
print('-----------------------------------')


shenm = list(zip(df['年假'] , df['班级']))
shenm.sort(key=lambda x: x[0])
res = []
for i in shenm:
        res.append(i[1])
print(res)




import matplotlib as mpl
import matplotlib.pyplot as plt

plt.plot()
plt.show()




































