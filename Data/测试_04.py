from business_duration import businessDuration
from dateutil import rrule
import pandas as pd
import datetime



# 'IT'   # 周一到周五
def forIT():
    received_time = pd.to_datetime('2022-01-06 00:00:00')
    complete_time = pd.to_datetime('2022-01-14 13:00:00')
    period = businessDuration(received_time, complete_time, unit='min')
    print(round(period/(60*24) , 2))



def forMX(): # 周一到周六
    received_time = datetime.datetime.strptime('2022-01-06 00:00:00', '%Y-%m-%d %H:%M:%S')
    complete_time = datetime.datetime.strptime('2022-01-14 13:00:00', '%Y-%m-%d %H:%M:%S')
    workdays = [x for x in range(7) if x not in [6]]
    time_period = rrule.rrule(rrule.MINUTELY, dtstart=received_time, until=complete_time, byweekday=workdays).count()
    print(round(time_period / (60 * 24), 2))

def forMX_02(): # 周一到周五及周六上午半天
    received_time = pd.to_datetime('2022-01-08 11:00:00')
    complete_time = pd.to_datetime('2022-01-14 13:00:00')

    print(type(complete_time) , received_time ==""  ,  received_time != None )



    # period = businessDuration(received_time, complete_time, unit='min')
    # print(period)


if __name__ == '__main__':
    # forIT()
    forMX_02()