import datetime


# 计算两个日期之间的工作日数,非天数.
class workDays():
    def __init__(self, start_date, end_date, days_off=None):
        """days_off:休息日,默认周六日, 以0(星期一)开始,到6(星期天)结束, 传入tupple
        没有包含法定节假日,
        """
        self.start_date = start_date
        self.end_date = end_date
        self.days_off = days_off

        if self.start_date > self.end_date:
            self.start_date,self.end_date = self.end_date, self.start_date
        if days_off is None:
            self.days_off = 5,6
        # 每周工作日列表
        self.days_work = [x for x in range(7) if x not in self.days_off]
        print(self.days_work)

    def workDays(self):
        """实现工作日的 iter, 从start_date 到 end_date , 如果在工作日内,yield 日期
        """
        # 还没排除法定节假日
        tag_date = self.start_date
        while True:
            if tag_date > self.end_date:
                break
            if tag_date.weekday() in self.days_work:
                yield tag_date
            tag_date += datetime.timedelta(days=1)

    def daysCount(self):
        """工作日统计,返回数字"""
        return len(list(self.workDays()))


    def weeksCount(self, day_start=0):
        """统计所有跨越的周数,返回数字
        默认周从星期一开始计算
        """
        day_nextweek = self.start_date
        while True:
            if day_nextweek.weekday() == day_start:  # day_nextweek.weekday() 获得某天是星期几
                break
            day_nextweek += datetime.timedelta(days=1)
        print(day_nextweek)
        # 区间在一周内
        if day_nextweek > self.end_date:
            return 1
        weeks = ((self.end_date - day_nextweek).days + 1)/7
        weeks = int(weeks)
        if ((self.end_date - day_nextweek).days + 1)%7:
            weeks += 1
        if self.start_date < day_nextweek:
            weeks += 1
        return weeks

if __name__ == '__main__':

    startdate = datetime.datetime.strptime('2022-01-15 12:15:29', '%Y-%m-%d %H:%M:%S')
    enddate   = datetime.datetime.strptime('2022-01-19 10:45:18', '%Y-%m-%d %H:%M:%S')

    print(startdate , enddate)

    work = workDays(start_date = startdate,end_date = enddate)  # 需要传入两个datetime格式日期
    print(work.weeksCount())  # 获取非工作日的天数
    total_times = enddate - startdate
    total_days = round (total_times.total_seconds()/86400 , 2)
    real_days = total_days - work.weeksCount()

    # 真正的 BD时效：
    print(real_days)

