import datetime
class workDays():
    def __init__(self, start_date, end_date, days_off=None):
        self.start_date = start_date
        self.end_date = end_date
        self.days_off = days_off
        if self.start_date > self.end_date:
            self.start_date,self.end_date = self.end_date, self.start_date
        if days_off is None:
            self.days_off = 6
        self.days_work = [x for x in range(7) if x != self.days_off]

    def workDays(self):
        tag_date = self.start_date
        while True:
            if tag_date > self.end_date:
                break
            if tag_date.weekday() in self.days_work:
                yield tag_date
            tag_date += datetime.timedelta(hours=1)

    def daysCount(self):
        return len(list(self.workDays()))

    def weeksCount(self, day_start=0):
        day_nextweek = self.start_date
        while True:
            if day_nextweek.weekday() == day_start:
                break
            day_nextweek += datetime.timedelta(days=1)
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
    import datetime

    startdate = datetime.datetime(2018, 1, 11, 20, 20, 20)
    enddate = datetime.datetime(2019, 1, 11, 20, 20, 20)
    work = workDays(startdate, enddate)  # 需要传入两个datetime格式日期
    print(list(work.workDays()))  # 获取一个元素为datetime日期格式的工作日期列表
    print(work.daysCount())  # 获取工作日期的天数
    print(work.weeksCount())  # 获取非工作日的天数
    for i in work.workDays():  # 获取每一个工作日期
        print(i)
    print(dir(work))  # 获取work的所有方法
