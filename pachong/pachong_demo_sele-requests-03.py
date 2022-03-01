# coding:utf-8
# 用webdriver登录并获取cookies，并用requests发送请求，以豆瓣为例
from selenium import webdriver
import requests
import time
import re
from pandas import DataFrame,Series


def main():
    # 从命令行参数获取登录用户名和密码
    user_name = '13505256139'
    password  = '13505256139'

    # 豆瓣登录页面URL
    login_url = 'https://accounts.douban.com/passport/login_popup?login_source=anony'

    # 获取chrome的配置
    opt = webdriver.ChromeOptions()
    # 在运行的时候不弹出浏览器窗口
    # opt.set_headless()

    # 获取driver对象
    driver = webdriver.Chrome(chrome_options=opt)
    # 打开登录页面
    driver.get(login_url)
    print ('opened login page...')
    driver.find_element_by_xpath('/html/body/div[1]/div[1]/ul[1]/li[2]').click()
    time.sleep(2)
    # 向浏览器发送用户名、密码，并点击登录按钮
    driver.find_element_by_xpath('//*[@id="username"]').send_keys(user_name)
    time.sleep(2)
    driver.find_element_by_xpath('//*[@id="password"]').send_keys(password)
    time.sleep(2)
    # 'account-tab-account'
    # 多次登录需要输入验证码，这里给一个手工输入验证码的时间
    driver.find_element_by_xpath('/html/body/div[1]/div[2]/div[1]/div[5]/a').click()
    print ('submited...')
    # 等待2秒钟
    time.sleep(2)

    # 创建一个requests session对象
    s = requests.Session()

    # 从driver中获取cookie列表（是一个列表，列表的每个元素都是一个字典）
    c = driver.get_cookies()
    cookies = {}
    print('****************************')
    # 把cookies设置到session中
    for cookie in c:
        cookies[cookie['name']] = cookie['value']
    # 关闭driver
    driver.quit()

    # 需要登录才能看到的页面URL
    page_url = 'https://www.douban.com/people/215786800/'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36'
    }

    # 获取该页面的HTML
    resp = requests.get(page_url, headers=headers,cookies=cookies )
    # resp.encoding = 'utf-8'
    print ('status_code = {0}'.format(resp.status_code))
    # print(resp.text)

    df = DataFrame()
    columns = ['我的广播']

    cons = re.findall('class="status-text">(.*?)</span>', resp.text, re.S)
    if cons:
        for each in cons:
            data = []
            data.append(each.rstrip("\n"))
            ser = Series(data, index=columns)
            df = df.append(ser, ignore_index=True)

    # # 将网页内容存入文件
    # with open('what.html', 'wb') as  fout:
    #     fout.write(resp.content)
    print(df)
    print ('------------------ end ----------------------')


if __name__ == '__main__':
    main()