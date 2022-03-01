#coding=utf-8

import pandas as pd

import pymysql
con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677", charset="utf8",
                      autocommit=True, database='logisticscore')
cur = con.cursor()


def getIossCode(trackCode):

    SQL = "select ioss_code from lg_order where order_no = '{}'".format(trackCode)
    # print(SQL)
    cur.execute(SQL)
    data = cur.fetchone()
    return data[0]
    # print(data)


if __name__ == '__main__':
    filePath = r'C:\Users\hp\Desktop\CNE九月欧盟数据.xlsx'
    df = pd.read_excel(filePath)
    df['ioss_code'] = None
    for i in range(df.shape[0]):
        orderNo = df.iloc[i, 0]
        # print(orderNo)
        iossCode = getIossCode(orderNo)
        df.iloc[i, 1] = iossCode

    df.to_csv(r'C:\Users\hp\Desktop\CNE九月欧盟数据_结果.csv', index=False, encoding="utf_8_sig")
    print(df.head(10))

