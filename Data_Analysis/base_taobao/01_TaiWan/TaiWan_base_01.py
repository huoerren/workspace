#coding=utf-8


import pymysql
import pandas as pd


def getDataExisted(dataFromPage):
    data_For_Compare = []
    if len(dataFromPage) >0:
        sql = 'select dian_id , dian_name , dian_comment  from dianpu where dian_name like "%' + dataFromPage[0] + '%"'
    else:
        sql = 'select dian_id , dian_name , dian_comment  from dianpu '

    conn = pymysql.connect(host='127.0.0.1', user='root', password='panxin', port=3306, db='test', charset='utf8')
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        columnDes = cursor.description  # 获取连接对象的描述信息    columnNames = [columnDes[i][0] for i in range(len(columnDes))]    df = pd.DataFrame([list(i) for i in data],columns=columnNames)    return df
        columnNames = [columnDes[i][0] for i in range(len(columnDes))]
        df = pd.DataFrame([list(i) for i in result], columns=columnNames)
        return df

        # for i in result:
        #     data_For_Compare.append(list(i))
        # return data_For_Compare

    except Exception as e:
        print(str(e))
    finally:
        cursor.close()
        conn.close()




if __name__ == '__main__':
    res = getDataExisted('海鲜')
    # print(res.columns) # ['dian_id', 'dian_name', 'dian_comment']
    print(res['dian_name'].value_counts())
    print(res.shape)
    # print(len(res))



