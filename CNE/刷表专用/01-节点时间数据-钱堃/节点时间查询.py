import tkinter as tk
from tkinter import filedialog, dialog
import os
import pandas as pd
import pymysql
from tkinter import *
import numpy as np

root = tk.Tk()
root.title('节点时间导出')
root.geometry('510x100')

con = pymysql.connect(host="139.198.189.25", port=44000, user="cnereader", passwd="read51096677",
                                  charset="utf8", autocommit=True, database='logisticscore')
cur = con.cursor()

def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df

def judge(a,b,c):
    flag1 = pd.isnull(a)
    flag2 = pd.isnull(b)
    flag3 = pd.isnull(c)
    if bool(1-flag1):
        return a
    elif bool(1-flag2):
        return b
    else:
        return c

def judge1(a,b):
    flag=pd.isnull(a)
    if flag:
        return b
    else:
        return a

def open_file():
    global file_path
    global file_text
    file_path = filedialog.askopenfilename(title=u'选择文件', initialdir=(os.path.expanduser('H:/')))
    if file_path is not None:
        df=pd.read_excel(file_path)
        try:
            list=[]
            new_df=pd.DataFrame()
            new_df['内单号']=df['内单号']
            read_tuple = tuple(df['内单号'].tolist())

            if (is_checked4.get()):
                #首扫
                S4 = """
                    SELECT 
                    order_no 内单号,
                    date_add(gmt_create,interval 8 hour) 首扫时间
                    FROM
                    lg_order lgo
                    where
                    lgo.order_no in {}
                    and lgo.is_deleted='n'
                    """.format(read_tuple)
                d4 = execude_sql(S4)
                df = pd.merge(df, d4, on=['内单号'], how='left')
            if (is_checked1.get()):
                #封袋
                S2 = """
                    SELECT 
                    order_no 内单号,
                    date_add(sealing_bag_time,interval 8 hour) 封袋时间
                    FROM
                    lg_order lgo,lg_bag_order_relation lbor,lg_bag lgb
                    where
                    lgo.order_no in {}
                    and lbor.bag_id=lgb.id
                    and lbor.order_id=lgo.id
                    and lbor.is_deleted='n'
                    and lgo.is_deleted='n'
                    and lgb.is_deleted='n'
                    """.format(read_tuple)
                d2 = execude_sql(S2)
                df = pd.merge(df, d2, on=['内单号'], how='left')
            if(is_checked.get()):
                #装车
                S1 = """
                SELECT 
                order_no 内单号,
                date_add(event_time,interval 8 hour) 装车时间
                FROM
                lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
                where
                lgo.order_no in {}
                and lbor.order_id=lgo.id
                and lbor.bag_id=tbe.bag_id
                
                and tbe.event_code='DEPS'
                
                and lgo.is_deleted='n'
                and lbor.is_deleted='n'
                and tbe.is_deleted='n'
                
                """.format(read_tuple)
                d1=execude_sql(S1)
                df=pd.merge(df,d1,on=['内单号'],how='left')
            if (is_checked5.get()):
                #起飞
                S5 = """
                    SELECT 
                    order_no 内单号,
                    date_add(event_time,interval 8 hour) 起飞时间
                    FROM
                    lg_order lgo,track_mawb_event tme
                    where
                    lgo.order_no in {}
                    and lgo.is_deleted='n'
                    and lgo.mawb_id=tme.mawb_id
                    and event_code in ("SDFO","DEPC","DEPT","LKJC")
                    """.format(read_tuple)
                d5 = execude_sql(S5)
                df = pd.merge(df, d5, on=['内单号'], how='left')
            if (is_checked6.get()):
                #落地
                S61 = """
                    SELECT 
                    order_no 内单号,
                    date_add(event_time,interval 8 hour) 主单落地时间
                    FROM
                    lg_order lgo,track_mawb_event tme
                    where
                    lgo.order_no in {}
                    and lgo.mawb_id=tme.mawb_id
                    and lgo.is_deleted='n'
                    and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
                """.format(read_tuple)
                S62 = """
                    SELECT 
                    order_no 内单号,
                    date_add(event_time,interval 8 hour) 包裹落地时间
                    FROM
                    lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
                    where
                    lgo.order_no in {}
                    and lbor.order_id=lgo.id
                    and lbor.bag_id=tbe.bag_id
                    
                    and lgo.is_deleted='n'
                    and lbor.is_deleted='n'
                    and tbe.is_deleted='n'
                    and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
                """.format(read_tuple)
                S63 = """
                    SELECT 
                    order_no 内单号,
                    date_add(event_time,interval 8 hour) 正式单落地时间
                    FROM
                    lg_order lgo,track_order_event toe
                    where
                    lgo.order_no in {}
                    and toe.order_id=lgo.id
                    and lgo.is_deleted='n'
                    and toe.is_deleted='n'
                    and event_code in ("ARIR","ABCD","ABAD","AECD","ARMA")
                """.format(read_tuple)
                d61 = execude_sql(S61)
                d62 = execude_sql(S62)
                d63 = execude_sql(S63)
                d6 = pd.DataFrame()
                d6['内单号'] = df['内单号']
                d6 = pd.merge(d6, d61, on=['内单号'], how='outer')
                d6 = pd.merge(d6, d62, on=['内单号'], how='outer')
                d6 = pd.merge(d6, d63, on=['内单号'], how='outer')
                d6['落地时间'] = d6.apply(lambda x: judge(x['主单落地时间'], x['包裹落地时间'], x['正式单落地时间']), axis=1)
                df = pd.merge(df, d6[['内单号','落地时间']], on=['内单号'], how='left')
            if (is_checked7.get()):
                #清关
                S71 = """
                    SELECT 
                    order_no 内单号,
                    date_add(event_time,interval 8 hour) 主单清关时间
                    FROM
                    lg_order lgo,track_mawb_event tme
                    where
                    lgo.order_no in{}
                    and tme.mawb_id=lgo.mawb_id
                    and lgo.is_deleted='n'
                    and tme.is_deleted='n'
                    and event_code in ("IRCM","PVCS","IRCN","RFIC")
                    """.format(read_tuple)
                d71 = execude_sql(S71)
                S72 = """
                    SELECT 
                    order_no 内单号,
                    date_add(event_time,interval 8 hour) 包裹清关时间
                    FROM
                    lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
                    where
                    lgo.order_no in {}
                    and lbor.order_id=lgo.id
                    and lbor.bag_id=tbe.bag_id
                    and lbor.is_deleted='n'
                    and lgo.is_deleted='n'
                    and tbe.is_deleted='n'
                    and event_code in ("IRCM","PVCS","IRCN","RFIC")
                """.format(read_tuple)
                d72 = execude_sql(S72)
                S73 = """
                    SELECT 
                    order_no 内单号,
                    date_add(event_time,interval 8 hour) 正式单清关时间
                    FROM
                    lg_order lgo,track_order_event toe
                    where
                    lgo.order_no in {}
                    and toe.order_id=lgo.id
                    and toe.is_deleted='n'
                    and lgo.is_deleted='n'
                    and event_code in ("IRCM","PVCS","IRCN","RFIC")
                """.format(read_tuple)
                d73 = execude_sql(S73)
                d7 = pd.DataFrame()
                d7['内单号'] = df['内单号']
                d7 = pd.merge(d7, d71, on=['内单号'], how='outer')
                d7 = pd.merge(d7, d72, on=['内单号'], how='outer')
                d7 = pd.merge(d7, d73, on=['内单号'], how='outer')
                d7['清关时间'] = d7.apply(lambda x: judge(x['主单清关时间'], x['包裹清关时间'],x['正式单清关时间']), axis=1)
                df = pd.merge(df, d7[['内单号', '清关时间']], on=['内单号'], how='left')

                # df = pd.merge(df, d7, on=['内单号'], how='left')
            if (is_checked2.get()):
                #交付
                S3 = """
                    SELECT 
                    order_no 内单号,
                    date_add(event_time,interval 8 hour) 交付时间
                    FROM
                    lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
                    where
                    lgo.order_no in{}
                    
                    and lbor.order_id=lgo.id
                    and lbor.bag_id=tbe.bag_id
                    
                    and lbor.is_deleted='n'
                    and lgo.is_deleted='n'
                    and tbe.is_deleted='n'
                    and event_code in ("JFMD")
                    """.format(read_tuple)
                d3 = execude_sql(S3)
                df = pd.merge(df, d3, on=['内单号'], how='left')
            if (is_checked8.get()):
                #妥投
                S8 = """
                    SELECT 
                    order_no 内单号,
                    date_add(delivery_date,interval 8 hour) 妥投时间
                    FROM
                    lg_order lgo
                    where
                    lgo.order_no in {}
                    and lgo.is_deleted='n'
                    """.format(read_tuple)
                d8 = execude_sql(S8)
                df = pd.merge(df, d8, on=['内单号'], how='left')
            if (is_checked9.get()):
                #加装车--全
                S9 = """
                    SELECT 
                    order_no 内单号,
                    date_add(event_time,interval 8 hour) 正常装车时间
                    FROM
                    lg_order lgo,lg_bag_order_relation lbor,track_bag_event tbe
                    where
                    lgo.order_no in {}
                    and lbor.order_id=lgo.id
                    and lbor.bag_id=tbe.bag_id
                    and lgo.is_deleted='n'
                    and lbor.is_deleted='n'
                    and tbe.is_deleted='n'
                    and tbe.event_code='DEPS'
                    """.format(read_tuple)
                d9 = execude_sql(S9)
                S10 = """
                    SELECT 
                    order_no 内单号,
                    date_add(event_time,interval 8 hour) 无包号装车时间
                    FROM
                    lg_order lgo,track_order_event toe
                    where
                    lgo.order_no in {}
                    and toe.order_id=lgo.id
                    and lgo.is_deleted='n'
                    and toe.is_deleted='n'
                    and toe.event_code in ('LSLD','LKSM')
                    """.format(read_tuple)
                d10 = execude_sql(S10)
                d11 = pd.DataFrame()
                d11['内单号']=df['内单号']
                d11 = pd.merge(d11, d10, on=['内单号'], how='outer')
                d11 = pd.merge(d11, d9, on=['内单号'], how='outer')
                d11['装车时间-全']=d11.apply(lambda x:judge1(x['正常装车时间'],x['无包号装车时间']),axis=1)
                df = pd.merge(df, d11[['内单号','装车时间-全']], on=['内单号'], how='left')
            df.to_excel(file_path[:-5]+'节点时间.xlsx',index=False)
        except:
            pass


'''窗体控件'''
# 标题显示
lab = tk.Label(root, text='选择节点：')
lab.grid(row=0, columnspan=3, sticky=tk.W)

# 多选框
frm = tk.Frame(root)
is_checked = BooleanVar()
is_checked1 = BooleanVar()
is_checked2 = BooleanVar()
is_checked4 = BooleanVar()
is_checked5 = BooleanVar()
is_checked6 = BooleanVar()
is_checked7 = BooleanVar()
is_checked8 = BooleanVar()
is_checked9 = BooleanVar()
ck4 = tk.Checkbutton(frm, text='首扫',variable=is_checked4)
ck1 = tk.Checkbutton(frm, text='装车',variable=is_checked)
ck2 = tk.Checkbutton(frm, text='封袋',variable=is_checked1)
ck5 = tk.Checkbutton(frm, text='起飞',variable=is_checked5)
ck6 = tk.Checkbutton(frm, text='落地',variable=is_checked6)
ck7 = tk.Checkbutton(frm, text='清关',variable=is_checked7)
ck3 = tk.Checkbutton(frm, text='交付',variable=is_checked2)
ck8 = tk.Checkbutton(frm, text='妥投',variable=is_checked8)
ck9 = tk.Checkbutton(frm, text='装车--全',variable=is_checked9)
ck1.grid(row=0, column=2) #3装车
ck2.grid(row=0, column=1) #2封袋
ck3.grid(row=0, column=6) #7交付
ck4.grid(row=0) #1首扫
ck5.grid(row=0, column=3) #4起飞
ck6.grid(row=0, column=4) #5落地
ck7.grid(row=0, column=5) #6清关
ck8.grid(row=0, column=7) #8妥投
ck9.grid(row=0, column=8) #9加装车-全
frm.grid(row=2)

lab_msg = tk.Label(root, text='')
lab_msg.grid(row=2, columnspan=3, sticky=tk.W)

bt1 = tk.Button(root, text='打开文件', width=15, height=2, command=open_file)
bt1.grid(row=3)
# bt1 = tk.Button(root, text='导出文件', width=15, height=2, command=open_file)
# bt1.grid(row=4)

root.mainloop()