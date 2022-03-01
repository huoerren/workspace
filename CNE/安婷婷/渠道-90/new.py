import pandas as pd
import openpyxl
import pymysql
import datetime,time
import pyecharts.options as opts
from pyecharts.charts import Line,Grid,Page
from pyecharts.globals import ThemeType
nows=datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
# 目的文件存放地址
file=r'F:\其他部门\分拣路由90分位妥投天数统计'
print(datetime.date.today())
import numpy as np
# #数仓连接
con = pymysql.connect(host="139.198.189.25",port=44000,user="cnereader",passwd="read51096677",charset="utf8",autocommit=True,database='logisticscore')
cur = con.cursor()

# days="BETWEEN '2021-03-31 16:00:00' and '"+time_yes+"'"1
# days="BETWEEN '2021-06-30 16:00:00' and '2021-06-30 16:00:00' "
days="BETWEEN '2021-06-27 16:00:00' and "+"'"+nows+"'"

print(days)

S1 ="""
SELECT 
date_format(date_add(lgo.gmt_create,interval 8 hour),'%Y-%m-%d') 业务日期, 
lgo.channel_code 渠道,
lgo.des 国家, 
lgo.supply_channel_code 尾端配送商,
(case when lgo.order_status=3 then '已妥投' else '其它' end ) 是否妥投,
count(1) 票数,
round(TIMESTAMPDIFF(hour,lgo.gmt_create,lgo.delivery_date)/24,1) 妥投时间间隔
FROM 
lg_order lgo
where
lgo.customer_id in(1151368,1181372,1151370,1181374)
and lgo.platform ='WISH_ONLINE'
and lgo.gmt_create {}
and lgo.is_deleted='n'
and lgo.channel_code in('CNE全球经济','CNE全球特惠','CNE全球优先','CNE全球通挂号','CNE全球通平邮')
group by
1,2,3,4,5,7
""".format(days)


def execude_sql(SQL):
    cur.execute(SQL)
    data = cur.fetchall()
    column = cur.description
    columns=[column[i][0] for i in range(len(column))]
    df=pd.DataFrame([list(i)for i in data],columns=columns)
    return df
d1=execude_sql(S1)
d1['业务日期']=pd.to_datetime(d1['业务日期'])
d1=d1.reset_index()
d1['year']=d1['业务日期'].map(lambda X:X.year)
d1['month']=d1['业务日期'].map(lambda X:X.month)
print(d1['month'])
# 淡旺季
# d1['淡旺季']='20210'#先默认淡季
# print(d1['业务日期'].dt.month)
# print(d1.loc[1,'业务日期'].year)
# print(d1)
# for i in d1.index:
#     # print(d1.loc[ i,'year'])
#     # print(dy.loc[i, 'month'])
#     if d1.loc[ i,'业务日期'].year==2020:
#         d1.loc[i,'淡旺季']='20201'
#     elif d1.loc[i,'业务日期'].month in [1,9,10,11,12]:
#         d1.loc[i, '淡旺季'] = '20211'
#     else:
#         d1['淡旺季']='20210'
def tj(a,b):
    if a==2020:
        return "20201"
    elif b in [1,9,10,11,12]:
        return "20211"
    else:
        return "20210"
d1['淡旺季']=d1.apply(lambda x:tj(x['year'],x['month']),axis=1)
# 添加周序数
# d1['周序数']=d1['业务日期'].dt.isocalendar().week
#这一天是周中的第几天，Monday=0, Sunday=6
# d1['dayofweek']=d1['业务日期'].dt.dayofweek
# print(d1.columns)
# z汇总周的起止日
dw = pd.DataFrame(pd.date_range(start='20210101',end=nows,periods=None,freq='D'),columns=['业务日期'])
dw['业务日期']=pd.to_datetime(dw['业务日期'])



# dw['周序数']=dw['业务日期'].dt.isocalendar().week


# 判断本年度 01月01号是不是在为第1周，如果是则返回 1 ，如果不是 返回 0
def pDate():
    if pd.to_datetime('2021-01-01').isocalendar()[1] == 1:
        return 1
    else:
        return 0

flag = pDate()

dw['计算列'] = dw['业务日期'].apply(lambda x: x.isocalendar()[0])
dw['周序数'] = dw['业务日期'].apply(lambda x: x.isocalendar()[1]+1 if flag == 0 else x.isocalendar()[1])
print('------------------ 修改前的 ----------------')
print(dw.head())

for i in dw.index:
    if dw.loc[i,'计算列'] == 2020:
        dw.loc[i , '周序数' ] = 1
print('---------------- 修改后的： -----------------')
print(dw.head())
print('------------- 注意点1 -----------------')
print(dw.columns.to_list())
del dw['计算列']
print('------------- 注意点2 -----------------')
print(dw.columns.to_list())

#-------------------------- 替换的代码 -by panxin  --- 结束 ------------------------


dw['moon']=dw['业务日期'].dt.month.astype('str')
dw['day']=dw['业务日期'].dt.day.astype('str')
dw['日期']=dw['moon']+'.'+dw['day']
dw_min=dw[['周序数','日期']].drop_duplicates(['周序数'],keep='first')
dw_max=dw[['周序数','日期']].drop_duplicates(['周序数'],keep='last')
dwm=pd.merge(dw_min,dw_max,on=['周序数'],how='outer')
dwm['周期']=dwm['日期_x']+'-'+dwm['日期_y']
print(dwm)
dw=pd.merge(dw,dwm,on=['周序数'],how='left')
# dw['周期']=dwm['周序数'].map(dict(zip(dwm['周序数'],dwm['周期'])))
# # dw=dw.set_index(['周序数','dayofweek']).stack().unstack(['dayofweek',-1])
print(dw)

d2=pd.merge(d1,dw[['业务日期','周序数','周期']],on=['业务日期'],how='left')
print(d2)

# dyw=pd.read_excel(u"F:\\其他部门\\妥投分位源数据往期.xlsx",dtype={'淡旺季': 'str'})
# dyw['淡旺季']=str(dyw['淡旺季'])
# print(type(dyw['淡旺季']))
# print(dyw)
# dyt = pd.concat([d2, dyw])
# dt =d10.append(dwq)
dyt=d2
print(dyt)
dyt['妥投时间间隔']=dyt['妥投时间间隔'].replace(np.nan,60)
# 按周聚合
d3=dyt.groupby(['淡旺季','周序数','周期','渠道', '国家', '尾端配送商', '是否妥投', '妥投时间间隔'])['票数'].sum().reset_index()
d3t=dyt.groupby(['淡旺季','周序数','周期','渠道', '国家',  '是否妥投', '妥投时间间隔'])['票数'].sum().reset_index()
print(d3)
print(d3t)
#总单量
# d4=dyt.groupby(['淡旺季','周序数','周期','渠道', '国家', '尾端配送商']).sum(['票数']).reset_index() # 原来的
d4=dyt.groupby(['淡旺季','周序数','周期','渠道', '国家', '尾端配送商'])['票数'].sum().reset_index() # 现在的
# d4t=dyt.groupby(['淡旺季','周序数','周期','渠道', '国家']).sum(['票数']).reset_index() # 原来的
d4t=dyt.groupby(['淡旺季','周序数','周期','渠道', '国家'])['票数'].sum().reset_index() # 现在的

d4=d4.rename(columns={'票数':'总票数'})
d4t=d4t.rename(columns={'票数':'总票数'})
print(d4)
print(d4t)
# 已妥投
# d4 = d4.sort_values(by=['总票数',],ascending=False)
d5=pd.merge(d3[d3.是否妥投=='已妥投'],d4,on=['淡旺季','周序数','周期','渠道', '国家', '尾端配送商'],how='right').sort_values(['周序数','渠道', '尾端配送商', '国家','妥投时间间隔'])
d5t=pd.merge(d3t[d3t.是否妥投=='已妥投'],d4t,on=['淡旺季','周序数','周期','渠道', '国家'],how='right').sort_values(['周序数','渠道',  '国家', '妥投时间间隔'])
# d5=d5.sort_values(['妥投时间间隔','周序数'])
print(d5)
print(d5t)
d5t['妥投票数']=d5t.groupby(['淡旺季','周序数','周期','渠道', '国家'])['票数'].cumsum()
d5['妥投票数']=d5.groupby(['淡旺季','周序数','周期','渠道', '国家', '尾端配送商'])['票数'].cumsum()
d5['妥投率']=d5['妥投票数']/d5['总票数']
d5t['妥投率']=d5t['妥投票数']/d5t['总票数']
d5.loc[d5[d5['妥投率']>=0.9].index,'是否达标']=1
d5t.loc[d5t[d5t['妥投率']>=0.9].index,'是否达标']=1
print(d5)
print(d5t)
# 达标的取其中妥投率最小 值
d6=d5[d5['是否达标']==1].copy(deep=True)
d6t=d5t[d5t['是否达标']==1].copy(deep=True)
d6=d6.drop_duplicates(['淡旺季', '周序数', '渠道', '国家', '尾端配送商','是否达标','总票数'],keep='first' ,)
d6t=d6t.drop_duplicates(['淡旺季', '周序数', '渠道', '国家','是否达标','总票数'],keep='first' ,)
print(d6)
print(d6t)
# 未达标的取其中妥投率最大 值
d7=d5[d5['是否达标'].isnull()==True].copy(deep=True)
d7t=d5t[d5t['是否达标'].isnull()==True].copy(deep=True)
d7=d7.drop_duplicates(['淡旺季', '周序数', '渠道', '国家', '尾端配送商','是否达标'],keep='last' ,)
d7t=d7t.drop_duplicates(['淡旺季', '周序数', '渠道', '国家', '是否达标'],keep='last' ,)
# print(d6.columns==d5.columns)
print(d7)
print(d7t)
# 以上交叉合并
# 1\取唯一目录
d8=d6.append(d7)
d8t=d6t.append(d7t)
d8=d8.drop_duplicates(['淡旺季', '周序数', '渠道', '国家', '尾端配送商'],keep='first').reset_index(drop=True)
d8t=d8t.drop_duplicates(['淡旺季', '周序数', '渠道', '国家'],keep='first').reset_index(drop=True)
d8.loc[d8['是否达标'].isnull()==True,'妥投时间间隔']=60
d8t.loc[d8t['是否达标'].isnull()==True,'妥投时间间隔']=60
print(d8)
print(d8t)
# 导入kpi
dk=pd.read_excel(r'D:\PBI\BI\Wish-4PL-SLA-1134.xlsx',sheet_name='KPI调整表',
                 dtype=dict(zip(['TTD（TDD)','年月'],['int','str',])))
dk=dk.rename(columns={'国家简码':'国家','国家':'国家1','年月':'淡旺季','物流渠道':'渠道','TTD（TDD)':'KPI'})
dk=dk.reindex(columns=['国家','淡旺季','渠道','KPI','期望D'])
# print(dk.columns)
dk.info()
d9=pd.merge(d8,dk,on=['淡旺季','渠道', '国家'],how='left')
d9t=pd.merge(d8t,dk,on=['淡旺季','渠道', '国家'],how='left')
d9=d9.sort_values(['渠道','总票数'],ascending=False)
d9t=d9t.sort_values(['渠道','总票数'],ascending=False)
d9=d9.sort_values(['国家','尾端配送商', '周序数'])
d9t=d9t.sort_values(['国家', '周序数'])
print(d9)
print(d9t)
d10=d9[['渠道', '国家', '尾端配送商',  '妥投时间间隔', '周序数','周期', '妥投率','KPI','总票数','期望D']].copy(deep=True)
d10t=d9t[['渠道', '国家',  '妥投时间间隔', '周序数','周期', '妥投率','KPI','总票数','期望D']].copy(deep=True)
print(d10)
print(d10t)
d10['主题']=d10['渠道']+'-'+d10['国家']+'-'+d10['尾端配送商']
d10t['主题']=d10t['渠道']+'-'+d10t['国家']
print(d10)
print(d10t)
# d10['主题']=d10['国家']+'-'+d10['尾端配送商']

# d10['分类']=d10['渠道']+'-'+d10['国家']
# d10 = d10.dropna(axis=0,how='any')
# d10t = d10t.dropna(axis=0,how='any')
pd.options.display.max_columns = None # print(d10[d10['主题']=='CNE全球优先-DE-快捷_DEDHL'])
print(d10)
print(d10t)
# d10 = d10.sort_values(by=['总票数'],ascending=False)
# d10 = d10.sort_values(by=['周序数'],ascending=True)
# print(d10)
# bf = r'F:\其他部门\渠道妥投天数统计.xlsx'
# writer = pd.ExcelWriter(bf)
# d10.to_excel(writer, '数据')
# dwq = pd.read_excel(u"F:\\其他部门\\渠道妥投天数统计 -往期.xlsx")
# print(dwq)
# dt = pd.concat([d10, dwq])
# # dt =d10.append(dwq)
# print(dt)
# dt = dt.dropna(axis=0,how='any')
# pd.options.display.max_columns = None # print(d10[d10['主题']=='CNE全球优先-DE-快捷_DEDHL'])
# print(dt)


# 作图*-----------------
# 类（共多少个图）
def data_ca(qd):
    dca =d10[d10['渠道']==qd]
    dca=dca[['国家', '尾端配送商','主题']].drop_duplicates(subset=None,keep='last',).reset_index(drop=True)
    return dca

# dca=d10[['渠道', '国家', '分类']].drop_duplicates(subset=None,keep='last' ,).reset_index(drop=True)
# print(dca)

#
# print(dca)
# def data_z(fl):
#     df=d10.loc[d10['分类']==fl,['渠道', '国家', '分类',  '周序数','周期','尾端配送商','妥投时间间隔','KPI' ]]
#     df1 =df.set_index(['渠道', '国家', '分类',  '周序数','周期','尾端配送商', ]).stack().unstack(['尾端配送商',-1,])
#     # df2 =df.set_index(['渠道', '国家', '分类',  '周序数','周期','尾端配送商', ]).stack().unstack(['尾端配送商',-1,])
#
#     return df1
# df0=data_z('CNE全球特惠-GB')
# df1=data_z('CNE全球特惠-GB')
#
# print(df1)
# print(df0.index.levels[0])
# print(list(df0.columns.levels[0]))
# print(list(df0[('特惠_UKTR48','妥投时间间隔')]))
#
# def p_Line(lb):
#     df=data_z(lb)
#     lx=list(df.index.levels[3])#周期
#
#     ly= list(df['妥投时间间隔'])
#     lk=list(df['KPI'])
#     lg=list(df['国家'])
#     c = (
#         Line({"theme": ThemeType.MACARONS})
#             .add_xaxis(lx)
#             .set_global_opts(
#             title_opts=opts.TitleOpts(title=lb),
#             tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
#             yaxis_opts=opts.AxisOpts(
#                 type_="value",
#                 axistick_opts=opts.AxisTickOpts(is_show=True),
#                 splitline_opts=opts.SplitLineOpts(is_show=True),
#             ),
#             xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
#         )
#             # .render("line_areastyle_boundary_gap.html")
#     )
#     for wd in list(df.columns.levels[0]):#'特惠_GBHERMES', '特惠_UKTR48'*****
#         c.add_yaxis("妥投时间间隔",list(df[(wd,'妥投时间间隔')]), is_smooth=True,
#                        label_opts=opts.LabelOpts(is_show=True, position="top"),
#                        areastyle_opts=opts.AreaStyleOpts(opacity=0.5),
#                     markline_opts=opts.MarkLineOpts(data=[opts.MarkLineItem(name='KPI',xcoord=max(lx),ycoord=list(df[(wd,'KPI')].dropna(how='any'))[0])]
#                     ))
#
#     return c
# page = Page(layout=Page.SimplePageLayout,
#             )
# # page.add(image1,image_sheet(pn1,"风控拒绝原因分布（渠道）") )
# # grid =Grid(init_opts=opts.InitOpts(width="1200px", height="1200px"))
# for z in dca['分类']:
#     df=d10[d10['分类']==z]
#     zt=z
#     page.add(p_Line( zt))
# page.render(r"F:\其它部门\渠道妥投天数统计.html")



def p_Line(df,zt):
    lx=list(df['周期'])
    ly= list(df['妥投时间间隔'])
    lk=list(df['KPI'])
    lq=list(df['期望D'])
    c = (
        Line({"theme": ThemeType.MACARONS})
            .add_xaxis(lx)
            .add_yaxis("妥投时间间隔",ly, is_smooth=True,
                       label_opts=opts.LabelOpts(is_show=True, position="top"),
                       areastyle_opts=opts.AreaStyleOpts(opacity=0.5),)
            # .add_yaxis("KPI", lk, label_opts=opts.LabelOpts(is_show=False),)
            .add_yaxis("KPI",lk , label_opts=opts.LabelOpts(is_show=False),
                       markpoint_opts=opts.MarkPointOpts(
                           data=[opts.MarkPointItem(name="KPI", coord=[lx[0], lk[0]], value=lk[0])]),
                       )
             .add_yaxis("期望D",lq , label_opts=opts.LabelOpts(is_show=False),
                       markpoint_opts=opts.MarkPointOpts(
                           data=[opts.MarkPointItem(name="期望D", coord=[lx[0], lq[0]], value=lq[0])]),
                       )
            .set_series_opts(
            # areastyle_opts=opts.AreaStyleOpts(opacity=0.5),
            # label_opts=opts.LabelOpts(is_show=False),
        )
            .set_global_opts(
            title_opts=opts.TitleOpts(title=zt),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
            yaxis_opts=opts.AxisOpts(
                type_="value",
                axistick_opts=opts.AxisTickOpts(is_show=True),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
            xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
        )
            # .render("line_areastyle_boundary_gap.html")
    )
    return c
for qd in set(d10['渠道']):
    dca=data_ca(qd)
    page = Page(layout=Page.SimplePageLayout,)
    d11=d10[d10['渠道']==qd].copy(deep=True).reset_index(drop=True)
    for z in dca['主题']:
        zt=z
        df=d11[d11['主题']==z]
        page.add(p_Line(df, zt))
    page.render(r"{}\{}妥投天数统计.html".format(file,qd))

def data_ca(qd1):
    dca1 =d10t[d10t['渠道']==qd1]
    dca1=dca1[['国家', '主题']].drop_duplicates(subset=None,keep='last',).reset_index(drop=True)
    return dca1

for qd in set(d10t['渠道']):
    dca1=data_ca(qd)
    page = Page(layout=Page.SimplePageLayout,)
    d11t=d10t[d10t['渠道']==qd].copy(deep=True).reset_index(drop=True)
    for z in dca1['主题']:
        zt=z
        df=d11t[d11t['主题']==z]
        page.add(p_Line(df, zt))
    page.render(r"{}\{}妥投天数统计total.html".format(file,qd))

# d1.to_excel(r'F:\PBI临时文件\渠道妥投天数统计.excel',index=False)
bf = r'{}\渠道妥投天数概率统计.xlsx'.format(file)
writer = pd.ExcelWriter(bf)
d10.to_excel(writer, '数据')
d10t.to_excel(writer, '数据total')
dyt.to_excel(writer, '原数据')
d3.to_excel(writer, 'd3')
d4.to_excel(writer, 'd4')
d5.to_excel(writer, 'd5')
d6.to_excel(writer, 'd6')
d7.to_excel(writer, 'd7')
d8.to_excel(writer, 'd8')
d9.to_excel(writer, 'd9')
writer.save()