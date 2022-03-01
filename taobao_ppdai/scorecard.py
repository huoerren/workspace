# -*- coding: utf-8 -*-
"""
Created on Sat Apr 25 13:39:46 2020

@author: Lenovo
"""

#%%
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

''''''''''''''''''''''''     一、加载python包与函数       ''''''''''''''''''''''''''''''

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
import pandas as pd
from pandas import Series
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as stats
from sklearn.metrics import roc_curve,auc
from pylab import mpl
mpl.rcParams['font.sans-serif'] = ['SimHei'] # 为了能在plt显示中文字符
mpl.rcParams['axes.unicode_minus']=False 

import os
os.chdir('D:\liushaoxuan_pe\评分卡')

def cal_ks(point,Y,section_num=20):
    Y=pd.Series(Y)
    sample_num=len(Y)
    
    bad_percent=np.zeros([section_num,1])
    good_percent=np.zeros([section_num,1])

    
    point=pd.DataFrame(point)
    sorted_point=point.sort_values(by=0)
    total_bad_num=len(np.where(Y==1)[0])
    total_good_num=len(np.where(Y==0)[0])
    
    for i in range(0,section_num):
        split_point=sorted_point.iloc[int(round(sample_num*(i+1)/section_num))-1]
        position_in_this_section=np.where(point<=split_point)[0]
        bad_percent[i]=len(np.where(Y.iloc[position_in_this_section]==1)[0])/total_bad_num
        good_percent[i]=len(np.where(Y.iloc[position_in_this_section]==0)[0])/total_good_num
        
    ks_value=bad_percent-good_percent

    return ks_value,bad_percent,good_percent

#%%
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

''''''''''''''''''''''''     二、数据预处理       ''''''''''''''''''''''''''''''

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
# 读取数据与数据预处理
conpany_data = pd.read_excel('建模样本.xlsx')
conpany_data = conpany_data.replace(99991231, np.nan)
conpany_data['还款意愿'] = conpany_data[['利润总额增长率','债务增长率']].sum(axis=1)
dateparse = lambda dates: pd.datetime.strptime(dates,'%Y%m') if dates != 'nan' else np.nan

conpany_data['结束任期'] = conpany_data['结束任期'].astype(str)
conpany_data['结束任期'] = conpany_data['结束任期'].apply(lambda x: x[0:6])
conpany_data['开始任期'] = conpany_data['开始任期'].astype(str)
conpany_data['开始任期'] = conpany_data['开始任期'].apply(lambda x: x[0:6])
conpany_data['结束任期'] = conpany_data['结束任期'].apply(dateparse)
conpany_data['开始任期'] = conpany_data['开始任期'].apply(dateparse)

conpany_data['任职年限'] = (conpany_data['结束任期']-conpany_data['开始任期'])
conpany_data['任职年限'] = conpany_data['任职年限'].apply(lambda x: x.days/365)

cols_name = conpany_data.columns.tolist()
renameDict = {'证券代码':'company_code',
              '所属证监会行业名称                                                [行业级别] 门类行业':'industry',
              '企业性质':'nature1',
              '员工总数':'employee',
              '成立年限':'year',
              '还款意愿':'will',
              '国有=1，否则=0':'becountry',
              '流动比率':'current',
              '速动比率':'quick',
              '资产负债率':'debttoassets',
              '有形净值债务率':'debttotangibleequity',
              '经营性现金流负债比率':'cashtocurliabs',
              '每股收益':'eps',
              '总资产报酬率':'roa1',
              '总资产净利率':'roa2',
              '净资产收益率':'roe',
              '销售毛利率':'grossprofitmargin',
              '营业利润率':'optogr',
              '应收账款周转率':'arturn',
              '流动资产周转率':'caturn',
              '总资产周转率':'assetsturn1',
              '营业收入增长率':'yoyor',
              '营业利润增长率':'yoyop',
              '利润总额增长率':'yoyebt',
              '性别（男1女0）':'sex',
              '年龄':'age',
              '学历（高中及以下0，大专1，本科/学士2，硕士/研究生/MBA3，博士4）空值':'edu',
              '任职年限':'opyear',
              '应交税费':'taxespayable',
              '所得税':'tax',
              '应交税费增长率(取值为INF是上年为0，本年不为0)取值为M是本年上年均为0':'paytaxgrorate',
              '所得税增长率(取值为INF是上年为0，本年不为0)取值为M是本年上年均为0':'taxgrowrate',
              '是否为坏客户，是=1，否=0':'isbad'
              }
conpany_data = conpany_data.rename(columns=renameDict)
cols_list = list(renameDict.values())
used_data = conpany_data[cols_list]

used_data.nature1.value_counts()
used_data = used_data.replace('公众企业','其他企业')
used_data = used_data.replace('地方国有企业','其他企业')
used_data = used_data.replace('中央国有企业','其他企业')
used_data = used_data.replace('外资企业','其他企业')
used_data = used_data.replace('其他企业','其他企业')
used_data.nature1.value_counts()
used_data = used_data.replace('民营企业','PrivateEnterprise')
used_data = used_data.replace('其他企业','OtherEnterprises')

used_data.industry.value_counts()
used_data = used_data.replace('制造业','C')
used_data = used_data.replace('信息传输、软件和信息技术服务业','I')
used_data = used_data.replace('科学研究和技术服务业','M')
used_data = used_data.replace('建筑业','E')
used_data = used_data.replace('租赁和商务服务业','L')
used_data = used_data.replace('批发和零售业','F')
used_data = used_data.replace('房地产业','K')
used_data = used_data.replace('金融业','J')
used_data = used_data.replace('交通运输、仓储和邮政业','G')
used_data = used_data.replace('水利、环境和公共设施管理业','N')
used_data = used_data.replace('电力、热力、燃气及水生产和供应业','D')
used_data = used_data.replace('居民服务、修理和其他服务业','O')
used_data = used_data.replace('农、林、牧、渔业','A')
used_data = used_data.replace('卫生和社会工作','Q')
used_data = used_data.replace('采矿业','B')
used_data = used_data.replace('住宿和餐饮业','H')
used_data = used_data.replace('教育','P')
used_data = used_data.replace('住宿和餐饮业','H')
used_data = used_data.replace('文化、体育和娱乐业','R')

used_data.industry.value_counts()

one_hot_df = pd.get_dummies(used_data[['nature1','industry']])
Data = pd.concat([used_data, one_hot_df], axis=1)
del Data['nature1']
del Data['industry']

Data.to_excel('Data.xlsx', index=False)
Data = pd.read_excel('Data.xlsx')
#%%
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

''''''''''''''''''''''''     二、探索分析与画图       ''''''''''''''''''''

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
#通过Boxplot观察每个变量的极值问题

def outlier_check(df,c_name):
    p=df[[c_name]].boxplot(return_type='dict')
    x_outliers=p['fliers'][0].get_xdata()
    y_outliers = p['fliers'][0].get_ydata()
    for j in range(1):
        plt.annotate(y_outliers[j], xy=(x_outliers[j], y_outliers[j]), xytext=(x_outliers[j] + 0.02, y_outliers[j]))
    plt.savefig('{}_box_plot.jpg'.format(c_name), dpi = 600)
    plt.show()
    
outlier_check_list = cols_list[3:]
outlier_check_list.remove('edu')
for i in outlier_check_list:
    outlier_check(Data,i)

Data = Data[Data['will'] < 2000]
Data = Data[Data['arturn'] < 200]
Data = Data[Data['caturn'] < 4]
Data = Data[Data['assetsturn1'] < 4]
Data = Data[Data['yoyor'] < 140]
Data = Data[Data['yoyop'] < 618]
Data = Data[Data['yoyebt'] < 1000]
Data = Data[Data['taxespayable'] < 57321448]
Data = Data[Data['tax'] < 20779777]
Data = Data[Data['age'] < 90]

#df_train=df_train[df_train['year']<6]

#缺失值处理NA
def check_na(df):
    print(df.count(axis=0))
    
check_na(Data)
len(Data)

Data = Data.replace(np.nan, -99)
# 探索性分析 画直方图和密度图
def plt_hist(df,c_name):
    plt.hist(df[c_name])
    plt.title('distributed of {}'.format(c_name))
    plt.savefig('{}_distributed.jpg'.format(c_name), dpi = 600)
    plt.show()
for i in outlier_check_list:
    plt_hist(Data,i)

def plt_kde(df,c_name):
    Data[c_name].plot(kind='kde')
    plt.title('Density of {}'.format(c_name))
    plt.savefig('{}_Density.jpg'.format(c_name), dpi = 600)
    plt.show()
for i in outlier_check_list:
    plt_kde(Data,i)


#%%
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

''''''''''''''''''''''''     三、变量分箱与选择       ''''''''''''''''''''

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
#数据拆分,0.3的测试集完全采用随机抽样，那么违约样本的比例可能出现失真。
#因此，我们采取的方法是在违约与非违约的样本中各按一定的比例进行抽样

def data_sample(inputX,index,test_Ratio=0.3):
    from random import sample
    data_array=np.atleast_1d(inputX)
    class_array=np.unique(data_array)
    test_list=[]
    #train_list=[]
    for c in class_array:
        temp=[]
        for i,value in enumerate(data_array):
            if value==c:
                temp.append(index[i])
        test_list.extend(sample(temp,int(len(temp)*test_Ratio)))
    return list(set(index) - set(test_list)), test_list

def split_sample(df):
    train_list,test_list=data_sample(df['isbad'].tolist(),df.index.tolist())
    df_train_section=df.ix[train_list,:]
    df_test_section=df.ix[test_list,:]
    return df_train_section,df_test_section

df_train,df_test=split_sample(Data)


#woe离散化，先建excel,再将训练集导入一个excel


#####################
cols_names = list(df_train.columns)
features = cols_names[1:]
features.remove('isbad')
Y_train =df_train['isbad']
X_train =df_train[features]

Y_test =df_test['isbad']
X_test =df_test[features]
    #测试集占比30%
    # print(Y_train)
train = pd.concat([Y_train, X_train], axis=1)
test = pd.concat([Y_test, X_test], axis=1)
clasTest = test.groupby('isbad')['isbad'].count()
train.to_csv('TrainData.csv',index=False)
test.to_csv('TestData.csv',index=False)


"""
    # 连续变量离散化
    cutx3 = [ninf, 0, 1, 3, 5, pinf]
    cutx6 = [ninf, 1, 2, 3, 5, pinf]
    cutx7 = [ninf, 0, 1, 3, 5, pinf]
    cutx8 = [ninf, 0,1,2, 3, pinf]
    cutx9 = [ninf, 0, 1, 3, pinf]
    cutx10 = [ninf, 0, 1, 2, 3, 5, pinf]

"""
from statsmodels.stats.outliers_influence import variance_inflation_factor as vif
def cal_vif(df, vif_columns):
    """
    计算VIF
    """
    vif_df = df.loc[:, vif_columns].fillna(-999)
    columns = vif_df.columns.tolist()
    vif_ma = vif_df.as_matrix()
    result = {}
    for k, v in enumerate(columns):
        result[v] = vif(vif_ma, k)
    vif_result = pd.Series(result, name='vif')
    vif_result.index.name = 'variable'
    vif_result = vif_result.reset_index()
    return (vif_result)

vif_df = cal_vif(Data, cols_list[3:])

import seaborn as sns 

corr = Data[cols_list[3:]].corr()#计算各变量的相关性系数
corr = corr.reset_index()
#xticks = ['x0','x1','x2','x3','x4','x5','x6','x7','x8','x9','x10']#x轴标签
f,ax = plt.subplots(figsize=(12,9))
ax.set_xticklabels(corr,rotation='horizontal')
sns.heatmap(corr, cmap='rainbow',vmax =0.9,square=True)
label_y = ax.get_yticklabels()
plt.setp(label_y , rotation = 360)
label_x = ax.get_xticklabels()
plt.setp(label_x , rotation = 90)
plt.savefig('features_corr.jpg', dpi = 600, bbox_inches = 'tight')
plt.show()

cols_list.remove('quick')
cols_list.remove('will')
cols_list.remove('caturn')
cols_list.remove('roa2')
cols_list.remove('isbad')

#%%
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

''''''''''''''''''''''''     三、变量分箱与选择-定义自动分箱函数,IV值画图       ''''''''''''''''''''

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
# 定义自动分箱函数,IV值画图

def monoto_bin(Y, X, n=10):
    r = 0
    total_good = Y.sum()
    total_bad =Y.count()-total_good
    while np.abs(r) < 1:
        d1 = pd.DataFrame({"X": X, "Y": Y, "Bucket": pd.qcut(X, n)})
        d2 = d1.groupby('Bucket', as_index = True)
        r, p = stats.spearmanr(d2.mean().X, d2.mean().Y)
        n = n - 1
    d3 = pd.DataFrame(d2.min().X, columns = ['min_' + X.name])
    d3['min_' + X.name] = d2.min().X
    d3['max_' + X.name] = d2.max().X
    d3[Y.name] = d2.sum().Y
    d3['total'] = d2.count().Y
    #d3[Y.name + '_rate'] = d2.mean().Y
    #好坏比，求woe,证据权重，自变量对目标变量有没有影响，什么影响
    d3['goodattr']=d3[Y.name]/total_good
    d3['badattr']=(d3['total']-d3[Y.name])/total_bad
    d3['woe'] = np.log(d3['goodattr']/d3['badattr'])
    #iv，信息值，自变量对于目标变量的影响程度
    iv = ((d3['goodattr']-d3['badattr'])*d3['woe']).sum()
    d4 = (d3.sort_values(by = 'min_' + X.name)).reset_index(drop = True)
    print ("=" * 80)
    print (d4)
    cut = []
    cut.append(float('-inf'))
    for i in range(1,n+1):
        qua =X.quantile(i/(n+1))
        cut.append(round(qua,4))
    cut.append(float('inf'))
    woe = list(d4['woe'].round(3))
    return d4,iv,cut,woe

Labels = ['employee', 'year','current', 'debttoassets', 'roa1', 'grossprofitmargin',
          'optogr', 'roe','taxgrowrate','arturn','age','sex','edu','cashtocurliabs','assetsturn1',
          'yoyor','yoyop','yoyebt']

ivlist = []
index = []
j = 'isbad'
for i in range(49):
    var_1 = 'dfx'+str(i)
    var_2 = 'ivx'+str(i)
    var_3 = 'cutx'+str(i)
    var_4 = 'woex'+str(i)
    
    exec(var_1+','+var_2+','+var_3+','+var_4+'= monoto_bin(Data[j],Data["'+Labels[i]+'"])')

    ivlist.append(eval(var_2))
    index.append('x'+str(i))

dfx3,ivx3,cutx3,woex3 = monoto_bin(Data['isbad'],Data['current'], 10)
dfx4,ivx4,cutx4,woex4 = monoto_bin(Data['isbad'],Data['debttoassets'], 10)
dfx5,ivx5,cutx5,woex5 = monoto_bin(Data['isbad'],Data['roa1'], 10)
dfx6,ivx6,cutx6,woex6 = monoto_bin(Data['isbad'],Data['grossprofitmargin'], 10)
dfx7,ivx7,cutx7,woex7 = monoto_bin(Data['isbad'],Data['optogr'], 10)
dfx8,ivx8,cutx8,woex8 = monoto_bin(Data['isbad'],Data['roe'], 10)
dfx9,ivx9,cutx9,woex9 = monoto_bin(Data['isbad'],Data['taxgrowrate'], 10)


def self_bin(Y,X,cat):
    good=Y.sum()
    bad=Y.count()-good
    d1=pd.DataFrame({'X':X,'Y':Y,'Bucket':pd.cut(X,cat)})
    d2=d1.groupby('Bucket', as_index = True)
    d3 = pd.DataFrame(d2.X.min(), columns=['min'])
    d3['min'] = d2.min().X
    d3['max'] = d2.max().X
    d3['sum'] = d2.sum().Y
    d3['total'] = d2.count().Y
    d3['rate'] = d2.mean().Y
    d3['woe'] = np.log((d3['rate'] / (1 - d3['rate'])) / (good / bad))
    d3['goodattribute'] = d3['sum'] / good
    d3['badattribute'] = (d3['total'] - d3['sum']) / bad
    iv = ((d3['goodattribute'] - d3['badattribute']) * d3['woe']).sum()
    d4 = (d3.sort_index(by='min'))
    print("=" * 60)
    print(d4)
    woe = list(d4['woe'].round(3))
    return d4, iv,woe

pinf = float('inf')#正无穷大
ninf = float('-inf')#负无穷大
cutx1 = [-999, 50, 100, 500,999]
cutx2 = [-999,10,20,999]
cutx10 = [-999, 0,1,4, 8, 999]
cutx11 = [-999, 40, 55, 999]
cutx12 = [-999, 0.5, 999]
cutx13 = [-999,1.5,2.5,3.5, 999]
cutx14 = [-999,-2,0,1, 999]
cutx15 = [-999,0.5,1,1.5, 999]
cutx16 = [-999,-0.5,0.5,1, 999]
cutx17 = [-999,-0.5,0.5,1, 999]
cutx18 = [-999,-3,-0.5,0.5,1,1.5, 999]
cutx19 = [-999,-50,0,1,3, 999]


dfx1, ivx1,woex1 = self_bin(Data['isbad'],Data['employee'],cutx1)
dfx2, ivx2,woex2 = self_bin(Data['isbad'],Data['year'],cutx2)
dfx10, ivx10,woex10 = self_bin(Data['isbad'],Data['arturn'],cutx10) 
dfx11, ivx11,woex11 = self_bin(Data['isbad'],Data['age'],cutx11) 
dfx12, ivx12,woex12 = self_bin(Data['isbad'],Data['sex'],cutx12) 
dfx13, ivx13,woex13 = self_bin(Data['isbad'],Data['edu'],cutx13)
dfx14, ivx14,woex14 = self_bin(Data['isbad'],Data['cashtocurliabs'],cutx14)
dfx15, ivx15,woex15 = self_bin(Data['isbad'],Data['assetsturn1'],cutx15)
dfx16, ivx16,woex16 = self_bin(Data['isbad'],Data['yoyor'],cutx16)
dfx17, ivx17,woex17 = self_bin(Data['isbad'],Data['yoyop'],cutx17)
dfx18, ivx18,woex18 = self_bin(Data['isbad'],Data['yoyebt'],cutx18)


Labels = ['employee', 'year','current', 'debttoassets', 'roa1', 'grossprofitmargin',
          'optogr', 'roe','taxgrowrate','arturn','age','sex','edu','cashtocurliabs','assetsturn1',
          'yoyor','yoyop','yoyebt']

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

''''''''''''''''''''''''     三、变量分箱与选择-IV值画图       ''''''''''''''''''''

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
ivlist=[ivx1,ivx2,ivx3,ivx4,ivx5,ivx6,ivx7,ivx8,ivx9,ivx10, ivx11, ivx12, ivx13, ivx15, ivx18]#各变量IV

index=['x1','x2','x3','x4','x5','x6','x7','x8','x9','x10', 'x11', 'x12', 'x13','x15','x18']#x轴的标签
fig1 = plt.figure(1)
ax1 = fig1.add_subplot(1, 1, 1)
x = np.arange(len(index))+1 #设置x轴柱子的个数
ax1.bar(x, ivlist, width=0.4)#生成柱状图
ax1.set_xticks(x)  #设置x轴的刻度
ax1.set_xticklabels(index, rotation=0, fontsize=12)
ax1.set_ylabel('IV(Information Value)', fontsize=14)
plt.savefig('变量IV值.jpg', dpi = 600)
plt.show()


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

''''''''''''''''''''''''     四、woe编码 将原始值转化为woe值       ''''''''''''''''''''

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

#替换成woe函数
def replace_woe(series,cut,woe):
    results=[]
    i=0
    while i<len(series):
        value=series[i]
        j=len(cut)-2
        m=len(cut)-2
        while j>=0:
            if value>=cut[j]:
                j=-1
            else:
                j -=1
                m -= 1
        results.append(woe[m])
        i += 1
    return results
    # 替换成woe
X_train=X_train.reset_index(drop=True,inplace=False)  #不能遗忘，否则会出错   
Y_train=Y_train.reset_index(drop=True,inplace=False)  #不能遗忘，否则会出错   

Labels = ['employee', 'year','current', 'debttoassets', 'roa1', 'grossprofitmargin',
          'optogr', 'roe','taxgrowrate','arturn','age','sex','edu','cashtocurliabs','assetsturn1',
          'yoyor','yoyop','yoyebt']
X_train['employee_woe'] = Series(replace_woe(X_train['employee'], cutx1, woex1))
X_train['year_woe'] = Series(replace_woe(X_train['year'], cutx2, woex2))
X_train['current_woe'] = Series(replace_woe(X_train['current'], cutx3, woex3))
X_train['debttoassets_woe'] = Series(replace_woe(X_train['debttoassets'], cutx4, woex4))
X_train['roa1_woe'] = Series(replace_woe(X_train['roa1'], cutx5, woex5))
X_train['grossprofitmargin_woe'] = Series(replace_woe(X_train['grossprofitmargin'], cutx6, woex6))
X_train['optogr_woe'] = Series(replace_woe(X_train['optogr'], cutx7, woex7))
X_train['roe_woe'] = Series(replace_woe(X_train['roe'], cutx8, woex8))
X_train['taxgrowrate_woe'] = Series(replace_woe(X_train['taxgrowrate'], cutx9, woex9))
X_train['arturn_woe'] = Series(replace_woe(X_train['arturn'], cutx10, woex10))
X_train['age_woe'] = Series(replace_woe(X_train['age'], cutx11, woex11))
X_train['sex_woe'] = Series(replace_woe(X_train['sex'], cutx12, woex12))
X_train['edu_woe'] = Series(replace_woe(X_train['edu'], cutx13, woex13))
X_train['cashtocurliabs_woe'] = Series(replace_woe(X_train['cashtocurliabs'], cutx14, woex14))
X_train['assetsturn1_woe'] = Series(replace_woe(X_train['assetsturn1'], cutx15, woex15))
X_train['yoyor_woe'] = Series(replace_woe(X_train['yoyor'], cutx16, woex16))
X_train['yoyop_woe'] = Series(replace_woe(X_train['yoyop'], cutx17, woex17))
X_train['yoyebt_woe'] = Series(replace_woe(X_train['yoyebt'], cutx18, woex18))

Data=Data.reset_index(drop=True,inplace=False)  #不能遗忘，否则会出错   

Data['employee_woe'] = Series(replace_woe(Data['employee'], cutx1, woex1))
Data['year_woe'] = Series(replace_woe(Data['year'], cutx2, woex2))
Data['current_woe'] = Series(replace_woe(Data['current'], cutx3, woex3))
Data['debttoassets_woe'] = Series(replace_woe(Data['debttoassets'], cutx4, woex4))
Data['roa1_woe'] = Series(replace_woe(Data['roa1'], cutx5, woex5))
Data['grossprofitmargin_woe'] = Series(replace_woe(Data['grossprofitmargin'], cutx6, woex6))
Data['optogr_woe'] = Series(replace_woe(Data['optogr'], cutx7, woex7))
Data['roe_woe'] = Series(replace_woe(Data['roe'], cutx8, woex8))
Data['taxgrowrate_woe'] = Series(replace_woe(Data['taxgrowrate'], cutx9, woex9))
Data['arturn_woe'] = Series(replace_woe(Data['arturn'], cutx10, woex10))
Data['age_woe'] = Series(replace_woe(Data['age'], cutx11, woex11))
Data['sex_woe'] = Series(replace_woe(Data['sex'], cutx12, woex12))
Data['edu_woe'] = Series(replace_woe(Data['edu'], cutx13, woex13))
Data['cashtocurliabs_woe'] = Series(replace_woe(Data['cashtocurliabs'], cutx14, woex14))
Data['assetsturn1_woe'] = Series(replace_woe(Data['assetsturn1'], cutx15, woex15))
Data['yoyor_woe'] = Series(replace_woe(Data['yoyor'], cutx16, woex16))
Data['yoyop_woe'] = Series(replace_woe(Data['yoyop'], cutx17, woex17))
Data['yoyebt_woe'] = Series(replace_woe(Data['yoyebt'], cutx18, woex18))



#%%
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

''''''''''''''''''''''''     五、模型分析       ''''''''''''''''''''

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
#导入数据
data = pd.read_csv('WoeData.csv')
#应变量
Y=data['SeriousDlqin2yrs']
#自变量，剔除对因变量影响不明显的变量
X=data.drop(['SeriousDlqin2yrs','DebtRatio','MonthlyIncome', 'NumberOfOpenCreditLinesAndLoans','NumberRealEstateLoansOrLines','NumberOfDependents'],axis=1)

model_features = [i+'_woe' for i in Labels]
model_features.remove('yoyor_woe')
model_features.remove('yoyop_woe')
model_features.remove('taxgrowrate_woe')
model_features.remove('arturn_woe')
model_features.remove('roe_woe')
model_features.remove('roa1_woe')
model_features.remove('current_woe')
model_features.remove('employee_woe')
model_features.remove('grossprofitmargin_woe')
model_features.remove('cashtocurliabs_woe')
model_features.remove('age_woe')

#%%
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

''''''''''''''''''''''''     五、模型分析-模型训练       ''''''''''''''''''''

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
import statsmodels.api as sm
X1=sm.add_constant(Data[model_features])
logit=sm.Logit(Data['isbad'],X1)

result=logit.fit()
print(result.summary()) #根据p值删选变量


resu = result.predict(X1)#进行预测
fpr, tpr, threshold = roc_curve(Data['isbad'], resu)
rocauc = auc(fpr, tpr)#计算AUC
plt.plot(fpr, tpr, 'b', label='AUC = %0.2f' % rocauc)#生成ROC曲线
plt.legend(loc='lower right')
plt.plot([0, 1], [0, 1], 'r--')
plt.xlim([0, 1])
plt.ylim([0, 1])
plt.ylabel('真正率')
plt.xlabel('假正率')
plt.savefig('auc曲线.jpg', dpi = 600)
plt.show()


ks_value,bad_percent,good_percent=cal_ks(-resu,Data['isbad'],section_num=20)
max_ks0=np.max(ks_value)
plt.figure()
plt.plot(list(range(0,21)),np.append([0],good_percent),'-r',label='Bad Percent')
plt.plot(list(range(0,21)),np.append([0],bad_percent),'-g',label='Good Percent')
plt.plot(list(range(0,21)),np.append([0],ks_value),'-b',label='KS value')
plt.legend(loc='lower right')
plt.ylabel('% of total Good/Bad')
plt.xlabel('% of population')
plt.title('KS of test')
plt.savefig('ks曲线.jpg', dpi = 600)
plt.show()

cumGood = good_percent
cumBad = bad_percent
area = 0
for i in range(1, len(cumGood)):
    area += 1 / 2 * (cumBad[i - 1] + cumBad[i]) * (cumGood[i] - cumGood[i - 1])
gini = 2 * (area - 0.5)

# 模型检验

Y_test = test['SeriousDlqin2yrs']
#自变量，剔除对因变量影响不明显的变量，与模型变量对应
X_test = test.drop(['SeriousDlqin2yrs', 'DebtRatio', 'MonthlyIncome', 'NumberOfOpenCreditLinesAndLoans','NumberRealEstateLoansOrLines', 'NumberOfDependents'], axis=1)

X_test=X_test.reset_index(drop=True,inplace=False)  #不能遗忘，否则会出错   
Y_test=Y_test.reset_index(drop=True,inplace=False)  #不能遗忘，否则会出错   

X_test['employee_woe'] = Series(replace_woe(X_test['employee'], cutx1, woex1))
X_test['year_woe'] = Series(replace_woe(X_test['year'], cutx2, woex2))
X_test['current_woe'] = Series(replace_woe(X_test['current'], cutx3, woex3))
X_test['debttoassets_woe'] = Series(replace_woe(X_test['debttoassets'], cutx4, woex4))
X_test['roa1_woe'] = Series(replace_woe(X_test['roa1'], cutx5, woex5))
X_test['grossprofitmargin_woe'] = Series(replace_woe(X_test['grossprofitmargin'], cutx6, woex6))
X_test['optogr_woe'] = Series(replace_woe(X_test['optogr'], cutx7, woex7))
X_test['roe_woe'] = Series(replace_woe(X_test['roe'], cutx8, woex8))
X_test['taxgrowrate_woe'] = Series(replace_woe(X_test['taxgrowrate'], cutx9, woex9))
X_test['arturn_woe'] = Series(replace_woe(X_test['arturn'], cutx10, woex10))
X_test['age_woe'] = Series(replace_woe(X_test['age'], cutx11, woex11))
X_test['sex_woe'] = Series(replace_woe(X_test['sex'], cutx12, woex12))
X_test['edu_woe'] = Series(replace_woe(X_test['edu'], cutx13, woex13))
X_test['cashtocurliabs_woe'] = Series(replace_woe(X_test['cashtocurliabs'], cutx14, woex14))
X_test['assetsturn1_woe'] = Series(replace_woe(X_test['assetsturn1'], cutx15, woex15))
X_test['yoyor_woe'] = Series(replace_woe(X_test['yoyor'], cutx16, woex16))
X_test['yoyop_woe'] = Series(replace_woe(X_test['yoyop'], cutx17, woex17))
X_test['yoyebt_woe'] = Series(replace_woe(X_test['yoyebt'], cutx18, woex18))


X3 = sm.add_constant(X_test[model_features])
resu = result.predict(X3)#进行预测

fpr, tpr, threshold = roc_curve(Y_test, resu)
rocauc = auc(fpr, tpr)#计算AUC

plt.plot(fpr, tpr, 'b', label='AUC = %0.2f' % rocauc)#生成ROC曲线
plt.legend(loc='lower right')
plt.plot([0, 1], [0, 1], 'r--')
plt.xlim([0, 1])
plt.ylim([0, 1])
plt.ylabel('真正率')
plt.xlabel('假正率')
plt.show()

ks_value,bad_percent,good_percent=cal_ks(-resu,Y_test,section_num=20)
max_ks0=np.max(ks_value)
plt.figure()
plt.plot(list(range(0,21)),np.append([0],bad_percent),'-r',label='Bad Percent')
plt.plot(list(range(0,21)),np.append([0],good_percent),'-g',label='Good Percent')
plt.plot(list(range(0,21)),np.append([0],ks_value),'-b',label='KS value')
plt.legend(loc='lower right')
plt.ylabel('% of total Good/Bad')
plt.xlabel('% of population')


#%%
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

''''''''''''''''''''''''     六、评分卡生成       ''''''''''''''''''''

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
coe = result.params
# 生成评分卡
import math
# 我们取600分为基础分值，PDO为20（每高20分好坏比翻一倍），好坏比取20。
#coe 模型系数
p = 20 / math.log(2)
q = 650 - 20 * math.log(20) / math.log(2)
baseScore = round(q + p * coe[0], 0)

#计算分数函数 计算评分卡
def get_score(coe,woe,factor):
    scores=[]
    for w in woe:
        score=round(coe*w*factor,0)
        scores.append(score)
    return scores

x2 = get_score(coe[1],woex2,p)
x5 = get_score(coe[2],woex5,p)
x7 = get_score(coe[3],woex7,p)
x12 = get_score(coe[4],woex12,p)
x13 = get_score(coe[5],woex13,p)
x15 = get_score(coe[6],woex15,p)
x18 = get_score(coe[7],woex18,p)

#%%
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

''''''''''''''''''''''''     六、评分卡生成-打分函数       ''''''''''''''''''''

'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
#打分函数
def compute_score(series,cut,score):
    list = []
    i = 0
    while i < len(series):
        value = series[i]
        j = len(cut) - 2
        m = len(cut) - 2
        while j >= 0:
            if value >= cut[j]:
                j = -1
            else:
                j -= 1
                m -= 1
        list.append(-score[m])
        i += 1
    return list

test1 = pd.read_csv('TestData.csv')
test1['BaseScore']=Series(np.zeros(len(test1)))+baseScore
test1['x2'] = Series(compute_score(test1['year'], cutx2, x2))
test1['x5'] = Series(compute_score(test1['debttoassets'], cutx5, x5))
test1['x7'] = Series(compute_score(test1['optogr'], cutx7, x7))
test1['x12'] = Series(compute_score(test1['sex'], cutx12, x12))
test1['x13'] = Series(compute_score(test1['edu'], cutx13, x13))
test1['x15'] = Series(compute_score(test1['assetsturn1'], cutx15, x15))
test1['x18'] = Series(compute_score(test1['yoyebt'], cutx18, x18))

test1['Score'] = test1['x2'] + test1['x5'] + test1['x7'] + test1['x12'] +test1['x13'] + test1['x15'] +test1['x18'] + baseScore
test1.to_csv('ScoreData_test.csv', index=False)


train1 = pd.read_csv('TrainData.csv')
train1['BaseScore']=Series(np.zeros(len(train1)))+baseScore
train1['x2'] = Series(compute_score(train1['year'], cutx2, x2))
train1['x5'] = Series(compute_score(train1['debttoassets'], cutx5, x5))
train1['x7'] = Series(compute_score(train1['optogr'], cutx7, x7))
train1['x12'] = Series(compute_score(train1['sex'], cutx12, x12))
train1['x13'] = Series(compute_score(train1['edu'], cutx13, x13))
train1['x15'] = Series(compute_score(train1['assetsturn1'], cutx15, x15))
train1['x18'] = Series(compute_score(train1['yoyebt'], cutx18, x18))

train1['Score'] = train1['x2'] + train1['x5'] + train1['x7'] + train1['x12'] +train1['x13'] + train1['x15'] +train1['x18'] + baseScore
train1.to_csv('ScoreData_train.csv', index=False)

all_data = pd.concat([train1,test1], axis=0)
import feature_engineer_dev as fe
bins_dict = {'Score':[0,480, 500,510,520,530,540,560,580, 600,1000]}
bin_s = fe.feature_stats_series(all_data, 'Score', 'isbad', bins_dict=bins_dict, bin_count=20, bin_type=6,
                         bad_weight=1, good_weight=1, max_leaf_nodes=15,min_samples_leaf=2000,datatype='num',qushi='up')
bin_1 = fe.feature_stats_series(train1, 'Score', 'isbad', bins_dict=bins_dict, bin_count=20, bin_type=6,
                         bad_weight=1, good_weight=1, max_leaf_nodes=15,min_samples_leaf=2000,datatype='num',qushi='up')

bin_2 = fe.feature_stats_series(test1, 'Score', 'isbad', bins_dict=bins_dict, bin_count=20, bin_type=6,
                         bad_weight=1, good_weight=1, max_leaf_nodes=15,min_samples_leaf=2000,datatype='num',qushi='up')


def cal_psi(s1,s2):
    """[summary]
    
    Arguments:
        s1 {series} -- [description]
        s2 {series} -- [description]
    
    Returns:
        [type] -- [description]
    """
    s1[s1==0] = 0.0000001
    s2[s2==0] = 0.0000001
    s3 = s2-s1
    s4 = np.log(s2/s1)
    psi = np.dot(s3,s4)
    return psi 

cal_psi(bin_2['bin_pct'], bin_1['bin_pct'])


plt.hist(test1['Score'])
plt.xlabel('Score')
plt.ylabel('num')
plt.title('Score Distribution')
plt.savefig('评分分布.jpg', dpi = 600)
plt.show()


"""
cutx2 [-999, 10, 20, 999] [-7.0, 6.0, 6.0]
cutx5 [-inf, -0.2452, -0.1035, -0.0111, 0.0321, 0.08, inf] [14.0, 3.0, 2.0, -1.0, -7.0, -21.0]
cutx7 [-inf, -0.312, -0.0395, 0.0398, inf] [9.0, -0.0, -4.0, -9.0]
cutx12 [-999, 0.5, 999] [12.0, -2.0]
cutx13 [-999, 1.5, 2.5, 3.5, 999] [-3.0, -1.0, 2.0, 26.0]
cutx15 [-999, 0.5, 1, 1.5, 999] [8.0, -7.0, -2.0, -4.0]
cutx18 [-999, -3, -0.5, 0.5, 1, 1.5, 999] [4.0, -5.0, -1.0, 10.0, 4.0, -9.0]
"""







