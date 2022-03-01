
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as stats   # 统计学库
from scipy.stats import norm  # 用于拟合正态分布曲线
import statsmodels.api as sm
from statsmodels.stats.outliers_influence import variance_inflation_factor

pd.set_option('display.max_columns',100) #a就是你要设置显示的最大列数参数
pd.set_option('display.max_rows',100000) #b就是你要设置显示的最大的行数参数
pd.set_option('display.width',10000000) #x就是你要设置的显示的宽度，防止轻易换行

plt.rcParams['font.sans-serif'] = ['SimHei'] # 用来正常显示中文标签
plt.rcParams['font.family']='sans-serif'
plt.rcParams['axes.unicode_minus'] = False # 用来正常显示负号

df01 = pd.read_csv('E:/LoanStats_securev1_2018Q1.csv',encoding='gbk')
df02 = pd.read_csv('E:/LoanStats_securev1_2018Q2.csv',encoding='gbk' )
df03 = pd.read_csv('E:/LoanStats_securev1_2018Q3.csv',encoding='gbk')
df04 = pd.read_csv('E:/LoanStats_securev1_2018Q4.csv',encoding='gbk')
dfAll   =  pd.concat([df01,df02,df03,df04])
# print(df.shape) #(495242, 150)

exColumns = [ 'issue_d', 'int_rate', 'total_be_limit', 'funded_amnt', 'addr_state', 'annual_inc', 'term',
              'tot_hi_cred_lim', 'mo_sin_old_rev_tl_op', 'rovol_bal' , 'revol_ufil', 'sub_grade',
              'installment', 'num_bc_sats', 'open_acc', 'mo_sin_old_il_acct', 'acc_open_past_24mths',
              'mths_since_last_delinq', 'total_bc_limit', 'inq_last_6mths', 'total_rev_hi_lim', 'bc_open_to_buy',
              'mths_since_recent_inq', 'chargeoff_recent_inq', 'collection_recovery_fee',
              'delinq_2yrs', 'tot_cur_bal', 'total_bal_ex_mort',
              'home_ownership', 'dti', 'mo_sin_rcnt_tl', 'mths_since_recent_bc', 'tot_coll_amt', 'pct_tl_nvr_dlq',
              'fico_range_high', 'num_bc_tl', 'num_rev_tl_bal_gt_0', 'num_actv_rev_tl' , 'num_sats',
              'num_tl_op_past_12m', 'num_rev_accts', 'percent_bc_gt_75', 'num_il_tl', 'revol-bal', 'bc_util', 'grade','loan_status']

inColumns = dfAll.columns.values.tolist()
pluColumns = []
for i in exColumns:
    if i in inColumns:
        pass
    else:
        pluColumns.append(i)

# 从外界提供的列中删除掉数据表格中没有的列
for i in pluColumns:
    exColumns.remove(i)
#  df 是利用列举列组装形成的 新的 dataframe
df = dfAll [exColumns]
print('-------------df 1 -------------')
print(df.columns.values.tolist())
print(len(df.columns.values.tolist()))

df = df[df['loan_status'].isin(['Fully Paid','Charged Off'])]  # 选取df中loan_status(本例是目标列)

#1 对缺失率的 判断 ：若是某一列 缺失率达到 65%，则该列特征将被删除

def dealMissed(data,rateStd = 0.65):
    missColumns = []
    columns = data.columns.values.tolist()
    for column in columns:
        rate = sum(pd.isnull(data[column])) / len(data[column])
        if rate > rateStd:
            print(column)
            missColumns.append(column)
    if len(missColumns) >0 :
        data = data.drop(missColumns, axis=1)
    return data

df = dealMissed(df)

print('-------------df 2 -------------')
print(df.columns.values.tolist())
print(len(df.columns.values.tolist()))

temp = df['loan_status']

# 2-同值化处理 : 一般而言，如果某个特征的同值化程度达到75%，则认为该特征没有区分类别的效果，要舍去该特征
def primaryvalue_ratio(data, ratiolimit = 0.75):
    recordcount = data.shape[0]
    x = []
    for col in data.columns:
        primaryvalue = data[col].value_counts().index[0]
        ratio = float(data[col].value_counts().iloc[0])/recordcount
        x.append([ratio,primaryvalue])
    feature_primaryvalue_ratio = pd.DataFrame(x,index = data.columns)
    feature_primaryvalue_ratio.columns = ['primaryvalue_ratio','primaryvalue']
    needcol = feature_primaryvalue_ratio[feature_primaryvalue_ratio['primaryvalue_ratio']< ratiolimit]
    needcol = needcol.reset_index()
    select_data = data[list(needcol['index'])]
    return select_data

df = primaryvalue_ratio(df)

print('------------- df 3 -------------')
print(df.columns.values.tolist())
print(len(df.columns.values.tolist()))

#3 删除自定义的 特征
dropColumns = ['addr_state','issue_d','sub_grade']
df = df.drop(dropColumns,axis=1)

# -------------- --------------
df['loan_status'] = temp

#4 对缺失值的填充 : 采用中位值填充
for col_name in df.columns:
    cnt = list(df[col_name].isna()).count(True)
    # print(col_name + ' - '+ str(cnt))
    if cnt >0 :
        # print( col_name +" : "+ str(df[col_name].dtype))
        df[col_name] = df[col_name].fillna(df[col_name].median())

print('-------------df 4 -------------')
print(df.columns.values.tolist())
print(len(df.columns.values.tolist()))

#5 分类变量 转成 数值型
def coding(col, codeDict):
    colCoded = pd.Series(col, copy=True)
    for key, value in codeDict.items():
        colCoded.replace(key, value, inplace=True)
    return colCoded
df["loan_status"] = coding(df["loan_status"], {'Fully Paid':0,'Charged Off':1})

mapping_dict = {
               "emp_length":
                   {"10+ years": 11,"9 years": 10,"8 years": 9,
                    "7 years": 8,"6 years": 7,"5 years": 6,"4 years":5,
                    "3 years": 4,"2 years": 3,"1 year": 2,"< 1 year": 1,
                    "n/a": 0},
               "grade":
                   {"A": 0,"B": 1,"C": 2, "D": 3, "E": 4,"F": 5,"G": 6},
               "verification_status":
                   {"Not Verified":0,"Source Verified":1,"Verified":2},
                "verification_status_joint":
                   {"Not Verified":0,"Source Verified":1,"Verified":2},
               "purpose":
                   {"credit_card":0,"home_improvement":1,"debt_consolidation":2,
                    "other":3,"major_purchase":4,"medical":5,"small_business":6,
                    "car":7,"vacation":8,"moving":9, "house":10,
                    "renewable_energy":11,"wedding":12},
               "home_ownership":
                   {"MORTGAGE":0,"OTHER":1,"ANY":2,"OWN":3,"RENT":4}
                }
df = df.replace(mapping_dict)

df['term'] = df['term'].str.rstrip('months').astype('float')
df['int_rate'] = df['int_rate'].str.rstrip('%').astype('float')

print(df.info())
print('----------------- df 5 -----------------')
print(df.columns.values.tolist())
print(len(df.columns.values.tolist()))

#6 相关性检验（皮尔森系数 > 0.75时，则认为两者共线性强，需要删除其中一个）

print(df.corr())
sns.set(font_scale=0.4)
sns.heatmap(df.corr() ,annot=True,cmap="RdYlGn" ,linewidths=0.2)
plt.xticks(rotation=90)
fig = plt.gcf()
fig.set_size_inches(18,16)
plt.show()

# 7 计算 vif 值
features = []
vif = []
columns = df.columns.values.tolist()
for i in range(len(columns)):
    # print(  str(i)+ ' -- 列名称 : '+  columns[i]  )
    features.append(columns[i] )
    vfactor2 = variance_inflation_factor(df[columns].values,i)
    vif.append(vfactor2)
    # print(vfactor2)
df_grade = pd.DataFrame(features, columns=['features'])
df_grade = pd.concat([df_grade, pd.DataFrame(vif,columns=['vif'])],axis=1)
# print(df_grade)

# 8 删除线性相关性高 特征（综合 7，6 步 删除线性相关性高且 vif 值大的特征）

delColumns01 = ['int_rate','funded_amnt','tot_hi_cred_lim','num_bc_tl','open_acc',
              'acc_open_past_24mths','total_bc_limit','num_bc_tl','bc_util']

df = df.drop(delColumns01,axis=1)

print(df.info())
print('----------------- df 6 -----------------')
print(df.columns.values.tolist())
print(len(df.columns.values.tolist()))

#9 分箱 求woe ,iv  删除 vi< 0.05 的特征

def bin_distince(x,y,n=10): # x为待分箱的变量，y为target变量. n为分箱数量
    total = y.count()  # 计算总样本数
    bad = y.sum()      # 计算坏样本数
    good = y.count()-y.sum()  # 计算好样本数
    d1 = pd.DataFrame({'x':x,'y':y,'bucket':pd.cut(x,n)}) #利用pd.cut实现等距分箱
    d2 = d1.groupby('bucket',as_index=True)  # 按照分箱结果进行分组聚合
    d3 = pd.DataFrame(d2.x.min(),columns=['min_bin'])

    d3['min_bin'] = d2.x.min()  # 箱体的左边界
    d3['max_bin'] = d2.x.max()  # 箱体的右边界
    d3['bad']     = d2.y.sum()  # 每个箱体中坏样本的数量
    d3['total']   = d2.y.count() # 每个箱体的总样本数
    d3['bad_rate'] = d3['bad']/d3['total']  # 每个箱体中坏样本所占总样本数的比例
    d3['badattr'] = d3['bad']/bad   # 每个箱体中坏样本所占坏样本总数的比例
    d3['goodattr'] = (d3['total'] - d3['bad'])/good  # 每个箱体中好样本所占好样本总数的比例
    d3['woe'] = np.log(d3['goodattr']/d3['badattr'])  # 计算每个箱体的woe值
    iv = ((d3['goodattr']-d3['badattr'])*d3['woe']).sum()  # 计算变量的iv值
    d4 = (d3.sort_values(by='min_bin')).reset_index(drop=True) # 对箱体从大到小进行排序
    print('分箱结果：')
    print(d4)
    print('IV值为：')
    print(iv)
    print('----------- ========= ------------')
    print(d3)
    print('----------- ========= ------------')
    return iv

print('------------- df 7 -------------')
print(df.columns.values.tolist())
print(len(df.columns.values.tolist()))

print('===============================')
listFenDuan = [] # 用于存放 连续型数值的 特征的列名
columnsList = df.columns.values.tolist()
for i in columnsList:
    print( i + ' : '+ str(len(df[i].unique())))
    if len(df[i].unique()) > 10 :
        listFenDuan.append(i) # 收集需要分箱的特征名称（阀值设定为10，一个特征如果unique的值>10 ,则判定该特征是连续性数值）

#对连续型数值变量 进行分箱操作
ivValuesDictList = []
columnsPart01 = []
continuousValueColumns = listFenDuan

dictIV = {}
for i in continuousValueColumns:
    print(' ---------------- ' +  i + ' --------------')
    ivValue = bin_distince(df[i], df['loan_status'], n=10)
    if ivValue < 0.05: # 当 iv 值小于 0.05 时则表示该特征对数据结果判定的影响意义不大，可以省略该特征
        ivValuesDictList.append({i:ivValue})
    else:
        dictIV[i] = ivValue

if len(ivValuesDictList) >0:
    for j in ivValuesDictList:
        jKey = list(j.keys())[0]
        columnsPart01.append(jKey)
if len(columnsPart01) >0:
    df = df.drop(columnsPart01 , axis=1)

print('-------------df 8 -------------')
print(df.columns.values.tolist())
print(len(df.columns.values.tolist()))

# 10 描述性统计若干个变量
# # 借款人数在不同 的grade 上的分布
# sns.set(font_scale=0.7)
# sns.set_style("darkgrid")
# ax = sns.countplot(x="term", data=df, order = df['term'].value_counts().index , palette="Set2" )
# ax.set_xlabel('term')
# fig = plt.gcf()
# fig.set_size_inches(9,6)
# plt.show()
#
# # 借款人数在不同 verification_status 上的分布
# sns.set(font_scale=0.7)
# sns.set_style("darkgrid")
# ax = sns.countplot(x="home_ownership", data=df, order = df['home_ownership'].value_counts().index , palette="Set2" )
# ax.set_xlabel("home_ownership")
# fig = plt.gcf()
# fig.set_size_inches(9,6)
# plt.show()
#
# # 借款人数在不同 home_ownership 上的分布
# sns.set(font_scale=0.7)
# sns.set_style("darkgrid")
# ax = sns.countplot(x="num_actv_rev_tl", data=df, order = df['num_actv_rev_tl'].value_counts().index , palette="Set2" )
# ax.set_xlabel("num_actv_rev_tl")
# fig.set_size_inches(9,6)
# plt.show()
#
#
# large = 15; med = 10; small = 12
# params = {'axes.titlesize': large,
#           'legend.fontsize': 10,
#           'figure.figsize': (12, 8),
#           'axes.labelsize': med,
#           'axes.titlesize': med,
#           'xtick.labelsize': med,
#           'ytick.labelsize': med,
#           'figure.titlesize': large}
# plt.rcParams.update(params) # 设置 matplotlib 的各种参数
# plt.style.use('seaborn-whitegrid') # seaborn 主题
# sns.set_style("white")
#
# # 判断变量是否符合正态分布
# def norm_comparision_plot(data, figsize=(12, 10), color="#099DD9", ax=None, surround=True, grid=True):
#     plt.figure(figsize=figsize) # 设置图片大小
#     # fit=norm: 同等条件下的正态曲线(默认黑色线)；lw-line width 线宽
#     sns.distplot(data, fit=norm, color=color,kde_kws={"color" :color, "lw" :3 }, ax=ax)
#     (mu, sigma) = norm.fit(data)  # 求同等条件下正态分布的 mu 和 sigma
#     # 添加图例：使用格式化输入，loc='best' 表示自动将图例放到最合适的位置
#     plt.legend(['Normal dist. ($\mu=$ {:.2f} and $\sigma=$ {:.2f} )'.format(mu, sigma)] ,loc='best')
#     plt.ylabel('Frequency')
#     plt.title("Distribution")
#     if surround == True:
#         # trim=True-隐藏上面跟右边的边框线，left=True-隐藏左边的边框线
#         # offset：偏移量，x 轴向下偏移，更加美观
#         sns.despine(trim=True, left=True, offset=10)
#     if grid == True:
#         plt.grid(True)  # 添加网格线
#         plt.show()
#
#
# # norm_comparision_plot(data=df['funded_amnt'])
# # norm_comparision_plot(data=df['annual_inc'])
# # norm_comparision_plot(data=df['int_rate'])
# # norm_comparision_plot(data=df['tot_hi_cred_lim'])


# 建模调参
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import metrics
from sklearn.metrics import roc_auc_score,roc_curve
y = df['loan_status']  # 目标列
del df['loan_status']  # 特征列
x = df
print('-------- x : -----------------')
print(x.columns.values.tolist() )
print('============= ===============')
# 测试集为30%，训练集为70%
X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.3, random_state=0)

log_model = LogisticRegression(solver='newton-cg',multi_class='ovr',max_iter=10000 )
log_model.fit(X_train, y_train)
y_pre = log_model.predict_proba(X_test)
y_pred = log_model.predict(X_test)
y_0 = list(y_pre[:,1])

fpr,tpr,thresholds = roc_curve(y_test.values , y_0,pos_label=1 )  #计算fpr,tpr,thresholds
auc = roc_auc_score(y_test,y_0)    #计算auc
print('------------------')
print( 'AUC : '+ str(auc))
print('******************')
#画曲线图
plt.figure()
plt.legend(loc='lower right')
plt.plot(fpr,tpr,'r--')
plt.ylabel('TPR')
plt.xlabel('FPR')
plt.title('$ROC curve$')
plt.show()

#画ks曲线
plt.plot(tpr ,label='tpr')
plt.plot(fpr ,label='fpr')
plt.plot(tpr-fpr , label='ks')
plt.show()

# 计算ks
KS_max=0
best_thr=0
for i in range(len(fpr)):
    if(i==0):
        KS_max=tpr[i]-fpr[i]
        best_thr=thresholds[i]
    elif (tpr[i]-fpr[i]>KS_max):
        KS_max = tpr[i] - fpr[i]
        best_thr = thresholds[i]

print('最大KS为：',KS_max)
print('最佳阈值为：',best_thr)

print(" ---------------- 混合矩阵: -----------------------")
print(metrics.confusion_matrix(y_test, y_pred))
print(" ---------------- 准确率 （precision）| 召回率（recall）| f1-scrore : -----------------------")
print(metrics.classification_report(y_test, y_pred))
print('------------------------ 测试集准确率：---------------------')
print('测试集准确率: ' , log_model.score(X_test,y_test)) # 分数
print('---------------------- params  --------------------------')




# # 区分是字符串变量 or 数值变量
# cols = df.columns
# numberCols = []
# objCols = []
# for col in cols:
#     if str(df[col].dtype) == 'object':
#         objCols.append(col)
#     else:
#         numberCols.append(col)
#
# # print(numberCols)
# # print(objCols)
# # for i in objCols:
# #     print(str(len(df[i].unique())))





