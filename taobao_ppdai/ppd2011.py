
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as stats # 统计学库
from scipy.stats import norm  # 用于拟合正态分布曲线
import  os

pd.set_option('display.max_columns',1000)
pd.set_option('display.width',10100)

plt.rcParams['font.sans-serif'] = ['SimHei'] # 用来正常显示中文标签
plt.rcParams['font.family']='sans-serif'
plt.rcParams['axes.unicode_minus'] = False # 用来正常显示负号

os.chdir('D:/project_data/2011/')

# LC_data   = pd.read_csv('LC.csv')
# df = pd.read_csv('LC.csv' ,sep='delimiter',engine='python' ,encoding='ISO-8859-1')

df = pd.read_csv('2011.csv')
# print(df.shape)  # 21721,74

# 查看 application_type 这个特征的分组及分组数量 INDIVIDUAL：21721
# print(df.application_type.value_counts())

print('-----------------------------------')

# 以下变量是 没有数据 ，需要删除
# tot_coll_amt  \tot_cur_bal\ open_acc_6m \ open_il_6m\ open_il_12m \open_il_24m
# mths_since_rcnt_il\ total_bal_il \il_util \open_rv_12m \max_bal_bc \all_util \total_rev_hi_lim
# inq_fi \total_cu_tl\ inq_last_12m


# 删除下面的列 （下面的这些列对贷款没有作用）
# 'id','member_id','emp_title','url','desc','title','application_type'
dropColumns01 = ['id','member_id','emp_title','url','desc','title','application_type',
        'tot_coll_amt','tot_cur_bal','open_acc_6m','open_il_6m','open_il_12m' ,'open_il_24m',
        'verification_status_joint','mths_since_rcnt_il','total_bal_il','il_util','open_rv_12m',
        'max_bal_bc','all_util','total_rev_hi_lim','inq_fi','total_cu_tl','inq_last_12m','mths_since_last_major_derog',
        'annual_inc_joint','dti_joint','open_rv_24m','acc_now_delinq' ]

# print(df['acc_now_delinq'].value_counts())

dropColumns02 = ['zip_code','addr_state']
df = df.drop(dropColumns01,axis=1)
df = df.drop(dropColumns02 ,axis= 1 )

# print(df.shape)#  (21721, 46)
# print(df.info()) # 查看数据集的大致情况
#定义新函数 , 给出目标Y值
def coding(col, codeDict):
    colCoded = pd.Series(col, copy=True)
    for key, value in codeDict.items():
        colCoded.replace(key, value, inplace=True)
    return colCoded

# 0: good  ; 1 : bad ;  2:Indeterminate 处于 宽限期(倒数3个先前是2 )
df["loan_status"] = coding(df["loan_status"], {'Current':0,'Fully Paid':0,'Late (31-120 days)':1,'Charged Off':1,'Late (16-30 days)':0,'In Grace Period':0,'Default':0})

# print(df.groupby('loan_status').size()) # 查看bad 和 good 的各占比例 ：bad 样本比例少，后期需要做不平衡样本处理

# 展示 借款人数在不同 的grade 上的分布
sns.set_style("darkgrid")
ax = sns.countplot(x="grade", data=df, order = df['grade'].value_counts().index , palette="Set2" )
# plt.show()

sns.set_style("darkgrid")
ax = sns.countplot(x="emp_length", data=df, order = df['emp_length'].value_counts().index , palette="Set2" )
plt.xticks(rotation=-15)
plt.rcParams["axes.labelsize"] = 5
# plt.show()

sns.set_style("darkgrid")
ax = sns.countplot(x="verification_status", data=df, order = df['verification_status'].value_counts().index , palette="Set2" )
plt.xticks(rotation=-15)
plt.rcParams["axes.labelsize"] = 5
# plt.show()

sns.set_style("darkgrid")
ax = sns.countplot(x="home_ownership", data=df, order = df['home_ownership'].value_counts().index , palette="Set2" )
plt.rcParams["axes.labelsize"] = 1

# plt.show()

large = 22; med = 16; small = 12
params = {'axes.titlesize': large,
          'legend.fontsize': med,
          'figure.figsize': (16, 10),
          'axes.labelsize': med,
          'axes.titlesize': med,
          'xtick.labelsize': med,
          'ytick.labelsize': med,
          'figure.titlesize': large}
plt.rcParams.update(params) # 设置 matplotlib 的各种参数
plt.style.use('seaborn-whitegrid') # seaborn 主题
sns.set_style("white")


# 判断变量是否符合正态分布
def norm_comparision_plot(data, figsize=(12, 10), color="#099DD9", ax=None, surround=True, grid=True):
    plt.figure(figsize=figsize) # 设置图片大小
    # fit=norm: 同等条件下的正态曲线(默认黑色线)；lw-line width 线宽
    sns.distplot(data, fit=norm, color=color,kde_kws={"color" :color, "lw" :3 }, ax=ax)
    (mu, sigma) = norm.fit(data)  # 求同等条件下正态分布的 mu 和 sigma
    # 添加图例：使用格式化输入，loc='best' 表示自动将图例放到最合适的位置
    plt.legend(['Normal dist. ($\mu=$ {:.2f} and $\sigma=$ {:.2f} )'.format(mu, sigma)] ,loc='best')
    plt.ylabel('Frequency')
    plt.title("Distribution")
    if surround == True:
        # trim=True-隐藏上面跟右边的边框线，left=True-隐藏左边的边框线
        # offset：偏移量，x 轴向下偏移，更加美观
        sns.despine(trim=True, left=True, offset=10)
    if grid == True:
        plt.grid(True)  # 添加网格线


# loan_amnt,int_rate  的分布
# norm_comparision_plot(data=df['loan_amnt'])
# norm_comparision_plot(data=df['int_rate'])
# norm_comparision_plot(data=df['last_pymnt_amnt'])
# plt.show()

# 同值化处理 : 一般而言，如果某个特征的同值化程度达到90%，则认为该特征没有区分类别的效果，要舍去该特征
def primaryvalue_ratio(data, ratiolimit = 0.9):
    #首先计算每个变量的命中率,这个命中率是指 维度中占比最大的值的占比
    recordcount = data.shape[0]
    x = []
    #循环每一个列，并取出出现频率最大的那个值;index[0]是取列名,iloc[0]是取列名对应的值
    for col in data.columns:
        primaryvalue = data[col].value_counts().index[0]
        ratio = float(data[col].value_counts().iloc[0])/recordcount
        x.append([ratio,primaryvalue])
    feature_primaryvalue_ratio = pd.DataFrame(x,index = data.columns)
    feature_primaryvalue_ratio.columns = ['primaryvalue_ratio','primaryvalue']

    needcol = feature_primaryvalue_ratio[feature_primaryvalue_ratio['primaryvalue_ratio']<ratiolimit]
    needcol = needcol.reset_index()
    select_data = data[list(needcol['index'])]
    return select_data

# print(df.shape)  # (21721 ,43)
df = primaryvalue_ratio(df)

# 缺失值的处理:

#1、对于缺失率大于 0.95 的列则删除

def missed(data,rateStd = 0.94):
    missColumns = []
    columns = data.columns.values.tolist()
    for column in columns:
        rate = sum(pd.isnull(data[column])) / len(data[column])
        if rate > rateStd:
            missColumns.append(column)
    if len(missColumns) >0 :
        data = data.drop(missColumns, axis=1)
    return data

df = missed(df)
# print(df.shape) # (21721 ,34 )

#对于缺失率在 10%-80% 的变量单独和目标变量编组计算这些个变量的IV值

def getMidMiss(data):
    missColumns = []
    columns = data.columns.values.tolist()
    for column in columns:
        rate = sum(pd.isnull(data[column])) / len(data[column])
        # missColumns.append([column, rate])
        if rate >= 0.1 and rate <= 0.8:
            missColumns.append([column , rate])
    return missColumns

getMidMissColumns = getMidMiss(data=df)
# if len(getMidMissColumns) >0:

# 将可以数值化的列进行数值化
df['term'] = df['term'].str.rstrip('months').astype('float')
# df['revol_util'] = df['revol_util'].astype('float')
# df['int_rate'] = df['int_rate'].astype('float')

# print(df['term'].dtypes)


# 继续进行变量选择

# 对于变量是没有大的意义的可以删除
# next_pymnt_d : 客户下一个还款时间，没有意义
# last_pymnt_d ：最后一个还款日期，无意义
# last_credit_pull_d ：最近一个贷款的时间，没有意义
# sub_grade ： 与grade重复，分类太多
# title： title与purpose的信息基本重复，数据分类太多

delColumns = ['next_pymnt_d','last_pymnt_d','last_credit_pull_d','sub_grade','earliest_cr_line','issue_d' ]
df = df.drop(delColumns ,axis= 1 )
# print(df.shape) # (21721 ,28)

# issue_d ： 放款时间，申请模型用不上
# earliest_cr_line : 贷款客户第一笔借款日期

# 然后对字符型的数据进行编码：
mapping_dict = {"initial_list_status":
                   {"w": 0,"f": 1,},
               "emp_length":
                   {"10+ years": 11,"9 years": 10,"8 years": 9,
                    "7 years": 8,"6 years": 7,"5 years": 6,"4 years":5,
                    "3 years": 4,"2 years": 3,"1 year": 2,"< 1 year": 1,
                    "n/a": 0},
               "grade":
                   {"A": 0,"B": 1,"C": 2, "D": 3, "E": 4,"F": 5,"G": 6},
               "verification_status":
                   {"Not Verified":0,"Source Verified":1,"Verified":2},
               "purpose":
                   {"credit_card":0,"home_improvement":1,"debt_consolidation":2,
                    "other":3,"major_purchase":4,"medical":5,"small_business":6,
                    "car":7,"vacation":8,"moving":9, "house":10,
                    "renewable_energy":11,"wedding":12},
               "home_ownership":
                   {"MORTGAGE":0,"OTHER":1,"OWN":3,"RENT":4}}

df = df.replace(mapping_dict)
# print(df.shape) # (21721 ,27)

# 现在所有的 特征都是数值型的（包括连续型数值变量，分类型数值变量）
print(df.columns.values.tolist())
# print('=====================================')
# print(df.info())

# 分箱并计算woe值
def bin_distince(x,y,n=10): # x为待分箱的变量，y为target变量. n为分箱数量
    total = y.count()  # 计算总样本数
    bad = y.sum()      # 计算坏样本数
    good = y.count()-y.sum()  # 计算好样本数
    d1 = pd.DataFrame({'x':x,'y':y,'bucket':pd.cut(x,n)}) #利用pd.cut实现等距分箱
    d2 = d1.groupby('bucket',as_index=True)  # 按照分箱结果进行分组聚合
    d3 = pd.DataFrame(d2.x.min(),columns=['min_bin'])
    d3['min_bin'] = d2.x.min()  # 箱体的左边界
    d3['max_bin'] = d2.x.max()  # 箱体的右边界
    d3['bad'] = d2.y.sum()  # 每个箱体中坏样本的数量
    d3['total'] = d2.y.count() # 每个箱体的总样本数
    d3['bad_rate'] = d3['bad']/d3['total']  # 每个箱体中坏样本所占总样本数的比例
    d3['badattr'] = d3['bad']/bad   # 每个箱体中坏样本所占坏样本总数的比例
    d3['goodattr'] = (d3['total'] - d3['bad'])/good  # 每个箱体中好样本所占好样本总数的比例
    d3['woe'] = np.log(d3['goodattr']/d3['badattr'])  # 计算每个箱体的woe值
    iv = ((d3['goodattr']-d3['badattr'])*d3['woe']).sum()  # 计算变量的iv值
    d4 = (d3.sort_values(by='min_bin')).reset_index(drop=True) # 对箱体从大到小进行排序
    # print('分箱结果：')
    # print(d4)
    # print('IV值为：')
    # print(iv)
    return iv
    # cut = []
    # cut.append(float('-inf'))
    # for i in d4.min_bin:
    #     cut.append(i)
    # cut.append(float('inf'))
    # woe = list(d4['woe'].round(3))
    # return d4,iv,cut,woe

# bin_distince(df['loan_amnt'],df['loan_status'],n=10)
ivValuesDictList = []
columnsPart01 = []
continuousValueColumns = ['loan_amnt','funded_amnt','funded_amnt_inv','int_rate','installment','emp_length',
                          'annual_inc','dti','inq_last_6mths','delinq_2yrs','open_acc','revol_bal','revol_util',
                          'total_acc','total_pymnt','total_pymnt_inv','total_rec_prncp','total_rec_int','recoveries','last_pymnt_amnt']

for i in continuousValueColumns:
    ivValue = bin_distince(df[i], df['loan_status'], n=10)
    if ivValue < 0.02: # 当 iv 值小于 0.02时则表示该特征对数据结果判定的影响意义不大，可以省略该特征
        ivValuesDictList.append({i:ivValue})

if len(ivValuesDictList) >0:
    for j in ivValuesDictList:
        jKey = list(j.keys())[0]
        columnsPart01.append(jKey)
if len(columnsPart01) >0:
    df = df.drop(columnsPart01 , axis=1)

#print(df.shape) #(21721,22)

# 计算线性相关性
# def getHeatMap(df):
# #     dfData = df.corr()
# #     plt.subplots(figsize=(18, 18)) # 设置画面大小
# #     sns.heatmap(dfData, annot=True,cmap="Blues")
# #     sns.set(font_scale=1.5)
# #     plt.show()
# #
# # getHeatMap(df)

print('****************************************')
# print(df.corr()) # 计算变量之间的相关系数
# 获得共线性高的特征（皮尔森系数>0.75时，则认为两者共线性强，需要删除其中一个）
cols = ['funded_amnt','funded_amnt_inv','total_pymnt','total_pymnt_inv','total_rec_prncp','int_rate']
df = df.drop(cols , axis=1)
df['revol_util'] = df['revol_util'].fillna(df['revol_util'].median())
df['mths_since_last_delinq'] = df['mths_since_last_delinq'].fillna(df['mths_since_last_delinq'].median())

#print(df.shape) # (21721, 16)

# 建模调参
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn import metrics
from sklearn.metrics import roc_auc_score,roc_curve
y = df['loan_status']  # 目标列
del df['loan_status']  # 特征列
x = df

# 测试集为30%，训练集为70%
X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=0.3, random_state=0)

log_model = LogisticRegression(solver='newton-cg',multi_class='ovr' )
log_model.fit(X_train, y_train)
y_pre = log_model.predict_proba(X_test)
y_pred = log_model.predict(X_test)
y_0 = list(y_pre[:,1])

fpr,tpr,thresholds = roc_curve(y_test.values , y_0)  #计算fpr,tpr,thresholds
auc = roc_auc_score(y_test,y_0)           #计算auc
print('------------------')
print( 'AUC : '+ str(auc))
print('******************')
#画曲线图
plt.figure()
plt.plot(fpr,tpr)
plt.title('$ROC curve$')
plt.show()

#画ks曲线
plt.plot(tpr)
plt.plot(fpr)
plt.plot(tpr-fpr)
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

# 查看测试结果
print(" ---------------- 混合矩阵: -----------------------")
print(metrics.confusion_matrix(y_test, y_pred))

print(" ---------------- 准确率 （precision）| 召回率（recall）| f1-scrore : -----------------------")
print(metrics.classification_report(y_test, y_pred))

print('------------------------ 测试集准确率：---------------------')

print('测试集准确率: ' , log_model.score(X_test,y_test)) # 分数


# print(df['issue_d'].value_counts())
# pd_issue_d = pd.to_datetime(df['issue_d'])
# df['pd_issue_d'] = pd_issue_d
# print(df['pd_issue_d'].value_counts())
# df['pd_issue_d'] = pd_issue_d

# print(df.columns.values.tolist())
# print(df['loan_status'])



