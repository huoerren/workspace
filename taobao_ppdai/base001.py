import pandas as pd
import numpy as np
from statsmodels.stats.outliers_influence import variance_inflation_factor

# 宽表
data = pd.DataFrame([[15.9, 16.4, 19, 19.1, 18.8, 20.4, 22.7, 26.5, 28.1, 27.6, 26.3]
                        , [149.3, 161.2, 171.5, 175.5, 180.8, 190.7, 202.1, 212.1, 226.1, 231.9, 239]
                        , [4.2, 4.1, 3.1, 3.1, 1.1, 2.2, 2.1, 5.6, 5, 5.1, 0.7]
                        , [108.1, 114.8, 123.2, 126.9, 132.1, 137.7, 146, 154.1, 162.3, 164.3, 167.6]]).T

print('==============')
# print(data)
print('--------------')
# 自变量
X = data[[1, 2, 3]]
# print(X)

# ✨✨✨务必注意✨✨✨，一定要加上常数项
X[4] = 1
# print(X)
# 计算第2个变量的(第二列)的方差膨胀因子
vfactor = variance_inflation_factor(X[[1, 2, 3, 4]].values,1)
# print(vfactor)


data1 = {
    "a":[1,2,3],
    "b":[4,5,6],
    "c":[7,8,9],
    "d":[38,None,90]
}
df1 = pd.DataFrame(data1)
# print('====================')
# print(df1)
# print('====================')

Y = df1[['b','c','d']]
Y['e']  = 1
print(Y)
print('-------------------------')
vfactor2 = variance_inflation_factor(Y[['b','c','d','e']].values,2)
print(vfactor2)


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

# 区分是字符串变量 or 数值变量
cols = df.columns
numberCols = []
objCols = []

for col in cols:
    if str(df[col].dtype) == 'object':
        objCols.append(col)
    else:
        numberCols.append(col)
dfs = df.drop(objCols, axis=1)
dfs['changshuxiang'] = 1
columns = (dfs.columns.values.tolist())
columns = [ 'funded_amnt', 'annual_inc', 'tot_hi_cred_lim', 'num_bc_sats', 'open_acc', 'mo_sin_old_il_acct', 'acc_open_past_24mths', 'mths_since_last_delinq', 'total_bc_limit', 'inq_last_6mths', 'mths_since_recent_inq', 'total_bal_ex_mort',  'dti', 'mo_sin_rcnt_tl', 'mths_since_recent_bc', 'fico_range_high', 'num_rev_tl_bal_gt_0', 'num_actv_rev_tl', 'num_il_tl']
# ['home_ownership', 'int_rate', 'term']
dfs = dfs.dropna(axis=0, how='any')

features = []
vif = []
for i in range(len(columns)):
    # print(  str(i)+ ' -- 列名称 : '+  columns[i]  )
    features.append(columns[i] )
    vfactor2 = variance_inflation_factor(dfs[columns].values,i)
    vif.append(vfactor2)
    # print(vfactor2)
df_grade = pd.DataFrame(features, columns=['features'])
df_grade = pd.concat([df_grade, pd.DataFrame(vif,columns=['vif'])],axis=1)
print(df_grade)
