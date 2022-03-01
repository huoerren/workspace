import pandas as pd



def primaryvalue_ratio(data, ratiolimit = 0.8):
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

data = pd.DataFrame({'name':['wencky','fsefs','stany','barbio',None,'barbio','barbio','barbio','barbio','barbio'],
                      'age':[33,33,3,None,None,None,None,None,None,None],
                      'gender':[None,'m','m','m',None,'m','m','m','f','f']})


# primaryvalue_ratio(data)
num = data.isna().sum()
print('==============================')

def miss(x):
    print(sum(pd.isnull(x))/len(x))
    return(sum(pd.isnull(x))/len(x))
# miss(data['age'])


print('############################')

def drop_col(df, col_name, cutoff=0.3):
    n = len(df)
    cnt = df[col_name].count()
    print( " age : "+ str(cnt))
    if (float(cnt) / n) > cutoff:
        df = df.drop(col_name, axis=1 )
    return df




# 循环打印 df的列名
# print(data.columns.values.tolist())


def missed(data,rateStd = 0.85):
    missColumns = []
    columns = data.columns.values.tolist()
    for column in columns:
        rate = sum(pd.isnull(data[column])) / len(data[column])
        if rate > rateStd:
            missColumns.append(column)
    if len(missColumns) >0 :
        data = data.drop(missColumns, axis=1)
    return data


# print(missed(data))


data02 = pd.DataFrame({'sex':['f','f','m','m','m','f' ],
                      'yuwen':[ 90,87,67,77,77,86],
                      'shuxue':[76,76,85,96,68,98],
                       'y':[1,1,0,0,1,0]})

print(data02)

