#coding=utf-8

import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_columns',100)
pd.set_option('display.width',1000)

import csv
import warnings
warnings.simplefilter('ignore')


os.chdir('C:/Users/Administrator/Desktop')

data = pd.read_csv('processed_data.csv' , index_col=0)

x = data.drop(columns=['Churn'],axis=1)
y = data['Churn'].to_frame()

from sklearn.model_selection import train_test_split

x_train,x_test,y_train,y_test = train_test_split(x,y,test_size= 0.2,random_state= 0)

from sklearn.linear_model import LogisticRegression
lr = LogisticRegression(C=0.01,random_state= 0)
lr.fit(x_train,y_train)
y_proba = lr.predict_proba(x_test)
y_pred = lr.predict(x_test)

# 下采样 + bagging

from sklearn.ensemble import BaggingClassifier
from sklearn.model_selection import  cross_val_score

bagging = BaggingClassifier(LogisticRegression(C=0.01,class_weight='balanced'),
                            max_features=0.5,max_samples=0.5)

bagging.fit(x_train,y_train)
y_pre = bagging.predict(x_test)
y_proba = bagging.predict_proba(x_test)

scores = cross_val_score(bagging,y_pred.reshape(-1,1),
                         y_test,cv = 10)

from sklearn.metrics import roc_auc_score
from sklearn.metrics import roc_curve
fpr,tpr,thresholds = roc_curve(y_test,
                               bagging.predict_proba(x_test)[:,1])

df = pd.DataFrame({'fpr':fpr, 'tpr':tpr,'thresholds':thresholds })
df['ks_value'] = abs(df['tpr'] - df['fpr'])

#ks_diff 最大时 threshold 的值

ks_threshold = df.thresholds[df.ks_value.idxmax()]
ks_value = max(df['ks_value'])


print('ks_value is ' +str(np.round(ks_value,4)) + ' ks_threshold: ' + str(np.round(ks_threshold,4)))

# 调整 Threshold + Bagging

y_proba = bagging.predict_proba(x_test)
y_test_thr = y_proba[:,1] > ks_threshold
# print(y_test_thr)

# LR 调参


from sklearn.model_selection import KFold,cross_val_score , GridSearchCV
from sklearn.metrics import (make_scorer,accuracy_score,confusion_matrix,precision_recall_curve,
                             auc,roc_curve,roc_auc_score,recall_score,classification_report)


fold = KFold(n_splits=10,random_state= 0)
lr = LogisticRegression(class_weight='balanced')
score = make_scorer(recall_score)
c=[0.01,0.1,1,10,100] # c是正则化前的系数的倒数，其越小表示正则化强度越高
penalty = ['l1','l2']
param = {'C':c , 'penalty':penalty}
grd = GridSearchCV(lr,param,scoring=score,cv = fold)
grd.fit(x_train,y_train)
print(grd.best_score_, grd.best_params_,grd.scorer_)

import seaborn as sns

def metric (y_t,y_p,threshold):
    cm = confusion_matrix(y_t,y_p)
    ac = accuracy_score(y_t,y_p)
    rs = recall_score(y_t,y_p)
    df_cm = pd.DataFrame(cm,index=[i for i in ['0','1']],
                         columns=[k for k in ['0','1']])
    sns.heatmap(df_cm,annot=True , cmap='Blues' , fmt = 'g')
    plt.ylabel('True label')
    plt.xlabel('Predicted label')


# LR threshold 调参

lr = LogisticRegression(class_weight='balanced' ,C=0.01 ,
                   penalty='l2',random_state= 0)

lr.fit(x_train,y_train)
y_pred_undersample_proba = lr.predict_proba(x_test)
thresholds = [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]
j = 1
for i in thresholds:
    y_test_predictions_high_recall = y_pred_undersample_proba[:,1]>i # 正确时，y_test_predictions_high_recall 返回1 ；错误时，y_test_predictions_high_recall 返回 0
    plt.subplot(3,3,j)
    plt.subplots_adjust(wspace=1 ,hspace=1)
    j += 1
    metric(y_test,y_test_predictions_high_recall,i )

plt.show()









