#coding=utf-8

import matplotlib.pyplot as plt
import numpy as np
import os
import csv
import pandas as pd
import warnings
warnings.simplefilter('ignore')


os.chdir('C:/Users/Administrator/Desktop')

data = pd.read_csv('processed_data.csv' , index_col=0)
y = data['Churn'].to_frame()
data = data.drop('Churn' ,axis= 1)

from sklearn.model_selection import train_test_split
X_train,X_test,y_train,y_test = train_test_split(data,y,test_size=0.2,random_state= 7)

#开始建模

from sklearn import linear_model
from sklearn.metrics import accuracy_score

clf = linear_model.LogisticRegression(C=1.0,penalty='l2',tol=1e-6)
clf.fit(X_train,y_train)
predictions = clf.predict(X_test)
# print(accuracy_score(predictions,y_test))

# ration = y['Churn'].value_counts().to_frame()
# print(ration.loc[0])

clf = linear_model.LogisticRegression(C=1.0,penalty='l2',
                                      tol=1e-6,class_weight='balanced')
clf.fit(X_train,y_train)
predictions = clf.predict(X_test)
# print(accuracy_score(predictions,y_test))

clf = linear_model.LogisticRegression(C=1.0,penalty='l2',tol=1e-6)
clf.fit(X_train,y_train)
predictions = clf.predict(X_test)


from sklearn.metrics import roc_curve
clf_pre_1 = clf.predict_proba(X_test)[:,1]
fpr,tpr,thresholds = roc_curve(y_test,clf_pre_1)

# print(clf.predict_proba(X_test))
# print(clf.classes_)

from sklearn.metrics import roc_auc_score
logit_roc_auc = roc_auc_score(y_test,predictions)

plt.plot(fpr,tpr,label='ROC curve(area = %0.2f)'%logit_roc_auc)
plt.plot([0,1],[0,1],'k--')
plt.xlim([0.0,1.0])
plt.ylim([0.0,1.0])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.legend(loc='lower right')

plt.scatter(thresholds,y = thresholds,label='Threshold')
# plt.legend()




from sklearn.metrics import confusion_matrix
import seaborn as sns


array = confusion_matrix(y_test,predictions)
cm = pd.DataFrame(array,index=[0,1],columns=[0,1])
plt.figure()
plt.title('Confusion Matrix')
sns.heatmap(cm , annot = True ,cmap = plt.cm.Blues)

fpr,tpr,thresholds = roc_curve(y_test,clf.predict_proba(X_test)[:,1])
df = pd.DataFrame({'fpr':fpr,'tpr':tpr ,'thresholds':thresholds })
df['ks_value'] = abs(df['tpr'] - df['fpr'])
ks_value = df.ks_value.max()
# print(df.head())
print(df.ks_value.max())

# plt.show()



