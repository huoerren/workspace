#coding=utf-8


import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_columns',100)
pd.set_option('display.width',1000)

import numpy as np

import warnings
warnings.simplefilter('ignore')

import matplotlib.pyplot as plt
import seaborn as sns

import os
os.chdir('C:/Users/Administrator/Desktop')

from sklearn import tree
from sklearn.tree import DecisionTreeClassifier

from sklearn.model_selection import  cross_val_score

data = pd.read_csv('processed_data.csv' , index_col=0)

class_0 = data['Churn'].value_counts()[0]/ len(data)
class_1 = data['Churn'].value_counts()[1]/ len(data)

#

def decisionTree(data , criterion,max_depth):
    x = data.drop(columns=['Churn'], axis=1)
    y = data['Churn'].to_frame()

    clf = DecisionTreeClassifier(criterion=criterion ,max_depth=max_depth)
    clf.fit(x, y)
    dot_data = tree.export_graphviz(clf, out_file=None,
                                    feature_names=x.columns,
                                    class_names=['0', '1'],
                                    filled=True, rounded=True,
                                    special_characters=True)
    accuracy = np.mean(cross_val_score(clf,x,y,scoring='accuracy',cv=10))
    balanced_accuracy = np.mean(cross_val_score(clf,x,y,scoring='balanced_accuracy' ,cv = 10))
    f1_score = np.mean(cross_val_score(clf ,x ,y ,scoring='f1',cv = 10 ))
    print(round(accuracy,2) , round(balanced_accuracy,2),round(f1_score,2))


decisionTree(data,'entropy',7)












