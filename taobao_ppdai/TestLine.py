
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.datasets import load_iris
from sklearn.metrics import roc_auc_score,roc_curve
import matplotlib.pyplot as plt
import numpy as np

iris=load_iris()
#将iris的三类数据转化为二类数据,labels=1与labels=0合并为0，labels=2转化为1

iris.target[iris.target==1],iris.target[iris.target==2]= 0,1

x_train,x_test,y_train,y_test = train_test_split(iris.data,iris.target,test_size=0.3, random_state=0) #拆分训练集与测试集

model = LogisticRegression(solver='newton-cg',multi_class='ovr')    #创建模型
model.fit(x_train,y_train)  #传入训练数据

#预测测试数据的lr概率值，返回i*j列的数据，i为样本数,j为类别数,ij表示第i个样本是j类的概率；第i个样本的所有类别概率和为1。
# 这里不能用model.predict()，因为输出的是0或1，并不是概率值，不能对后续的roc曲线进行计算
#另外model._predict_proba_lr可以用来计算的lr概率值
y_pre=model.predict_proba(x_test)
y_0=list(y_pre[:,1])    #取第二列数据，因为第二列概率为趋于0时分类类别为0，概率趋于1时分类类别为1

print(y_0)
print('==================')
print(y_test)

fpr,tpr,thresholds = roc_curve(y_test,y_0)  #计算fpr,tpr,thresholds
auc=roc_auc_score(y_test,y_0) #计算auc

#画曲线图
plt.figure()
plt.plot(fpr,tpr)
plt.title('$ROC curve$')
plt.show()

#
# #计算ks
# KS_max=0
# best_thr=0
# for i in range(len(fpr)):
#     if(i==0):
#         KS_max=tpr[i]-fpr[i]
#         best_thr=thresholds[i]
#     elif (tpr[i]-fpr[i]>KS_max):
#         KS_max = tpr[i] - fpr[i]
#         best_thr = thresholds[i]
#
# print('最大KS为：',KS_max)
# print('最佳阈值为：',best_thr)
#
