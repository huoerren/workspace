import numpy as np
from sklearn.linear_model import LinearRegression

coef0 = np.array([5,6,7,8,9,10,11,12])
X1 = np.random.rand(100,8)
y= np.dot(X1,coef0)+ np.random.normal(0,1.5,size=100)
training = np.random.choice([True,False],p=[0.8,0.2],size=100)
lr1 = LinearRegression()
lr1.fit(X1[training],y[training])
# 系数的均方误差MSE
print(((lr1.coef_-coef0)**2).sum()/8)
# 测试集准确率（R2）
print(lr1.score(X1[~training],y[~training]))


X2 = np.column_stack([X1,np.dot(X1[:,[0,1]],np.array([1,1]))+np.random.normal(0,0.05,size=100)])
X2 = np.column_stack([X2,np.dot(X2[:,[1,2,3]],np.array([1,1,1]))+np.random.normal(0,0.05,size=100)])
X3 = np.column_stack([X1,np.random.rand(100,2)])

import matplotlib.pyplot as plt
clf = LinearRegression()
vif2 = np.zeros((10,1))
for i in range(10):
    tmp=[k for k in range(10) if k!=i]
clf.fit(X2[:,tmp],X2[:,i])
vifi = 1/ (1-clf.score(X2[:,tmp],X2[:,i]))
vif2[i] = vifi

plt.figure()
ax = plt.gca()
ax.plot(vif2)
#ax.plot(vif3)
plt.xlabel('feature')
plt.ylabel('VIF')
plt.title('VIF coefficients of the features')
plt.axis('tight')
plt.show()
