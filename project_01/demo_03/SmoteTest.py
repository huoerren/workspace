#coding=utf-8

import pandas as pd

pd.set_option('expand_frame_repr',False)
pd.set_option('display.max_columns',100)
pd.set_option('display.width',1000)

from collections import Counter
from sklearn.datasets import make_classification


X,y = make_classification(n_classes=2 , class_sep=2 ,
                          weights=[0.8,0.2],
                          n_informative= 3 ,
                          n_samples= 1000)


print(Counter(y))

from imblearn.over_sampling import SMOTE

smo = SMOTE(random_state=100)
X_smo , y_smo = smo.fit_sample(X,y)

print(Counter(y_smo))

smo = SMOTE(sampling_strategy= 0.5 , random_state=123)
X_smo , y_smo = smo.fit_sample(X,y)
print(Counter(y_smo))

smo = SMOTE(sampling_strategy={0:900,1:700} ,random_state=123)
X_smo , y_smo = smo.fit_sample(X,y)
print(Counter(y_smo))






