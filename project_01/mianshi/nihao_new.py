#coding=utf-8


import warnings
warnings.filterwarnings('ignore')

import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_columns',100)
pd.set_option('display.width',1000)

from matplotlib import pyplot as plt
plt.rcParams['font.family']="SimHei"
import seaborn as sns

tips = sns.load_dataset("tips")
plt.figure(figsize=(5, 5))
sns.lmplot(x="total_bill",y="tip",hue="sex",data=tips)

plt.show()

