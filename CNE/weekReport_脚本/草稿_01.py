#coding=utf-8

import pymysql
import re

import pandas as pd
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 10000000)
pd.set_option('display.max_columns', 10000000)
pd.set_option('max_colwidth', 10000000)

import numpy as np
import os
import matplotlib.pyplot as plt
import seaborn as sns

plt.rcParams['font.sans-serif'] = ['SimHei'] # 用来正常显示中文标签
plt.rcParams['font.family']='sans-serif'
plt.rcParams['axes.unicode_minus'] = False # 用来正常显示负号


df = pd.DataFrame({"数量":[1 ,6160 ,27229 ,8141 ,10942 ,7532 ]  })

# sns.catplot(x='数量' , data=df , dodge=True)
sns.boxplot(x='数量' , data=df  )
plt.show()


