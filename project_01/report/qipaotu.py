import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

#
# def DrawBubble(read_name):#气泡图
#
#     # sns.set(style = "whitegrid")#设置样式
#     fp = pd.read_excel(read_name)#数据来源
#     x = fp.people#X轴数据
#     y = fp.price#Y轴数据
#     z = fp.price#用来调整各个点的大小s
#     # cm = plt.cm.get_cmap('RdYlBu')  cmap = cm,
#     fig,ax = plt.subplots(figsize = (12,10))
#     #注意s离散化的方法，因为需要通过点的大小来直观感受其所表示的数值大小
#     #我所使用的是当前点的数值减去集合中的最小值后+0.1再*1000
#     #参数是X轴数据、Y轴数据、各个点的大小、各个点的颜色
#     bubble = ax.scatter(x, y , s = (z - np.min(z) + 0.1) * 1000, c = z)
#     fig.colorbar(bubble)
#     ax.set_xlabel('people of cities', fontsize = 15)#X轴标签
#     ax.set_ylabel('price of something', fontsize = 15)#Y轴标签
#     plt.show()
#
#
# if __name__=='__main__':
#
#     DrawBubble("C:\\Users\\Administrator\\Desktop\\nihao.xlsx")#气泡图
#



import numpy as np

a= np.array([12,13,17,28,39])
print(a )