#coding='utf-8'


# 加载情感分析模块
from snownlp import SnowNLP
from snownlp import sentiment
import  pandas as pd


pd.set_option('expand_frame_repr', False) # 禁止数据换行显示
pd.set_option('display.max_columns',100)
pd.set_option('display.width',1000)

# text = pd.read_excel("C:/Users/panxin/Desktop/demo/sfds.xlsx")

# df = df[['Merchant','Content_review']]
# print(df.head())


test_dict = {'comment':['这件衣服真的太好了',
                        '其实都还可以，就是价格有点贵',
                        '服务态度太差了',
                        '我很喜欢，价格划算',
                        '给妈妈买的衣服，她很开心'],
             'label':[None,None,None,None,None],
             'True':[1,-1,-1,1,1]
             }

text = pd.DataFrame(test_dict)
text.columns=['comment', 'predict', 'True_cls']
print(text)
print('-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-')

# 本文将此保存文件移至原模型处，并修改了__init__.py
# sentiment.train('C:/anaconda_3.6/Lib/site-packages/snownlp-0.12.3-py3.6.egg/snownlp/sentiment/neg.txt',
#                 'C:/anaconda_3.6/Lib/site-packages/snownlp-0.12.3-py3.6.egg/snownlp/sentiment/pos.txt')
# sentiment.save("./sentiment_my.marshal")

proba = text.comment.map(lambda x : SnowNLP(x).sentiments)
# # 保存至表格字段中
text['proba'] = proba

# print(text)

fenci = [SnowNLP(i).keywords() for i in text.comment]
print('**********************************************')
# tt=[["今天星期日，又到了一个周末，可是我还要加班，非常的生气不开心！"],
#     ['还好今天星期一要上班，不然周末该如何是好'],
#     ['周末来啦，还是很开心呢'],
#     ['周末还是来了！'],
#     ['相信未来一切光明。'],
#     ['周末来啦'],
#     ['周末来啦，还是很开心呢'],
#     ['周末来啦，还是很开心呢']   ]
#
# print(SnowNLP(tt).sim(['周末了，开心']))


# 计算BM2.5 相似性
# s = SnowNLP([[u'这篇', u'文章',u'非常好'],
#              [u'那篇', u'文章',u'一般般'],
#              [u'这个']])
# s.tfs.idf

s = SnowNLP(fenci)
print(s.sim(['衣服','漂亮']))# []

print('@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')

# 使用map高阶函数速度要比for循环快

text['proba'].map(lambda x: 1 if x > 0.6 else -1)
predict = []
for i in proba:
    if i > 0.6:
        predict.append(1)
    else:
        predict.append(-1)

text['predict'] = predict
print(text)




