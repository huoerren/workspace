#coding=utf-8


from snownlp import SnowNLP
import pandas as pd
import jieba

pd.set_option('expand_frame_repr', False) # 禁止数据换行显示
pd.set_option('display.max_columns',100)
pd.set_option('display.width',1000)


def getDatas():
    df = pd.read_excel('C:/Users/panxin/Desktop/taiwan.xlsx')
    return df


def getNames(name):
    # 获得参数一
    names = []
    n_len = len(name)
    for i in range(n_len):
        names.append(name[0: n_len - i])
    return names


def dealComment(df,param1,param2):
    # 添加 stopword (详情可以 google 自然语言处理-停用词 )
    with open('C:/Users/panxin/Desktop/demo/chineseStopWords.txt') as f:
        str_text = f.read()
        unicode_text = str_text.encode('unicode_escape').decode()
        f.close()  # stopwords文本中词的格式是'一词一行'
    # fenci = [SnowNLP(i).keywords(50) for i in df.comments] # 通过 SnowNLP包 对评论文本信息进行分析挖掘

    fenci = []
    for i in df.comments:
        words_list = []
        word_generator = jieba.cut(i , cut_all=False) # 利用自然语言处理包 jieba 将‘评论’ 剪开
        for word in word_generator:
            if word.strip() not in unicode_text:
                words_list.append(word)
        fenci.append(words_list)

    s = SnowNLP(fenci)
    print('=================================================')
    proba = df.comments.map(lambda x: SnowNLP(x).sentiments) # snownlp 会给出情感判断量化得分 [积极的 -> 1，消极的 -> 0]
    # # 保存至表格字段中
    df['proba']   = proba
    df['l_store'] = s.sim([param1,param2]) # 判断 填写词 和 每个餐厅 评论的关联性【snownlp 给出联系性的量化得分】
    df['l_store_abs'] = df['l_store'].abs()

    #打印结果 ：结果的排除法则是 第一法则:先按照 情感判断排序， 第二法则: 情感判断一致的情况下，按照给出的关键词和 该餐厅的评论的关联性得分排序
    df = df.sort_values(by=['proba','l_store_abs'],ascending=(False ,False ))
    if df.shape[0]>2:
        print(df.head(3))
    else:
        print(df)



if __name__ == '__main__':

    #加载原始数据
    df = getDatas()  # 列名 [ 'dian_name', 'dian_comment' ]
    inpStr = input("Enter your input: ");
    args = inpStr.split('；')
    names = getNames(args[0])  # 获得 参数一 相适配的 餐厅名称集合

    dianNames = [] # 存放不重复的店铺名称
    datas = []     # 存放最后要处理的数据

    if len(names)>0 :
        for i in names:
            dList = df[df.dian_name.str.contains(i)].values.tolist() # 将店名模糊匹配的得到的店铺转为集合形式
            #遍历集合
            if len(dList) >0:
                for j in dList:  # j的数据格式是 [店名，该店平价]
                    if j[0] in dianNames:
                        continue
                    else:
                        dianNames.append(j[0])
                        datas.append(j)

    # 将待处理的数据转为 dataframe 类型
    if len(datas) >0:
        df_name_comments = pd.DataFrame(datas)
        df_name_comments.columns = ['name', 'comments']
        data_recomend = dealComment(df_name_comments,args[1],args[2] )
    else:
        print('抱歉，本系统暂无该数据相关记录！')






