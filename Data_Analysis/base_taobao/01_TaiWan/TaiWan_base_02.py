#coding=utf-8

from snownlp import SnowNLP
import pymysql
import pandas as pd
import jieba


pd.set_option('expand_frame_repr', False) # 禁止数据换行显示
pd.set_option('display.max_columns',100)
pd.set_option('display.width',1000)

def getDataExisted(dataFromPage):
    data_For_Compare = []
    if len(dataFromPage) >0:
        sql = 'select dian_id , dian_name , dian_comment  from dianpu where dian_name like "%' + dataFromPage + '%"'
    # else:
    #     sql = 'select dian_id , dian_name , dian_comment  from dianpu '

    conn = pymysql.connect(host='127.0.0.1', user='root', password='panxin', port=3306, db='test', charset='utf8')
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        result = cursor.fetchall()

        # columnDes = cursor.description  # 获取连接对象的描述信息    columnNames = [columnDes[i][0] for i in range(len(columnDes))]    df = pd.DataFrame([list(i) for i in data],columns=columnNames)    return df
        # columnNames = [columnDes[i][0] for i in range(len(columnDes))]
        # df = pd.DataFrame([list(i) for i in result], columns=columnNames)
        # return df

        for i in result:
            data_For_Compare.append(list(i))
        return data_For_Compare

    except Exception as e:
        print(str(e))
    finally:
        cursor.close()
        conn.close()


def getNames(args):
    # 获得参数一
    name = args[0]
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
    inpStr = input("Enter your input: ");
    args = inpStr.split('；')
    print(args)

    datas = []
    ids = []

    names = getNames(args[0]) # 获得 参数一 相适配的 餐厅名称集合
    for name in names:
        res = getDataExisted(name) # 通过 餐厅名称 获得 数据记录ID，餐厅名称，评论 <dian_id , dian_name , dian_comment>
        for i in res:
            if i[2] in ids: # （数据库中会有重复记录，本篇通过 评论(i[2]) 去重 ）
                continue
            else:
                ids.append(i[2])
                datas.append(i)

    df = pd.DataFrame(datas) # df 为 去重后 的数据集合
    df.columns = ['id', 'name', 'comment']  # 对 数据的 列名 分别重命名
    # 由于 店名 <--> 评论 是一对多的关系，所以需要将同一家店名的多条评论集合起来,使得 店名<--->评论 形成一对一的关系
    name_comments = []
    nList = list(df['name'].unique())
    if len(nList) >0:
        for name in nList:
            comments  = ''
            commentList =  df[df['name'] == name]['comment'].tolist()
            name_comments.append([name ,' '.join(commentList)])
    df_name_comments = pd.DataFrame(name_comments)
    df_name_comments.columns=['name', 'comments']
    data_recomend = dealComment(df_name_comments,args[1],args[2] )

    print('===============================================')






