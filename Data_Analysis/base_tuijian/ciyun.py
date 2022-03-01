import matplotlib.pyplot as plt
import jieba
from wordcloud import WordCloud

#1.读出歌词
text = open('C:/Users/panxin/Desktop/demo/demo.txt','r' ).read()
#2.把歌词剪开
cut_text = jieba.cut(text)
# print(type(cut_text))
# print(next(cut_text))
# print(next(cut_text))
#3.以空格拼接起来
result = " ".join(cut_text)
# print(result)
# 4.生成词云
wc = WordCloud(
    font_path='simhei.ttf',     #字体路劲
    background_color='white',   #背景颜色
    width=1100,
    height=600,
    max_font_size=60,            #字体大小
    min_font_size=10,
    mask=plt.imread('C:/Users/panxin/Desktop/demo/beijingtu/demo_03.jpg'),  #背景图片
    max_words=1000
)

# print(result)

words_list = []
word_generator = jieba.cut(result, cut_all=False)  # 返回的是一个迭代器

print(type(word_generator))

# 添加 stopword
with open('C:/Users/panxin/Desktop/demo/chineseStopWords.txt') as f:
    str_text = f.read()
    unicode_text =  str_text.encode('unicode_escape').decode()
    f.close()  # stopwords文本中词的格式是'一词一行'

for word in word_generator:
    if word.strip() not in unicode_text:
        words_list.append(word)


result =  ' '.join(words_list)  # 注意是空格

wc.generate(result)
wc.to_file('C:/Users/panxin/Desktop/demo/jielun.png')    #图片保存

#5.显示图片
plt.figure('jielun')   #图片显示的名字
plt.imshow(wc)
plt.axis('off')        #关闭坐标
plt.show()