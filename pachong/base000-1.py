#coding=utf-8

import json
import re

# contentStr = ''
# with open('E:/data/pubmed-dataset/test2.txt', 'r') as f:
#     contentStr = f.readline()
#     print(contentStr)
#     print('----------------')

def writeLine(lineContent):
    pattern1 = re.compile(r'"article_text":(.*?)"abstract_text"', re.S)
    pattern2 = re.compile(r'"abstract_text":(.*?)"labels"', re.S)

    lis = re.findall(pattern1, lineContent)
    lis2= re.findall(pattern2, lineContent)
    if lis != None:
        con = lis[0]
        f = open('D:/taobao/data/now/val-abstract_text.txt', 'a')
        f.write(con.replace('</S>",','').replace('<S>','').replace('<S>,','')
                .replace('</S>", "<S>','').replace('["','').replace(']','').replace('[','')
                .replace('\"','').replace(' </S>\", ','') + "")
        f.close()

    if lis2 != None:
        con2=lis2[0]
        f2 = open('D:/taobao/data/now/val-article_text.txt', 'a')
        f2.write(con2.replace('</S>",','').replace('<S>','').replace('<S>,','')
                 .replace('</S>", "<S>','').replace('["','').replace(']','').replace('[','')
                 .replace('\"','').replace(' </S>\", ','')  + "")
        f2.close()

    print('-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-')


def main():
    f = open('D:/taobao/data/pubmed-dataset/val.txt')
    while True:
        line = f.readline()
        if not line: break
        writeLine(line)
    f.close()


# cons = re.findall('"abstract_text":(.*?)"labels"',contentStr)
# # print(type(cons))
# nimeide = "".join(cons)
#
# with open('E:/xiugai/test-abstract_text.txt','w+') as f:
#     f.write(nimeide)

if __name__ == '__main__':
    print('++++++++++++++++++++++++')
    main()