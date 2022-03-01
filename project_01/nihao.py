#coding=utf-8


import re


def writeLine(lineContent):
    pattern = re.compile(r'nihaoma(.*?)wode',re.S)
    lis = re.findall(pattern,lineContent)
    if lis != None :
        con = lis[0]
        f = open('D:/women/test-demo.txt','a')
        f.write(con+"\r\n")
        f.close()

    print('-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-')


def main():
    f = open('D:/women/demo.txt')
    line = f.readline()
    while line:
        line = f.readline()
        print(line)
        # writeLine(line)
        print('------------------------------')
    f.close()


def main2():
    f = open('D:/women/demo.txt')
    while True:
        line = f.readline()
        if not line: break
        writeLine(line)
    f.close()


if __name__ == '__main__':
    print('=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=')
    main2()


