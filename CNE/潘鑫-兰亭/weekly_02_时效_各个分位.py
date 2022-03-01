
import pandas as pd

import os
import re

def getAllShiXiao():

    # 判断 文件夹中所存在的Des
    df_ruku_chuku      = pd.read_csv('01_入库-出库.csv')
    df_chuku_zhuangche  = pd.read_csv('02_出库-装车.csv')

    # zhuangche = pd.read_csv('03_装车-出口清关.csv')
    # qifei = pd.read_csv('04_出口清关-起飞.csv')

    df_zhuangche_qifei = pd.read_csv('03_02_装车-起飞.csv')


    df_qifei_luodi    = pd.read_csv('05_起飞-落地.csv')
    df_luodi_qingguan = pd.read_csv('06_落地-进口清关.csv')

    df_qingguan_jiaofu = pd.read_csv('07_进口清关-交付.csv')
    df_jiaofu_tuotou   = pd.read_csv('08_交付-妥投.csv')

    df_shousao_tuotou = pd.read_csv('09_首扫-妥投.csv')

    # 首扫-封袋

    print('**************** 首扫-封袋 start  ; *********** ')
    # shoudao_fengdai = pd.merge(shousao, fengdai[["内单号", "封袋时间"]], left_on="内单号", right_on="内单号", how='inner')
    df_ruku_chuku['首扫时间'] = pd.to_datetime(df_ruku_chuku['首扫时间'])
    df_ruku_chuku['封袋时间'] = pd.to_datetime(df_ruku_chuku['封袋时间'])

    df_ruku_chuku["用时"] = (df_ruku_chuku["封袋时间"] - df_ruku_chuku["首扫时间"]).astype('timedelta64[s]')
    df_ruku_chuku["用时"] = round(df_ruku_chuku["用时"] / 86400, 2)

    dataframe01 = []
    for df1, df2 in df_ruku_chuku.groupby(['channel_name', 'des']):
        df3 = df2.sort_values(by=["用时"], ascending=True)
        dataframe01.append(['首扫-封袋', df1, df3.shape[0], df3.describe(percentiles=[0.1])['用时']['10%']] )
    df_shousao_fengdai = pd.DataFrame(dataframe01, columns=['阶段','分类','数量','10分位用时'] )
    print(df_shousao_fengdai)
    df_ruku_chuku.to_csv('new_01_入库-出库.csv', index=False, encoding="utf_8_sig")

    # print('**************** 首扫-封袋 end ; *********** ')

    print('**************** 封袋-装车 start ; *********** ')
    # 封袋-装车
    # fengdai_zhuangche = pd.merge(fengdai, zhuangche[["内单号", "装车时间"]], left_on="内单号", right_on="内单号", how='inner')
    df_chuku_zhuangche['装车时间'] = pd.to_datetime(df_chuku_zhuangche['装车时间'])
    df_chuku_zhuangche['封袋时间'] = pd.to_datetime(df_chuku_zhuangche['封袋时间'])

    df_chuku_zhuangche["用时"] = (df_chuku_zhuangche["装车时间"] - df_chuku_zhuangche["封袋时间"]).astype('timedelta64[s]')
    df_chuku_zhuangche["用时"] = round(df_chuku_zhuangche["用时"] / 86400, 2)


    dataframe02 = []
    for df1, df2 in df_chuku_zhuangche.groupby(['channel_name', 'des']):
        df3 = df2.sort_values(by=["用时"], ascending=True)
        # print(df3.describe(percentiles=[0.9])['用时'] )
        dataframe02.append(['封袋-装车' , df1, df3.shape[0], df3.describe(percentiles=[0.1])['用时']['10%']] )
    df_fengdai_zhuangche = pd.DataFrame(dataframe02, columns=['阶段','分类','数量','10分位用时'] )
    print(df_fengdai_zhuangche)
    df_chuku_zhuangche.to_csv('new_02_出库-装车.csv', index=False, encoding="utf_8_sig")

    # print('**************** 封袋-装车 : end ****************')

    # 装车-起飞
    print('**************** 装车-起飞 : start  ****************')
    # qifei_zhuangche = pd.merge(zhuangche, qifei[["内单号", "起飞时间"]], left_on="内单号", right_on="内单号", how='inner')
    df_zhuangche_qifei['装车时间'] = pd.to_datetime(df_zhuangche_qifei['装车时间'])
    df_zhuangche_qifei['起飞时间'] = pd.to_datetime(df_zhuangche_qifei['起飞时间'])

    df_zhuangche_qifei["用时"] = (df_zhuangche_qifei["起飞时间"] - df_zhuangche_qifei["装车时间"]).astype('timedelta64[s]')
    df_zhuangche_qifei["用时"] = round(df_zhuangche_qifei["用时"] / 86400, 2)

    dataframe03 = []
    for df1, df2 in df_zhuangche_qifei.groupby(['channel_name', 'des']):
        df3 = df2.sort_values(by=["用时"], ascending=True)
        dataframe03.append([ '装车-起飞', df1,  df3.shape[0], df3.describe(percentiles=[0.1])['用时']['10%']] )
    df_zhuangche_qifei_2 = pd.DataFrame(dataframe03, columns=['阶段','分类', '数量', '10分位用时'])
    print(df_zhuangche_qifei_2)
    df_zhuangche_qifei.to_csv('new_03_02_装车-起飞.csv', index=False, encoding="utf_8_sig")
    # print('**************** 装车-起飞 : end -----------')

    # 起飞-落地
    print('**************** 起飞-落地 : start  ****************')
    # qifei_luodi = pd.merge(qifei, luodi[["内单号", "落地时间"]], left_on="内单号", right_on="内单号", how='inner')
    df_qifei_luodi['落地时间'] = pd.to_datetime(df_qifei_luodi['落地时间'])
    df_qifei_luodi['起飞时间'] = pd.to_datetime(df_qifei_luodi['起飞时间'])

    df_qifei_luodi["用时"] = (df_qifei_luodi["落地时间"] - df_qifei_luodi["起飞时间"]).astype('timedelta64[s]')
    df_qifei_luodi["用时"] = round(df_qifei_luodi["用时"] / 86400, 2)

    dataframe04 = []
    for df1, df2 in df_qifei_luodi.groupby(['channel_name', 'des']):
        df3 = df2.sort_values(by=["用时"], ascending=True)
        # print(df3.describe(percentiles=[0.9])['用时'] )
        dataframe04.append(['起飞-落地', df1,  df3.shape[0],  df3.describe(percentiles=[0.1])['用时']['10%']])
    df_qifei_luodi_2 = pd.DataFrame(dataframe04, columns=['阶段','分类', '数量', '10分位用时'])
    print(df_qifei_luodi_2)
    # print('**************** 起飞-落地 : end -----------')
    df_qifei_luodi.to_csv('new_05_起飞-落地.csv', index=False, encoding="utf_8_sig")
    print('**************** 落地-清关 : start  ****************')

    # 落地-清关
    # luodi_qingguan = pd.merge(luodi, qingguan[["内单号", "进口清关时间"]], left_on="内单号", right_on="内单号", how='inner')
    df_luodi_qingguan['落地时间'] = pd.to_datetime(df_luodi_qingguan['落地时间'])
    df_luodi_qingguan['进口清关时间'] = pd.to_datetime(df_luodi_qingguan['进口清关时间'])

    df_luodi_qingguan["用时"] = (df_luodi_qingguan["进口清关时间"] - df_luodi_qingguan["落地时间"]).astype('timedelta64[s]')
    df_luodi_qingguan["用时"] = round(df_luodi_qingguan["用时"] / 86400, 2)

    dataframe05 = []
    for df1, df2 in df_luodi_qingguan.groupby(['channel_name', 'des']):
        df3 = df2.sort_values(by=["用时"], ascending=True)
        dataframe05.append([ '落地-清关' ,df1, df3.shape[0],  df3.describe(percentiles=[0.1])['用时']['10%']] )
    df_luodi_qingguan_2 = pd.DataFrame(dataframe05, columns=['阶段','分类', '数量', '10分位用时'])
    print(df_luodi_qingguan_2)
    df_luodi_qingguan.to_csv('new_06_落地-进口清关.csv', index=False, encoding="utf_8_sig")
    # print('**************** 落地-清关 : end -----------')

    print('**************** 清关-交付 : start  ****************')
    # 清关-交付
    # qingguan_jiaofu = pd.merge(qingguan, jiaofu[["内单号", "交付时间"]], left_on="内单号", right_on="内单号", how='inner')
    df_qingguan_jiaofu['交付时间'] = pd.to_datetime(df_qingguan_jiaofu['交付时间'])
    df_qingguan_jiaofu['进口清关时间'] = pd.to_datetime(df_qingguan_jiaofu['进口清关时间'])

    df_qingguan_jiaofu["用时"] = (df_qingguan_jiaofu["交付时间"] - df_qingguan_jiaofu["进口清关时间"]).astype('timedelta64[s]')
    df_qingguan_jiaofu["用时"] = round(df_qingguan_jiaofu["用时"] / 86400, 2)
    dataframe06 = []
    for df1, df2 in df_qingguan_jiaofu.groupby(['channel_name', 'des']):
        df3 = df2.sort_values(by=["用时"], ascending=True)
        # print(df3.describe(percentiles=[0.9])['用时'] )
        dataframe06.append([ '清关-交付',df1,  df3.shape[0],  df3.describe(percentiles=[0.1])['用时']['10%']] )
    df_qingguan_jiaofu_2 = pd.DataFrame(dataframe06, columns=['阶段','分类', '数量', '10分位用时'])
    print(df_qingguan_jiaofu_2)
    df_qingguan_jiaofu.to_csv('new_07_进口清关-交付.csv', index=False, encoding="utf_8_sig")
    # print('**************** 清关-交付 : end -----------')

    print('**************** 交付-妥投 : start  ****************')
    # 交付-妥投
    # df_jiaofu_tuotou = pd.merge(jiaofu, tuotou[["内单号", "妥投时间"]], left_on="内单号", right_on="内单号", how='inner')
    df_jiaofu_tuotou['交付时间'] = pd.to_datetime(df_jiaofu_tuotou['交付时间'])
    df_jiaofu_tuotou['妥投时间'] = pd.to_datetime(df_jiaofu_tuotou['妥投时间'])

    df_jiaofu_tuotou["用时"] = (df_jiaofu_tuotou["妥投时间"] - df_jiaofu_tuotou["交付时间"]).astype('timedelta64[s]')
    df_jiaofu_tuotou["用时"] = round(df_jiaofu_tuotou["用时"] / 86400, 2)

    dataframe07 = []
    for df1, df2 in df_jiaofu_tuotou.groupby(['channel_name', 'des']):
        df3 = df2.sort_values(by=["用时"], ascending=True)
        # print(df3.describe(percentiles=[0.9])['用时'] )
        dataframe07.append(['交付-妥投', df1, df3.shape[0], df3.describe(percentiles=[0.1])['用时']['10%']] )

    df_jiaofu_tuotou_2 = pd.DataFrame(dataframe07, columns=['阶段','分类', '数量', '10分位用时'])
    print(df_jiaofu_tuotou_2)

    df_jiaofu_tuotou.to_csv('new_08_交付-妥投.csv', index=False, encoding="utf_8_sig")
    # print('**************** 交付-妥投 : end -----------')

    print('**************** 首扫-妥投 : start  ****************')
    # 交付-妥投
    # df_jiaofu_tuotou = pd.merge(jiaofu, tuotou[["内单号", "妥投时间"]], left_on="内单号", right_on="内单号", how='inner')
    df_shousao_tuotou['首扫时间'] = pd.to_datetime(df_shousao_tuotou['首扫时间'])
    df_shousao_tuotou['妥投时间'] = pd.to_datetime(df_shousao_tuotou['妥投时间'])

    df_shousao_tuotou["用时"] = (df_shousao_tuotou["妥投时间"] - df_shousao_tuotou["首扫时间"]).astype('timedelta64[s]')
    df_shousao_tuotou["用时"] = round(df_shousao_tuotou["用时"] / 86400, 2)

    dataframe09 = []
    for df1, df2 in df_shousao_tuotou.groupby(['channel_name', 'des']):
        df3 = df2.sort_values(by=["用时"], ascending=True)
        # print(df3.describe(percentiles=[0.9])['用时'] )
        dataframe09.append(['首扫-妥投', df1, df3.shape[0], df3.describe(percentiles=[0.1])['用时']['10%']])

    df_shousao_tuotou_2 = pd.DataFrame(dataframe09, columns=['阶段', '分类', '数量', '10分位用时'])
    print(df_shousao_tuotou)
    df_shousao_tuotou.to_csv('new_09_首扫-妥投.csv', index=False, encoding="utf_8_sig")
    # print('**************** 首扫-妥投 : end -----------')

    print('--------------  合并最后的结果 ：  -----------')
    frames = [df_shousao_fengdai ,df_fengdai_zhuangche,df_zhuangche_qifei_2,df_qifei_luodi_2,df_luodi_qingguan_2,df_qingguan_jiaofu_2,df_jiaofu_tuotou_2,df_shousao_tuotou_2]

    resultDf = pd.concat(frames)
    print(resultDf.to_csv('结果集.csv' , index= False, encoding="utf_8_sig"))


def getAllFilePath():
    file_dir = r'C:\Users\hp\Desktop\cujia-11data'
    for root, dirs, files in os.walk(file_dir):
        if  len(dirs)> 0  :
            pass
        else:
            print('================= DES 分割线 ======================')
            zhmodel = re.compile(u'[\u4e00-\u9fa5]')  # 检查中文
            # zhmodel = re.compile(u'[^\u4e00-\u9fa5]')  #检查非中文
            match = zhmodel.search(root)  # 如果有中文，则说说明该目录是后来手动添加的，不需要处理（已经处理过了）
            if match:
                pass
            else:
                print('root_dir:', root)  # 当前目录路径
                os.chdir(root)
                ## 切换路径后 获得所有的 时效
                getAllShiXiao()




if __name__ == '__main__':

    # 获得所有路径
    getAllFilePath()
    # 获得所有分段时效
    # getAllShiXiao()



