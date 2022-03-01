# -*- coding:utf-8 -*-
# Author:Bemyid
from numpy import log
from pandas import DataFrame as df
import pandas as pd


def createDateset():
    dataSet = [
        [0, 1, 0],
        [0, 0, 0],
        [0, 1, 0],
        [1, 0, 1],
        [1, 0, 0],
        [1, 1, 1],
        [0, 1, 1],
        [1, 1, 1],
        [1, 0, 1],
        [1, 0, 1]]
    return dataSet


def calcWOE(dataset, col, targe):
    subdata = df(dataset.groupby(col)[col].count())
    suby    = df(dataset.groupby(col)[targe].sum())
    data    = df(pd.merge(subdata, suby, how="left", left_index=True, right_index=True))
    b_total = data[targe].sum()
    total   = data[col].sum()
    g_total = total - b_total
    data["bad"] = data.apply(lambda x: round(x[targe] / b_total, 3), axis=1)
    data["good"] = data.apply(lambda x: round((x[col] - x[targe]) / g_total, 3), axis=1)
    data["WOE"] = data.apply(lambda x: log(x.bad / x.good), axis=1)
    return data.loc[:, ["bad", "good", "WOE"]]


def calcIV(dataset):
    dataset["IV"] = dataset.apply(lambda x: (x.bad - x.good) * x.WOE, axis=1)
    IV = sum(dataset["IV"])
    return IV


if __name__ == '__main__':
    data = createDateset()
    data = df(data, columns=["x1", "x2", "y"])
    data_WOE = calcWOE(data, "x1", "y")
    print(data_WOE)
    # print('======================')
    # data_IV = calcIV(data_WOE)
    # print(data_IV)


