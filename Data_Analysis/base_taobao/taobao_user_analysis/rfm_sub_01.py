#coding=utf-8


import  pandas as pd
import numpy  as np


# filePath = 'E:/数据/tianchi_mobile_recommend_train_user/tianchi_mobile_recommend_train_user.csv'
#
# tb = pd.read_csv(filePath)
# print(tb.shape)                    # (12256906, 6)
# print(tb.duplicated().sum())       # 4092866
# tb.drop_duplicates(inplace= True)    #(8164040, 6)
# tb.dropna(axis= 0 )

def behavior_type(type):
    if type == 1:
        return 'pv'
    elif type == 2:
        return 'favor'
    elif type == 3:
        return 'cart'
    else:
        return 'buy'

# tb['behavior_type'] = tb['behavior_type'].apply(behavior_type)
# tb['date'] = tb['time'].apply(lambda x:x.split(' ')[0])
# tb['time'] = tb['time'].apply(lambda x:x.split(' ')[1])
#
# print(tb.head())

# tb.to_csv('E:/数据/tianchi_mobile_recommend_train_user/tb_user_2014.csv' ,index= False )
# print(tb.drop_duplicates('user_id').count())


# 用户行为的漏斗模型
filePath = 'E:/数据/tianchi_mobile_recommend_train_user/tb_user_2014.csv'
tb = pd.read_csv(filePath)
tb_behavior_type = tb['behavior_type'].value_counts().reset_index()
# print(tb_behavior_type) # pv ,cart ,favor  ,buy

# zhongcaolv = (tb_behavior_type['favor'] + tb_behavior_type['cart'] ) / tb_behavior_type['pv']
# goumailv = tb_behavior_type['buy'] / tb_behavior_type['pv']

tb_buy = tb[tb['behavior_type'] == 'buy']
# print(tb_buy.drop_duplicates('user_id').count()/10000) # 得： 88.6% 是付费用户
tb_two_buy = tb_buy.groupby(['user_id']).count() # 这种聚合是 “次数” 的 聚合。
tb_two_buy[tb_two_buy['behavior_type'] >= 2].count() / 1000  # 获得复购率 81.7%
# print(tb_two_buy.sort_values('behavior_type' ,ascending= False)['behavior_type'].mean())
# print(tb_two_buy.sort_values('behavior_type' ,ascending= False)['behavior_type'].describe())
# print(tb_two_buy.sort_values('behavior_type' , ascending= False)['behavior_type'].median())

tb.to_csv(filePath ,index=False)
tb_buy.to_csv('E:/数据/tianchi_mobile_recommend_train_user/tb_buy.csv',index= False)
tb_buy['date'] = tb_buy['date'].apply(pd.to_datetime) # 将字符串时间字段变为datatime 类型，方便后续时间间隔计算

# 构造 R 值
tb_buy_rfm = tb_buy[['user_id','date']]
# print(tb_buy_rfm)
r = tb_buy_rfm.groupby('user_id')['date'].max().reset_index() # 获得每个用户最近的一次购物时间
r['R'] = (pd.to_datetime('2014-12-19') - r['date']).dt.days # 获得每个用户最近的一次购物时间 距离 2014-12-19 的间隔

buy_tb = tb_buy.groupby('user_id')['behavior_type'].count().reset_index()
# print(buy_tb['behavior_type'].describe())
tb_buy_rfm['日期标签'] = tb_buy_rfm['date'].astype(str)
dup_f = tb_buy_rfm.groupby(['user_id','日期标签'])['date'].count().reset_index()
f = dup_f.groupby('user_id')['date'].count().reset_index()
f.columns = ['user_id' , 'frequency_buy']

# 集合 R值和F值

rfm = pd.merge(r,f ,left_on='user_id' ,right_on= 'user_id' ,how= 'inner')
rfm['R_score'] = pd.cut(rfm['R'],bins=[0,3,5,10,20,50] , labels =[5,4,3,2,1] , right= False).astype(float)
rfm['F_score'] = pd.cut(rfm['frequency_buy'] ,bins=[1,2,6,10,15,50] ,labels=[1,2,3,4,5] ,right= False).astype(float)

rfm['R是否大于均值'] = (rfm['R'] > rfm['R'].mean()) * 1
rfm['F是否大于均值'] = (rfm['frequency_buy'] > rfm['frequency_buy'].mean()) * 1

rfm['user_value'] = (rfm['R是否大于均值'] * 10 )+ (rfm['F是否大于均值'] * 1 )

def user_value (x):
    if x == 0:
        return '重要挽留客户'
    elif x == 1:
        return '重要保持客户'
    elif x == 10:
        return '重要发展客户'
    else:
        return '重要价值客户'

rfm['user_category'] = rfm['user_value'].apply(user_value)
rfm_user_cat = rfm['user_category'].value_counts().reset_index()
print(rfm_user_cat.head())

'''
重要价值的用户比较少，但是却是一个比较优质的客户，所以可以有针对性地给这类客户提供 VIP服务，比如现在的淘宝VIP会员卡等等。
重要挽留客户占比也很大，对于这类客户，最近消费时间间隔较远，并且消费频次低，我们需要主动联系客户，调查清楚哪里出现了问题，
    比如通过短信，邮件，APP推送等唤醒客户。对于重要发展客户，消费频次低，我们需要提升他的消费频率，可以通过优惠券叠加等活动来刺激消费；
重要保持客户，消费时间间隔较远，但是消费频次高，有可能就是需要买东西的时候，就高频购买，不需要就不再购物，对于这类客户，需要主动联系，
    了解客户的需求，及时满足这类用户的需求。
重要发展客户，


'''




