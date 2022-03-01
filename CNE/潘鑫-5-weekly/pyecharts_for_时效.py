from pyecharts.charts import *
from pyecharts  import options  as opts
# from pyecharts import Faker

def Month_12_youxian_KPI():
    bar = Bar(init_opts=opts.InitOpts(theme='light', width='2300px', height='1000px'))
    bar.add_xaxis([ 'GR','HU','LT','PL','RO','SE','SK' ])
    # stack值一样的系列会堆叠在一起
    bar.add_yaxis('90分位妥投时效',  [ 23.44,12.44,13.4,13.7,23.44,15.98,16.88 ], stack='stack1' )
    bar.add_yaxis('KPI',  [ 16,16,20,14,22,16,16
 ], stack='stack2' )

    bar.set_global_opts(xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(font_size=25)),
                        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(font_size=25)),
                        legend_opts=opts.LegendOpts(item_width=45, item_height=30,
                                                    textstyle_opts=opts.TextStyleOpts(font_family="微软雅黑",
                                                                                      font_size="23")))

    bar.set_series_opts(label_opts=opts.LabelOpts(position='inside', font_size=25, color='black'), )


    bar.reversal_axis()
    return bar



def Month_12_wish_youxian():
    bar = Bar(init_opts=opts.InitOpts(theme='light', width='2300px', height='1000px'))
    bar.add_xaxis([ 'NL','DE','IT','CZ','US','GB'
 ])
    # stack值一样的系列会堆叠在一起
    bar.add_yaxis('首扫-封袋',  [ 0.38,0.34,0.31,0.83,0.7,0.28 ], stack='stack1' , bar_min_height=50,bar_min_width=55 )
    bar.add_yaxis('封袋-装车',  [ 1.33,1.39,2.17,3.07,5.53,1.01 ], stack='stack1' , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('装车-起飞',  [ 1.84,1.18,1.09,1.12,1.69,2.36 ], stack='stack1'  , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('起飞-落地',  [ 0.49,0.65,0.55,0.53,0.82,0.57 ], stack='stack1' , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('落地-清关',  [ 3.3,2.26,3.24,2.81,2.03,1.96 ], stack='stack1' , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('清关-交付',  [ 3.21,3.78,1.89,2.17,1.07,1.04 ], stack='stack1' , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('交付-妥投',  [2.84,5.11,4.76,4.18,6.05,3.05 ], stack='stack1', bar_min_height=50,bar_min_width=55)

    bar.set_global_opts(xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts( formatter="{value}天", font_size=25),  type_='value' ),
                        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(font_size=25)),
                        legend_opts=opts.LegendOpts(item_width=45, item_height=30,
                                                    textstyle_opts=opts.TextStyleOpts(font_family="微软雅黑",
                                                                                      font_size="23")))

    bar.set_series_opts(label_opts=opts.LabelOpts(position='inside', font_size=23, color='black'), )

    bar.reversal_axis()
    return bar



def Month_12_wish_tehui():
    bar = Bar(init_opts=opts.InitOpts(theme='light', width='2300px', height='1000px'))
    bar.add_xaxis([  'LT ','IE ','HU ','PL ','SK ','AT ' ])
    # stack值一样的系列会堆叠在一起
    bar.add_yaxis('首扫-封袋',  [0.97,0.33,0.68,0.53,1.34,1.04], stack='stack1' , bar_min_height=50,bar_min_width=55 )
    bar.add_yaxis('封袋-装车',  [2.42,3.34,3.44,2.2,2.1,2.16], stack='stack1' , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('装车-起飞',  [1.88,1.12,1.16,2.22,1.26,2.23], stack='stack1'  , bar_min_height=50,bar_min_width=55 )
    bar.add_yaxis('起飞-落地',  [3.99,0.57,0.53 ,4.4,0.63 ,4.03 ], stack='stack1' , bar_min_height=50,bar_min_width=55 )
    bar.add_yaxis('落地-清关',  [2.22,0.87,0.97,2.93,3.03,2.81], stack='stack1'  , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('清关-交付',  [7.15,15.21,4.2,5.08,7.06,3.82 ], stack='stack1'  , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('交付-妥投',  [2.89,4.85,3.84,2.948,3.91,4.7 ], stack='stack1'  , bar_min_height=50,bar_min_width=55)

    bar.set_global_opts(xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts( formatter="{value}天", font_size=25),  type_='value'),
                        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(font_size=25)),
                        legend_opts=opts.LegendOpts(item_width=45, item_height=30,
                                                    textstyle_opts=opts.TextStyleOpts(font_family="微软雅黑",
                                                                                      font_size="23")))

    bar.set_series_opts(label_opts=opts.LabelOpts(position='inside', font_size=25, color='black'), )

    bar.reversal_axis()
    return bar

# chart = Month_12_tehui()
# chart.render('12_Month_tehui_demo_01.html')
def Month_12_cujia_tehui():
    bar = Bar(init_opts=opts.InitOpts(theme='light', width='2300px', height='1000px'))
    bar.add_xaxis(['GR','HU','LT','PL','RO','SE','SK'
])
    # stack值一样的系列会堆叠在一起
    bar.add_yaxis('首扫-封袋',  [1.535,1.04,1.1,1.1,2.65,1.11,1.69], stack='stack1' , bar_min_height=50,bar_min_width=55 )
    bar.add_yaxis('封袋-装车',  [2.41,4.3,1.37,2.06,1.76,1.95,1.47], stack='stack1' , bar_min_height=50,bar_min_width=55 )
    bar.add_yaxis('装车-起飞',  [2.09,1.19,2.09,2.14,2.14,2.45,1.45], stack='stack1', bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('起飞-落地',  [1.16,0.53,6.53,4.4,4.08,4.12,4.4 ], stack='stack1' , bar_min_height=50,bar_min_width=55 )
    bar.add_yaxis('落地-清关',  [3.046,2.56,2.95,2.83,2.89,2.83,3.06], stack='stack1', bar_min_height=50,bar_min_width=55 )
    bar.add_yaxis('清关-交付',  [10.54,4.2,6.86,5.06,23.89,6.78,6.7], stack='stack1', bar_min_height=50,bar_min_width=55 )
    bar.add_yaxis('交付-妥投',  [7.04,3.83,2.89,2.94,2.50,4.59,4.98], stack='stack1', bar_min_height=50,bar_min_width=55)

    bar.set_global_opts(xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts( formatter="{value}天", font_size=25),  type_='value'),
                        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(font_size= 25)),
                        legend_opts= opts.LegendOpts(item_width =45, item_height =30, textstyle_opts= opts.TextStyleOpts(font_family="微软雅黑", font_size="23")    )        )

    bar.set_series_opts(label_opts=opts.LabelOpts(position='inside' , font_size= 25 ,color='black' ),)


    bar.reversal_axis()
    return bar

def Month_12_wish_quanqiutong_guahao():
    bar = Bar(init_opts=opts.InitOpts(theme='light', width='2300px', height='200px'))
    bar.add_xaxis([  'SE ' ])
    # stack值一样的系列会堆叠在一起
    bar.add_yaxis('首扫-封袋',  [0.37,], stack='stack1'  , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('封袋-装车',  [1.95,], stack='stack1'  , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('装车-起飞',  [2.39,], stack='stack1'  , bar_min_height=50,bar_min_width=55 )
    bar.add_yaxis('起飞-落地',  [4.4,], stack='stack1'  ,  bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('落地-清关',  [2.83,], stack='stack1' ,  bar_min_height=50,bar_min_width=55 )
    bar.add_yaxis('清关-交付',  [6.12,], stack='stack1'  , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('交付-妥投',  [4.58,], stack='stack1' ,  bar_min_height=50,bar_min_width=55 )

    bar.set_global_opts(xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts( formatter="{value}天", font_size=25),  type_='value'),
                        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(font_size=25)),
                        legend_opts=opts.LegendOpts(item_width=45, item_height=30,
                                                    textstyle_opts=opts.TextStyleOpts(font_family="微软雅黑",
                                                                                      font_size="23")))

    bar.set_series_opts(label_opts=opts.LabelOpts(position='inside', font_size=25, color='black'), )

    bar.reversal_axis()
    return bar


def Month_12_quanqiutongGuaHao():
    bar = Bar(init_opts=opts.InitOpts(theme='light', width='1300px', height='250px'))
    bar.add_xaxis(
        ['SE- 22.58/ 32' ])
    # stack值一样的系列会堆叠在一起
    bar.add_yaxis('首扫-封袋', [ 0.37], stack='stack1' , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('封袋-装车', [ 1.95], stack='stack1' , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('装车-起飞', [2.39], stack='stack1' ,  bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('起飞-落地', [ 4.4], stack='stack1' ,  bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('落地-清关', [ 2.42], stack='stack1' , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('清关-交付', [ 6.78], stack='stack1' , bar_min_height=50,bar_min_width=55)
    bar.add_yaxis('交付-妥投', [ 4.56], stack='stack1' , bar_min_height=50,bar_min_width=55)

    bar.set_global_opts(xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts( formatter="{value}天", font_size=25),  type_='value'),
                        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(font_size=25)),
                        legend_opts=opts.LegendOpts(item_width=45, item_height=30,
                                                    textstyle_opts=opts.TextStyleOpts(font_family="微软雅黑",
                                                                                      font_size="23")))

    bar.set_series_opts(label_opts=opts.LabelOpts(position='inside', font_size=25, color='black'), )

    bar.reversal_axis()
    return bar


# chart = Month_12_quanqiutongGuaHao()
# chart.render('12_Month_quanqiutongGuaHao_demo_01.html')




if __name__ == '__main__':
    # pass
    # Month_12_wish_youxian().render('12_Month_wish_优先.html')
    # Month_12_wish_tehui().render('12_Month_wish_特惠.html')
    Month_12_wish_quanqiutong_guahao().render('12_Month_wish_全球通挂号.html')
    # Month_12_cujia_youxian().render('12_Month_促佳_优先.html')
    # Month_12_cujia_tehui().render('12_Month_促佳_特惠.html')




    # Month_12_youxian().render('12_Month_促佳_特惠_demo_01.html')
    # Month_12_youxian_KPI().render('12_Month_促佳_特惠_KPI_demo_01.html')


    # Month_12_quanqiutongGuaHao().render('12_Month_quanqiutongGuaHao_demo_01.html')





