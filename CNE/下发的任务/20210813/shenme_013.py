from pyecharts.charts import *
from pyecharts import options as opts
from pyecharts.faker import Faker


def bar_with_default_selected_series():
    bar = Bar(init_opts=opts.InitOpts(theme='light',
                                      width='1000px',
                                      height='600px'))
    bar.add_xaxis(['2011.09.02 - 2011.09.12'])
    # 默认选中A C
    bar.add_yaxis('A', [200], stack='stack1')
    bar.add_yaxis('B', [156], stack='stack1')
    bar.add_yaxis('C', [596], stack='stack1')
    return bar


chart = bar_with_default_selected_series()
# chart.render_notebook()
chart.render('shenme_013.html')


