import streamlit as st
from streamlit_echarts import st_echarts
st.set_page_config(layout="wide")

option = {
    "tooltip": {
        "trigger": 'item',
        "formatter": "{a} <br/>{b}: {c} ({d}%)"
    },
    "color": ["#27D9C8", "#D8D8D8"],
    "title": {
        "text": "80%",
        "left": "center",
        "top": "50%",
        "textStyle": {
            "color": "#27D9C8",
            "fontSize": 36,
            "align": "center"
        }
    },
    "graphic": {
        "type": "text",
        "left": "center",
        "top": "40%",
        "style": {
            "text": "运动达标率",
            "textAlign": "center",
            "fill": "#333",
            "fontSize": 20,
            "fontWeight": 700
        }
    },
    "series": [
        {
            "name": '平邮-入库情况',
            "type": 'pie',
            "radius": ['65%', '70%'],
            "avoidLabelOverlap": "false",
            "label": {
                "normal": {
                    "show": "false",
                    "position": 'center'
                },

            },
            "data": [
                {"value": 80, "name": '入库数量'},
                {"value": 20, "name": '未入库数量'},

            ]
        }
    ]
};


option_02 = {

    "tooltip": {
        "trigger": 'item',
        "formatter": "{a} <br/>{b}: {c} ({d}%)"
    },
    "color": ["#27D9C8", "#D8D8D8"],
    "title": {
        "text": "80%",
        "left": "center",
        "top": "50%",
        "textStyle": {
            "fontSize": 36,
            "align": "center"
        }
    },
    "graphic": {
        "type": "text",
        "left": "center",
        "top": "40%",
        "style": {
            "text": "60000",
            "textAlign": "center",
            "fontSize": 30,
            "fontWeight": 720
        }
    },
    "series": [
        {
            "name": 'Amazon抓取情况',
            "type": 'pie',
            "radius": ['65%', '70%'],
            "avoidLabelOverlap": "false",
            "label": {
                "normal": {
                    "show": "false",
                    "position": 'center'
                },
            },

            "data": [
                {"value": 22, "name": '已抓取'},
                {"value": 11, "name": '未抓取'},

            ]
        }
    ]
};


st_echarts(options=option)

st_echarts(options=option_02)
