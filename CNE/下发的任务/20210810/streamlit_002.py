#coding=utf-8

import pandas as pd
import streamlit as st
import pandas as pd

st.title('just a test !')



@st.cache
def load_data():
    df = pd.read_excel('C:/Users/hp/Desktop/demoData.xlsx')
    return df


da = load_data()
print(da.columns.to_list())
event_list = da['class'].unique()
event_type = st.sidebar.selectbox(
    "can you try ?",
    event_list
)
# st.table(da)
part_df = da[da["class"]== event_type ]
st.write(f"根据筛选，数据包含 {len(part_df)} 行")
# st.table(part_df)
# st.map(part_df)






