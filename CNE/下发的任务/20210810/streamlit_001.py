#coding=utf-8

import pandas as pd
import streamlit as st


st.write('Hello, world!')
x = st.slider('x')
st.write(x, 'squared is', x * x)



read_and_cache_csv = st.cache(pd.read_csv)

BUCKET = "https://streamlit-self-driving.s3-us-west-2.amazonaws.com/"
data = read_and_cache_csv(BUCKET + "labels.csv.gz", nrows=1000)
print(data.head(10))
print(data.columns.to_list())


desired_label = st.selectbox('Filter to:', ['car', 'truck'])

st.write(data[data.label == desired_label])




