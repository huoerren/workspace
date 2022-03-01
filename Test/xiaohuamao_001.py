import pandas as pd
import numpy as np
import streamlit.components.v1 as components
import streamlit as st
from pyecharts.components import Table

df = pd.DataFrame(np.random.randn(100, 30),columns=('col %d' % i for i in range(30)))

table = Table()
headers = list(df.columns)
rows=[]
for i in range(0,len(df)):
    rows.append(df.iloc[i,:])
c=table.add(headers, rows).render_embed()

print(type(c))
print(type(table.add(headers, rows)))

components.html(c, width=1500, height=700)


