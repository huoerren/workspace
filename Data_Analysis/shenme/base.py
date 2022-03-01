



import pandas as pd

data = {"key":['hua','hua','gou','gou','gou','shenm','gou','gou','gou','shenm','me','ming','ming'],
            'value':[ 34,23,43,24,37,64,23,45,23,23,43,24,37 ]}

nihao = pd.DataFrame(data)
df = nihao.groupby('key').size()
print(df)
print('---------------------------------')
dff = df.reset_index(name='count')
print(dff)