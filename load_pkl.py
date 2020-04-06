import pandas as pd

pa = '/Users/mmw/Documents/GitHub/ml_val/data_all/quarter_y/y_59.pkl'
df = pd.read_pickle(pa)

print(df.head(10))
print(df.tail(10))

df.to_csv('pkl.csv')