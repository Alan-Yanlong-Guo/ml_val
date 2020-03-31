import pandas as pd

df = pd.read_pickle('/Users/mmw/Documents/GitHub/ml_val/data_all/xy_data/y_ts.pkl')

df.to_csv('y_ts.csv')