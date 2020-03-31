import pandas as pd

df = pd.read_csv('/Users/mmw/Downloads/datashare/datashare.csv')

print(df.shape)
df.iloc[-1000:, :].to_csv('/Users/mmw/Downloads/datashare/datalast1000.csv')
# df.to_csv('y_1975.csv')