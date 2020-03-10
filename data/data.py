import datahub as hub
h = hub.Handle.create('CRSP')
df = h.read('DailyStockByCUSIP', cusip = '36720410', start=19960101, end=19961231)
print(df)