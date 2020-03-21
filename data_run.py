import pickle
from data_all.month_x import build_temp6

with open('temp2.pkl', 'rb') as handle:
    temp2 = pickle.load(handle)

with open('compq6.pkl', 'rb') as handle:
    compq6 = pickle.load(handle)

temp6 = build_temp6(compq6, temp2)
print('build_temp6 finished')
with open('temp6.pkl', 'wb') as handle:
    pickle.dump(temp2, handle)
