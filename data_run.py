import pickle
from data_all.month_x import build_temp3, build_temp4, build_temp6

with open('temp2.pkl', 'rb') as handle:
    temp2 = pickle.load(handle)

with open('compq6.pkl', 'rb') as handle:
    compq6 = pickle.load(handle)

temp3 = build_temp3(compq6, temp2)
temp4 = build_temp4(temp3)
temp6 = build_temp6(temp4)
print('build_temp6 finished')
with open('temp6.pkl', 'wb') as handle:
    pickle.dump(temp6, handle)
