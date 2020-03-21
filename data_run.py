import pickle


from data_all.annual_x import build_ccm_jun
with open('ccm_data.pkl', 'rb') as handle:
    ccm_data = pickle.load(handle)

ccm_jun = build_ccm_jun(ccm_data)

with open('ccm_jun.pkl', 'wb') as handle:
    pickle.dump(ccm_jun, handle)

print('CCM JUN DONE!')
from data_all.quarter_x import build_compq6
compq6, temp2 = build_compq6(ccm_jun)

with open('compq6.pkl', 'wb') as handle:
    pickle.dump(compq6, handle)

with open('temp2.pkl', 'wb') as handle:
    pickle.dump(temp2, handle)


from data_all.month_x import build_temp6
temp6 = build_temp6(compq6, temp2)
with open('temp6.pkl', 'wb') as handle:
    pickle.dump(temp2, handle)
