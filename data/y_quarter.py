import pandas as pd
import numpy as np
from utils.data_tools import permnos_to_gvkeys, gvkey_to_permno
from global_settings import conn


def build_compq(permnos):
    gvkeys = permnos_to_gvkeys(permnos)
    compq = conn.raw_sql(f"""
                         select 
                         fyearq, fqtr, apdedateq, datadate, pdateq, fdateq, f.gvkey, REVTQ, REQ, EPSPIQ, ACTQ, 
                         INVTQ, LCTQ, CHQ, CSHOQ, abs(PRCCQ) as PRCCQ, NIQ, ATQ, LTQ, GDWLQ, OPEPSQ, OIADPQ, OIBDPQ, 
                         PIQ, CEQQ
                         from comp.fundq as f
                         where f.gvkey in {gvkeys}
                         and REVTQ != 'NaN'
                         """)

    compq.fillna(value=np.nan, inplace=True)
    compq.dropna(axis='rows', how='any', subset=['fyearq', 'fqtr'], inplace=True)
    compq['fyearq'].astype(int)
    compq['fqtr'].astype(int)
    compq['datadate'] = pd.to_datetime(compq['datadate'])
    compq['permno'] = compq['gvkey'].apply(gvkey_to_permno)

    compq['quickq'] = (compq['actq'] - compq['invtq']) / compq['lctq']
    compq['curratq'] = compq['actq'] / compq['lctq']
    compq['cashrratq'] = compq['chq'] / compq['lctq']
    compq['peq'] = (compq['cshoq']*compq['prccq']) / compq['niq']
    compq['roeq'] = compq['niq'] / (compq['prccq']*compq['prccq'])
    compq['roaq'] = compq['niq'] / (compq['atq'] - compq['ltq'])
    compq['pbq'] = (compq['prccq']*compq['cshoq']) / (compq['ceqq'])

    return compq
