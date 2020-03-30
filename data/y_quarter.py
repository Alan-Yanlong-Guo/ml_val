import pandas as pd
import numpy as np
from tools.utils import permnos_to_gvkeys
from global_settings import conn


def build_compq(permnos):
    gvkeys = permnos_to_gvkeys(permnos)
    compq = conn.raw_sql(f"""
                         select 
                         fyearq, fqtr, apdedateq, datadate, pdateq, fdateq, f.gvkey, REVTQ, REQ, EPSPIQ, ACTQ, 
                         INVTQ, LCTQ, CHQ, CSHOQ, PRCCQ, NIQ, ATQ, LTQ, GDWLQ
                         from comp.fundq as f
                         where f.gvkey in {gvkeys}
                         and REVTQ != 'NaN'
                         """)

    compq.fillna(value=np.nan, inplace=True)
    compq.dropna(axis='rows', how='any', subset=['fyearq', 'fqtr'], inplace=True)
    compq['fyearq'].astype(int)
    compq['fqtr'].astype(int)
    compq['datadate'] = pd.to_datetime(compq['datadate'])
    compq['permno'] = compq['gvkey']

    compq['quickq'] = (compq['actq'] - compq['invtq']) / compq['lctq']
    compq['curratq'] = compq['actq'] / compq['lctq']
    compq['cashrratq'] = compq['chq'] / compq['lctq']
    compq['peq'] = (compq['cshoq']*compq['prccq']) / compq['niq']
    compq['roeq'] = compq['niq'] / (compq['prccq']*compq['prccq'])
    compq['roaq'] = compq['niq'] / (compq['atq'] - compq['ltq'])

    return compq
