import pandas as pd
import numpy as np
import wrds
from tools.utils import tic_to_permno
conn = wrds.Connection(wrds_username='dachxiu')
conn.create_pgpass_file()


def build_compq(tics):
    compq = conn.raw_sql(f"""
                         select 
                         fyearq, fqtr, apdedateq, datadate, pdateq, fdateq, f.gvkey, REVTQ, REQ, EPSPIQ, ACTQ, 
                         INVTQ, LCTQ, CHQ, CSHOQ, PRCCQ, NIQ, ATQ, LTQ, GDWLQ, tic
                         from comp.fundq as f
                         where tic in {tics}
                         and REVTQ != 'NaN'
                         """)

    compq.fillna(value=np.nan, inplace=True)
    compq.dropna(axis='rows', how='any', subset=['fyearq', 'fqtr'], inplace=True)
    compq['fyearq'].astype(int)
    compq['fqtr'].astype(int)
    compq['datadate'] = pd.to_datetime(compq['datadate'])
    compq['permno'] = compq['tic'].apply(tic_to_permno)
    compq.drop(['tic'], axis=1, inplace=True)

    compq['quickq'] = (compq['actq'] - compq['invtq']) / compq['lctq']
    compq['curratq'] = compq['actq'] / compq['lctq']
    compq['cashrratq'] = compq['chq'] / compq['lctq']
    compq['peq'] = (compq['cshoq']*compq['prccq']) / compq['niq']
    compq['roeq'] = compq['niq'] / (compq['prccq']*compq['prccq'])
    compq['roaq'] = compq['niq'] / (compq['atq'] - compq['ltq'])

    return compq