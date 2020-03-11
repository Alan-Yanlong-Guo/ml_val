import pandas as pd
import numpy as np
import wrds
conn = wrds.Connection(wrds_username='dachxiu')


def build_compq(tic):
    compq = conn.raw_sql(f"""
                         select 
                         fyearq, fqtr, apdedateq, datadate, pdateq, fdateq, f.gvkey, REVTQ, REQ, EPSPIQ, ACTQ, INVTQ, 
                         LCTQ, CHQ, CSHOQ, PRCCQ, NIQ, ATQ, LTQ, GDWLQ, ACTQ
                         from comp.fundq as f
                         where tic = '{tic}'
                         and REVTQ != 'NaN'
                         """)

    compq.fillna(value=np.nan, inplace=True)
    compq['fyearq'].astype(int)
    compq['fqtr'].astype(int)

    compq['datadate'] = pd.to_datetime(compq['datadate'])
    compq['year'] = compq['datadate'].dt.year
    compq['quickq'] = (compq['actq'] - compq['invtq']) / compq['lctq']
    compq['curratq'] = compq['actq'] / compq['lctq']
    compq['cashrratq'] = compq['chq'] / compq['lctq']
    compq['peq'] = (compq['cshoq']*compq['prccq']) / compq['niq']
    compq['roeq'] = compq['niq'] / (compq['prccq']*compq['prccq'])
    compq['roaq'] = compq['niq'] / (compq['atq'] - compq['ltq'])

    return compq