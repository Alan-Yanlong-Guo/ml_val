import pandas as pd
import numpy as np
import wrds
conn = wrds.Connection(wrds_username='dachxiu')


def annual_data(year, tic, current=False):
    """
    :param year: [int] data year
    :param tic: [str] ticker symbol
    :param current: [bool] if return the current value
    :return:
    """
    compa = conn.raw_sql(f"""
                         select
                         apdedate, datadate, pdate, fdate, f.gvkey, REVT, EBIT, EBITDA, RE, EPSPI, GP, OPITI, ACT, INVT, 
                         LCT, CH, OANCF, DVP,  DVC, PRSTKC, NI, CSHO, PRCC_F, NI, mkvalt, BKVLPS, AT, LT, DVT, ICAPT, 
                         XINT, DLCCH, DLTT, GDWL, GWO, CAPX, DLC, SEQ
                         from comp.funda as f
                         where tic = '{tic}'
                         and REVT != 'NaN' 
                         """)

    compa.fillna(value=np.nan, inplace=True)
    compa['datadate'] = pd.to_datetime(compa['datadate'])
    compa['year'] = compa['datadate'].dt.year
    compa['gma'] = compa['gp'] / compa['revt']
    compa['operprof'] = compa['opiti'] / compa['revt']
    compa['quick'] = (compa['act'] - compa['invt']) / compa['lct']
    compa['currat'] = compa['act'] / compa['lct']
    compa['cashrrat'] = compa['ch'] / compa['lct']
    compa['cftrr'] = compa['oancf'] / compa['revt']
    compa['dpr'] = (compa['dvp'] + compa['dvc'] + compa['prstkc']) / compa['ni']
    compa['pe'] = (compa['csho']*compa['prcc_f']) / compa['ni']
    compa['pb'] = (compa['mkvalt']) / (compa['bkvlps'])
    compa['roe'] = compa['ni'] / (compa['csho']*compa['prcc_f'])
    compa['roa'] = compa['ni'] / (compa['at'] - compa['lt'])
    compa['roic'] = (compa['ni'] - compa['dvt']) / compa['icapt']
    compa['cod'] = compa['xint'] / (compa['dlcch'] + compa['dltt'])
    compa['capint'] = compa['capx'] / compa['at']
    compa['lev'] = (compa['dltt'] + compa['dlc']) / compa['seq']

    compa_y = compa.set_index(['year'], inplace=False)
    compa_y = compa_y.sort_index(inplace=False)
    index_y = compa_y.iloc[:, :5]
    compa_y = compa_y.iloc[:, 5:]

    compa_y_diff = compa_y.diff() / compa_y

    if current:
        index_y = index_y.iloc[[-1], :]
        compa_y = compa_y.iloc[[-1], :]
        compa_yoy = compa_y_diff.iloc[[-1], :]

    else:
        index_y = index_y.loc[[year], :]
        compa_y = compa_y.loc[[year], :]
        compa_yoy = compa_y_diff.loc[[year], :]

    compa_yoy.columns = [col_name + '_yoy' for col_name in compa_y.columns]

    return index_y, compa_y, compa_yoy


def quarter_data(year, quarter, tic, current=False):
    compq = conn.raw_sql(f"""
                         select 
                         FQTR, apdedateq, datadate, pdateq, fdateq, f.gvkey, REVTQ, REQ, EPSPIQ, ACTQ, INVTQ, LCTQ, CHQ, 
                         CSHOQ, PRCCQ, NIQ, ATQ, LTQ, GDWLQ, ACTQ
                         from comp.fundq as f
                         where tic = '{tic}'
                         and REVTQ != 'NaN'
                         """)

    compq.fillna(value=np.nan, inplace=True)
    compq['datadate'] = pd.to_datetime(compq['datadate'])
    compq['year'] = compq['datadate'].dt.year
    compq['quickq'] = (compq['actq'] - compq['invtq']) / compq['lctq']
    compq['curratq'] = compq['actq'] / compq['lctq']
    compq['cashrratq'] = compq['chq'] / compq['lctq']
    compq['peq'] = (compq['cshoq']*compq['prccq']) / compq['niq']
    compq['roeq'] = compq['niq'] / (compq['prccq']*compq['prccq'])
    compq['roaq'] = compq['niq'] / (compq['atq'] - compq['ltq'])

    compq_q = compq.set_index(['year', 'fqtr'], inplace=False)
    compq_q = compq_q.sort_index(inplace=False)
    index_q = compq_q.iloc[:, :5]
    compq_q = compq_q.iloc[:, 5:]

    compq_y = compq[compq['fqtr'] == quarter]
    compq_y = compq_y.set_index(['year', 'fqtr'], inplace=False)
    compq_y = compq_y.sort_index(inplace=False)
    compq_y = compq_y.iloc[:, 5:]

    compq_q_diff = compq_q.diff() / compq_q
    compq_y_diff = compq_y.diff() / compq_y

    if current:
        index_q = index_q.iloc[[-1], :]
        compq_q = compq_q.iloc[[-1], :]
        compq_qoq = compq_q_diff.iloc[[-1], :]
        compq_yoy = compq_y_diff.iloc[[-1], :]

    else:
        index_q = index_q.loc[[(year, quarter)], :]
        compq_q = compq_q.loc[[(year, quarter)], :]
        compq_qoq = compq_q_diff.loc[[(year, quarter)], :]
        compq_yoy = compq_y_diff.loc[[(year, quarter)], :]

    compq_qoq.columns = [col_name + '_qoq' for col_name in compq_q.columns]
    compq_yoy.columns = [col_name + '_yoy' for col_name in compq_q.columns]

    return index_q, compq_q, compq_qoq, compq_yoy
