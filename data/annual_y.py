import wrds
import pandas as pd
import numpy as np
conn = wrds.Connection(wrds_username='dachxiu')


def build_compa(tic):
    compa = conn.raw_sql(f"""
                         select
                         fyear, apdedate, datadate, pdate, fdate, f.gvkey, REVT, EBIT, EBITDA, RE, EPSPI, GP, OPITI, 
                         ACT, INVT, LCT, CH, OANCF, DVP,  DVC, PRSTKC, NI, CSHO, PRCC_F, NI, mkvalt, BKVLPS, AT, LT, 
                         DVT, ICAPT, XINT, DLCCH, DLTT, GDWL, GWO, CAPX, DLC, SEQ
                         from comp.funda as f
                         where tic = '{tic}'
                         and REVT != 'NaN' 
                         and f.indfmt='INDL'
                         and f.datafmt='STD'
                         and f.popsrc='D'
                         and f.consol='C'
                         """)

    compa.fillna(value=np.nan, inplace=True)
    compa['fyear'].astype(int)
    compa['datadate'] = pd.to_datetime(compa['datadate'])

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

    return compa

