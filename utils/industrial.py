import pandas as pd
import numpy as np
import wrds
import os
import pickle
from global_settings import DATA_FOLDER
from datetime import datetime
conn = wrds.Connection(wrds_username='dachxiu')

sics_c = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
sics_f = ['01', '02', '07', '08', '09', '10', '12', '13', '14', '15', '16', '17', '20', '21', '22', '23', '24',
          '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41',
          '42', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59',
          '60', '61', '62', '63', '64', '65', '67', '70', '72', '73', '75', '76', '78', '79', '80', '81', '82',
          '83', '86', '87', '88', '89', '97', '99']

filter_list_i = ['revt', 'ebit', 'ebitda', 're', 'gp', 'dvc', 'oancf']
filter_list_j = ['epspi', 'gma', 'operprof', 'quick', 'currat', 'cashrrat', 'cftrr', 'dpr', 'pe', 'pb', 'roe', 'roa',
                 'roic', 'cod', 'capint', 'lev']


def build_compa(year):
    compa = conn.raw_sql(f"""
                         select
                         fyear, apdedate, datadate, pdate, fdate, sic, f.gvkey, REVT, EBIT, EBITDA, RE, EPSPI, GP,
                         OPINCAR, ACT, INVT, LCT, CH, OANCF, DVP,  DVC, PRSTKC, NI, CSHO, PRCC_F, mkvalt, BKVLPS, AT,
                                 LT, DVT, ICAPT, XINT, DLCCH, DLTT, GDWL, GWO, CAPX, DLC, SEQ
                         from comp.names as c, comp.funda as f
                         where f.fyear = {year}
                         and f.gvkey=c.gvkey
                         and REVT != 'NaN'
                         and f.indfmt='INDL'
                         and f.datafmt='STD'
                         and f.popsrc='D'
                         and f.consol='C'
                         """)

    compa.fillna(value=np.nan, inplace=True)
    compa['fyear'].astype(int)
    compa['fqtr'] = 4
    compa['datadate'] = pd.to_datetime(compa['datadate'])

    compa['gma'] = compa['gp'] / compa['revt']
    compa['operprof'] = compa['opincar'] / compa['revt']
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


def build_table(compa, compa_s1, compa_s3, compa_s5, year, cf):

    if cf == 'c':
        sics = sics_c
    elif cf == 'f':
        sics = sics_f
    else:
        raise Exception('Invalid Coarse Fine Type')

    columns_ = ['_'.join([_, cf, 'sum']) for _ in filter_list_i] + ['_'.join([_, cf, 'med']) for _ in filter_list_i] + \
               ['_'.join([_, cf, 'med']) for _ in filter_list_j]
    columns_1o1 = [_ + '_1o1' for _ in columns_]
    columns_1o1r = [_ + '_1o1r' for _ in columns_]
    columns_3o3r = [_ + '_3o3r' for _ in columns_]
    columns_5o5r = [_ + '_5o5r' for _ in columns_]
    industrial = pd.DataFrame(columns=columns_ + columns_1o1 + columns_1o1r + columns_3o3r + columns_5o5r)

    for sic in sics:
        if cf == 'c':
            compa_ = compa[compa['sic'].apply(lambda _: str(_).zfill(4)[:1] == sic)]
            compa_s1_ = compa_s1[compa_s1['sic'].apply(lambda _: str(_).zfill(4)[:1] == sic)]
            compa_s3_ = compa_s3[compa_s3['sic'].apply(lambda _: str(_).zfill(4)[:1] == sic)]
            compa_s5_ = compa_s5[compa_s5['sic'].apply(lambda _: str(_).zfill(4)[:1] == sic)]
        else:
            compa_ = compa[compa['sic'].apply(lambda _: str(_).zfill(4)[:2] == sic)]
            compa_s1_ = compa_s1[compa_s1['sic'].apply(lambda _: str(_).zfill(4)[:2] == sic)]
            compa_s3_ = compa_s3[compa_s3['sic'].apply(lambda _: str(_).zfill(4)[:2] == sic)]
            compa_s5_ = compa_s5[compa_s5['sic'].apply(lambda _: str(_).zfill(4)[:2] == sic)]

        compa_i_sum, compa_i_med, compa_j_med = sum_med(compa_, filter_list_i, filter_list_j)
        compa_i_sum_s1, compa_i_med_s1, compa_j_med_s1 = sum_med(compa_s1_, filter_list_i, filter_list_j)
        compa_i_sum_s3, compa_i_med_s3, compa_j_med_s3 = sum_med(compa_s3_, filter_list_i, filter_list_j)
        compa_i_sum_s5, compa_i_med_s5, compa_j_med_s5 = sum_med(compa_s5_, filter_list_i, filter_list_j)
        compa_i_sum_1o1 = compa_i_sum - compa_i_sum_s1
        compa_i_med_1o1 = compa_i_med - compa_i_med_s1
        compa_j_med_1o1 = compa_j_med - compa_j_med_s1
        compa_i_sum_1o1r = compa_i_sum.div(compa_i_sum_s1) - 1
        compa_i_med_1o1r = compa_i_med.div(compa_i_med_s1) - 1
        compa_j_med_1o1r = compa_j_med.div(compa_j_med_s1) - 1
        compa_i_sum_3o3r = compa_i_sum.div(compa_i_sum_s3).pow(1/3) - 1
        compa_i_med_3o3r = compa_i_med.div(compa_i_med_s3).pow(1/3) - 1
        compa_j_med_3o3r = compa_j_med.div(compa_j_med_s3).pow(1/3) - 1
        compa_i_sum_5o5r = compa_i_sum.div(compa_i_sum_s5).pow(1/5) - 1
        compa_i_med_5o5r = compa_i_med.div(compa_i_med_s5).pow(1/5) - 1
        compa_j_med_5o5r = compa_j_med.div(compa_j_med_s5).pow(1/5) - 1

        industrial_ = np.concatenate([compa_i_sum, compa_i_med, compa_j_med], axis=0)
        industrial_1o1 = np.concatenate([compa_i_sum_1o1, compa_i_med_1o1, compa_j_med_1o1], axis=0)
        industrial_1o1r = np.concatenate([compa_i_sum_1o1r, compa_i_med_1o1r, compa_j_med_1o1r], axis=0)
        industrial_3o3r = np.concatenate([compa_i_sum_3o3r, compa_i_med_3o3r, compa_j_med_3o3r], axis=0)
        industrial_5o5r = np.concatenate([compa_i_sum_5o5r, compa_i_med_5o5r, compa_j_med_5o5r], axis=0)
        industrial_ = np.concatenate([industrial_, industrial_1o1, industrial_1o1r, industrial_3o3r, industrial_5o5r], axis=0)

        industrial = industrial.append(pd.DataFrame([industrial_], columns=industrial.columns))

    industrial.index = pd.Index(sics)
    industrial.index.rename('sic', inplace=True)

    with open(os.path.join(DATA_FOLDER, 'industrial', '_'.join(['industrial', str(year), cf]) + '.pkl'), 'wb') as handle:
        pickle.dump(industrial, handle)


def sum_med(compa_, filter_list_i, filter_list_j):
    compa_.replace({0: np.nan, np.inf: np.nan, -np.inf: np.nan}, inplace=True)
    compa_i = compa_[filter_list_i]
    compa_j = compa_[filter_list_j]
    compa_i_sum = compa_i.sum(axis=0, skipna=True, numeric_only=True)
    compa_i_med = compa_i.median(axis=0, skipna=True, numeric_only=True)
    compa_j_med = compa_j.median(axis=0, skipna=True, numeric_only=True)

    return compa_i_sum, compa_i_med, compa_j_med


def run_build_table(year):
    print(f'{datetime.now()} Working on Year {year}')
    compa = build_compa(year)
    compa_s1 = build_compa(year-1)
    compa_s3 = build_compa(year-3)
    compa_s5 = build_compa(year-5)
    build_table(compa, compa_s1, compa_s3, compa_s5, year, 'c')
    build_table(compa, compa_s1, compa_s3, compa_s5, year, 'f')


if __name__ == '__main__':
    for year in np.arange(2019, 2020):
        run_build_table(year)
