from data.y_annual import build_compa
from data.y_quarter import build_compq
import pandas as pd
from global_settings import DATA_FOLDER, ccm
from tools.utils import y_filter
import pickle
import os
import numpy as np

def run_build_annual_y(permnos, group):
    compa = build_compa(permnos)
    permnos = set(compa['permno'].tolist())
    compa_a = compa.set_index(['permno', 'fyear', 'fqtr'], inplace=False)
    compa_a = compa_a.sort_index(inplace=False)
    compa_id = compa_a.iloc[:, :5]
    compa_a = compa_a.iloc[:, 5:]
    compa_a = y_filter(compa_a, 'annual')

    compa_aoa = pd.DataFrame()
    compa_3o3 = pd.DataFrame()
    compa_5o5 = pd.DataFrame()
    for permno in permnos:
        compa_a_ = compa_a.loc[[permno], :]
        compa_a_s1_ = compa_a_.shift(1)
        compa_a_s3_ = compa_a_.shift(3)
        compa_a_s5_ = compa_a_.shift(5)
        compa_aoa_ = (compa_a_ / compa_a_s1_) - 1
        compa_3o3_ = (compa_a_ / compa_a_s3_).pow(1/3) - 1
        compa_5o5_ = (compa_a_ / compa_a_s5_).pow(1/5) - 1
        compa_aoa = pd.concat([compa_aoa, compa_aoa_], axis=0)
        compa_3o3 = pd.concat([compa_3o3, compa_3o3_], axis=0)
        compa_5o5 = pd.concat([compa_5o5, compa_5o5_], axis=0)
    compa_aoa.columns = [col_names + '_aoa' for col_names in compa_a.columns]
    compa_3o3.columns = [col_names + '_3o3' for col_names in compa_a.columns]
    compa_5o5.columns = [col_names + '_5o5' for col_names in compa_a.columns]

    y_a = pd.concat([compa_id, compa_a, compa_aoa, compa_3o3, compa_5o5], axis=1)

    with open(os.path.join(DATA_FOLDER, 'annual_y', '_'.join(['y', group]) + '.pkl'), 'wb') as handle:
        pickle.dump(y_a, handle)


def run_build_quarter_y(permnos, group):
    compq = build_compq(permnos)
    permnos = set(compq['permno'].tolist())
    compq_q = compq.set_index(['permno', 'fyearq', 'fqtr'], inplace=False)
    compq_q = compq_q.sort_index(inplace=False)
    compq_id = compq_q.iloc[:, :5]
    compq_q = compq_q.iloc[:, 5:]
    compq_q = y_filter(compq_q, 'quarter')

    compq_aoa = pd.DataFrame()
    for quarter in [1, 2, 3, 4]:
        compq_a = compq[compq['fqtr'] == quarter]
        compq_a = compq_a.set_index(['permno', 'fyearq', 'fqtr'], inplace=False)
        compq_a = compq_a.sort_index(inplace=False)
        compq_a = compq_a.iloc[:, 5:]
        compq_a = y_filter(compq_a, 'quarter')
        for permno in permnos:
            try:
                compq_a_ = compq_a.loc[[permno], :]
                compq_a_s1_ = compq_a_.shift(1)
                compq_aoa_ = (compq_a_ / compq_a_s1_) - 1
                compq_aoa = pd.concat([compq_aoa, compq_aoa_], axis=0)
            except KeyError:
                pass

    compq_aoa.columns = [col_names + '_aoa' for col_names in compq_a.columns]
    y_q = pd.concat([compq_id, compq_q], axis=1)
    y_q = pd.merge(y_q, compq_aoa, left_index=True, right_index=True)

    with open(os.path.join(DATA_FOLDER, 'quarter_y', '_'.join(['y', group]) + '.pkl'), 'wb') as handle:
        pickle.dump(y_q, handle)


def run_build_y(group):
    permnos = tuple([_ for _ in ccm['permno'] if str(_)[:2] == group])
    run_build_annual_y(permnos, group)
    run_build_quarter_y(permnos, group)


if __name__ == '__main__':
    pass

    # for group in groups:
    #     print(f'{datetime.now()} Working on group with permno starting with ' + group)
    #     run_build_y(group)
    # pool = Pool(14)
    # pool.map(run_build_xy, years)
