from data.y_annual import build_compa
from data.y_quarter import build_compq
import pandas as pd
import os
from global_settings import DATA_FOLDER, links_df
import pickle
import string


def run_build_annual_y(tics, group):
    compa = build_compa(tics)
    compa_a = compa.set_index(['permno', 'fyear', 'fqtr'], inplace=False)
    compa_a = compa_a.sort_index(inplace=False)
    compa_id_a = compa_a.iloc[:, :5]
    compa_a = compa_a.iloc[:, 5:]

    compa_aoa = compa_a.diff() / compa_a
    compa_aoa.columns = [col_name + '_aoa' for col_name in compa_a.columns]

    y_a = pd.concat([compa_id_a, compa_a, compa_aoa], axis=1)

    with open(os.path.join(DATA_FOLDER, 'annual_y', '_'.join(['y', group]) + '.pkl'), 'wb') as handle:
        pickle.dump(y_a, handle)


def run_build_quarter_y(tics, group):
    compq = build_compq(tics)
    compq_q = compq.set_index(['permno', 'fyearq', 'fqtr'], inplace=False)
    compq_q = compq_q.sort_index(inplace=False)
    compq_id_q = compq_q.iloc[:, :5]
    compq_q = compq_q.iloc[:, 5:]

    compq_qoq = compq_q.diff() / compq_q
    compq_qoq.columns = [col_name + '_qoq' for col_name in compq_q.columns]

    y_q = pd.concat([compq_id_q, compq_q, compq_qoq], axis=1)

    compq_aoa = pd.DataFrame()
    for quarter in [1, 2, 3, 4]:
        compq_a = compq[compq['fqtr'] == quarter]
        compq_a = compq_a.set_index(['permno', 'fyearq', 'fqtr'], inplace=False)
        compq_a = compq_a.sort_index(inplace=False)
        compq_a = compq_a.iloc[:, 5:]

        compq_aoa_ = compq_a.diff() / compq_a
        compq_aoa_.columns = [col_name + '_aoa' for col_name in compq_a.columns]
        compq_aoa = pd.concat([compq_aoa, compq_aoa_], axis=0)

    y_q = pd.merge(y_q, compq_aoa, left_index=True, right_index=True)

    with open(os.path.join(DATA_FOLDER, 'quarter_y', '_'.join(['y', group]) + '.pkl'), 'wb') as handle:
        pickle.dump(y_q, handle)


def run_build_y(group):
    tic = tuple([symbol for symbol in links_df['SYMBOL'] if symbol[0] == group])
    run_build_annual_y(tic, group)
    run_build_quarter_y(tic, group)


if __name__ == '__main__':
    alphabet = [_ for _ in string.ascii_uppercase]
    for group in alphabet:
        run_build_y(group)
