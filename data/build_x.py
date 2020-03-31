from data.x_annual import build_comp, build_crsp_m, build_dlret, build_crsp, build_ccm_data, build_ccm_jun
from data.x_quarter import build_compq6
from data.x_month import build_temp6
from global_settings import DATA_FOLDER, links_df
import os
import pickle
import string
from multiprocessing import Pool

def run_build_ccm_jun(tics, group):
    comp = build_comp(tics)
    crsp_m = build_crsp_m(tics)
    dlret = build_dlret(tics)
    crsp_jun = build_crsp(crsp_m, dlret)
    ccm_data = build_ccm_data(tics, comp, crsp_jun)
    ccm_jun = build_ccm_jun(ccm_data)

    with open(os.path.join(DATA_FOLDER, 'annual_x', '_'.join(['x', group]) + '.pkl'), 'wb') as handle:
        pickle.dump(ccm_jun, handle)

    return ccm_jun

def run_build_compq6(tics, group, ccm_jun):
    compq6, temp2 = build_compq6(tics, ccm_jun)

    with open(os.path.join(DATA_FOLDER, 'quarter_x', '_'.join(['x', group]) + '.pkl'), 'wb') as handle:
        pickle.dump(compq6, handle)

    return compq6, temp2

def run_build_temp6(tics, group, temp2, compq6):
    temp6 = build_temp6(tics, temp2, compq6)

    with open(os.path.join(DATA_FOLDER, 'month_x', '_'.join(['x', group]) + '.pkl'), 'wb') as handle:
        pickle.dump(temp6, handle)

def run_build_x(group):
    tics = tuple([symbol for symbol in links_df['SYMBOL'] if symbol[0] == group])
    ccm_jun = run_build_ccm_jun(tics, group)
    compq6, temp2 = run_build_compq6(tics, group, ccm_jun)
    run_build_temp6(tics, group, temp2, compq6)

if __name__ == '__main__':
    alphabet = [_ for _ in string.ascii_uppercase]
    for group in alphabet:
        print(group)
        run_build_x(group)
