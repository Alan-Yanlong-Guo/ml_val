from data.x_annual import build_comp, build_crsp_m, build_dlret, build_crsp, build_ccm_data, build_ccm_jun
from data.x_quarter import build_compq6
from data.x_month import build_temp6
from global_settings import DATA_FOLDER, ccm, groups
import os
import pickle
import numpy as np


def run_build_ccm_jun(permnos, group):
    comp = build_comp(permnos)
    crsp_m = build_crsp_m(permnos)
    dlret = build_dlret(permnos)
    crsp_jun = build_crsp(crsp_m, dlret)
    ccm_data = build_ccm_data(permnos, comp, crsp_jun)
    ccm_jun = build_ccm_jun(ccm_data)

    with open(os.path.join(DATA_FOLDER, 'annual_x', '_'.join(['x', group]) + '.pkl'), 'wb') as handle:
        pickle.dump(ccm_jun, handle)

    return ccm_jun


def run_build_compq6(permnos, group, ccm_jun):
    compq6, temp2 = build_compq6(permnos, ccm_jun)

    with open(os.path.join(DATA_FOLDER, 'quarter_x', '_'.join(['x', group]) + '.pkl'), 'wb') as handle:
        pickle.dump(compq6, handle)

    return compq6, temp2


def run_build_temp6(group, temp2, compq6):
    temp6 = build_temp6(temp2, compq6)

    with open(os.path.join(DATA_FOLDER, 'month_x', '_'.join(['x', group]) + '.pkl'), 'wb') as handle:
        pickle.dump(temp6, handle)


def run_build_x(group):
    permnos = tuple([_ for _ in ccm['permno'] if str(_)[:2] == group])
    ccm_jun = run_build_ccm_jun(permnos, group)
    compq6, temp2 = run_build_compq6(permnos, group, ccm_jun)
    run_build_temp6(group, temp2, compq6)


if __name__ == '__main__':
    group = '26'
    run_build_x(group)
    permnos = tuple([_ for _ in ccm['permno'] if str(_)[:2] == group])
    run_build_ccm_jun(permnos, group)

    # for group in groups:
    #     print(f'{datetime.now()} Working on group with permno starting with ' + group)
    #     run_build_x(group)
