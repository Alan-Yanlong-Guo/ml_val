import pickle
import os
import pandas as pd
from global_settings import DATA_FOLDER, ccm, groups
from tools.utils import horizon
import numpy as np
import datetime
from tools.utils import x_filter
from multiprocessing import Pool


def load_x_y(group):
    with open(os.path.join(DATA_FOLDER, 'annual_y', '_'.join(['y', group]) + '.pkl'), 'rb') as handle:
        y_annual = pickle.load(handle)

    with open(os.path.join(DATA_FOLDER, 'quarter_y', '_'.join(['y', group]) + '.pkl'), 'rb') as handle:
        y_quarter = pickle.load(handle)

    with open(os.path.join(DATA_FOLDER, 'annual_x', '_'.join(['x', group]) + '.pkl'), 'rb') as handle:
        x_annual = pickle.load(handle)
        x_annual['fqtr'] = 4
        x_annual.set_index(['permno', 'fyear', 'fqtr'], inplace=True)
        x_annual.sort_index(inplace=True)
        x_annual = pd.concat([x_annual.iloc[:, :5], x_filter(x_annual.iloc[:, 5:], 'annual')], axis=1)

    with open(os.path.join(DATA_FOLDER, 'quarter_x', '_'.join(['x', group]) + '.pkl'), 'rb') as handle:
        x_quarter = pickle.load(handle)
        x_quarter.rename(columns={'lpermno': 'permno'}, inplace=True)
        x_quarter.rename(columns={'fyearq': 'fyear'}, inplace=True)
        x_quarter.set_index(['permno', 'fyear', 'fqtr'], inplace=True)
        x_quarter.sort_index(inplace=True)
        x_quarter = pd.concat([x_quarter.iloc[:, :5], x_filter(x_quarter.iloc[:, 5:], 'quarter')], axis=1)

    with open(os.path.join(DATA_FOLDER, 'month_x', '_'.join(['x', group]) + '.pkl'), 'rb') as handle:
        x_month = pickle.load(handle)
        x_month.set_index(['permno', 'year', 'month'], inplace=True)
        x_month.sort_index(inplace=True)
        x_month = pd.concat([x_month.iloc[:, :5], x_filter(x_month.iloc[:, 5:], 'month')], axis=1)

    return y_annual, y_quarter, x_annual, x_quarter, x_month


def load_industrial(year, cf):
    assert cf in ['c', 'f'], 'Invalid Coarse Fine Type'
    with open(os.path.join(DATA_FOLDER, 'industrial', '_'.join(['industrial', str(year), cf]) + '.pkl'), 'rb') as handle:
        industrial = pickle.load(handle)
    industrial.reset_index(drop=False, inplace=True)
    industrial['year'] = year
    industrial.set_index(['year', 'sic'], inplace=True)

    return industrial


def build_x_line(permno, x_annual, x_quarter, x_month, y_annual, y_quarter, x_ay, x_qy, x_qq, x_my, x_mm, aq):
    x_annual = x_annual.loc[[(permno, x_ay, 4)], :]
    x_annual = x_annual.iloc[:, 5:]
    x_quarter = x_quarter.loc[[(permno, x_qy, x_qq)], :]
    x_index = x_quarter.iloc[:, :5]
    x_quarter = x_quarter.iloc[:, 5:]
    x_month = x_month.loc[[(permno, x_my, x_mm)], :]
    x_month = x_month.iloc[:, 5:]

    # Filter Data
    y_annual = y_annual.loc[[(permno, x_ay, 4)], :]
    y_annual = y_annual.iloc[:, 5:]
    industrial_c, industrial_f = load_industrial(x_ay, 'c'), load_industrial(x_ay, 'f')

    if aq == 'a':
        x_line_y = y_annual.reset_index(drop=True)
    else:
        y_quarter = y_quarter.loc[[(permno, x_qy, x_qq)], :]
        y_quarter = y_quarter.iloc[:, 5:]
        x_line_y = pd.concat([y_annual.reset_index(drop=True), y_quarter.reset_index(drop=True)], axis=1)

    if np.shape(x_annual)[0] == 1 and np.shape(x_quarter)[0] == 1 and np.shape(x_month)[0] == 1 and np.shape(x_line_y)[0] == 1:
        sic_c, sic_f = str(x_annual['sic'][0]).zfill(4)[:1], str(x_annual['sic'][0]).zfill(4)[:2]
        industrial_c = industrial_c.loc[[(x_ay, sic_c)], :]
        industrial_f = industrial_c.loc[[(x_ay, sic_f)], :]

        x_line = pd.concat([x_index.reset_index(drop=True), x_annual.reset_index(drop=True),
                            x_quarter.reset_index(drop=True), x_month.reset_index(drop=True),
                            x_line_y, industrial_c.reset_index(drop=True),
                            industrial_f.reset_index(drop=True)], axis=1)
        x_line.index = x_index.index

    else:
        x_line = pd.DataFrame(columns=list(x_index.columns) + list(x_annual.columns) + list(x_quarter.columns) +
                                      list(x_month.columns) + list(x_line_y.columns) + list(industrial_c.columns) +
                                      list(industrial_f.columns))

    x_line.drop(['sic'], axis=1, inplace=True)

    return x_line


def build_y_line(permno, date_type, y_annual, y_quarter, y_ay, y_qy, y_qq, aq):
    if aq == 'a':
        y_annual[date_type] = pd.to_datetime(y_annual[date_type])
        y_annual = y_annual.loc[[(permno, y_ay, 4)], :]
        y_my, y_mm = y_annual[date_type].dt.year[0], y_annual[date_type].dt.month[0]
        y_line_ = y_annual

    else:
        y_quarter[date_type + 'q'] = pd.to_datetime(y_quarter[date_type + 'q'])
        y_quarter = y_quarter.loc[[(permno, y_qy, y_qq)], :]
        y_my, y_mm = y_quarter[date_type + 'q'].dt.year[0], y_quarter[date_type + 'q'].dt.month[0]
        y_line_ = y_quarter

    if np.shape(y_line_)[0] == 1:
        y_line = y_line_
    else:
        y_line = pd.DataFrame(columns=list(y_line_.columns))

    return y_line, y_my, y_mm


def build_xy(year, dy, dq, aq, group):
    y_annual, y_quarter, x_annual, x_quarter, x_month = load_x_y(group)
    y_quarter.rename(columns={'datadate': 'datadateq'}, inplace=True)
    x_month.rename(columns={'datadate': 'datadateq'}, inplace=True)

    date_type = 'fdate' if dy == 0 else 'datadate'
    y_annual.dropna(subset=[date_type], inplace=True, axis=0)
    y_quarter.dropna(subset=[date_type + 'q'], inplace=True, axis=0)

    # Build yearly data for the group
    x_df_, y_df_ = pd.DataFrame(), pd.DataFrame()
    permnos = tuple([_ for _ in ccm['permno'] if str(_).zfill(4)[:2] == group])
    y_qy = year
    quarters = [4] if aq == 'a' else [1, 2, 3, 4]
    for quarter in quarters:
        y_qq = quarter
        y_ay = year if y_qq == 4 else year - 1
        for permno in permnos:
            try:
                y_line, y_my, y_mm = build_y_line(permno, date_type, y_annual, y_quarter, y_ay, y_qy, y_qq, aq)
                x_ay, x_qy, x_qq, x_my, x_mm = horizon(y_ay, y_qy, y_qq, y_my, y_mm, dy, dq)
                x_line = build_x_line(permno, x_annual, x_quarter, x_month, y_annual, y_quarter, x_ay, x_qy, x_qq, x_my, x_mm, aq)

                if np.shape(y_line)[0] == 1 and np.shape(x_line)[0] == 1:
                    x_df_ = pd.concat([x_df_, x_line], axis=0)
                    y_df_ = pd.concat([y_df_, y_line], axis=0)

            except KeyError:
                pass

    return x_df_, y_df_


def run_build_xy(year, dy=1, dq=0, aq='a'):
    print(f'{datetime.datetime.now()} Working on year {year}')
    if aq == 'a':
        assert dq == 0, 'Invalid dq value'

    x_df, y_df = pd.DataFrame(), pd.DataFrame()
    for group in groups:
        print(f'{datetime.datetime.now()} Working on group {group}')
        x_df_, y_df_ = build_xy(year, dy, dq, aq, group)
        x_df = pd.concat([x_df, x_df_], axis=0)
        y_df = pd.concat([y_df, y_df_], axis=0)

    folder = '_'.join(['xy', aq, str(dy), str(dq)])
    if not os.path.exists(os.path.join(DATA_FOLDER, folder)):
        os.mkdir(os.path.join(DATA_FOLDER, folder))

    with open(os.path.join(DATA_FOLDER, folder, '_'.join(['x', str(year)]) + '.pkl'), 'wb') as handle:
        pickle.dump(x_df, handle)

    with open(os.path.join(DATA_FOLDER, folder, '_'.join(['y', str(year)]) + '.pkl'), 'wb') as handle:
        pickle.dump(y_df, handle)


def run_load_xy(years, set_name, dy=1, dq=0, save_dir='xy_data'):
    folder = '_'.join(['xy', 'q', str(dy), str(dq)])
    if not os.path.exists(os.path.join(DATA_FOLDER, folder)):
        raise Exception('Preprocessed xy data folder not found')
    if not os.path.exists(os.path.join(DATA_FOLDER, save_dir)):
        os.mkdir(os.path.join(DATA_FOLDER, save_dir))

    x_df_set, y_df_set = pd.DataFrame(), pd.DataFrame()

    for year in years:
        with open(os.path.join(DATA_FOLDER, folder, '_'.join(['x', str(year)]) + '.pkl'), 'rb') as handle:
            x_df_ = pickle.load(handle)
        with open(os.path.join(DATA_FOLDER, folder, '_'.join(['y', str(year)]) + '.pkl'), 'rb') as handle:
            y_df_ = pickle.load(handle)
        x_df_set = pd.concat([x_df_set, x_df_], axis=0)
        y_df_set = pd.concat([y_df_set, y_df_], axis=0)

    with open(os.path.join(DATA_FOLDER, save_dir, '_'.join(['x', str(set_name)]) + '.pkl'), 'wb') as handle:
        pickle.dump(x_df_set, handle)

    with open(os.path.join(DATA_FOLDER, save_dir, '_'.join(['y', str(set_name)]) + '.pkl'), 'wb') as handle:
        pickle.dump(y_df_set, handle)


if __name__ == '__main__':
    years = np.arange(1975, 2020)
    pool = Pool(16)
    pool.map(run_build_xy, years)
    # run_build_xy(2017)
