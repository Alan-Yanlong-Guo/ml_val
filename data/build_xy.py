import pickle
import os
import pandas as pd
from tools.utils import tics_to_permnos
from global_settings import DATA_FOLDER, links_df
from tools.utils import x_filter, horizon
import numpy as np
from tqdm import tqdm_notebook
import string
import datetime
from multiprocessing import Pool


def load_x_y(group):
    with open(os.path.join(DATA_FOLDER, 'annual_y', '_'.join(['y', group]) + '.pkl'), 'rb') as handle:
        y_annual = pickle.load(handle)

    with open(os.path.join(DATA_FOLDER, 'quarter_y', '_'.join(['y', group]) + '.pkl'), 'rb') as handle:
        y_quarter = pickle.load(handle)

    with open(os.path.join(DATA_FOLDER, 'annual_x', '_'.join(['x', group]) + '.pkl'), 'rb') as handle:
        x_annual = pickle.load(handle)

    with open(os.path.join(DATA_FOLDER, 'quarter_x', '_'.join(['x', group]) + '.pkl'), 'rb') as handle:
        x_quarter = pickle.load(handle)

    with open(os.path.join(DATA_FOLDER, 'month_x', '_'.join(['x', group]) + '.pkl'), 'rb') as handle:
        x_month = pickle.load(handle)

    return y_annual, y_quarter, x_annual, x_quarter, x_month


def line_x(permno, x_annual, x_quarter, x_month, x_ay, x_qy, x_qq, x_my, x_mm):
    # Slice Data
    x_annual = x_annual.loc[[(permno, x_ay, 4)], :]
    x_annual = x_annual.iloc[:, 5:]
    x_quarter = x_quarter.loc[[(permno, x_qy, x_qq)], :]
    x_index = x_quarter.iloc[:, :5]
    x_quarter = x_quarter.iloc[:, 5:]

    x_month = x_month.loc[[(permno, x_my, x_mm)], :]
    x_month = x_month.iloc[:, 5:]

    # Filter Data
    x_annual, x_quarter, x_month = x_filter(x_annual, x_quarter, x_month)
    x = pd.concat([x_index.reset_index(drop=True), x_annual.reset_index(drop=True), x_quarter.reset_index(drop=True), x_month.reset_index(drop=True)],
                  axis=1)

    return x


def line_y(permno, y_annual, y_quarter, y_ay, y_qy, y_qq):
    # Slice Data
    y_annual['fdate'] = pd.to_datetime(y_annual['fdate'])
    y_quarter['fdateq'] = pd.to_datetime(y_quarter['fdateq'])

    if y_qq == 4:
        y_annual = y_annual.loc[[(permno, y_ay, 4)], :]
    else:
        y_annual = y_annual.loc[[(permno, y_ay-1, 4)], :]

    y_annual = y_annual.iloc[:, 5:]

    y_quarter = y_quarter.loc[[(permno, y_qy, y_qq)], :]
    y_index = y_quarter.iloc[:, :5]
    y_my, y_mm = y_quarter['fdateq'].dt.year[0], y_quarter['fdateq'].dt.month[0]
    y_quarter = y_quarter.iloc[:, 5:]

    # Filter Data
    y = pd.concat([y_index.reset_index(drop=True), y_annual.reset_index(drop=True), y_quarter.reset_index(drop=True)], axis=1)

    return y, y_my, y_mm


def build_xy(year, dy, dq, group):
    y_annual, y_quarter, x_annual, x_quarter, x_month = load_x_y(group)
    y_annual.dropna(subset=['fdate'], inplace=True, axis=0)
    y_quarter.dropna(subset=['fdateq'], inplace=True, axis=0)
    tics = tuple([symbol for symbol in links_df['SYMBOL'] if symbol[0] == group])
    permnos = tics_to_permnos(tics)

    x_df = pd.DataFrame()
    y_df = pd.DataFrame()
    for permno in permnos:
        y_qy = year
        for quarter in [1, 2, 3, 4]:
            y_qq = quarter
            if quarter == 4:
                y_ay = year
            else:
                y_ay = year - 1
            try:
                y, y_my, y_mm = line_y(permno, y_annual, y_quarter, y_ay, y_qy, y_qq)
                x_ay, x_qy, x_qq, x_my, x_mm = horizon(y_ay, y_qy, y_qq, y_my, y_mm, dy, dq)
                x = line_x(permno, x_annual, x_quarter, x_month, x_ay, x_qy, x_qq, x_my, x_mm)

                if np.shape(x)[0] == 1 and np.shape(y)[0] == 1:
                    x_df = pd.concat([x_df, x], axis=0)
                    y_df = pd.concat([y_df, y], axis=0)

            except KeyError:
                pass

    x_df.reset_index(drop=True, inplace=True)
    y_df.reset_index(drop=True, inplace=True)

    return x_df, y_df


def run_build_xy(year, dy=1, dq=0):
    alphabet = [_ for _ in string.ascii_uppercase]
    print(f'{datetime.datetime.now()} Working on year {year}')
    x_df = pd.DataFrame()
    y_df = pd.DataFrame()
    for group in alphabet:
        print(f'{datetime.datetime.now()} Working on group {group}')
        x_df_, y_df_ = build_xy(year, dy, dq, group)
        x_df = pd.concat([x_df, x_df_], axis=0)
        y_df = pd.concat([y_df, y_df_], axis=0)

    folder = '_'.join(['xy', str(dy), str(dq)])
    with open(os.path.join(DATA_FOLDER, folder, '_'.join(['x', str(year)])), 'wb') as handle:
        pickle.dump(x_df, handle)

    with open(os.path.join(DATA_FOLDER, folder, '_'.join(['y', str(year)])), 'wb') as handle:
        pickle.dump(y_df, handle)


if __name__ == '__main__':
    years = np.arange(1987, 2017)
    pool = Pool(14)
    pool.map(run_build_xy, years)

