import pickle
import os
import pandas as pd
from tools.utils import tics_to_permnos
from global_settings import DATA_FOLDER, links_df
from tools.utils import x_filter, y_filter, horizon
from global_settings import TRAIN_YEAR, CROSS_YEAR, TEST_YEAR
import numpy as np
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
    x = pd.concat([x_index.reset_index(drop=True), x_annual.reset_index(drop=True), x_quarter.reset_index(drop=True),
                   x_month.reset_index(drop=True)], axis=1)

    return x


def line_y(permno, y_annual, y_quarter, y_ay, y_qy, y_qq, date):
    # Slice Data
    y_annual[date] = pd.to_datetime(y_annual[date])
    y_quarter[date + 'q'] = pd.to_datetime(y_quarter[date + 'q'])

    if y_qq == 4:
        y_annual = y_annual.loc[[(permno, y_ay, 4)], :]
    else:
        y_annual = y_annual.loc[[(permno, y_ay-1, 4)], :]

    y_annual = y_annual.iloc[:, 5:]

    y_quarter = y_quarter.loc[[(permno, y_qy, y_qq)], :]
    y_index = y_quarter.iloc[:, :5]
    y_my, y_mm = y_quarter[date + 'q'].dt.year[0], y_quarter[date + 'q'].dt.month[0]
    y_quarter = y_quarter.iloc[:, 5:]

    # Filter Data
    y_annual, y_quarter = y_filter(y_annual, y_quarter)
    y = pd.concat([y_index.reset_index(drop=True), y_annual.reset_index(drop=True), y_quarter.reset_index(drop=True)],
                  axis=1)

    return y, y_my, y_mm


def build_xy(year, dy, dq, group):
    y_annual, y_quarter, x_annual, x_quarter, x_month = load_x_y(group)
    y_quarter.rename({'datadate': 'datadateq'}, inplace=True)
    x_quarter.rename({'datadate': 'datadateq'}, inplace=True)
    x_month.rename({'datadate': 'datadateq'}, inplace=True)
    date = 'fdate' if dy == 0 else 'datadate'

    y_annual.dropna(subset=[date], inplace=True, axis=0)
    y_quarter.dropna(subset=[date + 'q'], inplace=True, axis=0)
    tics = tuple([symbol for symbol in links_df['SYMBOL'] if symbol[0] == group])
    permnos = tics_to_permnos(tics)

    x_df_ = pd.DataFrame()
    y_df_ = pd.DataFrame()

    # Build yearly data for the group
    for permno in permnos:
        y_qy = year
        for quarter in [1, 2, 3, 4]:
            y_qq = quarter
            if quarter == 4:
                y_ay = year
            else:
                y_ay = year - 1
            try:
                y, y_my, y_mm = line_y(permno, y_annual, y_quarter, y_ay, y_qy, y_qq, date)
                x_ay, x_qy, x_qq, x_my, x_mm = horizon(y_ay, y_qy, y_qq, y_my, y_mm, dy, dq)
                x = line_x(permno, x_annual, x_quarter, x_month, x_ay, x_qy, x_qq, x_my, x_mm, date)

                if np.shape(x)[0] == 1 and np.shape(y)[0] == 1:
                    x_df_ = pd.concat([x_df_, x], axis=0)
                    y_df_ = pd.concat([y_df_, y], axis=0)

            except KeyError:
                pass

    x_df_.reset_index(drop=True, inplace=True)
    y_df_.reset_index(drop=True, inplace=True)

    return x_df_, y_df_


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
    if not os.path.exists(os.path.join(DATA_FOLDER, folder)):
        os.mkdir(os.path.join(DATA_FOLDER, folder))

    with open(os.path.join(DATA_FOLDER, folder, '_'.join(['x', str(year)]) + '.pkl'), 'wb') as handle:
        pickle.dump(x_df, handle)

    with open(os.path.join(DATA_FOLDER, folder, '_'.join(['y', str(year)]) + '.pkl'), 'wb') as handle:
        pickle.dump(y_df, handle)


def run_load_xy(years, set_name, dy=1, dq=0):
    folder = '_'.join(['xy', str(dy), str(dq)])
    if not os.path.exists(os.path.join(DATA_FOLDER, folder)):
        raise Exception('Folder not found')

    x_df_set, y_df_set = pd.DataFrame(), pd.DataFrame()

    for year in years:
        with open(os.path.join(DATA_FOLDER, folder, '_'.join(['x', str(year)]) + '.pkl'), 'rb') as handle:
            x_df_ = pickle.load(handle)
        with open(os.path.join(DATA_FOLDER, folder, '_'.join(['y', str(year)]) + '.pkl'), 'rb') as handle:
            y_df_ = pickle.load(handle)
        x_df_set = pd.concat([x_df_set, x_df_], axis=0)
        y_df_set = pd.concat([y_df_set, y_df_], axis=0)

    with open(os.path.join(DATA_FOLDER, folder, '_'.join(['x', str(set_name)]) + '.pkl'), 'wb') as handle:
        pickle.dump(x_df_set, handle)

    with open(os.path.join(DATA_FOLDER, folder, '_'.join(['y', str(set_name)]) + '.pkl'), 'wb') as handle:
        pickle.dump(y_df_set, handle)


if __name__ == '__main__':
    years = np.arange(1987, 2017)
    pool = Pool(14)
    pool.map(run_build_xy, years)
