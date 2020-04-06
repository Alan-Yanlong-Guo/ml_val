import pickle
from lightgbm import LGBMRegressor
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sklearn
import os
from datetime import datetime

def run_load_xy(years, dy=0, dq=1, save_dir='xy_data', aq='q'):
    DATA_FOLDER = '/nfs/home/mingweim/ml_val/data_all'

    folder = '_'.join(['xy', aq, str(dy), str(dq)])
    if not os.path.exists(os.path.join(DATA_FOLDER, folder)):
        raise Exception('Preprocessed xy data folder not found')

    x_df_set, y_df_set = pd.DataFrame(), pd.DataFrame()

    for year in years:
        with open(os.path.join(DATA_FOLDER, folder, '_'.join(['x', str(year)]) + '.pkl'), 'rb') as handle:
            x_df_ = pickle.load(handle)
        with open(os.path.join(DATA_FOLDER, folder, '_'.join(['y', str(year)]) + '.pkl'), 'rb') as handle:
            y_df_ = pickle.load(handle)
        x_df_set = pd.concat([x_df_set, x_df_], axis=0)
        y_df_set = pd.concat([y_df_set, y_df_], axis=0)

    return x_df_set, y_df_set


def train_test_process(data_start=1975, tr_start=1975, tr_duration=20, ts_duration=1, dy=0, dq=1, plot=False):

    tr_years = [data_start+i for i in range(tr_duration+tr_start-data_start)]
    print(tr_years)
    ts_years = [tr_start+tr_duration+i for i in range(ts_duration)]
    print(ts_years)

    x_tr_set, y_tr_set = run_load_xy(tr_years, dy=dy, dq=dq)
    x_ts_set, y_ts_set = run_load_xy(ts_years, dy=dy, dq=dq)

    x_tr = x_tr_set.iloc[:,5:]
    x_tr.replace([np.inf, -np.inf, 'inf', '-inf'], np.NaN, inplace=True)
    x_tr = x_tr.fillna(0).astype('float32')

    y_tr = y_tr_set.iloc[:,5:]
    y_tr.replace([np.inf, -np.inf, 'inf', '-inf'], np.NaN, inplace=True)
    y_tr = y_tr.fillna(0).astype('float32')

    x_ts = x_ts_set.iloc[:,5:]
    x_ts.replace([np.inf, -np.inf, 'inf', '-inf'], np.NaN, inplace=True)
    x_ts = x_ts.fillna(0).astype('float32')

    y_ts = y_ts_set.iloc[:,5:]
    y_ts.replace([np.inf, -np.inf, 'inf', '-inf'], np.NaN, inplace=True)
    y_ts = y_ts.fillna(0).astype('float32')

    oosr2 = {}
    y_trues = {}
    y_preds = {}
    max1 = {}
    max2 = {}
    max3= {}
    max4 = {}
    max5 = {}

    # year on year growth prediction

    for item in y_tr.columns:
        if 'revtq_1o1' not in item:
            continue
        # param_test = {'max_depth': [1, 2, 4], 'num_leaves': [2, 6], 'n_estimators': [20, 100, 200]}
        param_test = {'max_depth': [1], 'num_leaves': [2], 'n_estimators': [2]}
        mod = LGBMRegressor(objective='regression_l2', zero_as_missing=True)
        clf = sklearn.model_selection.GridSearchCV(mod , param_grid = param_test, scoring='r2', cv=3)
        clf.fit(x_tr.values, y_tr[item].values)
        predictions = clf.predict(x_ts.values)
        y_true = y_ts[item].values
        plt.bar(range(len(clf.best_estimator_.feature_importances_)), clf.best_estimator_.feature_importances_)
        plt.show()
        max_fea = []
        max_5 = clf.best_estimator_.feature_importances_.argsort()[-5:][::-1]
        for index in max_5:
            max_fea.append(x_tr.columns[index])
        max1[item], max2[item], max3[item], max4[item], max5[item] = max_fea[0], max_fea[1], max_fea[2], max_fea[3], max_fea[4]
        oosr2[item] = sklearn.metrics.r2_score(y_true, predictions)
        print(tr_start, item, oosr2[item], max_fea)
        print(datetime.now(), clf.best_params_)
        y_trues[item] = y_true
        y_preds[item] = predictions

    return oosr2, max1, max2, max3, max4, max5, y_trues, y_preds


oosr2df, max1df, max2df, max3df, max4df, max5df = None, None, None, None, None, None
y_true_array_dict = None
y_pred_array_dict = None


start_training_year = 1975
end_training_year = 1984


for tr_start in range(start_training_year, end_training_year, 1):

    oosr2, max1, max2, max3, max4, max5, y_trues, y_preds = train_test_process(data_start=start_training_year, tr_start=tr_start, tr_duration=3, ts_duration=1, dy=0, dq=1)
    oosr2_cum = {}

    if y_true_array_dict is None:
        y_true_array_dict = y_trues
        y_pred_array_dict = y_preds

    else:
        for key in y_trues.keys():
            y_true_array_dict[key] = np.concatenate([y_true_array_dict[key], y_trues[key]])
            y_pred_array_dict[key] = np.concatenate([y_pred_array_dict[key], y_preds[key]])
            oosr2_cum[key] = sklearn.metrics.r2_score(y_true_array_dict[key], y_pred_array_dict[key])

    if oosr2df is None:
        oosr2df = pd.DataFrame.from_dict(oosr2, orient='index', columns=[tr_start])
        max1df = pd.DataFrame.from_dict(max1, orient='index', columns=[tr_start])
        max2df = pd.DataFrame.from_dict(max2, orient='index', columns=[tr_start])
        max3df = pd.DataFrame.from_dict(max3, orient='index', columns=[tr_start])
        max4df = pd.DataFrame.from_dict(max4, orient='index', columns=[tr_start])
        max5df = pd.DataFrame.from_dict(max5, orient='index', columns=[tr_start])

    else:
        oosr2df = pd.concat([oosr2df, pd.DataFrame.from_dict(oosr2, orient='index', columns=[tr_start])], axis=1)
        max1df = pd.concat([max1df, pd.DataFrame.from_dict(max1, orient='index', columns=[tr_start])], axis=1)
        max2df = pd.concat([max2df, pd.DataFrame.from_dict(max2, orient='index', columns=[tr_start])], axis=1)
        max3df = pd.concat([max3df, pd.DataFrame.from_dict(max3, orient='index', columns=[tr_start])], axis=1)
        max4df = pd.concat([max4df, pd.DataFrame.from_dict(max4, orient='index', columns=[tr_start])], axis=1)
        max5df = pd.concat([max5df, pd.DataFrame.from_dict(max5, orient='index', columns=[tr_start])], axis=1)

    oosr2df.drop(columns=['avg'])
    oosr2df = pd.concat([oosr2df, pd.DataFrame.from_dict(oosr2_cum, orient='index', columns=['avg'])], axis=1)

    oosr2df.to_csv(os.path.join('/nfs/home/mingweim/ml_val/train/result', '_'.join(["oosr2", 'q', str(0), str(1), str(start_training_year), str(end_training_year)]) + '.csv'))
    max1df.to_csv(os.path.join('/nfs/home/mingweim/ml_val/train/result', '_'.join(["max1", 'q', str(0), str(1), str(start_training_year), str(end_training_year)]) + '.csv'))
    max2df.to_csv(os.path.join('/nfs/home/mingweim/ml_val/train/result', '_'.join(["max2", 'q', str(0), str(1), str(start_training_year), str(end_training_year)]) + '.csv'))
    max3df.to_csv(os.path.join('/nfs/home/mingweim/ml_val/train/result', '_'.join(["max3", 'q', str(0), str(1), str(start_training_year), str(end_training_year)]) + '.csv'))
    max4df.to_csv(os.path.join('/nfs/home/mingweim/ml_val/train/result', '_'.join(["max4", 'q', str(0), str(1), str(start_training_year), str(end_training_year)]) + '.csv'))
    max5df.to_csv(os.path.join('/nfs/home/mingweim/ml_val/train/result', '_'.join(["max5", 'q', str(0), str(1), str(start_training_year), str(end_training_year)]) + '.csv'))
