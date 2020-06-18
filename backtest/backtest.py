import pandas as pd
import os
import numpy as np
import datetime


def predict_year_return(year_test_i, year_test_f, holding_period):
    # Find business date and unique date + get sp500
    business_date_list_full = np.array(SP500_TABLE.index)
    sp500_list_full = np.array(SP500_TABLE['Adj Close'])
    business_date_list = []
    sp_500_list = []
    for index, business_date in enumerate(business_date_list_full):
        year = int(business_date.split('-')[0])
        month = int(business_date.split('-')[1])

        if year in np.arange(year_test_i, year_test_f + 1):
            if year == 2017:
                if month <= 7:
                    business_date_list.append(business_date_list_full[index])
                    sp_500_list.append((sp500_list_full[index + 1] - sp500_list_full[index]) / sp500_list_full[index])
            else:
                business_date_list.append(business_date_list_full[index])
                sp_500_list.append((sp500_list_full[index + 1] - sp500_list_full[index]) / sp500_list_full[index])

    # Initialize return and turnover list
    long_equal_return_list = []
    short_equal_return_list = []
    long_value_return_list = []
    short_value_return_list = []
    short_permno_list = []
    long_permno_list = []

    # Initialize portfolio
    for id, business_date in enumerate(business_date_list):
        word_occur_return_df_day = word_occur_return_df_test.loc[word_occur_return_df_test['est_date'] == business_date]
        word_occur_return_df_day = combine_permno(word_occur_return_df_day, dictionary)
        long_permnos, short_permnos, long_equal_returns, short_equal_returns, long_caps, short_caps \
            = predict_day_return(params, word_occur_return_df_day, dictionary, delay=1)

        long_permno_list.append(long_permnos)
        short_permno_list.append(short_permnos)

        # Daily return
        long_equal_return, long_value_return = daily_return(long_permnos, long_equal_returns, long_caps)
        long_equal_return_list.append(long_equal_return)
        long_value_return_list.append(long_value_return)

        short_equal_return, short_value_return = daily_return(short_permnos, short_equal_returns, short_caps)
        short_equal_return_list.append(short_equal_return)
        short_value_return_list.append(short_value_return)

    return_df = pd.DataFrame({'long_equal_return': long_equal_return_list,
                              'long_value_return': long_value_return_list,
                              'short_equal_return': short_equal_return_list,
                              'short_value_return': short_value_return_list,
                              'sp500_return': sp_500_list,
                              'short_permno': short_permno_list,
                              'long_permno': long_permno_list}, index=business_date_list)

    return return_df


def daily_return(permno_array, equal_return_array, cap_array):
    if len(permno_array) != 0:
        equal_return = np.mean(equal_return_array)
        value_weight = cap_array / np.sum(cap_array)
        value_return = np.sum(np.multiply(equal_return_array, value_weight))
    else:
        equal_return, value_return = 0.0, 0.0

    return equal_return, value_return
