from global_settings import conn, sp500_full
import pandas as pd
import numpy as np
from backtest.analysis import compute_stats
from tqdm import tqdm_notebook
import datetime
import os


def backtest(year_test_i, year_test_f):
    year_full = pd.DatetimeIndex(sp500_full['Date']).year
    sp500 = sp500_full[(year_full >= year_test_i) & (year_full <= year_test_f)]
    sp500.reset_index(inplace=True, drop=True)

    # Initialize return list
    short_permnos, long_permnos = [], []
    long_equals, short_equals, ls_equals = [], [], []
    long_values, short_values, ls_values = [], [], []

    # Long-short portfolio
    for business_day in tqdm_notebook(sp500['Date']):
        long_permno, short_permno = construct_portfolio(business_day)
        long_permnos.append(long_permno); short_permnos.append(short_permno)

        long_equal, long_value = trade_portfolio(business_day, long_permno, ls='long')
        short_equal, short_value = trade_portfolio(business_day, short_permno, ls='short')
        ls_equal, ls_value = long_equal + short_equal, long_value + short_value
        long_equals.append(long_equal); short_equals.append(short_equal); ls_equals.append(ls_equal)
        long_values.append(long_value); short_values.append(short_value); ls_values.append(ls_value)

    return_df = pd.DataFrame({'short_permno': short_permnos, 'long_permno': long_permnos,
                              'long_equal': long_equals, 'short_equal': short_equals, 'ls_equal': ls_equals,
                              'long_value': long_values, 'short_value': short_values, 'ls_value': ls_values,
                              'sp500': sp500['Return']}, index=sp500['Date'])

    return return_df


def trade_portfolio(business_day, permno, ls):
    assert ls in ['long', 'short'], 'invalid ls type'
    if len(permno) == 0:
        return 0.0, 0.0

    daily_df = conn.raw_sql(f"""
                            select a.date, a.permno, b.ticker, b.shrcd, b.siccd, a.ret, 
                            abs(a.prc) as prc, a.shrout, a.cfacpr, a.cfacshr
                            from crsp.dsf as a
                            left join crsp.msenames as b
                            on a.permno = b.permno
                            and b.namedt <= a.date
                            and a.date <= b.nameendt
                            and a.date = '{business_day}'
                            where b.permno in {permno}
                            """)
    daily_df['date'] = pd.to_datetime(daily_df['date'])

    equal_weight = np.ones_like(daily_df['ret']) / len(permno)
    equal = sum(daily_df['ret'] * equal_weight)

    value_weight_ = (daily_df['prc'] / daily_df['cfacpr']) * (daily_df['shrout'] * daily_df['cfacshr'])
    value_weight = value_weight_ / value_weight_.sum()
    value = sum(daily_df['ret'] * value_weight)

    if ls == 'short':
        equal, value = -equal, -value

    return equal, value


# def construct_portfolio(business_day):
#     long_permno, short_permno = tuple(['14593', '90319']), tuple(['25785', '12490'])
#     return long_permno, short_permno

