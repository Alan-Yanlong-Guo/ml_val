from global_settings import sp500_full
from utils.model_tools import construct_daily
import numpy as np

# def construct_portfolio(business_day):
#     long_permno, short_permno = tuple(['84788', '89393']), tuple(['78877', '53613'])
#     # LONG: AMAZON, NETFLIX; SHORT: CHESAPEAKE, MICRON
#     return long_permno, short_permno


def construct_portfolio(business_day, lag=5):
    # permno = ['84788', '89393', '78877', '53613']
    permno = ['14593', '90319', '13407', '10107']  # APPLE, GOOGLE, FACEBOOK, MICROSOFT
    idx = list(sp500_full['Date']).index(business_day)
    business_lag = sp500_full['Date'][idx - lag]
    recent = []

    for p in permno:
        daily_df_0 = construct_daily(business_day, p)
        daily_df_l = construct_daily(business_lag, p)
        adjprc_0 = float(daily_df_0['prc']) / float(daily_df_0['cfacpr'])
        adjprc_l = float(daily_df_l['prc']) / float(daily_df_l['cfacpr'])
        recent.append(adjprc_0/adjprc_l - 1)

    short_permno = list(np.array(permno)[np.array(recent).argsort()[:len(permno)//2]])
    long_permno = list(np.array(permno)[np.array(recent).argsort()[len(permno)//2:]])

    return long_permno, short_permno
