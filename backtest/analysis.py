import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
sns.set()


def compute_drawdown_duration_peaks(dd):
    iloc = np.unique(np.r_[(dd == 0).values.nonzero()[0], len(dd) - 1])
    iloc = pd.Series(iloc, index=dd.index[iloc])
    df = iloc.to_frame('iloc').assign(prev=iloc.shift())
    df = df[df['iloc'] > df['prev'] + 1].astype(int)
    # If no drawdown since no trade, avoid below for pandas sake and return nan series
    if not len(df):
        return (dd.replace(0, np.nan),) * 2
    df['duration'] = df['iloc'].map(dd.index.__getitem__) - df['prev'].map(dd.index.__getitem__)
    df['peak_dd'] = df.apply(lambda row: dd.iloc[row['prev']:row['iloc'] + 1].max(), axis=1)
    df = df.reindex(dd.index)

    return df['duration'], df['peak_dd']


def compute_stats(return_df, ls, ev):
    assert ls in ['long', 'short', 'ls'], 'invalid ls type'
    assert ev in ['equal', 'value'], 'invalid ev type'

    returns = return_df['_'.join([ls, ev])]
    equities = returns.add(1).cumprod()
    dds = 1 - equities / np.maximum.accumulate(equities)
    dd_durs, dd_peaks = compute_drawdown_duration_peaks(pd.Series(dds, index=return_df.index))

    stats = pd.Series(dtype=object)
    stats.loc['Start'] = return_df.index[0]
    stats.loc['End'] = return_df.index[-1]
    stats.loc['Duration'] = stats.End - stats.Start
    stats.loc['Equity Final [$]'] = equities[-1]
    stats.loc['Equity Peak [$]'] = equities.max()
    stats.loc['Return [%]'] = (equities[-1] - 1) * 100

    stats.loc['Max. Drawdown [%]'] = max_dd = -np.nan_to_num(dds.max()) * 100
    stats.loc['Avg. Drawdown [%]'] = -dd_peaks.mean() * 100
    stats.loc['Max. Drawdown Duration'] = dd_durs.max()
    stats.loc['Avg. Drawdown Duration'] = dd_durs.mean().round('S')

    stats.loc['Win Rate [%]'] = win_rate = (returns > 0).sum() / len(returns) * 100
    stats.loc['Best Trade [%]'] = returns.max() * 100
    stats.loc['Worst Trade [%]'] = returns.min() * 100
    stats.loc['Avg. Trade [%]'] = returns.mean() * 100
    stats.loc['Profit Factor'] = returns[returns > 0].sum() / abs(returns[returns < 0].sum())
    stats.loc['Expectancy [%]'] = returns[returns > 0].mean() * win_rate - returns[returns < 0].mean() * (100 - win_rate)

    mean_return = returns.mean()
    stats.loc['Sharpe Ratio'] = mean_return / returns.std()
    stats.loc['Sortino Ratio'] = mean_return / returns[returns < 0].std()
    stats.loc['Calmar Ratio'] = mean_return / (-max_dd / 100)

    return stats


def plot_return(return_df):
    timestamps = np.array(return_df.index)
    equities = lambda ls: np.log(np.array(return_df[ls].add(1).cumprod()))

    fig = plt.figure(1, figsize=(14, 7))
    plt.plot(timestamps, equities('ls_equal'), 'k-')
    plt.plot(timestamps, equities('long_equal'), 'b-')
    plt.plot(timestamps, -equities('short_equal'), 'r-')
    plt.plot(timestamps, equities('ls_value'), 'k--')
    plt.plot(timestamps, equities('long_value'), 'b--')
    plt.plot(timestamps, -equities('short_value'), 'r--')
    plt.legend(['L-S EW', 'L EW', 'S EW', 'L-S VW', 'L VW', 'S VW'])
    # plt.xlim(cumulative_num_day_list[0], cumulative_num_day_list[-1])
    # plt.xticks(cumulative_num_day_list[0::2], year_list[0::2])
    plt.grid('on')
    plt.show()

    return fig