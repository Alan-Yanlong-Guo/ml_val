import pandas as pd
import numpy as np
import wrds
from tools.utils import tics_to_permnos
from pandas.tseries.offsets import *
conn = wrds.Connection(wrds_username='dachxiu')
conn.create_pgpass_file()

def build_temp6(tics, temp2, compq6):
    permno = tics_to_permnos(tics)

    def lag(df, col, n=1, on='gvkey'):
        z = df.groupby(on)[col].shift(n)
        z = z.reset_index()
        z = z.sort_values('index')
        z = z.set_index('index')
        return z[col]

    z = pd.merge(compq6.rename(columns={'lpermno': 'permno'}), temp2, on='permno')

    z['date_l'] = z['date'] + pd.TimedeltaIndex([-10]*len(z), 'M')
    z['date_u'] = z['date'] + pd.TimedeltaIndex([-5]*len(z), 'M')
    z['datadate_q'] = pd.to_datetime(z['datadate_q'])
    z = z[(z['datadate_q'] >= z['date_l']) & (z['datadate_q'] <= z['date_u'])]

    z['rdq_date_u'] = z['date'] + pd.TimedeltaIndex([-5]*len(z), 'M')
    z['rdq'] = pd.to_datetime(z['rdq'])
    temp3 = z[np.busday_offset(z['rdq'].values.astype('datetime64[D]'), 1, roll='forward') < z['rdq_date_u']]

    temp3 = temp3.sort_values(['permno','date','datadate_q'], ascending=[True,True,False])
    temp3 = temp3.drop_duplicates(['permno','date'])

    lst = compq6[(compq6['lpermno'].notna()) & (compq6['rdq'].notna())]
    lst = lst.sort_values(['lpermno','rdq'])
    lst = lst.drop_duplicates(['lpermno','rdq'])
    lst = lst[['lpermno','rdq']]

    lst['eamonth'] = 1
    temp3['date'] = pd.to_datetime(temp3['date'])
    lst['rdq'] = pd.to_datetime(lst['rdq'])

    temp3['month'] = temp3['date'].dt.month
    temp3['year'] = temp3['date'].dt.year
    lst['month'] = lst['rdq'].dt.month
    lst['year'] = lst['rdq'].dt.year
    temp3 = pd.merge(temp3, lst, how='left', left_on=['permno', 'month', 'year'], right_on=['lpermno', 'month', 'year'])

    temp3['ms'] = temp3['m1'] + temp3['m2'] + temp3['m3'] + temp3['m4'] + temp3['m5'] + temp3['m6'] + temp3['m7'] + temp3['m8']

    # TODO: Check this condition
    # temp3 = temp3[(temp3['siccd'] >= 7000) & (temp3['siccd'] <= 9999)]

    ibessum = conn.raw_sql(f"""
                            select ticker, cusip, fpedats, statpers, ANNDATS_ACT,
                            numest, ANNTIMS_ACT, medest, meanest, actual, stdev from ibes.statsum_epsus
                            where ticker in {tics}
                            and fpi='1'
                            and statpers<ANNDATS_ACT
                            and measure='EPS'
                            and (fpedats-statpers)>=0;
                            """)
    ibessum = ibessum[(ibessum['medest'].notna()) & (ibessum['fpedats'].notna())]
    ibessum = ibessum.sort_values(['ticker','cusip','statpers','fpedats'], ascending=[True,True,True,False])
    ibessum = ibessum.sort_values(['ticker','cusip','statpers'])
    ibessum = ibessum.drop_duplicates(['ticker','cusip','statpers'])

    ibessum['disp'] = ibessum['stdev']/np.abs(ibessum['meanest'])
    ibessum.loc[ibessum['meanest']==0, 'disp'] = ibessum['stdev']/0.01
    ibessum['chfeps'] = np.nan

    ibessum2 = conn.raw_sql(f"""
                            select ticker, cusip, fpedats, statpers, ANNDATS_ACT,
                            numest, ANNTIMS_ACT, medest, meanest, actual, stdev from ibes.statsum_epsus
                            where ticker in {tics}
                            and fpi='0'
                            """)
    ibessum2 = ibessum2[(ibessum2['medest'].notna()) & (ibessum2['meanest'].notna())]
    ibessum2 = ibessum2.sort_values(['cusip','statpers'])
    ibessum2 = ibessum2.drop_duplicates(['cusip','statpers'])
    ibessum2 = ibessum2.rename(columns={'meanest':'fgr5yr'})

    ibessum2b = pd.merge(ibessum, ibessum2[['fgr5yr', 'ticker','cusip','statpers']], how='left', on=['ticker','statpers','cusip'])
    #exactly the same number as in sas (1445522 rows)

    rec = conn.raw_sql(f"""
                       select * from ibes.recdsum
                       where ticker in {tics}
                       """)
    rec = rec[(rec['statpers'].notna()) & (rec['meanrec'].notna())]

    ibessum2b = pd.merge(ibessum2b, rec[['meanrec','ticker','cusip','statpers']], how='left', on=['ticker','cusip','statpers'])
    ibessum2b = ibessum2b.sort_values(['ticker','statpers'])

    ibessum2c = ibessum2b.copy()
    ibessum2c['chrec'] = ibessum2b['meanrec'] - (1/2)*lag(ibessum2b, 'meanrec', 1, 'ticker') - (1/2)*lag(ibessum2b, 'meanrec', 2, 'ticker') \
        - (1/3)*lag(ibessum2b, 'meanrec', 3, 'ticker') - (1/3)*lag(ibessum2b, 'meanrec', 4, 'ticker') - (1/3)*lag(ibessum2b, 'meanrec', 5, 'ticker')

    names = conn.raw_sql(f"""
                          select * from crsp.msenames
                          where permno in {permno}
                          and ncusip != ''
                          """)
    names = names.sort_values(['permno','ncusip'])
    names = names.drop_duplicates(['permno','ncusip'])

    ibessum2b = pd.merge(ibessum2c, names[['permno','ncusip']], how='left', left_on=['cusip'], right_on=['ncusip'])
    temp4 = pd.merge(temp3, ibessum2b, how='left', on='permno')
    temp4['sfe'] = temp4['meanest']/np.abs(temp4['prccq'])
    temp4['statpers'] = pd.to_datetime(temp4['statpers'])
    temp4 = temp4[temp4['statpers'].isna() | (temp4['statpers'] >= (temp4['date'] + pd.TimedeltaIndex([-4]*len(temp4), 'M')) + MonthBegin(-1))]
    temp4 = temp4[temp4['statpers'].isna() | (temp4['statpers'] <= (temp4['date'] + pd.TimedeltaIndex([-1]*len(temp4), 'M')) + MonthEnd(0))]

    temp4 = temp4.sort_values(['permno', 'date', 'statpers'], ascending=[True, True, False])
    temp4 = temp4.drop_duplicates(['permno','date'])
    temp4 = temp4.rename(columns={'numest':'nanalyst'})

    temp4['year'] = temp4['date'].dt.year
    temp4.loc[(temp4['year'] >= 1989) & (temp4['nanalyst'].isna()), 'nanalyst'] = 0
    temp4.loc[(temp4['year'] >= 1989) & (temp4['fgr5yr'].isna()), 'ltg'] = 0
    temp4.loc[(temp4['year'] >= 1989) & (temp4['fgr5yr'].notna()), 'ltg'] = 1

    temp4.loc[(temp4['year'] < 1989), ['disp','chfeps','meanest','nanalyst','sfe','ltg','fgr5yr']] = np.nan
    temp4.loc[(temp4['year'] < 1994), ['meanrec','chrec']] = np.nan

    ewret = temp4.groupby('date')['ret'].mean().reset_index()
    temp4 = temp4.drop(['ret'], axis=1, inplace=False)
    temp4 = pd.merge(temp4, ewret, how='left', on=['date'])
    temp4 = temp4.sort_values(['permno','date'])
    temp4['count']=temp4.groupby(['permno']).cumcount()

    def lag(df, col, n=1, on='permno'):
        z = df.groupby(on)[col].shift(n)
        z = z.reset_index()
        z = z.sort_values('index')
        z = z.set_index('index')
        return z[col]

    temp6 = temp4.copy()
    temp6['chnanalyst'] = temp6['nanalyst'] - lag(temp6, 'nanalyst', 3)
    temp6['mom6m'] = ((1+lag(temp6,'ret',2)) * (1+lag(temp6, 'ret', 3)) * (1+lag(temp6,'ret',4)) * (1+lag(temp6, 'ret', 5)) * (1+lag(temp6,'ret',6))) -1
    temp6['mom12m'] = ((1+lag(temp6,'ret',2)) * (1+lag(temp6, 'ret', 3)) * (1+lag(temp6,'ret',4)) * (1+lag(temp6, 'ret', 5)) * (1+lag(temp6,'ret',6)) * \
        (1+lag(temp6,'ret',7))*(1+lag(temp6,'ret',8))*(1+lag(temp6,'ret',9))*(1+lag(temp6,'ret',10))*(1+lag(temp6,'ret',11))*(1+lag(temp6,'ret',12)))-1
    temp6['mom36m'] = ((1+lag(temp6,'ret',2)) * (1+lag(temp6, 'ret', 3)) * (1+lag(temp6,'ret',4)) * (1+lag(temp6, 'ret', 5)) * (1+lag(temp6,'ret',6)) * \
        (1+lag(temp6,'ret',7))*(1+lag(temp6,'ret',8))*(1+lag(temp6,'ret',9))*(1+lag(temp6,'ret',10))*(1+lag(temp6,'ret',11))*(1+lag(temp6,'ret',12)) * \
        (1+lag(temp6,'ret',13))*(1+lag(temp6,'ret',14))*(1+lag(temp6,'ret',15))*(1+lag(temp6,'ret',16))*(1+lag(temp6,'ret',17))*(1+lag(temp6,'ret',18)) * \
        (1+lag(temp6,'ret',19))*(1+lag(temp6,'ret',20))*(1+lag(temp6,'ret',21))*(1+lag(temp6,'ret',22))*(1+lag(temp6,'ret',23))*(1+lag(temp6,'ret',24)) * \
        (1+lag(temp6,'ret',25))*(1+lag(temp6,'ret',26))*(1+lag(temp6,'ret',27))*(1+lag(temp6,'ret',28))*(1+lag(temp6,'ret',29))*(1+lag(temp6,'ret',30)) * \
        (1+lag(temp6,'ret',31))*(1+lag(temp6,'ret',32))*(1+lag(temp6,'ret',33))*(1+lag(temp6,'ret',34))*(1+lag(temp6,'ret',35))*(1+lag(temp6,'ret',36)))-1
    temp6['mom1m'] = lag(temp6, 'ret', 1)
    temp6['dolvol'] = np.log(lag(temp6, 'ret', 2) * lag(temp6, 'prc', 2))
    temp6['chmom'] = ((1+lag(temp6,'ret',1)) * (1+lag(temp6,'ret',2)) * (1+lag(temp6,'ret',3)) * (1+lag(temp6,'ret',4)) * (1+lag(temp6,'ret',5)) * (1+lag(temp6,'ret',6)))-1\
        - ((1+lag(temp6,'ret',7))*(1+lag(temp6,'ret',8))*(1+lag(temp6,'ret',9))*(1+lag(temp6,'ret',10))*(1+lag(temp6,'ret',11))*(1+lag(temp6,'ret',12))-1)
    temp6['turn'] = (lag(temp6,'vol',1) + lag(temp6,'vol',2) + lag(temp6,'vol',3))/(3*temp6['shrout'])
    temp6['retcons_pos'] = 0
    temp6.loc[(lag(temp6,'ret',1)>0) & (lag(temp6,'ret',2)>0) & (lag(temp6,'ret',3)>0) & (lag(temp6,'ret',4)>0) & (lag(temp6,'ret',5)>0) & (lag(temp6,'ret',6)>0), 'retcons_pos'] = 1
    temp6['retcons_pos'] = 0
    temp6.loc[(lag(temp6,'ret',1)<0) & (lag(temp6,'ret',2)<0) & (lag(temp6,'ret',3)<0) & (lag(temp6,'ret',4)<0) & (lag(temp6,'ret',5)<0) & (lag(temp6,'ret',6)<0), 'retcons_pos'] = 1

    temp6['moms12m'] = (lag(temp6,'ret',1) + lag(temp6,'ret',2) + lag(temp6,'ret',3) + lag(temp6,'ret',4) + lag(temp6,'ret',5) + lag(temp6,'ret',6) + lag(temp6,'ret',7) + lag(temp6,'ret',8) + lag(temp6,'ret',9) + lag(temp6,'ret',10) + lag(temp6,'ret',11))/11.0
    temp6['x_1'] = temp6['ret'] - temp6['retx']
    temp6['dpqq'] = (temp6['x_1']*temp6['shrout']+lag(temp6,'x_1')*lag(temp6,'shrout')+lag(temp6,'x_1',2)*lag(temp6,'shrout'))/temp6['mvel1']

    return temp6
