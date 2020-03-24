import pandas as pd
import numpy as np
import wrds
from pandas.tseries.offsets import *
conn = wrds.Connection(wrds_username='dachxiu')
old_err_state = np.seterr(divide='raise')
ignored_states = np.seterr(**old_err_state)


def lag(df, col, n=1, on='gvkey'):
    return df.groupby(on)[col].shift(n)


def build_comp():

    comp = conn.raw_sql(f"""
                        select
                        fyear, apdedate, datadate, pdate, fdate, c.gvkey, f.cusip as cnum, datadate as datadate_a, 
                        c.cik, sic as sic2, sic, naics, sale, revt, cogs, xsga, xrd, xad, ib, ebitda, ebit, nopi, 
                        spi, pi, txp, ni, txfed, txfo, txt, xint, capx, oancf, dvt, ob, gdwlia, gdwlip, gwo, rect, act, 
                        che, ppegt, invt, at, aco, intan, ao, ppent, gdwl, fatb, fatl, lct, dlc, dltt, lt, dm, dcvt, 
                        cshrc, dcpstk, pstk, ap, lco, lo, drc, drlt, txdi, ceq, scstkc, emp, csho, /*addition*/
                        pstkrv, pstkl, txditc, datadate as year, /*market*/
                        abs(prcc_f) as prcc_f, csho*prcc_f as mve_f, /*HXZ*/
                        am, ajex, txdb, seq, dvc, dvp, dp, dvpsx_f, mib, ivao, ivst, sstk, prstkc,
                        dv, dltis, dltr, dlcch, oibdp, dvpa, tstkp, oiadp, xpp, xacc, re, ppenb,
                        ppenls, capxv, fopt, wcap
                        from comp.names as c, comp.funda as f
                        where f.gvkey=c.gvkey
                        /*get consolidated, standardized, industrial format statements*/
                        and f.indfmt='INDL'
                        and f.datafmt='STD'
                        and f.popsrc='D'
                        and f.consol='C'
                        """)

    comp.cnum = comp.cnum.replace(' ', '').str.slice(0, 6)
    comp.sic2 = comp.sic2 + '12'
    comp.apdedate = pd.to_datetime(comp.apdedate)
    comp.datadate = pd.to_datetime(comp.datadate)
    comp.pdate = pd.to_datetime(comp.pdate)
    comp.fdate = pd.to_datetime(comp.fdate)
    comp.year = comp['datadate'].dt.year
    comp = comp.dropna(subset=['at', 'prcc_f', 'ni'])

    comp['ps_beme'] = np.where(comp['pstkrv'].isnull(), comp['pstkl'], comp['pstkrv'])
    comp['ps_beme'] = np.where(comp['ps_beme'].isnull(), comp['pstk'], comp['ps_beme'])
    comp['ps_beme'] = np.where(comp['ps_beme'].isnull(), 0, comp['ps_beme'])
    comp['txditc'] = comp['txditc'].fillna(0)
    comp['be'] = comp['ceq']+comp['txditc']-comp['ps_beme']
    comp['be'] = np.where(comp['be'] > 0, comp['be'], None)

    comp = comp.sort_values(by=['gvkey', 'datadate']).drop_duplicates(['gvkey', 'datadate'])
    comp['count'] = comp.groupby(['gvkey']).cumcount()

    return comp


def build_crsp_m():

    crsp_m = conn.raw_sql(f"""
                          select a.permno, a.permco, a.date, b.ticker, b.ncusip, b.shrcd, b.exchcd, b.siccd,
                          a.prc, a.ret, a.retx, a.shrout, a.vol
                          from crsp.msf as a
                          left join crsp.msenames as b
                          on a.permno=b.permno
                          where b.namedt<=a.date
                          and a.date<=b.nameendt
                          and b.exchcd between 1 and 3
                          and b.shrcd between 10 and 11
                          """)

    crsp_m['me'] = crsp_m['prc'].abs()*crsp_m['shrout']
    crsp_m['prca'] = crsp_m['prc'].abs()
    crsp_m['lprc'] = crsp_m.groupby(['permno','permco'])['prca'].shift(1)
    crsp_m['lme'] = crsp_m.groupby(['permno','permco'])['me'].shift(1)
    crsp_m[['permco', 'permno', 'shrcd', 'exchcd']] = crsp_m[['permco', 'permno', 'shrcd', 'exchcd']].astype(int)

    # Line up date to be end of month
    crsp_m['date'] = pd.to_datetime(crsp_m['date'])
    crsp_m['jdate'] = crsp_m['date']+MonthEnd(0)

    return crsp_m


def build_dlret():
    dlret = conn.raw_sql(f"""
                         select permno, dlret, dlstdt
                         from crsp.msedelist
                         """)
    dlret.permno = dlret.permno.astype(int)

    # Line up date to be end of month
    dlret['dlstdt'] = pd.to_datetime(dlret['dlstdt'])
    dlret['jdate'] = dlret['dlstdt']+MonthEnd(0)

    return dlret


def build_crsp(crsp_m, dlret):
    crsp = pd.merge(crsp_m, dlret, how='left', on=['permno', 'jdate'])
    crsp['dlret'] = crsp['dlret'].fillna(0)
    crsp['ret'] = crsp['ret'].fillna(0)
    crsp['retadj'] = (1 + crsp['ret']) * (1 + crsp['dlret']) - 1
    crsp = crsp.sort_values(['permno', 'date'])
    crsp = crsp.drop(['dlret', 'dlstdt'], axis=1)
    crsp = crsp.sort_values(by=['jdate', 'permco', 'me']).drop_duplicates()

    crsp_summe = crsp.groupby(['jdate', 'permco'])['me'].sum().reset_index()
    crsp_maxme = crsp.groupby(['jdate', 'permco'])['me'].max().reset_index()
    crsp1 = pd.merge(crsp, crsp_maxme, how='inner', on=['jdate', 'permco', 'me'])
    crsp1 = crsp1.drop(['me'], axis=1)
    crsp2 = pd.merge(crsp1, crsp_summe, how='inner', on=['jdate', 'permco'])
    crsp2 = crsp2.sort_values(by=['permno', 'jdate']).drop_duplicates()
    crsp2['year'] = crsp2['jdate'].dt.year
    crsp2['month'] = crsp2['jdate'].dt.month

    decme = crsp2[crsp2['month'] == 12]
    decme = decme[['permno', 'date', 'jdate', 'me', 'year']].rename(columns={'me': 'dec_me'})

    ### July to June dates
    crsp2['ffdate'] = crsp2['jdate'] + MonthEnd(-6)
    crsp2['ffyear'] = crsp2['ffdate'].dt.year
    crsp2['ffmonth'] = crsp2['ffdate'].dt.month
    crsp2['1+retx'] = 1 + crsp2['retx']
    crsp2 = crsp2.sort_values(by=['permno', 'date'])
    crsp2['cumretx'] = crsp2.groupby(['permno', 'ffyear'])['1+retx'].cumprod()
    crsp2['lcumretx'] = crsp2.groupby(['permno'])['cumretx'].shift(1)
    crsp2['lme'] = crsp2.groupby(['permno'])['me'].shift(1)
    crsp2['count'] = crsp2.groupby(['permno']).cumcount()
    crsp2['lme'] = np.where(crsp2['count'] == 0, crsp2['me'] / crsp2['1+retx'], crsp2['lme'])
    mebase = crsp2[crsp2['ffmonth'] == 1][['permno', 'ffyear', 'lme']].rename(columns={'lme': 'mebase'})

    # merge result back together
    crsp3 = pd.merge(crsp2, mebase, how='left', on=['permno', 'ffyear'])
    crsp3['wt'] = np.where(crsp3['ffmonth'] == 1, crsp3['lme'], crsp3['mebase'] * crsp3['lcumretx'])
    decme['year'] = decme['year'] + 1
    decme = decme[['permno', 'year', 'dec_me']]

    # Info as of June
    crsp3_jun = crsp3[crsp3['month'] == 6]
    crsp_jun = pd.merge(crsp3_jun, decme, how='inner', on=['permno', 'year'])
    crsp_jun = crsp_jun.sort_values(by=['permno', 'jdate']).drop_duplicates()
    crsp_jun = crsp_jun.drop(columns=['count'])

    return crsp_jun


def build_ccm_data(comp, crsp_jun):

    ccm = conn.raw_sql(f"""
                       select gvkey, lpermno as permno, linktype, linkprim,
                       linkdt, linkenddt
                       from crsp.ccmxpf_linktable
                       where substr(linktype,1,1)='L'
                       and linkprim in ('P', 'C')
                       """)

    ccm['linkdt'] = pd.to_datetime(ccm['linkdt'])
    ccm['linkenddt'] = pd.to_datetime(ccm['linkenddt'])
    # if linkenddt is missing then set to next month's june (Changed)
    ccm['linkenddt'] = ccm['linkenddt'].fillna(pd.to_datetime('today')+YearEnd(0)+MonthEnd(6))
    ccm['linkenddt'] = ccm['linkenddt'].dt.date
    ccm['linkenddt'] = pd.to_datetime(ccm['linkenddt'])

    ccm1 = pd.merge(comp, ccm, how='left', on=['gvkey'])
    ccm1['yearend'] = ccm1['datadate']+YearEnd(0)
    ccm1['jdate'] = ccm1['yearend']+MonthEnd(6)

    ccm2 = ccm1[(ccm1['jdate'] >= ccm1['linkdt']) & (ccm1['jdate'] <= (ccm1['linkenddt']))]
    ccm2 = ccm2.drop(columns=['datadate_a','linktype','linkdt','linkenddt'])

    ccm_data = pd.merge(ccm2, crsp_jun, how='left', on=['permno', 'jdate'])

    ccm_data = ccm_data[ccm_data.dec_me != 0]
    ccm_data['beme'] = ccm_data['be']*1000/ccm_data['dec_me']

    # drop duplicates
    ccm_data = ccm_data.sort_values(by=['permno', 'date']).drop_duplicates()
    ccm_data = ccm_data.sort_values(by=['gvkey', 'date']).drop_duplicates()

    # Note: Different from SAS, Python count start from zero, will see if I need to add 1 to better serve the need
    ccm_data['count'] = ccm_data.groupby(['gvkey']).cumcount()

    # Parallel to the cleaning step for 'dr'
    ccm_data['dr'] = np.where(ccm_data.drc.notna() & ccm_data.drlt.notna(), ccm_data.drc+ccm_data.drlt, None)
    ccm_data['dr'] = np.where(ccm_data.drc.notna() & ccm_data.drlt.isna(), ccm_data.drc, ccm_data['dr'])
    ccm_data['dr'] = np.where(ccm_data.drc.isna() & ccm_data.drlt.notna(), ccm_data.drlt, ccm_data['dr'])
    # Parallel to the cleaning step for 'dc'
    ccm_data.loc[(ccm_data['dcvt'].isna()) & (ccm_data['dcpstk'].notna()) & (ccm_data['pstk'].notna()) & (ccm_data['dcpstk'] > ccm_data['pstk']), 'dc'] = ccm_data['dcpstk'] - ccm_data['pstk']
    ccm_data.loc[(ccm_data['dcvt'].isna()) & (ccm_data['dcpstk'].notna()) & (ccm_data['pstk'].isna()), 'dc'] = ccm_data['dcpstk']
    ccm_data.loc[(ccm_data['dc'].isna()), 'dc'] = ccm_data['dcvt']

    ccm_data['xint'] = ccm_data['xint'].fillna(0)
    ccm_data['xsga'] = ccm_data['xsga'].fillna(0)

    ccm_data = ccm_data.sort_values(by=['permno', 'date']).drop_duplicates()

    return ccm_data


def build_ccm_jun(ccm_data):
    ccm_jun = ccm_data.copy()
    ccm_jun['mve6b'] = ccm_jun['dec_me']
    ccm_jun['ep'] = ccm_jun.ib/ccm_jun.mve_f
    ccm_jun['cashpr'] = (ccm_jun.mve_f+ccm_jun.dltt-ccm_jun['at'])/ccm_jun.che
    ccm_jun['dy'] = ccm_jun.dvt/ccm_jun.mve_f
    ccm_jun['lev'] = ccm_jun['lt']/ccm_jun.mve_f
    ccm_jun['sp'] = ccm_jun.sale/ccm_jun.mve_f
    ccm_jun['roic'] = (ccm_jun.ebit-ccm_jun.nopi)/(ccm_jun.ceq+ccm_jun['lt']-ccm_jun.che)
    ccm_jun['rd_sale'] = ccm_jun.xrd/ccm_jun.sale
    ccm_jun['rd_mve'] = ccm_jun['xrd']/ccm_jun['mve_f']
    ccm_jun['sp'] = ccm_jun.sale/ccm_jun.mve_f #duplicate?

    # treatment for lagged terms
    ccm_jun['lagat']=ccm_jun.groupby(['permno'])['at'].shift(1)
    ccm_jun['lat'] = ccm_jun['lagat']
    ccm_jun['lagcsho']=ccm_jun.groupby(['permno'])['csho'].shift(1)
    ccm_jun['laglt']=ccm_jun.groupby(['permno'])['lt'].shift(1)
    ccm_jun['lagact']=ccm_jun.groupby(['permno'])['act'].shift(1)
    ccm_jun['lagche']=ccm_jun.groupby(['permno'])['che'].shift(1)
    ccm_jun['lagdlc']=ccm_jun.groupby(['permno'])['dlc'].shift(1)
    ccm_jun['lagtxp']=ccm_jun.groupby(['permno'])['txp'].shift(1)
    ccm_jun['laglct']=ccm_jun.groupby(['permno'])['lct'].shift(1)
    ccm_jun['laginvt']=ccm_jun.groupby(['permno'])['invt'].shift(1)
    ccm_jun['lagemp']=ccm_jun.groupby(['permno'])['emp'].shift(1)
    ccm_jun['lagsale']=ccm_jun.groupby(['permno'])['sale'].shift(1)
    ccm_jun['lagib']=ccm_jun.groupby(['permno'])['ib'].shift(1)
    ccm_jun['lag2at']=ccm_jun.groupby(['permno'])['at'].shift(2)
    ccm_jun['lagrect']=ccm_jun.groupby(['permno'])['rect'].shift(1)
    ccm_jun['lagcogs']=ccm_jun.groupby(['permno'])['cogs'].shift(1)
    ccm_jun['lagxsga']=ccm_jun.groupby(['permno'])['xsga'].shift(1)
    ccm_jun['lagppent']=ccm_jun.groupby(['permno'])['ppent'].shift(1)
    ccm_jun['lagdp']=ccm_jun.groupby(['permno'])['dp'].shift(1)
    ccm_jun['lagxad']=ccm_jun.groupby(['permno'])['xad'].shift(1)
    ccm_jun['lagppegt']=ccm_jun.groupby(['permno'])['ppegt'].shift(1)
    ccm_jun['lagceq']=ccm_jun.groupby(['permno'])['ceq'].shift(1)
    ccm_jun['lagcapx']=ccm_jun.groupby(['permno'])['capx'].shift(1)
    ccm_jun['lag2capx']=ccm_jun.groupby(['permno'])['capx'].shift(2)
    ccm_jun['laggdwl']=ccm_jun.groupby(['permno'])['gdwl'].shift(1)
    ccm_jun['lagdvt']=ccm_jun.groupby(['permno'])['dvt'].shift(1)
    ccm_jun['lagob']=ccm_jun.groupby(['permno'])['ob'].shift(1)
    ccm_jun['lagaco']=ccm_jun.groupby(['permno'])['aco'].shift(1)
    ccm_jun['lagintan']=ccm_jun.groupby(['permno'])['intan'].shift(1)
    ccm_jun['lagao']=ccm_jun.groupby(['permno'])['ao'].shift(1)
    ccm_jun['lagap']=ccm_jun.groupby(['permno'])['ap'].shift(1)
    ccm_jun['laglco']=ccm_jun.groupby(['permno'])['lco'].shift(1)
    ccm_jun['laglo']=ccm_jun.groupby(['permno'])['lo'].shift(1)
    ccm_jun['lagdr']=ccm_jun.groupby(['permno'])['dr'].shift(1)
    ccm_jun['lagxrd']=ccm_jun.groupby(['permno'])['xrd'].shift(1)
    ccm_jun['lagni']=ccm_jun.groupby(['permno'])['ni'].shift(1)
    ccm_jun['lagdltt']=ccm_jun.groupby(['permno'])['dltt'].shift(1)

    ccm_jun['agr']=np.where(ccm_jun['at'].isna() | ccm_jun.lagat.isna(), np.NaN, (ccm_jun.lagat-ccm_jun['at'])/ccm_jun.lagat)
    ccm_jun['gma']=(ccm_jun['revt']-ccm_jun['cogs'])/ccm_jun['lagat']
    ccm_jun['chcsho']=ccm_jun.csho/ccm_jun.lagcsho -1
    ccm_jun['lgr']=ccm_jun['lt']/ccm_jun.laglt -1
    ccm_jun['acc']=(ccm_jun.ib-ccm_jun.oancf)/(ccm_jun['at']+ccm_jun.lagat) * 2
    ccm_jun.loc[ccm_jun['oancf'].isna(), 'acc'] = (((ccm_jun['act'] - lag(ccm_jun, 'act')) - (ccm_jun['che'] - lag(ccm_jun, 'che'))) \
        - (((ccm_jun['lct'] - lag(ccm_jun,'lct')) - (ccm_jun['dlc'] - lag(ccm_jun,'dlc')) - (ccm_jun['txp'] - lag(ccm_jun, 'txp'))) - ccm_jun['dp'])) \
        / ((ccm_jun['at']+lag(ccm_jun,'at'))/2)
    ccm_jun.loc[ccm_jun['ib'] != 0, 'pctacc'] = (ccm_jun['ib'] - ccm_jun['oancf'])/np.abs(ccm_jun['ib'])
    ccm_jun.loc[ccm_jun['ib'] == 0, 'pctacc'] = (ccm_jun['ib'] - ccm_jun['oancf'])/(0.01)
    ccm_jun.loc[(ccm_jun['oancf'].isna()) & (ccm_jun['ib'] != 0), 'pctacc'] = (((ccm_jun['act'] - lag(ccm_jun,'act')) - (ccm_jun['che'] - lag(ccm_jun,'che'))) \
        - ((ccm_jun['lct'] - lag(ccm_jun,'lct'))-(ccm_jun['dlc']-lag(ccm_jun,'dlc')) - (ccm_jun['txp'] - lag(ccm_jun,'txp')) - ccm_jun['dp'] ))/np.abs(ccm_jun['ib'])
    ccm_jun.loc[(ccm_jun['oancf'].isna()) & (ccm_jun['ib'] == 0), 'pctacc'] = (((ccm_jun['act'] - lag(ccm_jun,'act')) - (ccm_jun['che'] - lag(ccm_jun,'che'))) \
        - ((ccm_jun['lct'] - lag(ccm_jun,'lct'))-(ccm_jun['dlc']-lag(ccm_jun,'dlc')) - (ccm_jun['txp'] - lag(ccm_jun,'txp')) - ccm_jun['dp'] ))/0.01
    ccm_jun['cfp']= (ccm_jun['ib']-(ccm_jun['act']-ccm_jun['lagact']-(ccm_jun['che']-ccm_jun['lagche'])))\
                    -(ccm_jun['lct']-ccm_jun['laglct']-(ccm_jun['dlc']-ccm_jun['lagdlc'])\
                      -(ccm_jun['txp']-ccm_jun['lagtxp'])-ccm_jun['dp'])/ccm_jun['mve_f']
    ccm_jun['cfp']=np.where(ccm_jun['oancf'].notna(),ccm_jun['oancf']/ccm_jun['mve_f'], ccm_jun['cfp'])
    ccm_jun['absacc']=ccm_jun['acc'].abs()
    ccm_jun['chinv']=2*(ccm_jun['invt']-ccm_jun['laginvt'])/(ccm_jun['at']+ccm_jun['lagat'])
    ccm_jun['spii']=np.where((ccm_jun['spi']!=0)&ccm_jun['spi'].notna(), 1, 0)
    ccm_jun['spi']=2*ccm_jun['spi']/(ccm_jun['at']+ccm_jun['lagat'])
    ccm_jun['cf']=2*ccm_jun['oancf']/(ccm_jun['at']+ccm_jun['lagat'])
    ccm_jun['cf']=np.where(ccm_jun['oancf'].isna(), (ccm_jun['ib']-(ccm_jun['act']-ccm_jun['lagact']-(ccm_jun['che']-ccm_jun['lagche'])))\
                    -(ccm_jun['lct']-ccm_jun['laglct']-(ccm_jun['dlc']-ccm_jun['lagdlc'])\
                      -(ccm_jun['txp']-ccm_jun['lagtxp'])-ccm_jun['dp'])/((ccm_jun['at']+ccm_jun['lagat'])/2),ccm_jun['cf'])
    ccm_jun['hire']=(ccm_jun['emp']-ccm_jun['lagemp'])/ccm_jun['lagemp']
    ccm_jun['hire']=np.where(ccm_jun['emp'].isna() | ccm_jun['lagemp'].isna(), 0, ccm_jun['hire'])
    ccm_jun['sgr']=ccm_jun['sale']/ccm_jun['lagsale'] -1
    ccm_jun['chpm']=ccm_jun['ib']/ccm_jun['sale']-ccm_jun['lagib']/ccm_jun['lagsale']
    ccm_jun['chato']=(ccm_jun['sale']/((ccm_jun['at']+ccm_jun['lagat'])/2)) - (ccm_jun['lagsale']/((ccm_jun['lagat']+ccm_jun['lag2at'])/2))
    ccm_jun['pchsale_pchinvt']=((ccm_jun['sale']-(ccm_jun['lagsale']))/(ccm_jun['lagsale']))-((ccm_jun['invt']-(ccm_jun['laginvt']))/(ccm_jun['laginvt']))
    ccm_jun['pchsale_pchrect']=((ccm_jun['sale']-(ccm_jun['lagsale']))/(ccm_jun['lagsale']))-((ccm_jun['rect']-(ccm_jun['lagrect']))/(ccm_jun['lagrect']))
    ccm_jun['pchgm_pchsale']=(((ccm_jun['sale']-ccm_jun['cogs'])-((ccm_jun['lagsale'])-(ccm_jun['lagcogs'])))/((ccm_jun['lagsale'])-(ccm_jun['lagcogs'])))-((ccm_jun['sale']-(ccm_jun['lagsale']))/(ccm_jun['lagsale']))
    ccm_jun['pchsale_pchxsga']=((ccm_jun['sale']-(ccm_jun['lagsale']))/(ccm_jun['lagsale']) )-((ccm_jun['xsga']\
            -(ccm_jun['lagxsga'])) /(ccm_jun['lagxsga']) )
    ccm_jun['depr']=ccm_jun['dp']/ccm_jun['ppent']
    ccm_jun['pchdepr']=((ccm_jun['dp']/ccm_jun['ppent'])-((ccm_jun['lagdp'])/(ccm_jun['lagppent'])))/((ccm_jun['lagdp'])/(ccm_jun['lagppent']))
    ccm_jun['chadv']=np.log(1+ccm_jun['xad'])-np.log((1+(ccm_jun['lagxad'])))
    ccm_jun['invest']=((ccm_jun['ppegt']-(ccm_jun['lagppegt'])) +  (ccm_jun['invt']-(ccm_jun['laginvt'])) ) / (ccm_jun['lagat'])
    ccm_jun['invest']=np.where(ccm_jun['ppegt'].isna(), ((ccm_jun['ppent']-(ccm_jun['lagppent'])) +  (ccm_jun['invt']-(ccm_jun['laginvt'])) ) / (ccm_jun['lagat']), ccm_jun['invest'])
    ccm_jun['egr']=((ccm_jun['ceq']-(ccm_jun['lagceq']))/(ccm_jun['lagceq']))
    ccm_jun['capx']=np.where(ccm_jun['capx'].isna() & ccm_jun['count']>=1,ccm_jun['ppent']-(ccm_jun['lagppent']), ccm_jun['capx'])
    ccm_jun['pchcapx']=(ccm_jun['capx']-ccm_jun['lagcapx'])/ccm_jun['lagcapx']
    ccm_jun['grcapx']=(ccm_jun['capx']-ccm_jun['lag2capx'])/ccm_jun['lag2capx']
    ccm_jun['grGW'] = np.nan
    ccm_jun['grGW']=(ccm_jun['gdwl']-lag(ccm_jun, 'gdwl'))/lag(ccm_jun, 'gdwl')
    ccm_jun['grGW']=np.where((ccm_jun['gdwl'].isna()) | (ccm_jun['gdwl']==0), 0, ccm_jun['grGW'])
    ccm_jun['grGW']=np.where((ccm_jun['gdwl'].notna()) & (ccm_jun['gdwl']!=0) & (ccm_jun['grGW'].isna()), 1, ccm_jun['grGW'])
    ccm_jun['woGW']=np.where((ccm_jun['gdwlia'].notna()&ccm_jun['gdwlia']!=0)|(ccm_jun['gdwlip'].notna()&(ccm_jun['gdwlip']!=0))|\
                                                                              (ccm_jun['gwo'].notna()&ccm_jun['gwo']!=0) , 1, 0)
    ccm_jun['tang']=(ccm_jun['che']+ccm_jun['rect']*0.715+ccm_jun['invt']*0.547+ccm_jun['ppent']*0.535)/ccm_jun['at']
    ccm_jun['sic']=ccm_jun['sic'].astype(int)
    ccm_jun['sin']=np.where(ccm_jun['sic'].between(2100,2199) | ccm_jun['sic'].between(2080,2085) | (ccm_jun['naics'].isin(['7132', '71312', \
                                                                                 '713210', '71329', '713290', '72112', '721120'])), 1, 0)
    ccm_jun['act']=np.where(ccm_jun['act'].isna(), ccm_jun['che']+ccm_jun['rect']+ccm_jun['invt'],ccm_jun['act'])
    ccm_jun['lct']=np.where(ccm_jun['lct'].isna(), ccm_jun['ap'], ccm_jun['lct'])
    ccm_jun['currat']=ccm_jun['act']/ccm_jun['lct']
    ccm_jun['pchcurrat']= ((ccm_jun['act']/ccm_jun['lct']) - (lag(ccm_jun, 'act')/lag(ccm_jun, 'lct'))) / (lag(ccm_jun,'act')/lag(ccm_jun,'lct'))
    ccm_jun['quick']=(ccm_jun['act']-ccm_jun['invt'])/ccm_jun['lct']
    ccm_jun['pchquick']=((ccm_jun['act']-ccm_jun['invt'])/ccm_jun['lct'] - ((ccm_jun['lagact'])-(ccm_jun['laginvt']))/(ccm_jun['laglct']) )/ (((ccm_jun['lagact'])-(ccm_jun['laginvt']))/(ccm_jun['laglct']))
    ccm_jun['salecash']=ccm_jun['sale']/ccm_jun['che']
    ccm_jun['salerec']=ccm_jun['sale']/ccm_jun['rect']
    ccm_jun['saleinv']=ccm_jun['sale']/ccm_jun['invt']
    ccm_jun['pchsaleinv']=((ccm_jun['sale']/ccm_jun['invt'])-((ccm_jun['lagsale'])/(ccm_jun['laginvt'])) ) / ((ccm_jun['lagsale'])/(ccm_jun['laginvt']))
    ccm_jun['cashdebt']=(ccm_jun['ib']+ccm_jun['dp'])/((ccm_jun['lt']+(ccm_jun['laglt']))/2)
    ccm_jun['realestate']=(ccm_jun['fatb']+ccm_jun['fatl'])/ccm_jun['ppegt']
    ccm_jun['realestate']=np.where(ccm_jun['ppegt'].isna(), (ccm_jun['fatb']+ccm_jun['fatl'])/ccm_jun['ppent'], ccm_jun['realestate'])
    ccm_jun['divi']=np.where((ccm_jun['dvt'].notna() & ccm_jun['dvt']>0) & ((ccm_jun['lagdvt'])==0 | (ccm_jun['lagdvt'].isna())),1,0)
    ccm_jun['divo']=np.where((ccm_jun['dvt'].isna() | ccm_jun['dvt']==0) & ((ccm_jun['lagdvt'])>0 & (ccm_jun['lagdvt'].notna())),1,0)
    ccm_jun['obklg']=ccm_jun['ob']/((ccm_jun['at']+(ccm_jun['lagat']))/2)
    ccm_jun['chobklg']=(ccm_jun['ob']-(ccm_jun['lagob']))/((ccm_jun['at']+(ccm_jun['lagat']))/2)


    ccm_jun['securedind']=np.where(ccm_jun['dm'].notna() &ccm_jun['dm']!=0, 1, 0)
    ccm_jun['secured']=ccm_jun['dm']/ccm_jun['dltt']
    ccm_jun['convind']=np.where((ccm_jun['dc'].notna() & ccm_jun['dc']!=0) | (ccm_jun['cshrc'].notna() & ccm_jun['cshrc']!=0) , 1, 0)
    ccm_jun['dc']=ccm_jun['dc'].astype(float)
    ## There will be inf in the result
    ccm_jun['conv']=ccm_jun['dc']/ccm_jun['dltt']


    ccm_jun['grltnoa']=((ccm_jun['rect']+ccm_jun['invt']+ccm_jun['ppent']+ccm_jun['aco']+ccm_jun['intan']+ccm_jun['ao']-ccm_jun['ap']-ccm_jun['lco']-ccm_jun['lo'])\
                        -((ccm_jun['lagrect'])+(ccm_jun['laginvt'])+(ccm_jun['lagppent'])+(ccm_jun['lagaco'])+(ccm_jun['lagintan'])+(ccm_jun['lagao'])-(ccm_jun['lagap'])\
                          -(ccm_jun['laglco'])-(ccm_jun['laglo'])) -(ccm_jun['rect']-(ccm_jun['lagrect'])+ccm_jun['invt']-(ccm_jun['laginvt'])+ccm_jun['aco']-(ccm_jun['lagaco'])\
                            -(ccm_jun['ap']-(ccm_jun['lagap'])+ccm_jun['lco']-(ccm_jun['laglco'])) -ccm_jun['dp']))/((ccm_jun['at']+(ccm_jun['lagat']))/2)

    ccm_jun['chdrc']=np.divide(ccm_jun['dr']-ccm_jun['lagdr'], (ccm_jun['at']+ccm_jun['lagat'])/2,
                               out=np.zeros_like(ccm_jun['dr']-ccm_jun['lagdr']),
                               where=(ccm_jun['at']+ccm_jun['lagat'])/2 != 0)

    ccm_jun['xrd/lagat']=ccm_jun['xrd']/(ccm_jun['lagat'])
    ccm_jun['lag(xrd/lagat)']=ccm_jun.groupby(['permno'])['xrd/lagat'].shift(1)
    ccm_jun['rd']=np.where(((ccm_jun['xrd']/ccm_jun['at'])-ccm_jun['lag(xrd/lagat)'])/ccm_jun['lag(xrd/lagat)']>0.05, 1, 0)
    ccm_jun['rdbias']=(ccm_jun['xrd']/(ccm_jun['lagxrd']))-1-ccm_jun['ib']/(ccm_jun['lagceq'])
    ccm_jun['roe']=ccm_jun['ib']/(ccm_jun['lagceq'])


    ccm_jun['ps_beme']=np.where(ccm_jun['pstkrv'].isnull(), ccm_jun['pstkl'], ccm_jun['pstkrv'])
    ccm_jun['ps_beme']=np.where(ccm_jun['ps_beme'].isnull(),ccm_jun['pstk'], ccm_jun['ps_beme'])
    ccm_jun['ps_beme']=np.where(ccm_jun['ps_beme'].isnull(),0,ccm_jun['ps_beme'])
    ccm_jun['txditc']=ccm_jun['txditc'].fillna(0)
    ccm_jun['be']=ccm_jun['ceq']+ccm_jun['txditc']-ccm_jun['ps_beme']
    ccm_jun['be']=np.where(ccm_jun['be']>0,ccm_jun['be'],np.NaN)
    ccm_jun['operprof']=np.where(ccm_jun['be'].notna() & ccm_jun['revt'].notna() & (ccm_jun['cogs'].notna() | ccm_jun['xsga'].notna() | ccm_jun['xint'].notna()),\
                              (ccm_jun['revt']-ccm_jun['cogs']-ccm_jun['xsga']-ccm_jun['xint'])/ccm_jun['be'], np.nan)

    ccm_jun['ps']=(ccm_jun['ni']>0).astype(int) +(ccm_jun['oancf']>0)+(ccm_jun['ni']/ccm_jun['at'] > (ccm_jun['lagni'])/(ccm_jun['lagat']))+(ccm_jun['oancf']>ccm_jun['ni'])+(ccm_jun['dltt']/ccm_jun['at'] < (ccm_jun['lagdltt'])/(ccm_jun['lagat']))\
                    +(ccm_jun['act']/ccm_jun['lct'] > (ccm_jun['lagact'])/(ccm_jun['laglct'])) +((ccm_jun['sale']-ccm_jun['cogs'])/ccm_jun['sale'] > ((ccm_jun['lagsale'])-(ccm_jun['lagcogs']))/(ccm_jun['lagsale']))\
                    + (ccm_jun['sale']/ccm_jun['at'] > (ccm_jun['lagsale'])/(ccm_jun['lagat']))+ (ccm_jun['scstkc']==0)


    def tr_fyear(row):
        if row['fyear']<=1978:
            value = 0.48
        elif row['fyear']<=1986:
            value = 0.46
        elif row['fyear']==1987:
            value = 0.4
        elif row['fyear']>=1988 and row['fyear']<=1992:
            value = 0.34
        elif row['fyear']>=1993:
            value = 0.35
        else:
            value=''
        return value

    ccm_jun['tr']=ccm_jun.apply(tr_fyear, axis=1)
    ccm_jun['tb_1']=((ccm_jun['txfo']+ccm_jun['txfed'])/ccm_jun['tr'])/ccm_jun['ib']
    ccm_jun['tb_1']=np.where(ccm_jun['txfo'].isna() | ccm_jun['txfed'].isna(),((ccm_jun['txt']+ccm_jun['txdi'])/ccm_jun['tr'])/ccm_jun['ib'], ccm_jun['tb_1'])

    # 	if (txfo+txfed>0 or txt>txdi) and ib<=0 then
    # 		tb_1=1;
    #!!! Caution that for condition, when using | and &, one must apply parenthesis
    ccm_jun['tb_1']=np.where(((ccm_jun['txfo']+ccm_jun['txfed'])>0 | (ccm_jun['txt']>ccm_jun['txdi'])) & (ccm_jun['ib']<=0), 1, ccm_jun['tb_1'])

    # 	*variables that will be used in subsequent steps to get to final RPS;
    # 	*--prep for for Mohanram (2005) score;
    # 	roa=ni/((at+lag(at))/2);
    # 	cfroa=oancf/((at+lag(at))/2);
    ccm_jun['roa']=ccm_jun['ni']/((ccm_jun['at']+(ccm_jun['lagat']))/2)
    ccm_jun['cfroa']=ccm_jun['oancf']/((ccm_jun['at']+(ccm_jun['lagat']))/2)

    # 	if missing(oancf) then
    # 		cfroa=(ib+dp)/((at+lag(at))/2);
    # 	xrdint=xrd/((at+lag(at))/2);
    # 	capxint=capx/((at+lag(at))/2);
    # 	xadint=xad/((at+lag(at))/2);
    ccm_jun['cfroa']=np.where(ccm_jun['oancf'].isna(),ccm_jun['ib']+ccm_jun['dp'] /((ccm_jun['at']+(ccm_jun['lagat']))/2), ccm_jun['cfroa'])
    ccm_jun['xrdint']=ccm_jun['xrd']/((ccm_jun['at']+(ccm_jun['lagat']))/2)
    ccm_jun['capxint']=ccm_jun['capx']/((ccm_jun['at']+(ccm_jun['lagat']))/2)
    ccm_jun['xadint']=ccm_jun['xad']/((ccm_jun['at']+(ccm_jun['lagat']))/2)

    # 	/*HXZ*/
    # 	adm=xad/mve6b;
    # 	gad=(xad-lag(xad))/lag(xad);
    # 	rdm=xrd/mve6b;
    # 	rds=xrd/sale;
    # 	ol=(cogs+xsga)/at;
    # 	rc_1=xrd+0.8*lag(xrd)+0.6*lag2(xrd)+0.4*lag3(xrd)+0.2*lag4(xrd);

    #New lag terms for this section
    ccm_jun['lag2xrd']=ccm_jun.groupby(['permno'])['lagxrd'].shift(1)
    ccm_jun['lag3xrd']=ccm_jun.groupby(['permno'])['lag2xrd'].shift(1)
    ccm_jun['lag4xrd']=ccm_jun.groupby(['permno'])['lag3xrd'].shift(1)

    # Here I follow previous naming of mve6b as dec_me
    ccm_jun['adm']=ccm_jun['xad']/ccm_jun['dec_me']
    ccm_jun['gad']=(ccm_jun['xad']-(ccm_jun['lagxad']))/(ccm_jun['lagxad'])
    ccm_jun['rdm']=ccm_jun['xrd']/ccm_jun['dec_me']
    ccm_jun['rds']=ccm_jun['xrd']/ccm_jun['sale']
    ccm_jun['ol']=(ccm_jun['cogs']+ccm_jun['xsga'])/ccm_jun['at']
    ccm_jun['rc_1']=ccm_jun['xrd']+0.8*(ccm_jun['lagxrd'])+0.6*(ccm_jun['lag2xrd'])+0.4*(ccm_jun['lag3xrd'])+0.2*(ccm_jun['lag4xrd'])
    ccm_jun['rca'] = ccm_jun['rc_1']/ccm_jun['at']
    ccm_jun['eps_1'] = ccm_jun['ajex']/ccm_jun['prcc_f']
    ccm_jun['x_1']=ccm_jun['ppent']+ccm_jun['intan']+ccm_jun['ao']-ccm_jun['lo']+ccm_jun['dp']
    ccm_jun['etr'] = (ccm_jun['x_1'] - (lag(ccm_jun, 'x_1') + lag(ccm_jun,'x_1',2)+lag(ccm_jun,'x_1',3))/3) * (ccm_jun['eps_1'] - lag(ccm_jun, 'eps_1'))
    ccm_jun['x_2'] = ccm_jun['sale']/ccm_jun['emp']
    ccm_jun['lfe'] = (ccm_jun['x_2'] - lag(ccm_jun, 'x_2'))/lag(ccm_jun, 'x_2')
    ccm_jun['kz'] = -1.002*(ccm_jun['ib'] + ccm_jun['dp'])/lag(ccm_jun,'ppent') + 0.283*(ccm_jun['at']+ccm_jun['mve6b']-ccm_jun['ceq']-ccm_jun['txdb'])/ccm_jun['at'] + \
        3.139*(ccm_jun['dlc'] + ccm_jun['dltt'])/(ccm_jun['dlc'] + ccm_jun['dltt'] + ccm_jun['seq']) - 39.368*(ccm_jun['dvc']+ccm_jun['dvp'])/lag(ccm_jun,'ppent') -\
        1.315*(ccm_jun['che'])/lag(ccm_jun,'ppent')


    ccm_jun['cdd']=ccm_jun['dcvt']/(ccm_jun['dlc']+ccm_jun['dltt'])
    ccm_jun['roaq_a']=ccm_jun['ib']/(ccm_jun['lagat'])

    # must reindex in order to set back the value
    ccm_jun['roavol_1']=ccm_jun.groupby(['permno'])['roaq_a'].rolling(10).std(skipna=True).reset_index()['roaq_a']

    ccm_jun['cs_1']=(ccm_jun['ib']-(ccm_jun['act']-(ccm_jun['lagact'])-(ccm_jun['lct']-(ccm_jun['laglct']))-(ccm_jun['che']-(ccm_jun['lagche']))+ccm_jun['dlc']-(ccm_jun['lagdlc'])))/(ccm_jun['lagat'])
    ccm_jun['roavol_2']=ccm_jun.groupby(['permno'])['cs_1'].rolling(10).std().reset_index()['cs_1']
    ccm_jun['roavol_a']=ccm_jun['roavol_1']/ccm_jun['roavol_2']

    ccm_jun['gdwl']=ccm_jun['gdwl'].fillna(0)
    ccm_jun['intan']=ccm_jun['intan'].fillna(0)
    ccm_jun['ala']=ccm_jun['che']+0.75*(ccm_jun['act']-ccm_jun['che'])-0.5*(ccm_jun['at']-ccm_jun['act']-ccm_jun['gdwl']-ccm_jun['intan'])
    ccm_jun['alm']=ccm_jun['ala']/(ccm_jun['at']+ccm_jun['prcc_f']*ccm_jun['csho']-ccm_jun['ceq'])
    ccm_jun['ob_a']=ccm_jun['ob']/(0.5*ccm_jun['at']+0.5*(ccm_jun['lagat']))
    ccm_jun['x_3']=ccm_jun['capx']/ccm_jun['sale']
    ccm_jun['lagx_3']=ccm_jun.groupby(['permno'])['x_3'].shift(1)
    ccm_jun['lag2x_3']=ccm_jun.groupby(['permno'])['lagx_3'].shift(1)
    ccm_jun['lag3x_3']=ccm_jun.groupby(['permno'])['lag2x_3'].shift(1)
    ccm_jun['cinvest_a']=ccm_jun['x_3']/(((ccm_jun['lagx_3'])+(ccm_jun['lag2x_3'])+(ccm_jun['lag3x_3']))/3)-1
    ccm_jun['dpia'] = (ccm_jun['ppegt']-lag(ccm_jun,'ppegt') + ccm_jun['invt'] - lag(ccm_jun, 'invt'))/lag(ccm_jun, 'at')

    ccm_jun['dlc']=ccm_jun['dlc'].fillna(0)
    ccm_jun['dltt']=ccm_jun['dltt'].fillna(0)
    ccm_jun['mib']=ccm_jun['mib'].fillna(0)
    ccm_jun['pstk']=ccm_jun['pstk'].fillna(0)
    ccm_jun['ceq']=ccm_jun['ceq'].fillna(0)


    ccm_jun['noa']=((ccm_jun['at']-ccm_jun['che'])-(ccm_jun['at']-ccm_jun['dlc']-ccm_jun['dltt']-ccm_jun['mib']-ccm_jun['pstk']-ccm_jun['ceq']))/(ccm_jun['lagat'])
    ccm_jun['lagnoa']=ccm_jun.groupby(['permno'])['noa'].shift(1)
    ccm_jun['dnoa']=ccm_jun['noa']-(ccm_jun['lagnoa'])
    ccm_jun['lag3capx']=ccm_jun.groupby(['permno'])['capx'].shift(3)
    ccm_jun['pchcapx3']=ccm_jun['capx']/(ccm_jun['lag3capx'])-1
    ccm_jun['x_4']=ccm_jun['dlc']+ccm_jun['dltt']
    ccm_jun['lag5x_4']=ccm_jun.groupby(['permno'])['capx'].shift(5)
    ccm_jun['cdi']=np.log(ccm_jun['x_4']/(ccm_jun['lag5x_4']))

    ccm_jun['ivg']=ccm_jun['invt']/(ccm_jun['laginvt'])-1
    ccm_jun['dcoa']=(ccm_jun['act']-(ccm_jun['lagact'])-(ccm_jun['che']-(ccm_jun['lagche'])))/(ccm_jun['lagat'])
    ccm_jun['dcol']=(ccm_jun['lct']-(ccm_jun['laglct'])-(ccm_jun['dlc']-(ccm_jun['lagdlc'])))/(ccm_jun['lagat'])
    ccm_jun['dwc']=(ccm_jun['dcoa']-ccm_jun['dcol'])/(ccm_jun['lagat'])
    ccm_jun['lagivao']=ccm_jun.groupby(['permno'])['ivao'].shift(1)
    ccm_jun['dnca']=(ccm_jun['at']-ccm_jun['act']-ccm_jun['ivao']-((ccm_jun['lagat'])-(ccm_jun['lagact'])-(ccm_jun['lagivao'])))/(ccm_jun['lagat'])
    ccm_jun['dncl']=(ccm_jun['lt']-ccm_jun['lct']-ccm_jun['dltt']-((ccm_jun['laglt'])-(ccm_jun['laglct'])-(ccm_jun['lagdltt'])))/(ccm_jun['lagat'])
    ccm_jun['dnco']=(ccm_jun['dnca']-ccm_jun['dncl'])/(ccm_jun['lagat'])
    ccm_jun['lagivst']=ccm_jun.groupby(['permno'])['ivst'].shift(1)
    ccm_jun['lagpstk']=ccm_jun.groupby(['permno'])['pstk'].shift(1)
    ccm_jun['dfin']=(ccm_jun['ivst']+ccm_jun['ivao']-ccm_jun['dltt']-ccm_jun['dlc']-ccm_jun['pstk']-((ccm_jun['lagivst'])+(ccm_jun['lagivao'])-(ccm_jun['lagdltt'])-(ccm_jun['lagdlc'])-(ccm_jun['lagpstk'])))/(ccm_jun['lagat'])
    ccm_jun['ta']=(ccm_jun['dwc']+ccm_jun['dnco']+ccm_jun['dfin'])/(ccm_jun['lagat'])
    ccm_jun['dsti']=(ccm_jun['ivst']-(ccm_jun['lagivst']))/(ccm_jun['lagat'])
    ccm_jun['dfnl']=(ccm_jun['dltt']+ccm_jun['dlc']+ccm_jun['pstk']-((ccm_jun['lagdltt'])+(ccm_jun['lagdlc'])+(ccm_jun['lagpstk'])))/(ccm_jun['lagat'])
    ccm_jun['egr_hxz']=(ccm_jun['ceq']-(ccm_jun['lagceq']))/(ccm_jun['lagat'])

    ccm_jun['lagpstkrv']=ccm_jun.groupby(['permno'])['pstkrv'].shift(1)

    ccm_jun['txp']=ccm_jun['txp'].fillna(0)
    ccm_jun['poa']=(ccm_jun['act']-(ccm_jun['lagact'])-(ccm_jun['che']-(ccm_jun['lagche']))-(ccm_jun['lct']-(ccm_jun['laglct'])-(ccm_jun['dlc']-(ccm_jun['lagdlc']))-(ccm_jun['txp']-(ccm_jun['lagtxp'])))-ccm_jun['dp'])/(ccm_jun['ni'].abs())
    ccm_jun['nef']=(ccm_jun['sstk']-ccm_jun['prstkc']-ccm_jun['dv'])/((ccm_jun['at']+(ccm_jun['lagat']))/2)
    ccm_jun['dlcch']=ccm_jun['dlcch'].fillna(0)
    ccm_jun['ndf']=(ccm_jun['dltis']-ccm_jun['dltr']+ccm_jun['dlcch'])/((ccm_jun['at']+(ccm_jun['lagat']))/2)
    ccm_jun['nxf'] = ccm_jun['ndf'] + ccm_jun['nef']
    ccm_jun['atm']=ccm_jun['at']/ccm_jun['dec_me']
    ccm_jun['cp']=(ccm_jun['ib']+ccm_jun['dp'])/ccm_jun['dec_me']
    ccm_jun['op']=(ccm_jun['dvc']+ccm_jun['prstkc']-(ccm_jun['pstkrv']-(ccm_jun['lagpstkrv'])))/ccm_jun['dec_me']
    ccm_jun['nop']=(ccm_jun['dvc']+ccm_jun['prstkc']-(ccm_jun['pstkrv']-(ccm_jun['lagpstkrv']))-ccm_jun['sstk']+ccm_jun['pstkrv']-(ccm_jun['lagpstkrv']))/ccm_jun['dec_me']
    ccm_jun['em'] = (ccm_jun['mve6b'] + ccm_jun['dlc'] + ccm_jun['dltt'] + ccm_jun['pstkrv'] - ccm_jun['che'])/ccm_jun['oibdp']


    ccm_jun['dvpa']=ccm_jun['dvpa'].fillna(0)
    ccm_jun['tstkp']=ccm_jun['tstkp'].fillna(0)
    ccm_jun['ndp']=ccm_jun['dltt']+ccm_jun['dlc']+ccm_jun['pstk']+ccm_jun['dvpa']-ccm_jun['tstkp']-ccm_jun['che']
    ccm_jun['ebp']=(ccm_jun['ndp']+ccm_jun['ceq']+ccm_jun['tstkp']-ccm_jun['dvpa'])/(ccm_jun['ndp']+ccm_jun['dec_me'])
    ccm_jun['rna']=ccm_jun['oiadp']/(ccm_jun['lagnoa'])
    ccm_jun['pm']=ccm_jun['rna']/ccm_jun['sale']
    ccm_jun['ato']=ccm_jun['sale']/(ccm_jun['lagnoa'])
    ccm_jun['cto']=ccm_jun['sale']/(ccm_jun['lagat'])
    ccm_jun['gpa']=(ccm_jun['revt']-ccm_jun['cogs'])/ccm_jun['at']
    ccm_jun['rmw']=(ccm_jun['revt']-ccm_jun['cogs']-ccm_jun['xsga']-ccm_jun['xint'])/(ccm_jun['ceq']+ccm_jun['pstk'])
    ccm_jun['ole']=(ccm_jun['revt']-ccm_jun['cogs']-ccm_jun['xsga']-ccm_jun['xint'])/((ccm_jun['lagceq'])+(ccm_jun['lagpstk']))
    ccm_jun['opa']=(ccm_jun['revt']-ccm_jun['cogs']-ccm_jun['xsga']+ccm_jun['xrd'])/ccm_jun['at']
    ccm_jun['ola']=(ccm_jun['revt']-ccm_jun['cogs']-ccm_jun['xsga']+ccm_jun['xrd'])/(ccm_jun['lagat'])

    ccm_jun['lagxpp']=ccm_jun.groupby(['permno'])['xpp'].shift(1)
    ccm_jun['lagdrc']=ccm_jun.groupby(['permno'])['drc'].shift(1)
    ccm_jun['lagdrlt']=ccm_jun.groupby(['permno'])['drlt'].shift(1)
    ccm_jun['lagxacc']=ccm_jun.groupby(['permno'])['xacc'].shift(1)

    ccm_jun['cop']=(ccm_jun['revt']-ccm_jun['cogs']-ccm_jun['xsga']+ccm_jun['xrd']-(ccm_jun['rect']-(ccm_jun['lagrect']))-(ccm_jun['invt']-(ccm_jun['laginvt']))-\
             (ccm_jun['xpp']-(ccm_jun['lagxpp']))+ccm_jun['drc']-(ccm_jun['lagdrc'])+ccm_jun['drlt']-(ccm_jun['lagdrlt'])+ccm_jun['ap']-(ccm_jun['lagap'])+ccm_jun['xacc']-(ccm_jun['lagxacc']))/ccm_jun['at']
    ccm_jun['cla']=(ccm_jun['revt']-ccm_jun['cogs']-ccm_jun['xsga']+ccm_jun['xrd']-(ccm_jun['rect']-(ccm_jun['lagrect']))-(ccm_jun['invt']-(ccm_jun['laginvt']))-\
             (ccm_jun['xpp']-(ccm_jun['lagxpp']))+ccm_jun['drc']-(ccm_jun['lagdrc'])+ccm_jun['drlt']-(ccm_jun['lagdrlt'])+ccm_jun['ap']-(ccm_jun['lagap'])+ccm_jun['xacc']-(ccm_jun['lagxacc']))/ccm_jun['lagat']

    print('Check 1')
    ccm_jun['i_1']=np.where(ccm_jun['lt']>ccm_jun['at'],1,0)
    ccm_jun['i_2']=np.where((ccm_jun['ni']<0) & (ccm_jun['lagni']<0),1,0)
    ccm_jun['os']=-1.32-0.407*np.log(ccm_jun['at'])+6.03*(ccm_jun['dlc']+ccm_jun['dltt'])/ccm_jun['at']-1.43*(ccm_jun['act']-ccm_jun['lct'])/ccm_jun['at']+0.076*(ccm_jun['lct']/ccm_jun['act'])-1.72*ccm_jun['i_1']-2.37*ccm_jun['ni']/ccm_jun['at']-1.83*(ccm_jun['pi']+ccm_jun['dp'])/ccm_jun['lt']+0.285*ccm_jun['i_2']-0.521*(ccm_jun['ni']+(ccm_jun['lagni']))/((ccm_jun['ni'].abs())+ccm_jun['lagni'].abs())
    ccm_jun['zs']=1.2*(ccm_jun['act']-ccm_jun['lct'])/ccm_jun['at']+1.4*ccm_jun['re']/ccm_jun['at']+3.3*ccm_jun['oiadp']/ccm_jun['at']+0.6*ccm_jun['dec_me']/ccm_jun['lt']+ccm_jun['sale']/ccm_jun['at']
    ccm_jun['bi']=np.where(ccm_jun['be']>0,ccm_jun['at']/ccm_jun['be'],np.NaN)


    ccm_jun['pchsale_pchinvt_hxz']=(ccm_jun['sale']-(ccm_jun['lagsale']))/(0.5*ccm_jun['sale']+0.5*(ccm_jun['lagsale']))-(ccm_jun['invt']-(ccm_jun['laginvt']))/(0.5*ccm_jun['invt']+0.5*(ccm_jun['laginvt']))
    ccm_jun['pchsale_pchrect_hxz']=(ccm_jun['sale']-(ccm_jun['lagsale']))/(0.5*ccm_jun['sale']+0.5*(ccm_jun['lagsale']))-(ccm_jun['rect']-(ccm_jun['lagrect']))/(0.5*ccm_jun['rect']+0.5*(ccm_jun['lagrect']))
    ccm_jun['gm_1']=ccm_jun['sale']-ccm_jun['cogs']
    ccm_jun['laggm_1']=ccm_jun.groupby(['permno'])['gm_1'].shift(1)
    ccm_jun['pchgm_pchsale_hxz']=(ccm_jun['gm_1']-(ccm_jun['laggm_1']))/(0.5*(ccm_jun['gm_1']+(ccm_jun['laggm_1'])))-(ccm_jun['sale']-(ccm_jun['lagsale']))/(0.5*ccm_jun['sale']+0.5*(ccm_jun['lagsale']))
    ccm_jun['pchsale_pchxsga_hxz']=(ccm_jun['sale']-(ccm_jun['lagsale']))/(0.5*ccm_jun['sale']+0.5*(ccm_jun['lagsale']))-(ccm_jun['xsga']-(ccm_jun['lagxsga']))/(0.5*ccm_jun['xsga']+0.5*(ccm_jun['lagxsga']))
    ccm_jun['realestate_hxz']=(ccm_jun['fatb']+ccm_jun['fatl'])/ccm_jun['ppegt']


    ccm_jun['secured_hxz']=ccm_jun['dm']/(ccm_jun['dltt']+ccm_jun['dlc'])
    ccm_jun['agr_hxz']=ccm_jun['at']/(ccm_jun['lagat'])-1
    ccm_jun['lagx_1']=ccm_jun.groupby(['permno'])['x_1'].shift(1)
    ccm_jun['grltnoa_hxz']=(ccm_jun['x_1']-(ccm_jun['lagx_1']))/((ccm_jun['at']+(ccm_jun['lagat']))/2)
    ccm_jun['lagajex']=ccm_jun.groupby(['permno'])['ajex'].shift(1)
    ccm_jun['chcsho_hxz']=np.log(ccm_jun['csho']*ccm_jun['ajex'])-np.log((ccm_jun['lagcsho'])*(ccm_jun['lagajex']))
    ccm_jun['lagcapxv']=ccm_jun.groupby(['permno'])['capxv'].shift(1)
    ccm_jun['lag2capxv']=ccm_jun.groupby(['permno'])['capxv'].shift(2)
    ccm_jun['pchcapx_hxz']=(ccm_jun['capxv']-0.5*(ccm_jun['lagcapxv'])-0.5*(ccm_jun['lag2capxv']))/(0.5*(ccm_jun['lagcapxv'])+0.5*(ccm_jun['lag2capxv']))


    ccm_jun['txp']=ccm_jun['txp'].fillna(0)
    ccm_jun['acc_hxz']=(ccm_jun['act']-(ccm_jun['lagact'])-(ccm_jun['che']-(ccm_jun['lagche']))-(ccm_jun['lct']-(ccm_jun['laglct'])-(ccm_jun['dlc']-(ccm_jun['lagdlc']))-(ccm_jun['txp']-(ccm_jun['lagtxp'])))-ccm_jun['dp'])/(ccm_jun['lagat'])
    ccm_jun['pctacc_hxz']=(ccm_jun['dwc']+ccm_jun['dnco']+ccm_jun['dfin'])/(ccm_jun['ni'].abs())
    ccm_jun['lev_hxz']=(ccm_jun['dlc']+ccm_jun['dltt'])/ccm_jun['dec_me']
    ccm_jun['ep_hxz']=ccm_jun['ib']/ccm_jun['dec_me']
    ccm_jun['lagwcap']=ccm_jun.groupby(['permno'])['wcap'].shift(1)
    ccm_jun['cfp_hxz']=(ccm_jun['fopt']-(ccm_jun['wcap']-(ccm_jun['lagwcap'])))/ccm_jun['dec_me']

    ccm_jun['cfp_hxz']=np.where(ccm_jun['oancf'].notna(),(ccm_jun['fopt']-ccm_jun['oancf'])/ccm_jun['dec_me'],ccm_jun['cfp_hxz'])
    ccm_jun['tb_hxz']=ccm_jun['pi']/ccm_jun['ni']

    print('Check 2')
    ccm_jun = ccm_jun.sort_values(['sic2', 'fyear'])
    a=ccm_jun.groupby(['sic2','fyear'])['chpm'].mean()
    a=a.rename('meanchpm')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])
    ccm_jun['chpmia']=ccm_jun['chpm']-ccm_jun['meanchpm']

    a=ccm_jun.groupby(['sic2','fyear'])['chato'].mean()
    a=a.rename('meanchato')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])
    ccm_jun['chatoia']=ccm_jun['chato']-ccm_jun['meanchato']

    a=ccm_jun.groupby(['sic2','fyear'])['sale'].sum()
    a=a.rename('indsale')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])

    a=ccm_jun.groupby(['sic2','fyear'])['hire'].mean()
    a=a.rename('meanhire')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])
    ccm_jun['chempia']=ccm_jun['hire']-ccm_jun['meanhire']

    ccm_jun['beme']=ccm_jun['beme'].astype(float)
    a=ccm_jun.groupby(['sic2','fyear'])['beme'].mean()
    a=a.rename('meanbeme')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])
    ccm_jun['beme_ia']=ccm_jun['beme']-ccm_jun['meanbeme']
    ccm_jun['bm_ia'] = ccm_jun['beme_ia']

    a=ccm_jun.groupby(['sic2','fyear'])['pchcapx'].mean()
    a=a.rename('meanpchcapx')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])
    ccm_jun['pchcapx_ia']=ccm_jun['pchcapx']-ccm_jun['meanpchcapx']

    a=ccm_jun.groupby(['sic2','fyear'])['tb_1'].mean()
    a=a.rename('meantb_1')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])
    ccm_jun['tb']=ccm_jun['tb_1']-ccm_jun['meantb_1']

    a=ccm_jun.groupby(['sic2','fyear'])['cfp'].mean()
    a=a.rename('meancfp')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])
    ccm_jun['cfp_ia']=ccm_jun['cfp']-ccm_jun['meancfp']

    a=ccm_jun.groupby(['sic2','fyear'])['mve_f'].mean()
    a=a.rename('meanmve_f')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])
    ccm_jun['mve_ia']=ccm_jun['mve_f']-ccm_jun['meanmve_f']

    a=ccm_jun.groupby(['sic2','fyear'])['at'].sum()
    a=a.rename('indat')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])

    a=ccm_jun.groupby(['sic2','fyear'])['be'].sum()
    a=a.rename('indbe')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])

    a=ccm_jun.groupby(['sic2','fyear'])['pchcapx_hxz'].mean()
    a=a.rename('meanpchcapx_hxz')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])
    ccm_jun['pchcapx_ia_hxz']=ccm_jun['pchcapx_hxz']-ccm_jun['meanpchcapx_hxz']
    ccm_jun['herfraw']=(ccm_jun['sale']/ccm_jun['indsale'])*(ccm_jun['sale']/ccm_jun['indsale'])
    a=ccm_jun.groupby(['sic2','fyear'])['herfraw'].sum()
    a=a.rename('herf')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])

    print('Check 3')
    ccm_jun['haraw']=(ccm_jun['at']/ccm_jun['indat'])*(ccm_jun['at']/ccm_jun['indat'])
    a=ccm_jun.groupby(['sic2','fyear'])['haraw'].sum()
    a=a.rename('ha')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])

    ccm_jun['heraw']=(ccm_jun['be']/ccm_jun['indbe'])*(ccm_jun['be']/ccm_jun['indbe'])
    a=ccm_jun.groupby(['sic2','fyear'])['heraw'].sum()
    a=a.rename('he')
    ccm_jun=pd.merge(ccm_jun, a, how='left', on=['sic2','fyear'])

    ccm_jun = ccm_jun.sort_values(['sic2', 'fyear'])

    indmd=pd.DataFrame()
    indmd = ccm_jun.groupby(['sic2', 'fyear'], as_index=False)['roa'].median()
    indmd = indmd.rename(columns={'roa': 'md_roa'})
    indmd['md_cfroa'] = ccm_jun.groupby(['sic2', 'fyear'], as_index=False)['cfroa'].median()['cfroa']
    indmd['md_xrdint'] = ccm_jun.groupby(['sic2', 'fyear'], as_index=False)['xrdint'].median()['xrdint']
    indmd['md_capxint'] = ccm_jun.groupby(['sic2', 'fyear'], as_index=False)['capxint'].median()['capxint']
    indmd['md_xadint'] = ccm_jun.groupby(['sic2', 'fyear'], as_index=False)['xadint'].median()['xadint']
    ccm_jun=pd.merge(ccm_jun, indmd, how='left', on=['sic2','fyear'])

    ccm_jun = ccm_jun.sort_values(['gvkey', 'datadate'])
    ccm_jun['m1']=np.where(ccm_jun['roa']>ccm_jun['md_roa'], 1, 0)
    ccm_jun['m2']=np.where(ccm_jun['cfroa']>ccm_jun['md_cfroa'], 1, 0)
    ccm_jun['m3']=np.where(ccm_jun['oancf']>ccm_jun['ni'], 1, 0)
    ccm_jun['m4']=np.where(ccm_jun['xrdint']>ccm_jun['md_xrdint'], 1, 0)
    ccm_jun['m5']=np.where(ccm_jun['capxint']>ccm_jun['md_capxint'], 1, 0)
    ccm_jun['m6']=np.where(ccm_jun['xadint']>ccm_jun['md_xadint'], 1, 0)

    compr=conn.raw_sql("""
                      select splticrm, gvkey, datadate
                      from comp.adsprate
                      """)
    compr.datadate=pd.to_datetime(compr.datadate)

    ccm_jun['year']=ccm_jun['datadate'].dt.year
    compr['year']=compr['datadate'].dt.year
    ccm_jun=pd.merge(ccm_jun, compr[['splticrm','gvkey','year']], how='left', on=['gvkey','year'])

    ccm_jun = ccm_jun.sort_values(['gvkey', 'datadate'])

    print('Check 4')
    cpi=pd.DataFrame()
    cpi['fyear'] = list(reversed(range(1924, 2017+1)))
    cpi['cpi'] = [246.19,242.23,236.53,229.91,229.17,229.594,
                  224.939,218.056,214.537,215.303,207.342,201.6,195.3,188.9,
                  183.96,179.88,177.1,172.2,166.6,163,160.5,156.9,
                  152.4,148.2,144.5,140.3,136.2,130.7,124,118.3,
                  113.6,109.6,107.6,103.9,99.6,96.5,90.9,82.4,
                  72.6,65.2,60.6,56.9,53.8,49.3,44.2,41.8,
                  40.6,38.9,36.7,34.8,33.4,32.5,31.6,31,
                  30.7,30.2,29.9,29.6,29.2,28.9,28.2,27.3,
                  26.8,26.9,26.8,26.7,25.9,24,23.7,24.4,
                  22.2,19.7,18.1,17.6,17.3,16.3,14.7,14,13.8,14.1,
                  14.4,13.9,13.6,13.3,13.1,13.6,15.1,16.6,17.2,17.1,17.2,17.5,17.7,17]
    ccm_jun = pd.merge(ccm_jun, cpi, how='left', on='fyear')
    ccm_jun = ccm_jun.sort_values(['gvkey', 'datadate'])
    ccm_jun = ccm_jun.drop_duplicates(['gvkey','datadate'])

    credit_mapping = {'D': 1, 'C': 2, 'CC': 3, 'CCC-':4, 'CCC':5, 'CCC+':6, 'B-': 7, 'B': 8, 'B+':9, 'BB-':10, 'BB':11, 'BB+': 12, 'BBB-':13,
                      'BBB':14, 'BBB+': 15, 'A-':16, 'A':17, 'A+':18, 'AA-':19,'AA':20,'AA+':21,'AAA':22}
    ccm_jun['credrat'] = ccm_jun['splticrm'].map(credit_mapping)
    ccm_jun['credrat'] = ccm_jun['credrat']
    ccm_jun['credrat'] = 0
    ccm_jun.loc[ccm_jun['credrat'] < lag(ccm_jun, 'credrat'), 'credrat_dwn'] = 1
    ccm_jun.loc[ccm_jun['credrat'] >= lag(ccm_jun, 'credrat'), 'credrat_dwn'] = 0


    ccm_jun = ccm_jun.sort_values(['gvkey','datadate'])
    ccm_jun['avgat'] = (ccm_jun['at']+ccm_jun['lagat'])/2
    ccm_jun.loc[ccm_jun['count']==0, 'orgcap_1'] = (ccm_jun['xsga']/ccm_jun['cpi'])/(.1+.15)
    orgcap_1 = ccm_jun[['orgcap_1','xsga','cpi']]
    prev_row = None
    for i, row in orgcap_1.iterrows():
        if(np.isnan(row['orgcap_1'])):
            row['orgcap_1'] = prev_row['orgcap_1']*(1-0.15)+row['xsga']/row['cpi']
        prev_row = row
    ccm_jun['orgcap_1'] = orgcap_1['orgcap_1']

    ccm_jun['orgcap'] = ccm_jun['orgcap_1']/ccm_jun['avgat']
    ccm_jun.loc[ccm_jun['count'] == 0, 'orgcap'] = np.nan

    ccm_jun.loc[ccm_jun['count']==0, 'oc_1'] = ccm_jun['xsga']/(.1+.15)
    oc_1 = ccm_jun[['oc_1','xsga','cpi']]
    prev_row = None
    for i, row in oc_1.iterrows():
        if(np.isnan(row['oc_1'])):
            row['oc_1'] = (1-0.15)*prev_row['oc_1'] + row['xsga']/row['cpi']
        prev_row = row
    ccm_jun['oc_1'] = oc_1['oc_1']

    ccm_jun['oca'] = ccm_jun['oc_1']/ccm_jun['at']

    print('Check 5')
    mean_orgcap = ccm_jun.rename(columns={'orgcap':'orgcap_mean'}).groupby(['sic2','fyear'])['orgcap_mean'].mean()
    std_orgcap = ccm_jun.rename(columns={'orgcap':'orgcap_std'}).groupby(['sic2','fyear'])['orgcap_std'].std()
    ccm_jun = pd.merge(ccm_jun, mean_orgcap, on=['sic2','fyear'], how='left')
    ccm_jun = pd.merge(ccm_jun, std_orgcap, on=['sic2','fyear'], how='left')
    ccm_jun['orgcap_ia'] = (ccm_jun['orgcap']-ccm_jun['orgcap_mean'])/ccm_jun['orgcap_std']

    mean_oca = ccm_jun.rename(columns={'oca':'oca_mean'}).groupby(['sic2','fyear'])['oca_mean'].mean()
    std_oca = ccm_jun.rename(columns={'oca':'oca_std'}).groupby(['sic2','fyear'])['oca_std'].std()
    ccm_jun = pd.merge(ccm_jun, mean_oca, on=['sic2','fyear'], how='left')
    ccm_jun = pd.merge(ccm_jun, std_oca, on=['sic2','fyear'], how='left')
    ccm_jun['oca_ia'] = (ccm_jun['oca']-ccm_jun['oca_mean'])/ccm_jun['oca_std']

    ccm_jun.loc[ccm_jun['dvpsx_f']>0, 'dvpsx_1'] = 1
    ccm_jun.loc[ccm_jun['dvpsx_f']<=0, 'dvpsx_1'] = 0
    ccm_jun['ww'] = -0.091*(ccm_jun['ib']+ccm_jun['dp'])/ccm_jun['at'] - 0.062*ccm_jun['dvpsx_1'] + 0.021*ccm_jun['dltt']/ccm_jun['at'] -0.044*np.log(ccm_jun['at']) + 0.102*(ccm_jun['indsale']/lag(ccm_jun,'indsale')-1)-0.035*(ccm_jun['sale']/lag(ccm_jun,'sale')-1)

    return ccm_jun
