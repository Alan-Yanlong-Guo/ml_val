import pandas as pd
import numpy as np
from global_settings import conn
from utils.data_tools import permnos_to_gvkeys


def build_compq6(permnos, ccm_jun):
    gvkeys = permnos_to_gvkeys(permnos)

    def lag(df, col, n=1, on='gvkey'):
        return df.groupby(on)[col].shift(n)

    lnk = conn.raw_sql(f"""
                         select * from crsp.ccmxpf_linktable
                         where lpermno in {permnos}
                         """)
    lnk = lnk[lnk['linktype'].isin(['LU','LC','LD','LF','LN','LO','LS','LX'])]
    lnk = lnk[(2018 >= lnk['linkdt'].astype(str).str[0:4].astype(int)) | (lnk['linkdt'] == '.B') ]
    lnk = lnk[(lnk['linkenddt'].isna()) | ("1940" <= lnk['linkenddt'].astype(str).str[0:4])]
    lnk = lnk.sort_values(['gvkey','linkdt'])
    lnk['linkdt'] = pd.to_datetime(lnk['linkdt'])
    lnk['linkenddt'] = pd.to_datetime(lnk['linkenddt'])

    ccm_jun2 = pd.merge(lnk[['gvkey','linkdt','linkenddt','lpermno']], ccm_jun, on='gvkey', how='inner')
    ccm_jun2['datadate'] = pd.to_datetime(ccm_jun2['datadate'])
    ccm_jun2 = ccm_jun2[(ccm_jun2['linkdt'] <= ccm_jun2['datadate']) | (ccm_jun2['linkdt'] == '.B') ]
    ccm_jun2 = ccm_jun2[(ccm_jun2['datadate'] <= ccm_jun2['linkenddt']) | (ccm_jun2['linkenddt'].isna())]
    ccm_jun2 = ccm_jun2[(ccm_jun2['lpermno'] != '.') & ccm_jun2['gvkey'].notna()]

    temp = ccm_jun2[['gvkey', 'permno', 'datadate', 'fyear', 'sic2', 'cfp', 'ep', 'cashpr', 'dy',
            'lev', 'sp', 'roic', 'rd_sale', 'chadv', 'agr', 'invest', 'gma', 'chcsho', 'lgr', 'egr', 'chpm', 'chato',
            'chinv', 'hire', 'cf', 'acc', 'pctacc', 'absacc', 'spii', 'spi', 'sgr', 'pchsale_pchinvt',
            'pchsale_pchrect', 'pchgm_pchsale', 'pchsale_pchxsga', 'pchcapx', 'ps', 'divi', 'divo', 'obklg',
            'chobklg', 'securedind', 'secured', 'convind', 'conv', 'grltnoa', 'chdrc', 'rd', 'rdbias', 'chpmia',
            'chatoia', 'chempia', 'pchcapx_ia', 'tb', 'cfp_ia', 'mve_ia', 'herf', 'credrat', 'credrat_dwn',
            'orgcap', 'grcapx', 'depr', 'pchdepr', 'grGW', 'tang', 'woGW', 'sin', 'currat', 'pchcurrat', 'quick',
            'pchquick', 'orgcap_ia', 'adm', 'gad', 'rdm', 'rds', 'ol', 'ww', 'cdd', 'roavol_a', 'ala', 'alm', 'ob_a',
            'cinvest_a', 'noa', 'dnoa', 'pchcapx3', 'cdi', 'ivg', 'dcoa', 'dcol', 'dwc', 'dnca', 'dncl', 'dnco', 'dfin', 'ta',
            'dsti', 'dfnl', 'poa', 'nef', 'ndf', 'atm', 'cp', 'op', 'nop', 'ndp', 'ebp', 'rna', 'pm', 'ato', 'cto', 'gpa', 'rmw', 'ole',
            'opa', 'ola', 'cop', 'cla', 'os', 'zs', 'bi', 'oca', 'oca_ia', 'ha', 'he', 'pchsale_pchinvt_hxz',
            'pchsale_pchrect_hxz', 'pchgm_pchsale_hxz', 'pchsale_pchxsga_hxz', 'realestate_hxz',
            'secured_hxz', 'agr_hxz', 'grltnoa_hxz', 'chcsho_hxz', 'pchcapx_ia_hxz', 'acc_hxz', 'egr_hxz',
            'pctacc_hxz', 'lev_hxz', 'ep_hxz', 'cfp_hxz', 'tb_hxz', 'salecash', 'salerec', 'pchsaleinv',
            'cashdebt', 'realestate', 'roe', 'operprof', 'mve_f', 'm1','m2','m3','m4','m5','m6']]

    crsp_msf = conn.raw_sql(f"""
                          select ret, retx, prc, shrout, vol, date, permno from crsp.msf
                          where permno in {permnos}
                          """)
    crsp_msf = crsp_msf[crsp_msf['permno'].isin(temp['permno'])]
    crsp_msf['date'] = pd.to_datetime(crsp_msf['date'])
    crsp_msf = crsp_msf.sort_values('date')

    z = temp[['datadate','permno']]
    z['date_l'] = temp['datadate'] + pd.TimedeltaIndex([7]*len(z), 'M') + pd.TimedeltaIndex([-5]*len(z), 'd')
    z['date_u'] = temp['datadate'] + pd.TimedeltaIndex([20]*len(z), 'M')
    z = pd.merge(z, crsp_msf, on='permno', how='left')
    z['date'] = pd.to_datetime(z['date'])
    z = z[(z['date'] >= z['date_l']) & (z['date'] < z['date_u'])]

    temp2 = pd.merge(z, temp, on=['permno','datadate'], how='left')
    crsp_mseall = conn.raw_sql(f"""
                              select date, permno, exchcd, shrcd, siccd from crsp.mseall 
                              where permno in {permnos}
                              and exchcd in (1, 2, 3) and shrcd in (10, 11)
                              """)
    crsp_mseall = crsp_mseall.sort_values(['permno','exchcd','date'])
    mseall_min = crsp_mseall.groupby(['permno','exchcd'])['date'].min().reset_index().rename(columns={'date':'exchstdt'})
    mseall_max = crsp_mseall.groupby(['permno','exchcd'])['date'].max().reset_index().rename(columns={'date':'exchedt'})

    crsp_mseall = pd.merge(crsp_mseall, mseall_min, on=['permno','exchcd'])
    crsp_mseall = pd.merge(crsp_mseall, mseall_max, on=['permno','exchcd'])
    crsp_mseall = crsp_mseall.rename(columns={'date':'time_1'})
    crsp_mseall = crsp_mseall.sort_values(['permno','exchcd'])
    crsp_mseall = crsp_mseall.drop_duplicates(['permno','exchcd'])
    crsp_mseall['exchstdt'] = pd.to_datetime(crsp_mseall['exchstdt'])
    crsp_mseall['exchedt'] = pd.to_datetime(crsp_mseall['exchedt'])

    temp2 = pd.merge(temp2, crsp_mseall, on='permno', how='left')
    temp2 = temp2[((temp2['date']>=temp2['exchstdt']) & (temp2['date']<=temp2['exchedt']))]

    crsp_mseall_dl = conn.raw_sql(f"""
                                  select dlret, dlstcd, exchcd, date, permno from crsp.mseall
                                  where permno in {permnos}
                                  """)
    crsp_mseall_dl['date'] = pd.to_datetime(crsp_mseall_dl['date'])
    temp2 = pd.merge(temp2, crsp_mseall_dl, on=['date', 'permno'])

    temp2['exchcd'] = temp2['exchcd_x']

    temp2.loc[ (temp2['dlret'].isna()) & ((temp2['dlstcd']==500) | ((temp2['dlstcd']>=520) & (temp2['dlstcd']<=584))) & (temp2['exchcd'].isin([1,2])), 'dlret'] = -0.35
    temp2.loc[ (temp2['dlret'].isna()) & ((temp2['dlstcd']==500) | ((temp2['dlstcd']>=520) & (temp2['dlstcd']<=584))) & (temp2['exchcd'].isin([3])), 'dlret'] = -0.55
    temp2.loc[ (temp2['dlret'].notna()) & (temp2['dlret']<-1), 'dlret'] = -1
    temp2.loc[ (temp2['dlret'].isna()), 'dlret'] = 0 #TODO: wtf? this should not be 0... i think this should be not missing...
    temp2['ret'] = temp2['ret'] + temp2['dlret']

    temp2 = temp2.sort_values(['permno', 'date', 'datadate'], ascending=[True, True, False])
    temp2 = temp2.drop_duplicates(['permno', 'date'])

    temp2 = temp2.rename(columns={'datadate': 'time_2'})
    temp2['mve0'] = np.abs(temp2['prc'])*temp2['shrout']
    temp2['mvel1'] = lag(temp2, 'mve0')
    temp2['pps'] = lag(temp2, 'prc')

    comp_qtr = conn.raw_sql(f"""
                            select fyearq, fqtr, apdedateq, datadate, pdateq, fdateq, c.gvkey, f.cusip as cnum, 
                            datadate as datadate_q, rdq, sic as sic2,
                            ibq, saleq, txtq, revtq, cogsq, xsgaq,
                            atq, actq, cheq, lctq, dlcq, ppentq,
                            xrdq, rectq, invtq, ppegtq, txdbq, dlttq, dvpsxq, gdwlq, intanq, txditcq,
                            dpq, oibdpq, cshprq, ajexq, oiadpq, ivaoq, mibq, xintq, drcq, drltq, apq,
                            abs(prccq) as prccq, abs(prccq)*cshoq as mveq, ceqq, seqq, pstkq, atq, ltq, pstkrq
                            from comp.names as c, comp.fundq as f
                            where c.gvkey in {gvkeys}
                            and f.gvkey = c.gvkey
                            and f.indfmt='INDL'
                            and f.datafmt='STD'
                            and f.popsrc='D'
                            and f.consol='C'
                            """)

    comp_qtr.apdedateq = pd.to_datetime(comp_qtr.apdedateq)
    comp_qtr.datadate = pd.to_datetime(comp_qtr.datadate)
    comp_qtr.pdateq = pd.to_datetime(comp_qtr.pdateq)
    comp_qtr.fdateq = pd.to_datetime(comp_qtr.fdateq)

    comp_qtr = comp_qtr.loc[:,~comp_qtr.columns.duplicated()]
    comp_qtr['cshoq'] = comp_qtr['mveq'] / abs(comp_qtr['prccq'])
    comp_qtr = comp_qtr.sort_values(['gvkey','datadate_q'])
    comp_qtr = comp_qtr.drop_duplicates(['gvkey', 'datadate_q'])

    def lag(df, col, n=1, on='gvkey'):
        z = df.groupby(on)[col].shift(n)
        z = z.reset_index()
        z = z.sort_values('index')
        z = z.set_index('index')
        return z[col]

    compq3 = comp_qtr
    compq3.loc[compq3['pstkrq'].notna(), 'pstk'] = compq3['pstkrq']
    compq3.loc[compq3['pstkrq'].isna(), 'ptsk'] = compq3['pstkq']
    compq3['scal'] = compq3['seqq']
    compq3.loc[compq3['seqq'].isna(), 'scal'] = compq3['ceqq'] + compq3['pstk']
    compq3.loc[(compq3['seqq'].isna()) & ((compq3['ceqq'].isna()) | (compq3['pstk'].isna())),'scal'] = compq3['atq'] - compq3['ltq']

    compq3['chtx'] = (compq3['txtq']-lag(compq3, 'txtq', 4))/lag(compq3, 'atq', 4)
    compq3['roaq'] = compq3['ibq']/lag(compq3, 'atq')
    compq3['roeq'] = compq3['ibq']/lag(compq3, 'scal')
    compq3['rsup'] = (compq3['saleq']-lag(compq3, 'saleq', 4))/compq3['mveq']
    compq3['sacc'] = ( ((compq3['actq']-lag(compq3, 'actq')) - (compq3['cheq'] - lag(compq3, 'cheq')))
                      -((compq3['lctq']-lag(compq3, 'lctq')) - (compq3['dlcq'] - lag(compq3, 'dlcq')))) / compq3['saleq']
    compq3.loc[compq3['saleq'] <= 0, 'sacc'] =  ((compq3['actq']-lag(compq3, 'actq')) - (compq3['cheq'] - lag(compq3, 'cheq')))

    def trailing_std(df, col, n=15, on='gvkey'):
        z = df.groupby(on)[col].rolling(n).std()
        z = z.reset_index()
        z = z.sort_values('level_1')
        z = z.set_index('level_1')
        return z[col]
    compq3['stdacc'] = trailing_std(compq3, 'sacc', 16)
    compq3['sgrvol'] = trailing_std(compq3, 'rsup', 15)
    compq3['roavol'] = trailing_std(compq3, 'roaq', 15)
    compq3['scf'] = compq3['ibq']/compq3['saleq'] - compq3['sacc']
    compq3.loc[compq3['saleq']<=0, 'scf'] = compq3['ibq']/0.01 - compq3['sacc']
    compq3['stdcf'] = trailing_std(compq3, 'scf', 16)
    compq3['cash'] = compq3['cheq']/compq3['atq']
    compq3['cinvest'] = (compq3['ppentq'] - lag(compq3, 'ppentq'))/compq3['saleq'] - (1/3)*((lag(compq3,'ppentq') - lag(compq3, 'ppentq',2))/lag(compq3, 'saleq')) - (1/3)*((lag(compq3, 'ppentq',2) - lag(compq3, 'ppentq',3))/lag(compq3, 'saleq', 2)) - (1/3)*((lag(compq3,'ppentq',3) - lag(compq3, 'ppentq',4))/lag(compq3, 'saleq',3))
    compq3.loc[compq3['saleq'] <= 0, 'cinvest'] = (compq3['ppentq'] - lag(compq3, 'ppentq'))/0.01 - (1/3)*((lag(compq3,'ppentq') - lag(compq3, 'ppentq',2))/0.01) - (1/3)*((lag(compq3, 'ppentq',2) - lag(compq3, 'ppentq',3))/0.01) - (1/3)*((lag(compq3,'ppentq',3) - lag(compq3, 'ppentq',4))/0.01)
    compq3['che'] = compq3['ibq'] = lag(compq3, 'ibq',4)
    #compq3['nincr']
    #TODO: nincr

    compq3['rdmq'] = compq3['xrdq']/compq3['mveq']
    compq3['rdsq'] = compq3['xrdq']/compq3['saleq']
    compq3['olq'] = (compq3['cogsq'] + compq3['xsgaq'])/compq3['atq']
    compq3['tanq'] = (compq3['cheq'] + 0.715*compq3['rectq'] + 0.54*compq3['invtq'] + 0.535*compq3['ppegtq'])/compq3['atq']
    compq3['kzq'] = -1.002*((compq3['ibq'] + lag(compq3, 'ibq', 1) + lag(compq3, 'ibq', 2) + lag(compq3, 'ibq', 3) + compq3['dpq']) / lag(compq3,'ppentq')) \
        + 0.283*(compq3['atq'] + compq3['mveq'] - compq3['ceqq'] - compq3['txdbq'])/compq3['atq'] \
        - 3.139 * (compq3['dlcq'] + compq3['dlttq'])/(compq3['dlcq'] + compq3['dlttq'] + compq3['seqq']) \
        + 39.368*(compq3['dvpsxq'] * compq3['cshoq'] + lag(compq3, 'dvpsxq')*lag(compq3, 'cshoq') + lag(compq3, 'dvpsxq',2)*lag(compq3, 'cshoq',2) + lag(compq3, 'dvpsxq',3)*lag(compq3, 'cshoq', 3))/lag(compq3, 'ppentq')\
        - 1.315*compq3['cheq']/lag(compq3, 'ppentq')
    compq3.loc[compq3['gdwlq'].isna(), 'gdwlq'] = 0
    compq3.loc[compq3['intanq'].isna(), 'intanq'] = 0
    compq3['alaq'] = compq3['cheq'] + 0.75*(compq3['actq'] - compq3['cheq']) + 0.5*(compq3['atq'] - compq3['actq'] - compq3['gdwlq'] - compq3['intanq'])
    compq3['almq'] = compq3['alaq']/(compq3['atq']+compq3['mveq'] - compq3['ceqq'])
    compq3['laq'] = compq3['atq']/lag(compq3, 'atq') - 1

    compq3.loc[compq3['seqq'].isna(), 'seqq'] = compq3['ceqq'] + compq3['pstkq'] - compq3['ltq']
    compq3.loc[compq3['txditcq'].notna(), 'bmq'] = (compq3['seqq'] + compq3['txditcq'] - compq3['pstkq'])/compq3['mveq']
    compq3['dmq'] = (compq3['dlcq'] + compq3['dlttq'])/compq3['mveq']
    compq3['amq'] = compq3['atq']/compq3['mveq']
    compq3['epq'] = compq3['ibq']/compq3['mveq']
    compq3['cpq'] = (compq3['ibq'] + compq3['dpq'])/compq3['mveq']
    compq3['emq'] = (compq3['mveq'] + compq3['dlcq'] + compq3['dlttq'] + compq3['pstkq'] - compq3['cheq'])/compq3['oibdpq']
    compq3['spq'] = compq3['saleq']/compq3['mveq']
    compq3['ndpq'] = compq3['dlttq'] + compq3['dlcq'] + compq3['pstkq'] - compq3['cheq']
    compq3['ebpq'] = (compq3['ndpq'] + compq3['ceqq']) / (compq3['ndpq'] + compq3['mveq'])
    compq3['x_1'] = compq3['saleq']/(compq3['cshprq']*compq3['ajexq'])
    compq3['rs'] = (compq3['x_1'] - lag(compq3, 'x_1', 4)) / trailing_std(compq3, 'x_1', 6)
    compq3['droeq'] = compq3['roeq'] - lag(compq3, 'roeq', 4)
    compq3['droaq'] = compq3['roaq'] - lag(compq3, 'roaq', 4)

    compq3.loc[compq3['dlcq'].isna(), 'dlcq'] = 0
    compq3.loc[compq3['ivaoq'].isna(), 'ivaoq'] = 0
    compq3.loc[compq3['mibq'].isna(), 'mibq'] = 0
    compq3.loc[compq3['pstkq'].isna(), 'pstkq'] = 0
    compq3['noaq'] = (compq3['atq']-compq3['cheq']-compq3['ivaoq']) - (compq3['dlcq']-compq3['dlttq']-compq3['mibq']-compq3['pstkq']-compq3['ceqq'])/lag(compq3, 'atq')
    compq3['rnaq'] = compq3['oiadpq']/lag(compq3, 'noaq')
    compq3['pmq'] = compq3['oiadpq']/compq3['saleq']
    compq3['atoq'] = compq3['saleq']/lag(compq3,'noaq')
    compq3['ctoq'] = compq3['saleq']/lag(compq3,'atq')
    compq3['glaq'] = (compq3['revtq'] - compq3['cogsq'])/lag(compq3, 'atq')
    compq3['oleq'] = (compq3['revtq'] - compq3['cogsq'] - compq3['xsgaq'] - compq3['xintq'])/lag(compq3, 'bmq')
    compq3['olaq'] = (compq3['revtq'] - compq3['cogsq'] - compq3['xsgaq'] + compq3['xrdq'])/lag(compq3, 'atq')
    compq3['claq'] = ((compq3['revtq'] - compq3['cogsq'] - compq3['xsgaq'] + compq3['xrdq'] - (compq3['rectq']-lag(compq3, 'rectq')) - (compq3['invtq']-lag(compq3, 'invtq')) \
          + compq3['drcq'] - lag(compq3, 'drcq') + compq3['drltq'] - lag(compq3, 'drltq') + compq3['apq'] - lag(compq3, 'apq'))) / lag(compq3, 'atq')
    compq3['blq'] = compq3['atq']/compq3['bmq']
    compq3['sgq'] = compq3['saleq']/lag(compq3, 'saleq', 4)

    compq3.loc[compq3['dvpsxq'] > 0, 'dvpsxq_1'] = 1
    compq3.loc[compq3['dvpsxq'] <= 0, 'dvpsxq_1'] = 0
    temp_indsaleq = compq3.groupby(['sic2','fyearq'])['saleq'].sum().reset_index()
    temp_indsaleq = temp_indsaleq.rename(columns={'saleq':'indsaleq'})
    compq3 = pd.merge(compq3, temp_indsaleq, on=['fyearq','sic2'])
    compq3['wwq'] = -0.091*(compq3['ibq'] + compq3['dpq'])/compq3['atq'] - 0.062*compq3['dvpsxq_1'] + 0.021*compq3['dlttq']/compq3['atq'] \
        - 0.044 * np.log(compq3['atq']) + 0.102*(compq3['indsaleq']/lag(compq3, 'indsaleq') - 1) - 0.035*(compq3['saleq']/lag(compq3, 'saleq') - 1)

    temp_md_roavol = compq3.groupby(['fyearq','fqtr','sic2'])['roavol'].median().reset_index()
    temp_md_roavol = temp_md_roavol.rename(columns={'roavol' : 'md_roavol'})
    compq3 = pd.merge(compq3, temp_md_roavol, on=['fyearq','fqtr','sic2'])
    temp_md_sgrvol = compq3.groupby(['fyearq','fqtr','sic2'])['sgrvol'].median().reset_index()
    temp_md_sgrvol = temp_md_sgrvol.rename(columns={'sgrvol' : 'md_sgrvol'})
    compq3 = pd.merge(compq3, temp_md_sgrvol, on=['fyearq','fqtr','sic2'])
    compq3.loc[compq3['roavol'] < compq3['md_roavol'], 'm7'] = 1
    compq3.loc[compq3['roavol'] >= compq3['md_roavol'], 'm7'] = 0
    compq3.loc[compq3['sgrvol'] < compq3['md_sgrvol'], 'm8'] = 1
    compq3.loc[compq3['sgrvol'] >= compq3['md_sgrvol'], 'm8'] = 0

    # ibessum = conn.raw_sql(f"""
    #                         select ticker, cusip, fpedats, statpers, ANNDATS_ACT,
    #                         numest, ANNTIMS_ACT, medest, actual, stdev
    #                         from ibes.statsum_epsus
    #                         where ticker in {tics}
    #                         and fpi='6'
    #                         and statpers<ANNDATS_ACT
    #                         and measure='EPS'
    #                         and (fpedats-statpers)>=0
    #                         """)
    # ibessum = ibessum[(ibessum['medest'].notna()) & (ibessum['fpedats'].notna())]
    # ibessum = ibessum.sort_values(by=['cusip','fpedats','statpers'], ascending=[True,True,False])
    # ibessum = ibessum.drop_duplicates(['cusip', 'fpedats'])
    #
    # crsp_msenames = conn.raw_sql("""select * from crsp.msenames""")
    # crsp_msenames = crsp_msenames[crsp_msenames['ncusip'].notna()]
    # crsp_msenames = crsp_msenames.sort_values(['permno','ncusip'])
    # crsp_msenames = crsp_msenames.drop_duplicates(['permno','ncusip'])
    # names = crsp_msenames.rename(columns={'cusip':'cusip6'})
    #
    # ibessum2 = pd.merge(ibessum, names[['ncusip','cusip6']], left_on='cusip', right_on=['ncusip'], how='left')
    # ibessum2['cusip6'] = ibessum2['cusip6'].astype(str).str[0:6]

    compq3['cnum'] = compq3['cnum'].astype(str).str[0:6]
    # compq4 = pd.merge(compq3, ibessum2[['medest','actual','cusip6','fpedats']], left_on=['cnum','datadate_q'], right_on=['cusip6','fpedats'], how='left')
    compq4 = compq3
    compq4 = compq4.sort_values(['gvkey','datadate_q'])
    compq4 = compq4.drop_duplicates(['gvkey','datadate_q'])

    # compq4.loc[(compq4['medest'].isna()) | (compq4['actual']).isna(), 'sue'] = compq4['che']/compq4['mveq']
    # compq4.loc[(compq4['medest'].notna()) & (compq4['actual']).notna(), 'sue'] = (compq4['actual'] - compq4['medest'])/abs(compq4['prccq'])

    lnk = conn.raw_sql(f"""
                        select * from crsp.ccmxpf_linktable
                        where lpermno in {permnos}
                        """)
    lnk = lnk[lnk['linktype'].isin(['LU','LC','LD','LF','LN','LO','LS','LX'])]
    lnk = lnk[(2018 >= lnk['linkdt'].astype(str).str[0:4].astype(int)) | (lnk['linkdt'] == '.B') ]
    lnk = lnk[(lnk['linkenddt'].isna()) | ("1940" <= lnk['linkenddt'].astype(str).str[0:4])]
    lnk = lnk.sort_values(['gvkey','linkdt'])

    compq5 = pd.merge(compq4, lnk[['gvkey','linkdt','linkenddt','lpermno']], on='gvkey', how='inner')
    compq5 = compq5[(compq5['linkdt'] <= compq5['datadate_q']) | (compq5['linkdt'] == '.B') ]
    compq5 = compq5[(compq5['datadate_q'] <= compq5['linkenddt']) | (compq5['linkenddt'].isna())]
    compq5 = compq5[(compq5['lpermno'] != '.') & compq5['gvkey'].notna()]
    compq5 = compq5[(compq5['lpermno'].notna()) & (compq5['rdq'].notna())]

    crsp_dsf = conn.raw_sql(f"""
                          select vol, ret, permno, date from crsp.dsf as d
                          where d.permno in {permnos}
                          """)
    crsp_dsf = crsp_dsf[crsp_dsf['permno'].isin(compq5['lpermno'])]
    crsp_dsf['date'] = pd.to_datetime(crsp_dsf['date'])
    crsp_dsf = crsp_dsf.sort_values('date')

    compq5['temp_rdq'] = np.busday_offset(compq5['rdq'].values.astype('datetime64[D]'), -10, roll='forward')
    crsp_dsf['avgvol'] = trailing_std(crsp_dsf, 'vol', n=21, on='permno')
    compq5 = pd.merge(compq5, crsp_dsf[['date', 'permno','avgvol']], how='left', left_on=['temp_rdq','lpermno'], right_on=['date', 'permno'])

    compq5['temp_rdq'] = np.busday_offset(compq5['rdq'].values.astype('datetime64[D]'), 1, roll='forward')
    crsp_dsf['aeavol'] = trailing_std(crsp_dsf, 'vol', n=3, on='permno')
    crsp_dsf['ear'] = lag(crsp_dsf, 'ret', 0, on='permno') + lag(crsp_dsf, 'ret', 1, on='permno') + lag(crsp_dsf, 'ret', 2, on='permno')
    compq6 = pd.merge(compq5, crsp_dsf[['date', 'permno', 'aeavol', 'avgvol', 'ear']], how='left', left_on=['temp_rdq','lpermno'], right_on=['date', 'permno'])
    compq6['avgvol'] = compq6['avgvol_x']
    compq6['aeavol'] = (compq6['aeavol'] - compq6['avgvol'])/compq6['avgvol']

    compq6 = compq6[['fyearq', 'fqtr', 'apdedateq', 'datadate', 'pdateq', 'fdateq', 'gvkey', 'lpermno', 'datadate_q',
                     'rdq', 'chtx', 'roaq', 'rsup', 'stdacc', 'stdcf', 'sgrvol', 'rdmq', 'rdsq', 'olq', 'tanq', 'kzq',
                     'alaq', 'almq', 'laq', 'bmq', 'dmq', 'amq', 'epq', 'cpq', 'emq', 'spq', 'ndpq', 'ebpq', 'wwq',
                     'rs', 'droeq', 'droaq', 'noaq', 'rnaq', 'pmq', 'atoq', 'ctoq', 'glaq', 'oleq', 'olaq', 'claq',
                     'blq', 'sgq', 'roavol', 'cash', 'cinvest', 'm7', 'm8', 'prccq', 'roeq', 'aeavol', 'ear']]

    compq6 = compq6.drop_duplicates()

    return compq6, temp2
