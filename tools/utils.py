from global_settings import links_df, link_df, ccm
import pandas as pd
import os
from global_settings import TOOLS_FOLDER


# def tics_to_permnos(tics):
#     permnos = []
#     for tic in tics:
#         permno = int(links_df.loc[links_df['SYMBOL'] == tic]['PERMNO'])
#         permnos.append(permno)
#     permnos = tuple(permnos)
#     return permnos
#
#
# def tic_to_permno(tic):
#     permno = int(links_df.loc[links_df['SYMBOL'] == tic]['PERMNO'])
#     return permno


def permnos_to_gvkeys(permnos):
    gvkeys = []
    for permno in permnos:
        gvkey = list(set(ccm.loc[ccm['permno'] == permno]['gvkey']))[0]
        gvkeys.append(gvkey)
    gvkeys = tuple(gvkeys)
    return gvkeys


def permno_to_gvkey(permno):
    gvkey = list(set(ccm.loc[ccm['permno'] == permno]['gvkey']))[0]
    return gvkey


def gvkey_to_permno(gvkey):
    permno = list(set(ccm.loc[ccm['gvkey'] == gvkey]['permno']))[0]
    return permno


def permno_unique():
    for tic in list(set(link_df['SYMBOL'])):
        permno = set(link_df[link_df['SYMBOL'] == tic]['PERMNO'])
        print(tic)
        if len(permno) != 1:
            print(permno)


def tic_unique():
    for permno in list(set(link_df['PERMNO'])):
        tic = set(link_df[link_df['PERMNO'] == permno]['SYMBOL'])
        print(permno)
        if len(tic) != 1:
            print(tic)


def horizon(y_ay, y_qy, y_qq, y_my, y_mm, dy, dq):
    assert type(dy) == int and dy > 0, 'Invalid dy value'
    assert type(dq) == int and dq in [1, 2, 3, 4], 'Invalid dq value'

    x_ay = y_ay - dy
    if y_qq - dq < 0:
        x_qy = y_qy - dy - 1
        x_qq = (y_qq - dq) % 4
    elif y_qq - dq == 0:
        x_qy = y_qy - dy - 1
        x_qq = 4
    else:
        x_qy = y_qy - dy
        x_qq = y_qq - dq

    if y_mm - 3*dq < 0:
        x_my = y_my - dy - 1
        x_mm = (y_mm - 3*dq) % 12
    elif y_mm - 3*dq == 0:
        x_my = y_my - dy - 1
        x_mm = 12
    else:
        x_my = y_my - dy
        x_mm = y_mm - 3*dq

    return x_ay, x_qy, x_qq, x_my, x_mm


def x_filter(x, filter_type):
    if filter_type == 'annual':
        filter_list = ['absacc', 'acc', 'agr', 'bm_ia', 'cashdebt', 'cashpr', 'cfp', 'cfp_ia', 'chatoia',
                       'chcsho', 'chempia', 'chinv', 'chpmia', 'convind', 'currat', 'currat', 'depr', 'divi',
                       'divo', 'dy', 'egr', 'ep', 'gma', 'grcapx', 'grltnoa', 'herf', 'hire', 'invest', 'lev',
                       'lgr', 'mve_ia', 'operprof', 'orgcap', 'pchcapx_ia', 'pchcurrat', 'pchdepr',
                       'pchgm_pchsale', 'pchquick', 'pchsale_pchinvt', 'pchsale_pchrect', 'pchsale_pchxsga',
                       'pchsaleinv', 'pctacc', 'ps', 'quick', 'rd', 'rd_mve', 'rd_sale', 'realestate', 'roic',
                       'salecash', 'saleinv', 'salerec', 'secured', 'securedind', 'sgr', 'sin', 'sp', 'tang', 'tb']
    elif filter_type == 'quarter':
        filter_list = ['aeavol', 'cash', 'chtx', 'cinvest', 'ear', 'roaq', 'roavol', 'roeq', 'rsup', 'stdacc', 'stdcf']
    elif filter_type == 'month':
        filter_list = ['chmom', 'dolvol', 'mom12m', 'mom1m', 'mom36m', 'mom6m', 'mvel1', 'turn']
    else:
        raise Exception('Invalid Filter Type')
    x = x[filter_list]

    return x


def y_filter(y, filter_type):
    if filter_type == 'annual':
        filter_list = ['revt', 'ebit', 'ebitda', 're', 'epspi', 'gma', 'operprof', 'quick', 'currat',
                       'cashrrat', 'cftrr', 'dpr', 'pe', 'pb', 'roe', 'roa', 'roic', 'cod', 'capint', 'lev']
    elif filter_type == 'quarter':
        filter_list = ['revtq', 'req', 'epspiq', 'quickq', 'curratq', 'cashrratq', 'peq', 'roeq', 'roaq']
    else:
        raise Exception('Invalid Filter Type')

    y = y[filter_list]

    return y


def reduce_ccm(indexer='permno'):
    ccm_raw = pd.read_pickle(os.path.join(TOOLS_FOLDER, 'ccm_raw.pkl'))
    assert indexer in ['permno', 'gvkey'], 'Invalid Indexer Type'
    indexer_ = 'gvkey' if indexer == 'permno' else 'permno'
    indexer_filter = []

    for permno in ccm_raw[indexer]:
        if len(set(ccm_raw[ccm_raw[indexer] == permno][indexer_])) == 1:
            indexer_filter.append(permno)
    ccm = ccm_raw.loc[ccm_raw[indexer].isin(indexer_filter)]

    return ccm
