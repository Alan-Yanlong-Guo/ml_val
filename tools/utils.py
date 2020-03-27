from global_settings import links_df, link_df


def tics_to_permnos(tics):
    permnos = []
    for tic in tics:
        permno = int(links_df.loc[links_df['SYMBOL'] == tic]['PERMNO'])
        permnos.append(permno)
    permnos = tuple(permnos)
    return permnos


def tic_to_permno(tic):
    permno = int(links_df.loc[links_df['SYMBOL'] == tic]['PERMNO'])
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
    x_ay = y_ay - dy
    if y_qq - dq < 0:
        x_qy = y_qy - dy - 1
        x_qq = (y_qq - dq) % 4
    else:
        x_qy = y_qy - dy
        x_qq = y_qq - dq

    if y_mm - 3*dq < 0:
        x_my = y_my - dy - 1
        x_mm = (y_mm - 3*dq) % 12
    else:
        x_my = y_my - dy
        x_mm = y_mm - 3*dq

    return x_ay, x_qy, x_qq, x_my, x_mm


def x_filter(x_annual, x_quarter, x_month):
    x_annual = x_annual[['absacc', 'acc', 'agr', 'bm_ia', 'cashdebt', 'cashpr', 'cfp', 'cfp_ia', 'chatoia',
                         'chcsho', 'chempia', 'chinv', 'chpmia', 'convind', 'currat', 'currat', 'depr', 'divi',
                         'divo', 'dy', 'egr', 'ep', 'gma', 'grcapx', 'grltnoa', 'herf', 'hire', 'invest', 'lev',
                         'lgr', 'mve_ia', 'operprof', 'orgcap', 'pchcapx_ia', 'pchcurrat', 'pchdepr',
                         'pchgm_pchsale', 'pchquick', 'pchsale_pchinvt', 'pchsale_pchrect', 'pchsale_pchxsga',
                         'pchsaleinv', 'pctacc', 'ps', 'quick', 'rd', 'rd_mve', 'rd_sale', 'realestate', 'roic',
                         'salecash', 'saleinv', 'salerec', 'secured', 'securedind', 'sgr', 'sin', 'sp', 'tang', 'tb']]
    x_quarter = x_quarter[['aeavol', 'cash', 'chtx', 'cinvest', 'ear', 'roaq', 'roavol', 'roeq', 'rsup', 'stdacc', 'stdcf']]
    x_month = x_month[['chmom', 'dolvol', 'mom12m', 'mom1m', 'mom36m', 'mom6m', 'mvel1', 'turn']]

    return x_annual, x_quarter, x_month


def y_filter(y_annual, y_quarter):
    annual_list = ['revt', 'ebit', 'ebitda', 're', 'epspi', 'gma', 'operprof', 'quick', 'currat',
                         'cashrrat', 'cftrr', 'dpr', 'pe', 'pb', 'roe', 'roa', 'roic', 'cod', 'capint', 'lev']
    annual_list_aoa = [_ + '_aoa' for _ in annual_list]
    y_annual = y_annual[annual_list + annual_list_aoa]

    quarter_list = ['revtq', 'req', 'epspiq', 'quickq', 'curratq', 'cashrratq', 'peq', 'roeq', 'roaq']
    quarter_list_aoa = [_ + '_aoa' for _ in quarter_list]
    quarter_list_qoq = [_ + '_qoq' for _ in quarter_list]

    y_quarter = y_quarter[quarter_list + quarter_list_aoa + quarter_list_qoq]

    return y_annual, y_quarter