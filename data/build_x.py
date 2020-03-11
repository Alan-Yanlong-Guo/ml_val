from data.annual_x import build_comp, build_crsp_m, build_dlret, build_crsp, build_ccm_data, build_ccm_jun
from data.quarter_x import build_compq6


def annual_x(year, tic):
    comp = build_comp(tic)
    crsp_m = build_crsp_m(tic)
    dlret = build_dlret()
    crsp_jun = build_crsp(crsp_m, dlret)
    ccm_data = build_ccm_data(comp, crsp_jun)
    ccm_jun = build_ccm_jun(ccm_data)

    ccma = ccm_jun.set_index(['fyear'], inplace=False)
    ccma = ccma.sort_index(inplace=False)
    ccma_id = ccma.iloc[:, :5]
    ccma = ccma.iloc[:, 5:]

    ccma_id = ccma_id.loc[[year-1], :]
    ccma = ccma.loc[[year-1], :]

    return ccma_id, ccma, ccm_jun


def quarter_x(year, quarter, tic, ccm_jun):
    compq6 = build_compq6(tic, ccm_jun)
    
    compq = compq6.set_index(['fyearq', 'fqtr'], inplace=False)
    compq = compq.sort_index(inplace=False)
    compq_id = compq.iloc[:, :5]
    compq = compq.iloc[:, 5:]

    if quarter == 1:
        compq_id = compq_id.loc[[(year-1, 4)], :]
        compq = compq.loc[[(year-1, 4)], :]

    else:
        compq_id = compq_id.loc[[(year, quarter-1)], :]
        compq = compq.loc[[(year, quarter-1)], :]

    return compq_id, compq
