from data.annual_y import build_compa
from data.quarter_y import build_compq


def annual_y(year, tic, current=False):
    compa = build_compa(tic)
    compa_a = compa.set_index(['fyear'], inplace=False)
    compa_a = compa_a.sort_index(inplace=False)
    compa_id_a = compa_a.iloc[:, :5]
    compa_a = compa_a.iloc[:, 5:]

    compa_y_diff = compa_a.diff() / compa_a

    if current:
        compa_id_a = compa_id_a.iloc[[-1], :]
        compa_a = compa_a.iloc[[-1], :]
        compa_aoa = compa_y_diff.iloc[[-1], :]

    else:
        compa_id_a = compa_id_a.loc[[year], :]
        compa_a = compa_a.loc[[year], :]
        compa_aoa = compa_y_diff.loc[[year], :]

    compa_aoa.columns = [col_name + '_yoy' for col_name in compa_a.columns]

    return compa_id_a, compa_a, compa_aoa


def quarter_y(year, quarter, tic, current=False):
    compq = build_compq(tic)
    compq_q = compq.set_index(['fyearq', 'fqtr'], inplace=False)
    compq_q = compq_q.sort_index(inplace=False)
    compq_id_q = compq_q.iloc[:, :5]
    compq_q = compq_q.iloc[:, 5:]

    compq_a = compq[compq['fqtr'] == quarter]
    compq_a = compq_a.set_index(['fyearq', 'fqtr'], inplace=False)
    compq_a = compq_a.sort_index(inplace=False)
    compq_a = compq_a.iloc[:, 5:]

    compq_q_diff = compq_q.diff() / compq_q
    compq_y_diff = compq_a.diff() / compq_a

    if current:
        compq_id_q = compq_id_q.iloc[[-1], :]
        compq_q = compq_q.iloc[[-1], :]
        compq_qoq = compq_q_diff.iloc[[-1], :]
        compq_aoa = compq_y_diff.iloc[[-1], :]

    else:
        compq_id_q = compq_id_q.loc[[(year, quarter)], :]
        compq_q = compq_q.loc[[(year, quarter)], :]
        compq_qoq = compq_q_diff.loc[[(year, quarter)], :]
        compq_aoa = compq_y_diff.loc[[(year, quarter)], :]

    compq_qoq.columns = [col_name + '_qoq' for col_name in compq_q.columns]
    compq_aoa.columns = [col_name + '_yoy' for col_name in compq_q.columns]

    return compq_id_q, compq_q, compq_qoq, compq_aoa
