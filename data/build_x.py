from data.annual_x import build_comp, build_crsp_m, build_dlret, build_crsp, build_ccm_data, build_ccm_jun
from data.quarter_x import build_compq6
from data.month_x import build_temp6


class build_x:
    def __init__(self, year, quarter, tic):
        self.year = year
        self.quarter = quarter
        self.tic = tic

    def annual_x(self):
        comp = build_comp(self.tic)
        crsp_m = build_crsp_m(self.tic)
        dlret = build_dlret(self.tic)
        crsp_jun = build_crsp(crsp_m, dlret)
        ccm_data = build_ccm_data(self.tic, comp, crsp_jun)

        ccm_jun = build_ccm_jun(ccm_data)

        xa = ccm_jun.set_index(['fyear'], inplace=False)
        xa = xa.sort_index(inplace=False)
        xa_id = xa.iloc[:, :5]
        xa = xa.iloc[:, 5:]

        xa_id = xa_id.loc[[self.year-1], :]
        xa = xa.loc[[self.year-1], :]

        return xa_id, xa, ccm_jun

    def quarter_x(self, ccm_jun):
        compq6, temp2 = build_compq6(self.tic, ccm_jun)

        xq = compq6.set_index(['fyearq', 'fqtr'], inplace=False)
        xq = xq.sort_index(inplace=False)
        xq_id = xq.iloc[:, :5]
        xq = xq.iloc[:, 5:]

        if self.quarter == 1:
            xq_id = xq_id.loc[[(self.year-1, 4)], :]
            xq = xq.loc[[(self.year-1, 4)], :]

        else:
            xq_id = xq_id.loc[[(self.year, self.quarter-1)], :]
            xq = xq.loc[[(self.year, self.quarter-1)], :]

        return xq_id, xq, compq6, temp2

    def month_x(self, xq_id, compq6, temp2):
        temp6 = build_temp6(self.tic, temp2, compq6)
        month = int(xq_id['fdateq'].dt.month)

        temp6['fmon'] = temp6['date'].dt.month
        xm = temp6.set_index(['fyearq', 'fmon'], inplace=False)
        xm = xm.sort_index(inplace=False)

        if month == 1:
            xm = xm.loc[[(self.year-1, 12)], :]
        else:
            xm = xm.loc[[(self.year, month-1)], :]

        return xm


def run_build_x(year, quarter, tic):
    x_builder = build_x(year, quarter, tic)
    xa_id, xa, ccm_jun = x_builder.annual_x()
    xq_id, xq, compq6, temp2 = x_builder.quarter_x(ccm_jun)
    xm = x_builder.month_x(xq_id, compq6, temp2)

    return xa_id, xa, xq_id, xq, xm


if __name__ == '__main__':
    pass
