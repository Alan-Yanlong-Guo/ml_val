from global_settings import conn


def construct_daily(business_day, permno):
    assert isinstance(permno, (list, str)), 'invalid permno data type'
    if isinstance(permno, list):
        assert len(permno) != 0, 'zero permno list length'
    if isinstance(permno, str):
        permno = list([permno])
    permno = tuple(permno)

    daily_df = conn.raw_sql(f"""
                            select a.date, a.permno, b.ticker, b.shrcd, b.siccd, a.ret, 
                            abs(a.prc) as prc, a.shrout, a.cfacpr, a.cfacshr
                            from crsp.dsf as a
                            left join crsp.msenames as b
                            on a.permno = b.permno
                            and b.namedt <= a.date
                            and a.date <= b.nameendt
                            and a.date = '{business_day}'
                            where b.permno in {permno if len(permno) > 1 else '(' + permno[0] + ')'}
                            """)
    return daily_df
