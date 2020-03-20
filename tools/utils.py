from global_settings import links_df


def tic_to_permon(tic):
    permno = int(links_df.loc[links_df['SYMBOL'] == tic]['PERMNO'])
    return permno
